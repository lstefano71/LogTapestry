// LogTapestry.Ingester/DirectoryMonitor.cs
using LogTapestry.Core;

using Microsoft.Extensions.FileSystemGlobbing;
using Microsoft.Extensions.FileSystemGlobbing.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  public enum FileWorkType
  {
    FileAdded,
    FileChanged,
    FileRemovedOrRotated
  }

  public class FileWorkItem
  {
    public FileWorkType Type { get; set; }
    public long VolumeSerial { get; set; }
    public ulong FileId { get; set; }
    public string FilePath { get; set; }
    public long LastWriteTimeUtc { get; set; }
  }

  public class DirectoryMonitor
  {
    private readonly ILogger _logger;
    private readonly IngesterSettings _settings;
    private readonly IStateProvider _stateProvider;
    private readonly Channel<FileWorkItem> _channel;

    public DirectoryMonitor(IOptions<IngesterSettings> options,
      IStateProvider stateProvider,
      ILoggerFactory loggerFactory)
    {
      _settings = options.Value;
      _stateProvider = stateProvider;
      _logger = loggerFactory.CreateLogger("DirectoryMonitor");
      _channel = Channel.CreateUnbounded<FileWorkItem>();
    }

    public ChannelReader<FileWorkItem> WorkItems => _channel.Reader;

    public async Task RunAsync(ChannelWriter<FileWorkItem> writer, System.Threading.CancellationToken token)
    {
      await InitialScanAsync(writer);

      // Create matcher for include/exclude patterns
      var matcher = new Matcher();
      matcher.AddIncludePatterns(_settings.IncludePatterns);
      matcher.AddExcludePatterns(_settings.ExcludePatterns);

      var watcher = new FileSystemWatcher(_settings.Directory, "*.*") {
        IncludeSubdirectories = true,
        EnableRaisingEvents = true,
        InternalBufferSize = 64 * 1024
      };

      bool IsMatch(string path)
      {
        var dirRoot = new DirectoryInfo(_settings.Directory);
        var relPath = Path.GetRelativePath(_settings.Directory, path);
        var result = matcher.Match(relPath);
        return result.HasMatches;
      }

      void OnChanged(object sender, FileSystemEventArgs e)
      {
        if (!IsMatch(e.FullPath)) return;
        var fileIdObj = NtfsUtils.GetFileIdentifier(e.FullPath);
        if (fileIdObj == null) return;
        var id = fileIdObj.FileId;
        var diskWriteTime = File.GetLastWriteTimeUtc(e.FullPath).Ticks;
        writer.TryWrite(new FileWorkItem {
          Type = FileWorkType.FileChanged,
          VolumeSerial = fileIdObj.VolumeSerial,
          FileId = id,
          FilePath = e.FullPath,
          LastWriteTimeUtc = diskWriteTime
        });
      }

      void OnCreated(object sender, FileSystemEventArgs e)
      {
        if (!IsMatch(e.FullPath)) return;
        var fileIdObj = NtfsUtils.GetFileIdentifier(e.FullPath);
        if (fileIdObj == null) return;
        var id = fileIdObj.FileId;
        writer.TryWrite(new FileWorkItem {
          Type = FileWorkType.FileAdded,
          VolumeSerial = fileIdObj.VolumeSerial,
          FileId = id,
          FilePath = e.FullPath,
          LastWriteTimeUtc = File.GetLastWriteTimeUtc(e.FullPath).Ticks
        });
      }

      void OnDeleted(object sender, FileSystemEventArgs e)
      {
        // On deletion, we can't get file ID, so we rely on state reconciliation
        // Trigger a full scan to resync
        _ = InitialScanAsync(writer);
      }

      void OnRenamed(object sender, RenamedEventArgs e)
      {
        // Treat as deletion + creation
        _ = InitialScanAsync(writer);
      }

      void OnError(object sender, ErrorEventArgs e)
      {
        // Resync on error
        _ = InitialScanAsync(writer);
      }

      watcher.Changed += OnChanged;
      watcher.Created += OnCreated;
      watcher.Deleted += OnDeleted;
      watcher.Renamed += OnRenamed;
      watcher.Error += OnError;

      // Keep alive until cancellation requested
      while (!token.IsCancellationRequested) {
        await Task.Delay(500, token);
      }

      watcher.EnableRaisingEvents = false;
      watcher.Dispose();
    }

    private async Task InitialScanAsync(ChannelWriter<FileWorkItem> writer)
    {
      var trackedFiles = await _stateProvider.GetAllTrackedFilesAsync();
      var seen = new HashSet<ulong>();

      // Create matcher for include/exclude patterns
      var matcher = new Matcher();
      matcher.AddIncludePatterns(_settings.IncludePatterns);
      matcher.AddExcludePatterns(_settings.ExcludePatterns);
      var dirRoot = new DirectoryInfo(_settings.Directory);
      var dirWrapper = new DirectoryInfoWrapper(dirRoot);
      var matchResult = matcher.Execute(dirWrapper);
      foreach (var file in matchResult.Files) {
        var filePath = Path.Combine(_settings.Directory, file.Path);
        var fileIdObj = NtfsUtils.GetFileIdentifier(filePath);
        if (fileIdObj == null) continue;
        var id = fileIdObj.FileId;
        seen.Add(id);

        if (!trackedFiles.TryGetValue(id, out TrackedFileInfo? tracked)) {
          _logger.LogDebug("File added: {FilePath} (ID: {FileId})", filePath, id);
          await writer.WriteAsync(new FileWorkItem {
            Type = FileWorkType.FileAdded,
            VolumeSerial = fileIdObj.VolumeSerial,
            FileId = id,
            FilePath = filePath,
            LastWriteTimeUtc = File.GetLastWriteTimeUtc(filePath).Ticks
          });
        } else {
          var diskWriteTime = File.GetLastWriteTimeUtc(filePath).Ticks;
          if (diskWriteTime > tracked.LastWriteTimeUtc.Ticks) {
            _logger.LogDebug("File changed: {FilePath} (ID: {FileId})", filePath, id);
            await writer.WriteAsync(new FileWorkItem {
              Type = FileWorkType.FileChanged,
              VolumeSerial = fileIdObj.VolumeSerial,
              FileId = id,
              FilePath = filePath,
              LastWriteTimeUtc = diskWriteTime
            });
          }
        }
      }

      foreach (var kvp in trackedFiles) {
        if (!seen.Contains(kvp.Key)) {
          _logger.LogDebug("File removed or rotated: {FilePath} (ID: {FileId})", kvp.Value.FilePath, kvp.Value.FileId);
          await writer.WriteAsync(new FileWorkItem {
            Type = FileWorkType.FileRemovedOrRotated,
            VolumeSerial = kvp.Value.VolumeSerial,
            FileId = kvp.Value.FileId,
            FilePath = kvp.Value.FilePath,
            LastWriteTimeUtc = kvp.Value.LastWriteTimeUtc.Ticks
          });
        }
      }
    }
  }
}
