// LogTapestry.Ingester/DirectoryMonitor.cs
using LogTapestry.Core;

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
    private readonly IngesterSettings _settings;
    private readonly IStateProvider _stateProvider;
    private readonly Channel<FileWorkItem> _channel;

    public DirectoryMonitor(IngesterSettings settings, IStateProvider stateProvider)
    {
      _settings = settings;
      _stateProvider = stateProvider;
      _channel = Channel.CreateUnbounded<FileWorkItem>();
    }

    public ChannelReader<FileWorkItem> WorkItems => _channel.Reader;

    public async Task RunAsync(ChannelWriter<FileWorkItem> writer, System.Threading.CancellationToken token)
    {
      await InitialScanAsync(writer);

      var watcher = new FileSystemWatcher(_settings.Directory, "*.log") {
        IncludeSubdirectories = true,
        EnableRaisingEvents = true,
        InternalBufferSize = 64 * 1024
      };

      void OnChanged(object sender, FileSystemEventArgs e)
      {
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

      foreach (var filePath in Directory.EnumerateFiles(_settings.Directory, "*.log", SearchOption.AllDirectories)) {
        var fileIdObj = NtfsUtils.GetFileIdentifier(filePath);
        if (fileIdObj == null) continue;
        var id = fileIdObj.FileId;
        seen.Add(id);

        if (!trackedFiles.ContainsKey(id)) {
          Console.WriteLine($"[DirectoryMonitor] File added: {filePath} (ID: {id})");
          await writer.WriteAsync(new FileWorkItem {
            Type = FileWorkType.FileAdded,
            VolumeSerial = fileIdObj.VolumeSerial,
            FileId = id,
            FilePath = filePath,
            LastWriteTimeUtc = File.GetLastWriteTimeUtc(filePath).Ticks
          });
        } else {
          var tracked = trackedFiles[id];
          var diskWriteTime = File.GetLastWriteTimeUtc(filePath).Ticks;
          if (diskWriteTime > tracked.LastWriteTimeUtc.Ticks) {
            Console.WriteLine($"[DirectoryMonitor] File changed: {filePath} (ID: {id})");
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
          Console.WriteLine($"[DirectoryMonitor] File removed or rotated: {kvp.Value.FilePath} (ID: {kvp.Value.FileId})");
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
