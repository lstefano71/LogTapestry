using LogTapestry.Core;

using Microsoft.Extensions.FileSystemGlobbing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using System.Collections.Concurrent;
using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// File system watcher producer that generates real-time file events.
  /// </summary>
  public class WatcherProducer
  {
    private readonly ILogger<WatcherProducer> _logger;
    private readonly IngesterSettings _settings;
    private readonly Channel<FileEvent> _outputChannel;
    private readonly Matcher _matcher;
    private readonly ConcurrentQueue<string> _retryQueue = new();

    public WatcherProducer(ILogger<WatcherProducer> logger, IOptions<IngesterSettings> options)
    {
      _logger = logger;
      _settings = options.Value;
      _outputChannel = Channel.CreateUnbounded<FileEvent>();

      // Create matcher for include/exclude patterns
      _matcher = new Matcher();
      _matcher.AddIncludePatterns(_settings.IncludePatterns);
      _matcher.AddExcludePatterns(_settings.ExcludePatterns);
    }

    public ChannelReader<FileEvent> Reader => _outputChannel.Reader;

    /// <summary>
    /// Starts the file system watcher and begins producing events.
    /// </summary>
    public async Task RunAsync(CancellationToken token)
    {
      try {
        _logger.LogInformation("Starting file system watcher for directory: {Directory}", _settings.Directory);

        var watcher = new FileSystemWatcher(_settings.Directory, "*") {
          IncludeSubdirectories = true,
          EnableRaisingEvents = true,
          InternalBufferSize = _settings.FileSystemWatcherBufferSize
        };

        bool IsMatch(string path)
        {
          var relPath = Path.GetRelativePath(_settings.Directory, path);
          var result = _matcher.Match(relPath);
          return result.HasMatches;
        }

        void OnChanged(object sender, FileSystemEventArgs e)
        {
          if (!IsMatch(e.FullPath)) return;
          ProcessFileEvent(e.FullPath, FileWorkType.FileChanged);
        }

        void OnCreated(object sender, FileSystemEventArgs e)
        {
          if (!IsMatch(e.FullPath)) return;
          ProcessFileEvent(e.FullPath, FileWorkType.FileAdded);
        }

        void OnDeleted(object sender, FileSystemEventArgs e)
        {
          // For deletions, we can't get file ID, so we trigger a full scan
          _logger.LogWarning("File deleted: {FilePath}. Triggering full scan.", e.FullPath);
          // Note: In the new architecture, this would trigger a rescan of InitialScanProducer
        }

        void OnRenamed(object sender, RenamedEventArgs e)
        {
          // Treat rename as delete + create
          if (IsMatch(e.OldFullPath) || IsMatch(e.FullPath)) {
            _logger.LogWarning("File renamed from {OldPath} to {NewPath}. Triggering full scan.", e.OldFullPath, e.FullPath);
            // Note: In the new architecture, this would trigger a rescan
          }
        }

        void OnError(object sender, ErrorEventArgs e)
        {
          _logger.LogError(e.GetException(), "File system watcher error");
          // Note: In the new architecture, this would trigger a rescan
        }

        watcher.Changed += OnChanged;
        watcher.Created += OnCreated;
        watcher.Deleted += OnDeleted;
        watcher.Renamed += OnRenamed;
        watcher.Error += OnError;

        // Keep alive until cancellation
        while (!token.IsCancellationRequested) {
          // Process retry queue
          while (_retryQueue.TryDequeue(out var retryPath)) {
            if (!File.Exists(retryPath)) {
              continue; // File was deleted
            }

            if (!IsMatch(retryPath)) {
              continue;
            }

            ProcessFileEvent(retryPath, FileWorkType.FileAdded);
          }

          await Task.Delay(500, token);
        }

        watcher.EnableRaisingEvents = false;
        watcher.Dispose();
        _outputChannel.Writer.Complete();
      } catch (OperationCanceledException) {
        _logger.LogInformation("Watcher producer cancelled");
      } catch (Exception ex) {
        _logger.LogError(ex, "Error in watcher producer");
      }
    }

    private void ProcessFileEvent(string filePath, FileWorkType type)
    {
      try {
        var fileIdObj = NtfsUtils.GetFileIdentifier(filePath);
        if (fileIdObj == null) {
          _retryQueue.Enqueue(filePath);
          return;
        }

        var fileEvent = new FileEvent {
          FileId = fileIdObj.FileId,
          VolumeSerial = fileIdObj.VolumeSerial,
          FilePath = filePath,
          LastWriteTimeUtc = File.GetLastWriteTimeUtc(filePath).Ticks,
          Type = type
        };

        if (!_outputChannel.Writer.TryWrite(fileEvent)) {
          _logger.LogWarning("Failed to write file event to channel: {FilePath}", filePath);
        } else {
          _logger.LogDebug("Produced {Type} event for file: {FilePath}", type, filePath);
        }
      } catch (Exception ex) {
        _logger.LogError(ex, "Error processing file event for {FilePath}", filePath);
      }
    }
  }
}
