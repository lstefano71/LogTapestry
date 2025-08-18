// LogTapestry.Ingester/TailingManager.cs
using LogTapestry.Core;

using System.Collections.Concurrent;
using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  public class TailingManager
  {
    private readonly IStateProvider _stateProvider;
    private readonly ConcurrentDictionary<ulong, CancellationTokenSource> _activeTailers = new();

    public TailingManager(IStateProvider stateProvider)
    {
      _stateProvider = stateProvider;
    }

    public async Task RunAsync(ChannelReader<FileWorkItem> workChannel, ChannelWriter<ParsingResult> outputChannel, CancellationToken token)
    {
      await foreach (var workItem in workChannel.ReadAllAsync(token)) {
        switch (workItem.Type) {
          case FileWorkType.FileAdded:
          case FileWorkType.FileChanged:
            if (!_activeTailers.ContainsKey(workItem.FileId)) {
              var tailerCts = CancellationTokenSource.CreateLinkedTokenSource(token);
              _activeTailers[workItem.FileId] = tailerCts;
              Console.WriteLine($"[TailingManager] Starting tailer for: {workItem.FilePath} (ID: {workItem.FileId})");
              _ = Task.Run(() => TailingTask(workItem, outputChannel, tailerCts.Token));
            }
            break;
          case FileWorkType.FileRemovedOrRotated:
            if (_activeTailers.TryRemove(workItem.FileId, out var cts)) {
              Console.WriteLine($"[TailingManager] Stopping tailer for: {workItem.FilePath} (ID: {workItem.FileId})");
              cts.Cancel();
            }
            break;
        }
      }
    }

    private async Task TailingTask(FileWorkItem workItem, ChannelWriter<ParsingResult> outputChannel, CancellationToken token)
    {
      var tracked = await _stateProvider.GetTrackedFileAsync(workItem.FileId, workItem.VolumeSerial);
      long position = tracked?.Position ?? 0;

      using var fs = new FileStream(workItem.FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
      fs.Seek(position, SeekOrigin.Begin);

      // TODO: Pass correct PluginSettings from configuration
      var pluginSettings = new PluginSettings {
        Type = "regex",
        Name = "default",
        IncludePatterns = new List<string> { "*.log" },
        Config = new RegexPluginConfig {
          StartOfEntryRegex = @"^\d{4}-\d{2}-\d{2}",
          TimestampRegex = @"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)",
          TimestampIsUtc = true,
          LevelRegex = @"\b(INFO|WARN|ERROR)\b",
          FieldsRegexes = new List<FieldRegex>
              {
            new FieldRegex { Regex = @"user_id=(\d+)", FieldName = "user_id", Type = "long" },
            new FieldRegex { Regex = @"order_id=(\d+)", FieldName = "order_id", Type = "long" },
            new FieldRegex { Regex = @"free_space=(\d+)", FieldName = "free_space", Type = "long" },
            new FieldRegex { Regex = @"username=""([^""]+)""", FieldName = "username" },
            new FieldRegex { Regex = @"reason=""([^""]+)""", FieldName = "reason" },
            new FieldRegex { Regex = @"details=""([^""]+)""", FieldName = "details" }
        }
        }
      };

      var parser = new RegexLogParser(pluginSettings, workItem.FilePath);

      while (!token.IsCancellationRequested) {
        var buffer = new List<string>();
        using (var sr = new StreamReader(fs, leaveOpen: true)) {
          string? line;
          while ((line = await sr.ReadLineAsync()) != null) {
            buffer.Add(line);
          }
        }

        foreach (var result in parser.Parse(buffer.ToArray())) {
          await outputChannel.WriteAsync(result, token);
        }

        var newPosition = fs.Position;
        await _stateProvider.UpdateTrackedFileAsync(new TrackedFileInfo {
          VolumeSerial = workItem.VolumeSerial,
          FileId = workItem.FileId,
          FilePath = workItem.FilePath,
          Position = newPosition,
          LastWriteTimeUtc = new DateTime(workItem.LastWriteTimeUtc)
        });

        await Task.Delay(1000, token); // Polling interval

        // Rotation check
        if (fs.Length < newPosition) {
          var newId = NtfsUtils.GetFileIdentifier(workItem.FilePath);
          if (newId == null || newId.FileId != workItem.FileId) {
            break; // File rotated
          }
        }
      }
    }
  }
}
