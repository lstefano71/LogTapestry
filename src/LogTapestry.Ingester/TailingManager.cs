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
    private readonly PluginSettings _pluginSettings;

    public TailingManager(IStateProvider stateProvider, PluginSettings pluginSettings)
    {
      _stateProvider = stateProvider;
      _pluginSettings = pluginSettings;
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
              _ = Task.Run(() => TailingTask(workItem, outputChannel, tailerCts.Token), token);
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

      var parser = new RegexLogParser(_pluginSettings, workItem.FilePath);

      while (!token.IsCancellationRequested) {
        var buffer = new List<string>();
        using (var sr = new StreamReader(fs, leaveOpen: true)) {
          string? line;
          while ((line = await sr.ReadLineAsync(token)) != null) {
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
