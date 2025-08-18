// LogTapestry.Ingester/TailingManager.cs
using LogTapestry.Core;

using Microsoft.Extensions.FileSystemGlobbing;
using Microsoft.Extensions.Logging;

using System.Collections.Concurrent;
using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  public class TailingManager
  {
    private readonly ILogger _logger;
    private readonly IStateProvider _stateProvider;
    private readonly ConcurrentDictionary<ulong, CancellationTokenSource> _activeTailers = new();
    private readonly List<PluginSettings> _pluginSettingsList;
    private readonly Dictionary<PluginSettings, Matcher> _pluginMatchers;
    private readonly IngesterSettings _settings;

    public TailingManager(IStateProvider stateProvider, List<PluginSettings> pluginSettingsList, ILoggerFactory loggerFactory, IngesterSettings settings)
    {
      _stateProvider = stateProvider;
      _pluginSettingsList = pluginSettingsList;
      _logger = loggerFactory.CreateLogger("TailingManager");
      _settings = settings;
      _pluginMatchers = new Dictionary<PluginSettings, Matcher>();
      foreach (var plugin in _pluginSettingsList) {
        var matcher = new Matcher();
        matcher.AddIncludePatterns(plugin.IncludePatterns);
        matcher.AddExcludePatterns(plugin.ExcludePatterns);
        _pluginMatchers[plugin] = matcher;
      }
    }

    private PluginSettings? GetPluginForFile(string filePath)
    {
      var relPath = Path.GetFileName(filePath); // Use file name for matching
      foreach (var plugin in _pluginSettingsList) {
        var matcher = _pluginMatchers[plugin];
        if (matcher.Match(relPath).HasMatches)
          return plugin;
      }
      return null;
    }

    public async Task RunAsync(ChannelReader<FileWorkItem> workChannel,
      ChannelWriter<ParsingResult> outputChannel, CancellationToken token)
    {
      await foreach (var workItem in workChannel.ReadAllAsync(token)) {
        switch (workItem.Type) {
          case FileWorkType.FileAdded:
          case FileWorkType.FileChanged:
            if (!_activeTailers.ContainsKey(workItem.FileId)) {
              var plugin = GetPluginForFile(workItem.FilePath);
              if (plugin == null) {
                _logger.LogWarning("No plugin matched for file: {FilePath}", workItem.FilePath);
                break;
              }
              var tailerCts = CancellationTokenSource.CreateLinkedTokenSource(token);
              _activeTailers[workItem.FileId] = tailerCts;
              _logger.LogDebug("Starting tailer for: {FilePath} (ID: {FileId}) with plugin: {PluginName}", workItem.FilePath, workItem.FileId, plugin.Name);
              _ = Task.Run(() => TailingTask(workItem, outputChannel, tailerCts.Token, plugin), token);
            }
            break;
          case FileWorkType.FileRemovedOrRotated:
            if (_activeTailers.TryRemove(workItem.FileId, out var cts)) {
              _logger.LogDebug("Stopping tailer for: {FilePath} (ID: {FileId})", workItem.FilePath, workItem.FileId);
              cts.Cancel();
            }
            break;
        }
      }
    }

    private async Task TailingTask(FileWorkItem workItem, ChannelWriter<ParsingResult> outputChannel, CancellationToken token, PluginSettings plugin)
    {
      var tracked = await _stateProvider.GetTrackedFileAsync(workItem.FileId, workItem.VolumeSerial);
      long position = tracked?.Position ?? 0;

      using var fs = new FileStream(workItem.FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
      fs.Seek(position, SeekOrigin.Begin);

      var parser = new RegexLogParser(plugin, workItem.FilePath);

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
        if (tracked == null || newPosition != tracked.Position) {
          await _stateProvider.UpdateTrackedFileAsync(new TrackedFileInfo {
            VolumeSerial = workItem.VolumeSerial,
            FileId = workItem.FileId,
            FilePath = workItem.FilePath,
            Position = newPosition,
            LastWriteTimeUtc = new DateTime(workItem.LastWriteTimeUtc)
          });
          tracked = new TrackedFileInfo {
            VolumeSerial = workItem.VolumeSerial,
            FileId = workItem.FileId,
            FilePath = workItem.FilePath,
            Position = newPosition,
            LastWriteTimeUtc = new DateTime(workItem.LastWriteTimeUtc)
          };
        }

        await Task.Delay(_settings.PollingIntervalMs, token); // Polling interval

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
