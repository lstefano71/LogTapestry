using LogTapestry.Core;

using Microsoft.Extensions.FileSystemGlobbing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// Service for core file reading logic with proper dependency injection.
  /// Replaces the file handling in TailingManager with temporary file access.
  /// </summary>
  public class FileReader
  {
    private readonly ILogger<FileReader> _logger;
    private readonly IStateProvider _stateProvider;
    private readonly List<PluginSettings> _pluginSettings;
    private readonly Matcher _matcher;

    // Parser type mapping: maps plugin.Type to parser factory
    private readonly ILoggerFactory _loggerFactory;
    private static Func<PluginSettings, string, ILoggerFactory, ILogParser> RegexParserFactory =
      (settings, filePath, loggerFactory) => new RegexLogParser(settings, filePath, loggerFactory.CreateLogger<RegexLogParser>());
    private static readonly IReadOnlyDictionary<string, Func<PluginSettings, string, ILoggerFactory, ILogParser>> ParserFactories =
      new Dictionary<string, Func<PluginSettings, string, ILoggerFactory, ILogParser>>(StringComparer.OrdinalIgnoreCase) {
        ["regex"] = RegexParserFactory
      };

    public FileReader(
      ILogger<FileReader> logger,
      IStateProvider stateProvider,
      IOptions<LogTapestrySettings> settings,
      ILoggerFactory loggerFactory)
    {
      _logger = logger;
      _stateProvider = stateProvider;
      _pluginSettings = settings.Value.Plugins;
      _loggerFactory = loggerFactory;

      // Create matcher for plugin include/exclude patterns
      _matcher = new Matcher();
      foreach (var plugin in _pluginSettings) {
        _matcher.AddIncludePatterns(plugin.IncludePatterns);
        _matcher.AddExcludePatterns(plugin.ExcludePatterns);
      }
    }

    /// <summary>
    /// Reads and parses a file from the given position, then immediately closes it.
    /// This prevents file handle exhaustion in the worker pool model.
    /// </summary>
    public async Task ReadAndParseFileAsync(
      FileCheckRequest request,
      ChannelWriter<PositionUpdate> positionChannel,
      ChannelWriter<ParsingResult> parsingChannel,
      CancellationToken token)
    {
      try {
        // Get the plugin for this file
        var plugin = GetPluginForFile(request.FilePath);
        if (plugin == null) {
          _logger.LogWarning("No plugin matched for file: {FilePath}", request.FilePath);
          return;
        }

        // Select parser factory based on plugin.Type
        if (!ParserFactories.TryGetValue(plugin.Type, out var parserFactory)) {
          _logger.LogError("No parser available for plugin type: {Type} (file: {FilePath})", plugin.Type, request.FilePath);
          return;
        }

        // Get current tracked position
        var trackedPosition = await GetTrackedPositionAsync(request);
        var position = trackedPosition ?? 0;

        // Open file temporarily, read new content, then close immediately
        using var fs = new FileStream(request.FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        if (position > fs.Length) {
          // File was truncated, reset position
          position = 0;
        }

        fs.Seek(position, SeekOrigin.Begin);

        var parser = parserFactory(plugin, request.FilePath, _loggerFactory);
        var newPosition = position;
        var buffer = new List<string>();

        using (var sr = new StreamReader(fs, leaveOpen: true)) {
          string? line;
          while ((line = await sr.ReadLineAsync(token)) != null) {
            buffer.Add(line);
          }
        }

        // Parse the new content
        var results = parser.Parse([.. buffer]);

        // Send position update if we read new content
        if (buffer.Count > 0) {
          newPosition = fs.Position;

          var positionUpdate = new PositionUpdate {
            FileId = request.FileId,
            VolumeSerial = request.VolumeSerial,
            Position = newPosition,
            FilePath = request.FilePath,
            LastWriteTimeUtc = request.LastWriteTimeUtc
          };

          await positionChannel.WriteAsync(positionUpdate, token);
          _logger.LogDebug("Updated position for {FilePath}: {Position}", request.FilePath, newPosition);
        }

        // Send parsing results to the next pipeline stage
        foreach (var result in results) {
          if (result.IsSuccess && result.Entry != null) {
            // Send successful parsing result to the parsing pipeline
            await parsingChannel.WriteAsync(result, token);
            _logger.LogTrace("Sent parsed log entry from {FilePath} to parsing pipeline", request.FilePath);
          } else {
            // Send failed parsing result as well for error handling
            await parsingChannel.WriteAsync(result, token);
            _logger.LogWarning("Log parsing failed: {ErrorMessage}. Source: {Source}",
              result.ErrorMessage, result.Source);
          }
        }

        // Flush the parser to emit any buffered result (e.g., last line/event)
        var flushResult = parser.Flush();
        if (flushResult != null) {
          await parsingChannel.WriteAsync(flushResult, token);
          if (flushResult.IsSuccess && flushResult.Entry != null) {
            _logger.LogTrace("Sent flushed parsed log entry from {FilePath} to parsing pipeline", request.FilePath);
          } else {
            _logger.LogWarning("Log parsing failed on flush: {ErrorMessage}. Source: {Source}",
              flushResult.ErrorMessage, flushResult.Source);
          }
        }
      } catch (Exception ex) {
        _logger.LogError(ex, "Error reading file {FilePath}", request.FilePath);
      }
    }

    /// <summary>
    /// Checks if a file has been rotated by comparing file IDs.
    /// </summary>
    public bool IsFileRotated(string filePath, ulong originalFileId)
    {
      var currentId = NtfsUtils.GetFileIdentifier(filePath);
      return currentId == null || currentId.FileId != originalFileId;
    }

    /// <summary>
    /// Gets the last write time for a file without keeping it open.
    /// </summary>
    public long GetLastWriteTimeUtc(string filePath)
    {
      return File.GetLastWriteTimeUtc(filePath).Ticks;
    }

    private PluginSettings? GetPluginForFile(string filePath)
    {
      // Use file name for matching (same logic as original TailingManager)
      var relPath = Path.GetFileName(filePath);

      foreach (var plugin in _pluginSettings) {
        var pluginMatcher = new Matcher();
        pluginMatcher.AddIncludePatterns(plugin.IncludePatterns);
        pluginMatcher.AddExcludePatterns(plugin.ExcludePatterns);

        if (pluginMatcher.Match(relPath).HasMatches) {
          return plugin;
        }
      }

      return null;
    }

    private async Task<long?> GetTrackedPositionAsync(FileCheckRequest request)
    {
      var trackedFile = await _stateProvider.GetTrackedFileAsync(request.FileId, request.VolumeSerial);
      return trackedFile?.Position;
    }
  }
}
