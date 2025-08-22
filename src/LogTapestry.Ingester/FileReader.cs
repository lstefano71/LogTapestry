using LogTapestry.Core;

using Microsoft.Extensions.FileSystemGlobbing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using System.Runtime.CompilerServices;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// Service for core file reading logic with proper dependency injection.
  /// Replaces the file handling in TailingManager with temporary file access.
  /// </summary>
  public class FileReader
  {
    private readonly ILogger<FileReader> _logger;
    private readonly LiveStateService _liveStateService;
    private readonly IOptions<LogTapestrySettings> _settings;
    private readonly List<PluginSettings> _pluginSettings;

    // Parser type mapping: maps plugin.Type to parser factory
    private readonly ILoggerFactory _loggerFactory;
    private static readonly Func<PluginSettings, string, ILoggerFactory, ILogParser> RegexParserFactory =
      (settings, filePath, loggerFactory) => new RegexLogParser(settings, filePath, loggerFactory.CreateLogger<RegexLogParser>());
    private static readonly Func<PluginSettings, string, ILoggerFactory, ILogParser> CsvParserFactory =
      (settings, filePath, loggerFactory) => new CsvLogParser(settings, filePath, loggerFactory.CreateLogger<CsvLogParser>());
    private static readonly Func<PluginSettings, string, ILoggerFactory, ILogParser> SepCsvParserFactory =
      (settings, filePath, loggerFactory) => new SepCsvLogParser(settings, filePath, loggerFactory.CreateLogger<SepCsvLogParser>());
    private static readonly IReadOnlyDictionary<string, Func<PluginSettings, string, ILoggerFactory, ILogParser>> ParserFactories =
      new Dictionary<string, Func<PluginSettings, string, ILoggerFactory, ILogParser>>(StringComparer.OrdinalIgnoreCase) {
        ["regex"] = RegexParserFactory,
        ["csv"] = CsvParserFactory,
        ["sepcsv"] = SepCsvParserFactory
      };

    private readonly List<(PluginSettings Plugin, Matcher Matcher)> _pluginMatchers;

    public FileReader(
      ILogger<FileReader> logger,
      LiveStateService liveStateService,
      IOptions<LogTapestrySettings> settings,
      ILoggerFactory loggerFactory)
    {
      _logger = logger;
      _liveStateService = liveStateService;
      _settings = settings;
      _pluginSettings = settings.Value.Plugins;
      _loggerFactory = loggerFactory;
      // Precompute matchers for each plugin
      _pluginMatchers = [];
      foreach (var plugin in _pluginSettings) {
        var matcher = new Matcher();
        matcher.AddIncludePatterns(plugin.IncludePatterns);
        matcher.AddExcludePatterns(plugin.ExcludePatterns);
        _pluginMatchers.Add((plugin, matcher));
      }
    }

    /// <summary>
    /// Reads and parses a file from the given position and yields multiple DataBlocks.
    /// This is the new chunked checkpointing pipeline that ensures DataBlocks don't exceed MaxEntriesPerDataBlock.
    /// </summary>
    public async IAsyncEnumerable<DataBlock> ReadAndCreateDataBlocksAsync(
      FileCheckRequest request,
      [EnumeratorCancellation] CancellationToken token)
    {

      // Get the plugin for this file
      var plugin = InstantiateParserPlugin(request.FilePath);
      if (plugin == null) {
        _logger.LogWarning("No plugin matched for file: {FilePath}", request.FilePath);
        yield break;
      }

      // Select parser factory based on plugin.Type
      if (!ParserFactories.TryGetValue(plugin.Type, out var parserFactory)) {
        _logger.LogError("No parser available for plugin type: {Type} (file: {FilePath})", plugin.Type, request.FilePath);
        yield break;
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

      await using var parser = parserFactory(plugin, request.FilePath, _loggerFactory);
      var successfulEntries = new List<LogEntry>();
      var currentPosition = position;
      var emittedEntries = 0;
      var maxEntriesPerBlock = _settings.Value.Ingester.MaxEntriesPerDataBlock;

      while (!token.IsCancellationRequested)
      {
        currentPosition = fs.Position;
        
        // Parse next chunk using the new stream-based interface
        var parseResult = await parser.ParseNextChunkAsync(fs, token);
        
        // Collect successful parsing results
        foreach (var entry in parseResult.SuccessfulEntries)
        {
          successfulEntries.Add(entry);
          _logger.LogTrace("Parsed log entry from {FilePath}", request.FilePath);
        }
        
        // Log parsing failures
        foreach (var failure in parseResult.Failures)
        {
          _logger.LogWarning("Log parsing failed: {ErrorMessage}. Source: {Source}. Text: {Text}",
            failure.ErrorMessage, failure.Source, failure.ProblematicText);
        }
        
        // Check if we have any entries or if we reached end of file
        if (parseResult.SuccessfulEntries.Count == 0 && parseResult.Failures.Count == 0)
        {
          // No more data to process
          break;
        }
        
        // Update current position after parsing
        if (parser is IPositionAwareLogParser positionAwareParser)
        {
          // Use the parser's position tracking and sync the stream
          positionAwareParser.UpdateUnderlyingStreamPosition(fs);
          currentPosition = positionAwareParser.GetCurrentPosition();
        }
        else
        {
          currentPosition = fs.Position;
        }

        // Yield DataBlock if we've reached the entry limit
        if (successfulEntries.Count >= maxEntriesPerBlock)
        {
          var dataBlock = new DataBlock(
            FileId: request.FileId,
            VolumeSerial: request.VolumeSerial,
            FilePath: request.FilePath,
            EndPosition: currentPosition,
            LastWriteTime: new DateTime(request.LastWriteTimeUtc, DateTimeKind.Utc),
            Entries: [.. successfulEntries] // Create a copy
          );
          emittedEntries += successfulEntries.Count;
          _logger.LogDebug("Created DataBlock chunk for {FilePath}: {EntryCount} entries, position {Position}",
            request.FilePath, successfulEntries.Count, currentPosition);

          yield return dataBlock;
          // Update in-memory position cache only
          await _liveStateService.UpdatePosition(
            request.FileId,
            request.VolumeSerial,
            currentPosition,
            request.FilePath,
            new DateTime(request.LastWriteTimeUtc, DateTimeKind.Utc),
            PositionUpdateMode.InMemoryOnly
          );
          successfulEntries.Clear();
        }
      }

      // Yield final DataBlock if we have any remaining entries
      if (successfulEntries.Count > 0) {
        var dataBlock = new DataBlock(
          FileId: request.FileId,
          VolumeSerial: request.VolumeSerial,
          FilePath: request.FilePath,
          EndPosition: currentPosition,
          LastWriteTime: new DateTime(request.LastWriteTimeUtc, DateTimeKind.Utc),
          Entries: successfulEntries
        );

        _logger.LogDebug("Created final DataBlock chunk for {FilePath}: {EntryCount} entries, position {Position}",
          request.FilePath, successfulEntries.Count, currentPosition);

        yield return dataBlock;
        // Update in-memory position cache only for final DataBlock
        await _liveStateService.UpdatePosition(
          request.FileId,
          request.VolumeSerial,
          currentPosition,
          request.FilePath,
          new DateTime(request.LastWriteTimeUtc, DateTimeKind.Utc),
          PositionUpdateMode.InMemoryOnly
        );
      }

      if (emittedEntries == 0) {
        _logger.LogTrace("No new content for {FilePath}", request.FilePath);
      }
    }
    /// <summary>
    /// Legacy method - kept for reference but no longer used.
    /// The new stream-based parsers handle their own buffering.
    /// </summary>
    [Obsolete("Use stream-based parsing instead")]
    private static async IAsyncEnumerable<(IList<string>, long)> ReadBlockOfLines(FileStream fs, int maximumNumberOfLines,
      [EnumeratorCancellation] CancellationToken token)
    {
      // read at most maximumNumberOfLines lines from the file stream
      // this is a helper method to read lines in chunks
      var buffer = new List<string>();
      using var sr = new StreamReader(fs, leaveOpen: true);
      string? line;
      while ((line = await sr.ReadLineAsync(token)) != null) {
        buffer.Add(line);
        if (buffer.Count >= maximumNumberOfLines) {
          yield return (buffer, fs.Position);
          buffer.Clear();
        }
      }
      if (buffer.Count > 0) {
        yield return (buffer, fs.Position);
      }
    }

    /// <summary>
    /// Checks if a file has been rotated by comparing file IDs.
    /// </summary>
    public static bool IsFileRotated(string filePath, ulong originalFileId)
    {
      var currentId = NtfsUtils.GetFileIdentifier(filePath);
      return currentId == null || currentId.FileId != originalFileId;
    }

    /// <summary>
    /// Gets the last write time for a file without keeping it open.
    /// </summary>
    public static long GetLastWriteTimeUtc(string filePath)
    {
      return File.GetLastWriteTimeUtc(filePath).Ticks;
    }

    private PluginSettings? InstantiateParserPlugin(string filePath)
    {
      // Use file name for matching (same logic as original TailingManager)
      var relPath = Path.GetFileName(filePath);
      foreach (var (plugin, matcher) in _pluginMatchers) {
        if (matcher.Match(relPath).HasMatches) {
          return plugin;
        }
      }
      return null;
    }

    private async Task<long?> GetTrackedPositionAsync(FileCheckRequest request)
    {
      // Use LiveStateService for fast in-memory position lookup
      return await _liveStateService.GetPosition(request.FileId, request.VolumeSerial);
    }
  }
}
