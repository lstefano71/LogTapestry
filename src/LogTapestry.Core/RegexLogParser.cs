using Microsoft.Extensions.Logging;

using System.Text;
using System.Text.RegularExpressions;

namespace LogTapestry.Core
{
  public class LegacyRegexLogParser(PluginSettings settings, string sourceFile, ILogger? logger = null) : ILegacyLogParser
  {
    private readonly PluginSettings _settings = settings;
    private readonly string _sourceFile = sourceFile;
    private readonly ILogger? _logger = logger;
    private readonly Regex _startOfEntryRegex = settings.Config?.StartOfEntryRegex != null
          ? new Regex(settings.Config.StartOfEntryRegex, RegexOptions.Compiled)
          : new Regex("^$", RegexOptions.Compiled);

    // State for multi-line parsing
    private readonly StringBuilder _multiLineBuffer = new();
    private LogEntry? _inProgressEntry = null;
    private bool _entryParseFailed;

    public IEnumerable<ParsingResult> Parse(IList<string> lines)
    {
      // Single-line mode: StartOfEntryRegex is not set or empty
      if (_settings.Config?.StartOfEntryRegex == null || string.IsNullOrWhiteSpace(_settings.Config.StartOfEntryRegex)) {
        foreach (var line in lines) {
          if (!string.IsNullOrWhiteSpace(line)) {
            var (entry, singleLineParseFailed) = StartNewEntry(line);
            yield return FinalizeEntry(new StringBuilder(line), entry, singleLineParseFailed);
          }
        }
        yield break;
      }

      // Multi-line mode: maintain state across batches
      foreach (var line in lines) {
        if (_startOfEntryRegex.IsMatch(line)) {
          // If we have an in-progress entry, finalize and yield it
          if (_inProgressEntry != null) {
            yield return FinalizeEntry(_multiLineBuffer, _inProgressEntry, _entryParseFailed);
          }
          // Start new entry
          (_inProgressEntry, _entryParseFailed) = StartNewEntry(line);
          _multiLineBuffer.Clear();
          _multiLineBuffer.AppendLine(line);
        } else {
          // Continue accumulating lines for the current entry
          _multiLineBuffer.AppendLine(line);
        }
      }
      // Do not finalize here; leave incomplete entry buffered for next batch or flush
    }

    public ParsingResult? Flush()
    {
      // Emit any remaining buffered entry
      if (_inProgressEntry != null && _multiLineBuffer.Length > 0) {
        var result = FinalizeEntry(_multiLineBuffer, _inProgressEntry, _entryParseFailed);
        _inProgressEntry = null;
        _multiLineBuffer.Clear();
        _entryParseFailed = false;
        return result;
      }
      return null;
    }

    private (LogEntry, bool) StartNewEntry(string line)
    {
      var timestamp = DateTime.MinValue;
      var level = "UNKNOWN";
      var message = line;
      var fields = new Dictionary<string, object>();
      bool entryParseFailed;

      if (!string.IsNullOrEmpty(_settings.Config.TimestampRegex)) {
        var match = Regex.Match(line, _settings.Config.TimestampRegex);
        if (match.Success) {
          if (DateTime.TryParse(match.Groups[1].Value, out var parsedTimestamp)) {
            if (_settings.Config.TimestampIsUtc) {
              timestamp = DateTime.SpecifyKind(parsedTimestamp, DateTimeKind.Utc);
            } else {
              timestamp = parsedTimestamp;
            }
            return (new LogEntry(timestamp, level, message, _sourceFile, 0, fields, []), false);
          }
        }
      }
      _logger?.LogWarning("Plugin: {name}, failed to parse timestamp for line: {Line}", _settings.Name, line);
      entryParseFailed = true;
      return (new LogEntry(timestamp, level, message, _sourceFile, 0, fields, []), entryParseFailed);
    }

    private ParsingResult FinalizeEntry(StringBuilder multiLineBuffer, LogEntry inProgressEntry, bool entryParseFailed)
    {
      var finalMessage = multiLineBuffer.ToString().TrimEnd('\r', '\n');
      if (entryParseFailed) {
        return ParsingResult.Failure("Failed to parse timestamp", finalMessage, _sourceFile);
      }
      var fields = new Dictionary<string, object>();
      if (_settings.Config.FieldsRegexes != null) {
        foreach (var fieldRegex in _settings.Config.FieldsRegexes) {
          if (string.IsNullOrEmpty(fieldRegex.Regex) || string.IsNullOrEmpty(fieldRegex.FieldName)) continue;
          var matches = Regex.Matches(finalMessage, fieldRegex.Regex);
          if (matches.Count > 0) {
            var match = matches[^1];
            var valueStr = match.Groups[1].Value;
            object value = valueStr;
            if (fieldRegex.Type == "long") {
              if (long.TryParse(valueStr, out var longValue)) {
                value = longValue;
              }
            } else if (fieldRegex.Type == "double") {
              if (double.TryParse(valueStr, out var doubleValue)) {
                value = doubleValue;
              }
            }
            fields[fieldRegex.FieldName] = value;
          }
        }
      }
      var level = inProgressEntry.Level;
      if (!string.IsNullOrEmpty(_settings.Config.LevelRegex)) {
        var match = Regex.Match(finalMessage, _settings.Config.LevelRegex);
        if (match.Success) {
          level = match.Groups[1].Value;
        }
      }
      var entry = inProgressEntry with { Message = finalMessage, Fields = fields, Level = level, Ulid = inProgressEntry.Ulid };
      return ParsingResult.Success(entry);
    }
  }

  /// <summary>
  /// Stream-based regex parser that implements the new ILogParser interface.
  /// </summary>
  public class RegexLogParser : BaseLogParser
  {
    private readonly RegexPluginConfig _regexConfig;
    private readonly Regex _startOfEntryRegex;
    private readonly StringBuilder _lineBuffer;
    private readonly StringBuilder _multiLineBuffer;
    private LogEntry? _inProgressEntry;
    private bool _entryParseFailed;

    public RegexLogParser(PluginSettings settings, string sourceFile, ILogger? logger = null) 
      : base(settings, sourceFile, logger)
    {
      _regexConfig = settings.Config;
      _startOfEntryRegex = _regexConfig.StartOfEntryRegex != null
            ? new Regex(_regexConfig.StartOfEntryRegex, RegexOptions.Compiled)
            : new Regex("^$", RegexOptions.Compiled);
      _lineBuffer = new StringBuilder();
      _multiLineBuffer = new StringBuilder();
    }

    public override async Task<ParseChunkResult> ParseNextChunkAsync(Stream stream, CancellationToken token)
    {
      var successfulEntries = new List<LogEntry>();
      var failures = new List<ParsingFailure>();
      var linesProcessed = 0;
      var maxLinesPerChunk = 1000; // Configurable limit

      try
      {
        while (linesProcessed < maxLinesPerChunk && !token.IsCancellationRequested)
        {
          var line = await ReadNextCompleteLineAsync(stream, token);
          if (line == null)
          {
            // No more complete lines available
            break;
          }

          var results = ProcessLine(line);
          foreach (var result in results)
          {
            if (result.IsSuccess && result.Entry != null)
            {
              successfulEntries.Add(result.Entry);
            }
            else
            {
              failures.Add(new ParsingFailure(result.ErrorMessage ?? "Unknown error", SourceFile, result.UnparseableText));
            }
          }

          linesProcessed++;
        }

        // Process any remaining buffered entry at end of chunk
        var flushResult = FlushBufferedEntry();
        if (flushResult != null)
        {
          if (flushResult.IsSuccess && flushResult.Entry != null)
          {
            successfulEntries.Add(flushResult.Entry);
          }
          else
          {
            failures.Add(new ParsingFailure(flushResult.ErrorMessage ?? "Unknown error", SourceFile, flushResult.UnparseableText));
          }
        }
      }
      catch (Exception ex)
      {
        Logger?.LogError(ex, "Error parsing regex chunk from {SourceFile}", SourceFile);
        failures.Add(CreateFailure($"Regex parsing error: {ex.Message}"));
      }

      return new ParseChunkResult(successfulEntries, failures);
    }

    private async Task<string?> ReadNextCompleteLineAsync(Stream stream, CancellationToken token)
    {
      while (!token.IsCancellationRequested)
      {
        // Read more data into buffer if needed
        if (BufferLength == 0)
        {
          var bytesRead = await ReadIntoBufferAsync(stream, token);
          if (bytesRead == 0)
          {
            // End of stream - return any remaining data in line buffer
            if (_lineBuffer.Length > 0)
            {
              var remainingLine = _lineBuffer.ToString();
              _lineBuffer.Clear();
              return remainingLine;
            }
            return null;
          }
        }

        // Look for line endings using optimized byte search
        int newlinePos = IndexOfByte((byte)'\n');
        int crPos = IndexOfByte((byte)'\r');
        
        int lineEndPos = -1;
        int consumeLength = 0;
        
        if (newlinePos >= 0 && (crPos < 0 || newlinePos < crPos))
        {
          // Found \n first (or only)
          lineEndPos = newlinePos;
          consumeLength = newlinePos + 1;
        }
        else if (crPos >= 0)
        {
          // Found \r first
          lineEndPos = crPos;
          
          // Check for \r\n sequence using BufferContainsAt
          if (crPos + 1 < BufferLength && Buffer[crPos + 1] == (byte)'\n')
          {
            consumeLength = crPos + 2;
          }
          else
          {
            consumeLength = crPos + 1;
          }
        }
        
        if (lineEndPos >= 0)
        {
          // Found complete line
          
          // Append any data before the line ending to line buffer
          if (lineEndPos > 0)
          {
            AppendBufferToStringBuilder(_lineBuffer, 0, lineEndPos);
          }
          
          // Get the complete line
          var completeLine = _lineBuffer.ToString();
          _lineBuffer.Clear();
          
          // Remove processed data from buffer
          ConsumeBuffer(consumeLength);
          
          return completeLine;
        }
        else
        {
          // No complete line found, append all buffer data to line buffer
          if (BufferLength > 0)
          {
            AppendBufferToStringBuilder(_lineBuffer, 0, BufferLength);
            ConsumeBuffer(BufferLength);
          }
        }
      }

      return null;
    }

    private IEnumerable<ParsingResult> ProcessLine(string line)
    {
      // Single-line mode: StartOfEntryRegex is not set or empty
      if (string.IsNullOrWhiteSpace(_regexConfig.StartOfEntryRegex))
      {
        if (!string.IsNullOrWhiteSpace(line))
        {
          var (entry, singleLineParseFailed) = StartNewEntry(line);
          yield return FinalizeEntry(new StringBuilder(line), entry, singleLineParseFailed);
        }
        yield break;
      }

      // Multi-line mode: maintain state across lines
      if (_startOfEntryRegex.IsMatch(line))
      {
        // If we have an in-progress entry, finalize and yield it
        if (_inProgressEntry != null)
        {
          yield return FinalizeEntry(_multiLineBuffer, _inProgressEntry, _entryParseFailed);
        }
        // Start new entry
        (_inProgressEntry, _entryParseFailed) = StartNewEntry(line);
        _multiLineBuffer.Clear();
        _multiLineBuffer.AppendLine(line);
      }
      else
      {
        // Continue accumulating lines for the current entry
        _multiLineBuffer.AppendLine(line);
      }
    }

    private ParsingResult? FlushBufferedEntry()
    {
      // Emit any remaining buffered entry
      if (_inProgressEntry != null && _multiLineBuffer.Length > 0)
      {
        var result = FinalizeEntry(_multiLineBuffer, _inProgressEntry, _entryParseFailed);
        _inProgressEntry = null;
        _multiLineBuffer.Clear();
        _entryParseFailed = false;
        return result;
      }
      return null;
    }

    private (LogEntry, bool) StartNewEntry(string line)
    {
      var timestamp = DateTime.MinValue;
      var level = "UNKNOWN";
      var message = line;
      var fields = new Dictionary<string, object>();
      bool entryParseFailed;

      if (!string.IsNullOrEmpty(_regexConfig.TimestampRegex))
      {
        var match = Regex.Match(line, _regexConfig.TimestampRegex);
        if (match.Success)
        {
          if (DateTime.TryParse(match.Groups[1].Value, out var parsedTimestamp))
          {
            if (_regexConfig.TimestampIsUtc)
            {
              timestamp = DateTime.SpecifyKind(parsedTimestamp, DateTimeKind.Utc);
            }
            else
            {
              timestamp = parsedTimestamp;
            }
            return (new LogEntry(timestamp, level, message, SourceFile, 0, fields, []), false);
          }
        }
      }
      Logger?.LogWarning("Plugin: {name}, failed to parse timestamp for line: {Line}", Settings.Name, line);
      entryParseFailed = true;
      return (new LogEntry(timestamp, level, message, SourceFile, 0, fields, []), entryParseFailed);
    }

    private ParsingResult FinalizeEntry(StringBuilder multiLineBuffer, LogEntry inProgressEntry, bool entryParseFailed)
    {
      var finalMessage = multiLineBuffer.ToString().TrimEnd('\r', '\n');
      if (entryParseFailed)
      {
        return ParsingResult.Failure("Failed to parse timestamp", finalMessage, SourceFile);
      }
      var fields = new Dictionary<string, object>();
      if (_regexConfig.FieldsRegexes != null)
      {
        foreach (var fieldRegex in _regexConfig.FieldsRegexes)
        {
          if (string.IsNullOrEmpty(fieldRegex.Regex) || string.IsNullOrEmpty(fieldRegex.FieldName)) continue;
          var matches = Regex.Matches(finalMessage, fieldRegex.Regex);
          if (matches.Count > 0)
          {
            var match = matches[^1];
            var valueStr = match.Groups[1].Value;
            object value = valueStr;
            if (fieldRegex.Type == "long")
            {
              if (long.TryParse(valueStr, out var longValue))
              {
                value = longValue;
              }
            }
            else if (fieldRegex.Type == "double")
            {
              if (double.TryParse(valueStr, out var doubleValue))
              {
                value = doubleValue;
              }
            }
            fields[fieldRegex.FieldName] = value;
          }
        }
      }
      var level = inProgressEntry.Level;
      if (!string.IsNullOrEmpty(_regexConfig.LevelRegex))
      {
        var match = Regex.Match(finalMessage, _regexConfig.LevelRegex);
        if (match.Success)
        {
          level = match.Groups[1].Value;
        }
      }
      var entry = inProgressEntry with { Message = finalMessage, Fields = fields, Level = level, Ulid = inProgressEntry.Ulid };
      return ParsingResult.Success(entry);
    }
  }
}
