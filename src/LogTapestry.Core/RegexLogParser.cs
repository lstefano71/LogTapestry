using Microsoft.Extensions.Logging;

using System.Text;
using System.Text.RegularExpressions;

namespace LogTapestry.Core
{
  public class RegexLogParser : ILogParser
  {
    private readonly PluginSettings _settings;
    private readonly string _sourceFile;
    private readonly ILogger? _logger;
    private readonly Regex _startOfEntryRegex;

    public RegexLogParser(PluginSettings settings, string sourceFile, ILogger? logger = null)
    {
      _settings = settings;
      _sourceFile = sourceFile;
      _logger = logger;
      _startOfEntryRegex = settings.Config?.StartOfEntryRegex != null
          ? new Regex(settings.Config.StartOfEntryRegex, RegexOptions.Compiled)
          : new Regex("^$", RegexOptions.Compiled); // Default: match nothing
    }

    public IEnumerable<ParsingResult> Parse(string[] lines)
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

      // Multi-line mode: current logic
      StringBuilder multiLineBuffer = new();
      LogEntry? inProgressEntry = null;
      bool entryParseFailed = false;

      foreach (var line in lines) {
        if (_startOfEntryRegex.IsMatch(line)) {
          if (inProgressEntry != null) {
            yield return FinalizeEntry(multiLineBuffer, inProgressEntry, entryParseFailed);
          }
          (inProgressEntry, entryParseFailed) = StartNewEntry(line);
          multiLineBuffer.Clear();
          multiLineBuffer.AppendLine(line);
        } else {
          multiLineBuffer.AppendLine(line);
        }
      }
      // Finalize and emit the last entry if present
      if (inProgressEntry != null) {
        yield return FinalizeEntry(multiLineBuffer, inProgressEntry, entryParseFailed);
      }
    }

    public ParsingResult? Flush()
    {
      // Stateless: nothing to flush
      return null;
    }

    private (LogEntry, bool) StartNewEntry(string line)
    {
      var timestamp = DateTime.MinValue;
      var level = "UNKNOWN";
      var message = line;
      var fields = new Dictionary<string, object>();
      bool entryParseFailed = false;

      if (!string.IsNullOrEmpty(_settings.Config.TimestampRegex)) {
        var match = Regex.Match(line, _settings.Config.TimestampRegex);
        if (match.Success) {
          if (DateTime.TryParse(match.Groups[1].Value, out var parsedTimestamp)) {
            if (_settings.Config.TimestampIsUtc) {
              timestamp = DateTime.SpecifyKind(parsedTimestamp, DateTimeKind.Utc);
            } else {
              timestamp = parsedTimestamp;
            }
            return (new LogEntry(timestamp, level, message, _sourceFile, 0, fields, Array.Empty<byte>()), false);
          }
        }
      }
      _logger?.LogWarning("Plugin: {name}, failed to parse timestamp for line: {Line}", _settings.Name, line);
      entryParseFailed = true;
      return (new LogEntry(timestamp, level, message, _sourceFile, 0, fields, Array.Empty<byte>()), entryParseFailed);
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
}
