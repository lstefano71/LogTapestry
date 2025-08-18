using System.Text;
using System.Text.RegularExpressions;

namespace LogTapestry.Core
{
  public class RegexLogParser : ILogParser
  {
    private readonly StringBuilder _multiLineBuffer;
    private LogEntry? _inProgressEntry;
    private Regex _startOfEntryRegex;
    private readonly PluginSettings _settings;
    private readonly string _sourceFile;

    public RegexLogParser(PluginSettings settings, string sourceFile)
    {
      _settings = settings;
      _sourceFile = sourceFile;
      _multiLineBuffer = new StringBuilder();
      _startOfEntryRegex = settings.Config?.StartOfEntryRegex != null
          ? new Regex(settings.Config.StartOfEntryRegex, RegexOptions.Compiled)
          : new Regex("^$", RegexOptions.Compiled); // Default: match nothing
    }

    public IEnumerable<ParsingResult> Parse(string[] lines)
    {
      if (_startOfEntryRegex == null) yield break;
      foreach (var line in lines) {
        if (_startOfEntryRegex.IsMatch(line)) {
          if (_inProgressEntry != null) {
            yield return FinalizeEntry();
          }
          _inProgressEntry = StartNewEntry(line);
        } else {
          _multiLineBuffer.AppendLine(line);
        }
      }
    }

    public ParsingResult? Flush()
    {
      if (_inProgressEntry != null) {
        return FinalizeEntry();
      }
      return null;
    }

    private LogEntry StartNewEntry(string line)
    {
      _multiLineBuffer.Clear();
      _multiLineBuffer.AppendLine(line); // Always append with EOL

      var timestamp = DateTime.MinValue;
      var level = "UNKNOWN";
      var message = line;
      // Fields will be extracted in FinalizeEntry from the full message
      var fields = new Dictionary<string, object>();

      if (!string.IsNullOrEmpty(_settings.Config.TimestampRegex)) {
        var match = Regex.Match(line, _settings.Config.TimestampRegex);
        if (match.Success) {
          if (DateTime.TryParse(match.Groups[1].Value, out var parsedTimestamp)) {
            if (_settings.Config.TimestampIsUtc) {
              timestamp = DateTime.SpecifyKind(parsedTimestamp, DateTimeKind.Utc);
            } else {
              timestamp = parsedTimestamp;
            }
          }
        }
      }

      return new LogEntry(timestamp, level, message, _sourceFile, 0, fields);
    }

    private ParsingResult FinalizeEntry()
    {
      // Remove trailing EOL if present
      var finalMessage = _multiLineBuffer.ToString().TrimEnd('\r', '\n');
      if (_inProgressEntry != null) {
        // Extract fields from the full message
        var fields = new Dictionary<string, object>();
        if (_settings.Config.FieldsRegexes != null) {
          foreach (var fieldRegex in _settings.Config.FieldsRegexes) {
            if (string.IsNullOrEmpty(fieldRegex.Regex) || string.IsNullOrEmpty(fieldRegex.FieldName)) continue;
            var matches = Regex.Matches(finalMessage, fieldRegex.Regex);
            if (matches.Count > 0) {
              // Use the last match (in case of multiple occurrences)
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
        // Extract level from the full message
        var level = _inProgressEntry.Level;
        if (!string.IsNullOrEmpty(_settings.Config.LevelRegex)) {
          var match = Regex.Match(finalMessage, _settings.Config.LevelRegex);
          if (match.Success) {
            level = match.Groups[1].Value;
          }
        }
        var entry = _inProgressEntry with { Message = finalMessage, Fields = fields, Level = level };
        _inProgressEntry = null;
        _multiLineBuffer.Clear();
        return ParsingResult.Success(entry);
      }
      return ParsingResult.Failure("FinalizeEntry called with null _inProgressEntry", finalMessage, _sourceFile);
    }
  }
}
