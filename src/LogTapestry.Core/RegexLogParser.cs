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
      _multiLineBuffer.Append(line);

      var timestamp = DateTime.MinValue;
      var level = "UNKNOWN";
      var message = line;
      var fields = new Dictionary<string, object>();

      if (!string.IsNullOrEmpty(_settings.Config.TimestampRegex)) {
        var match = Regex.Match(line, _settings.Config.TimestampRegex);
        if (match.Success) {
          if (DateTime.TryParse(match.Groups[1].Value, out var parsedTimestamp)) {
            timestamp = parsedTimestamp;
          }
        }
      }

      if (!string.IsNullOrEmpty(_settings.Config.LevelRegex)) {
        var match = Regex.Match(line, _settings.Config.LevelRegex);
        if (match.Success) {
          level = match.Groups[1].Value;
        }
      }

      if (_settings.Config.FieldsRegexes != null) {
        foreach (var fieldRegex in _settings.Config.FieldsRegexes) {
          if (fieldRegex.Regex == null || fieldRegex.FieldName == null) continue;
          var match = Regex.Match(line, fieldRegex.Regex);
          if (match.Success) {
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

      return new LogEntry(timestamp, level, message, _sourceFile, 0, fields);
    }

    private ParsingResult FinalizeEntry()
    {
      var finalMessage = _multiLineBuffer.ToString();
      if (_inProgressEntry != null) {
        var entry = _inProgressEntry with { Message = finalMessage };
        _inProgressEntry = null;
        _multiLineBuffer.Clear();
        return ParsingResult.Success(entry);
      }
      return ParsingResult.Failure("FinalizeEntry called with null _inProgressEntry", finalMessage, _sourceFile);
    }
  }
}
