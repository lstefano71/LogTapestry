using Microsoft.Extensions.Logging;
using System.Globalization;
using System.Text;

namespace LogTapestry.Core
{
  /// <summary>
  /// CSV parser that processes CSV log files with configurable field mappings.
  /// </summary>
  public class CsvLogParser : BaseLogParser
  {
    private readonly CsvPluginConfig _csvConfig;
    private readonly StringBuilder _lineBuffer;
    private bool _headerProcessed;
    private string[]? _headers;
    private readonly Dictionary<string, int> _columnIndexMap;
    private StreamReader? _streamReader;

    public CsvLogParser(PluginSettings settings, string sourceFile, ILogger? logger = null) 
      : base(settings, sourceFile, logger)
    {
      _csvConfig = settings.CsvConfig;
      _lineBuffer = new StringBuilder();
      _headerProcessed = false;
      _columnIndexMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
    }

    public override async Task<ParseChunkResult> ParseNextChunkAsync(Stream stream, CancellationToken token)
    {
      var successfulEntries = new List<LogEntry>();
      var failures = new List<ParsingFailure>();
      var recordsProcessed = 0;
      var maxRecords = _csvConfig.MaxRecordsPerChunk;

      // Initialize stream reader if not already done
      if (_streamReader == null)
      {
        _streamReader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true);
      }

      try
      {
        while (recordsProcessed < maxRecords && !token.IsCancellationRequested)
        {
          var line = await _streamReader.ReadLineAsync(token);
          if (line == null)
          {
            // No more complete lines available
            break;
          }

          if (!_headerProcessed && _csvConfig.HasHeader)
          {
            ProcessHeader(line);
            _headerProcessed = true;
            continue; // Skip header row
          }

          var parseResult = ParseCsvRecord(line);
          if (parseResult.entry != null)
          {
            successfulEntries.Add(parseResult.entry);
          }
          else if (parseResult.failure != null)
          {
            failures.Add(parseResult.failure);
          }

          recordsProcessed++;
        }
      }
      catch (Exception ex)
      {
        Logger?.LogError(ex, "Error parsing CSV chunk from {SourceFile}", SourceFile);
        failures.Add(CreateFailure($"CSV parsing error: {ex.Message}"));
      }

      return new ParseChunkResult(successfulEntries, failures);
    }

    public override async ValueTask DisposeAsync()
    {
      _streamReader?.Dispose();
      _streamReader = null;
      await base.DisposeAsync();
    }

    private void ProcessHeader(string headerLine)
    {
      _headers = ParseCsvLine(headerLine);
      _columnIndexMap.Clear();
      
      for (int i = 0; i < _headers.Length; i++)
      {
        _columnIndexMap[_headers[i]] = i;
      }

      Logger?.LogDebug("Processed CSV header with {ColumnCount} columns: {Headers}", 
        _headers.Length, string.Join(", ", _headers));
    }

    private (LogEntry? entry, ParsingFailure? failure) ParseCsvRecord(string line)
    {
      try
      {
        var fields = ParseCsvLine(line);
        
        if (_headers != null && fields.Length != _headers.Length)
        {
          return (null, CreateFailure($"Column count mismatch. Expected {_headers.Length}, got {fields.Length}", line));
        }

        var timestamp = ExtractTimestamp(fields);
        var level = ExtractLevel(fields);
        var message = ExtractMessage(fields);
        var additionalFields = ExtractAdditionalFields(fields);

        var entry = CreateLogEntry(timestamp, level, message, additionalFields);
        return (entry, null);
      }
      catch (Exception ex)
      {
        Logger?.LogWarning(ex, "Failed to parse CSV record: {Line}", line);
        return (null, CreateFailure($"CSV record parsing failed: {ex.Message}", line));
      }
    }

    private string[] ParseCsvLine(string line)
    {
      var fields = new List<string>();
      var currentField = new StringBuilder();
      bool inQuotes = false;
      
      for (int i = 0; i < line.Length; i++)
      {
        char c = line[i];
        
        if (c == _csvConfig.QuoteChar[0])
        {
          if (inQuotes)
          {
            // Check if this is an escaped quote (double quote)
            if (i + 1 < line.Length && line[i + 1] == _csvConfig.QuoteChar[0])
            {
              // Escaped quote - add one quote to the field and skip the next character
              currentField.Append(c);
              i++; // Skip the next quote
            }
            else
            {
              // End of quoted field
              inQuotes = false;
            }
          }
          else
          {
            // Start of quoted field
            inQuotes = true;
          }
        }
        else if (c == _csvConfig.Delimiter[0] && !inQuotes)
        {
          // Field separator outside of quotes
          fields.Add(currentField.ToString());
          currentField.Clear();
        }
        else
        {
          // Regular character
          currentField.Append(c);
        }
      }

      // Add the last field
      fields.Add(currentField.ToString());
      return fields.ToArray();
    }

    private DateTime ExtractTimestamp(string[] fields)
    {
      if (string.IsNullOrEmpty(_csvConfig.TimestampColumn))
      {
        return DateTime.UtcNow;
      }

      if (!_columnIndexMap.TryGetValue(_csvConfig.TimestampColumn, out int columnIndex) || 
          columnIndex >= fields.Length)
      {
        throw new InvalidOperationException($"Timestamp column '{_csvConfig.TimestampColumn}' not found");
      }

      var timestampText = fields[columnIndex];
      
      if (string.IsNullOrEmpty(timestampText))
      {
        return DateTime.UtcNow;
      }

      DateTime timestamp;
      if (!string.IsNullOrEmpty(_csvConfig.TimestampFormat))
      {
        if (!DateTime.TryParseExact(timestampText, _csvConfig.TimestampFormat, 
            CultureInfo.InvariantCulture, DateTimeStyles.None, out timestamp))
        {
          throw new FormatException($"Failed to parse timestamp '{timestampText}' using format '{_csvConfig.TimestampFormat}'");
        }
      }
      else
      {
        if (!DateTime.TryParse(timestampText, out timestamp))
        {
          throw new FormatException($"Failed to parse timestamp '{timestampText}'");
        }
      }

      if (_csvConfig.TimestampIsUtc)
      {
        timestamp = DateTime.SpecifyKind(timestamp, DateTimeKind.Utc);
      }

      return timestamp;
    }

    private string ExtractLevel(string[] fields)
    {
      if (string.IsNullOrEmpty(_csvConfig.LevelColumn))
      {
        return "INFO";
      }

      if (!_columnIndexMap.TryGetValue(_csvConfig.LevelColumn, out int columnIndex) || 
          columnIndex >= fields.Length)
      {
        return "INFO";
      }

      return fields[columnIndex] ?? "INFO";
    }

    private string ExtractMessage(string[] fields)
    {
      if (string.IsNullOrEmpty(_csvConfig.MessageColumn))
      {
        // Use entire record as message
        return string.Join(_csvConfig.Delimiter, fields);
      }

      if (!_columnIndexMap.TryGetValue(_csvConfig.MessageColumn, out int columnIndex) || 
          columnIndex >= fields.Length)
      {
        return string.Join(_csvConfig.Delimiter, fields);
      }

      return fields[columnIndex] ?? "";
    }

    private Dictionary<string, object> ExtractAdditionalFields(string[] fields)
    {
      var additionalFields = new Dictionary<string, object>();

      foreach (var mapping in _csvConfig.FieldMappings)
      {
        if (!_columnIndexMap.TryGetValue(mapping.ColumnName, out int columnIndex) || 
            columnIndex >= fields.Length)
        {
          if (mapping.IsRequired)
          {
            Logger?.LogWarning("Required field '{FieldName}' (column '{ColumnName}') not found", 
              mapping.FieldName, mapping.ColumnName);
          }
          continue;
        }

        var value = fields[columnIndex];
        if (string.IsNullOrEmpty(value) && mapping.IsRequired)
        {
          Logger?.LogWarning("Required field '{FieldName}' is empty", mapping.FieldName);
          continue;
        }

        var convertedValue = ConvertFieldValue(value, mapping.Type);
        if (convertedValue != null)
        {
          additionalFields[mapping.FieldName] = convertedValue;
        }
      }

      return additionalFields;
    }

    private object? ConvertFieldValue(string value, string type)
    {
      if (string.IsNullOrEmpty(value))
      {
        return null;
      }

      try
      {
        return type.ToLowerInvariant() switch
        {
          "long" or "int64" => long.Parse(value, CultureInfo.InvariantCulture),
          "int" or "int32" => int.Parse(value, CultureInfo.InvariantCulture),
          "double" => double.Parse(value, CultureInfo.InvariantCulture),
          "float" => float.Parse(value, CultureInfo.InvariantCulture),
          "bool" or "boolean" => bool.Parse(value),
          "datetime" => DateTime.Parse(value, CultureInfo.InvariantCulture),
          _ => value
        };
      }
      catch (Exception ex)
      {
        Logger?.LogWarning(ex, "Failed to convert field value '{Value}' to type '{Type}'", value, type);
        return value; // Fall back to string representation
      }
    }
  }
}