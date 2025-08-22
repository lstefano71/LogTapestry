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
    private bool _headerProcessed;
    private string[]? _headers;
    private readonly Dictionary<string, int> _columnIndexMap;

    public CsvLogParser(PluginSettings settings, string sourceFile, ILogger? logger = null) 
      : base(settings, sourceFile, logger)
    {
      _csvConfig = settings.CsvConfig;
      _headerProcessed = false;
      _columnIndexMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
    }

    public override async Task<ParseChunkResult> ParseNextChunkAsync(Stream stream, CancellationToken token)
    {
      var successfulEntries = new List<LogEntry>();
      var failures = new List<ParsingFailure>();
      var recordsProcessed = 0;
      var maxRecords = _csvConfig.MaxRecordsPerChunk;

      try
      {
        while (recordsProcessed < maxRecords && !token.IsCancellationRequested)
        {
          var record = await ReadNextCsvRecordAsync(stream, token);
          if (record == null)
          {
            // No more complete records available
            break;
          }

          if (!_headerProcessed && _csvConfig.HasHeader)
          {
            ProcessHeader(record);
            _headerProcessed = true;
            continue; // Skip header row
          }

          var parseResult = ParseCsvRecord(record);
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

    private async Task<string?> ReadNextCsvRecordAsync(Stream stream, CancellationToken token)
    {
      WorkingStringBuilder.Clear(); // Use the shared StringBuilder
      bool inQuotes = false;
      
      while (!token.IsCancellationRequested)
      {
        // Read more data into buffer if needed
        if (BufferLength == 0)
        {
          var bytesRead = await ReadIntoBufferAsync(stream, token);
          if (bytesRead == 0)
          {
            // End of stream - return any remaining data as a record
            if (WorkingStringBuilder.Length > 0)
            {
              return WorkingStringBuilder.ToString();
            }
            return null;
          }
        }

        // Process buffer efficiently looking for record boundaries
        int startPos = 0;
        for (int i = 0; i < BufferLength; i++)
        {
          byte b = Buffer[i];
          
          if (b == (byte)_csvConfig.QuoteChar[0])
          {
            inQuotes = !inQuotes;
          }
          else if (!inQuotes && (b == (byte)'\n'))
          {
            // Found end of record outside quotes
            
            // Add the data up to (but not including) the newline
            if (i > startPos)
            {
              AppendBufferToStringBuilder(WorkingStringBuilder, startPos, i - startPos);
            }
            
            var result = WorkingStringBuilder.ToString();
            
            // Remove processed data from buffer (including the newline)
            ConsumeBuffer(i + 1);
            
            return result;
          }
          else if (!inQuotes && b == (byte)'\r')
          {
            // Handle \r\n or standalone \r
            int lineEndPos = i;
            int consumeLength;
            
            if (i + 1 < BufferLength && Buffer[i + 1] == (byte)'\n')
            {
              consumeLength = i + 2; // \r\n
            }
            else
            {
              consumeLength = i + 1; // standalone \r
            }
            
            // Add data up to \r
            if (lineEndPos > startPos)
            {
              AppendBufferToStringBuilder(WorkingStringBuilder, startPos, lineEndPos - startPos);
            }
            
            var result = WorkingStringBuilder.ToString();
            ConsumeBuffer(consumeLength);
            return result;
          }
        }

        // No complete record found, append all buffer data and continue
        if (BufferLength > startPos)
        {
          AppendBufferToStringBuilder(WorkingStringBuilder, startPos, BufferLength - startPos);
        }
        ConsumeBuffer(BufferLength);
      }

      return null;
    }

    public override async ValueTask DisposeAsync()
    {
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