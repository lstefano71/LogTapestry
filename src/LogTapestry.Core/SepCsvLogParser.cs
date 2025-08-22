using Microsoft.Extensions.Logging;
using nietras.SeparatedValues;
using System.Globalization;

namespace LogTapestry.Core
{
    /// <summary>
    /// High-performance CSV parser using nietras/Sep library with precise stream position tracking.
    /// Maintains stream position tracking required for checkpointing while leveraging Sep's optimized parsing.
    /// </summary>
    public class SepCsvLogParser : IPositionAwareLogParser
    {
        private readonly CsvPluginConfig _csvConfig;
        private readonly string _sourceFile;
        private readonly ILogger? _logger;
        private PositionTrackingStream? _trackingStream;
        private SepReader? _reader;
        private bool _headerProcessed;
        private string[]? _headers;
        private readonly Dictionary<string, int> _columnIndexMap;

        public SepCsvLogParser(PluginSettings settings, string sourceFile, ILogger? logger = null)
        {
            if (settings.CsvConfig == null)
            {
                throw new ArgumentException("CSV plugin configuration is required", nameof(settings));
            }

            _csvConfig = settings.CsvConfig;
            _sourceFile = sourceFile;
            _logger = logger;
            _headerProcessed = false;
            _columnIndexMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            _logger?.LogDebug("Initialized SepCsvLogParser for {SourceFile} with delimiter '{Delimiter}'", 
                _sourceFile, _csvConfig.Delimiter);
        }

        public async Task<ParseChunkResult> ParseNextChunkAsync(Stream stream, CancellationToken token)
        {
            var successfulEntries = new List<LogEntry>();
            var failures = new List<ParsingFailure>();
            var recordsProcessed = 0;
            var maxRecords = _csvConfig.MaxRecordsPerChunk;

            try
            {
                EnsureReaderInitialized(stream);
                
                if (_reader == null)
                {
                    return new ParseChunkResult(successfulEntries, failures);
                }

                while (recordsProcessed < maxRecords && !token.IsCancellationRequested)
                {
                    if (!_reader.MoveNext())
                    {
                        // No more records available
                        break;
                    }

                    var row = _reader.Current;
                    
                    if (!_headerProcessed && _csvConfig.HasHeader)
                    {
                        ProcessHeader(row);
                        _headerProcessed = true;
                        continue; // Skip header row
                    }

                    var parseResult = ParseCsvRecord(row);
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

                _logger?.LogTrace("Parsed {RecordCount} records, current position: {Position}", 
                    recordsProcessed, GetCurrentPosition());
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error parsing CSV chunk from {SourceFile}", _sourceFile);
                failures.Add(CreateFailure($"CSV parsing error: {ex.Message}"));
            }

            return new ParseChunkResult(successfulEntries, failures);
        }

        /// <summary>
        /// Gets the current stream position accounting for all read operations.
        /// This is the key method that enables proper checkpointing.
        /// </summary>
        public long GetCurrentPosition()
        {
            return _trackingStream?.CurrentPosition ?? 0;
        }

        /// <summary>
        /// Updates the underlying stream position to reflect the tracked reads.
        /// Should be called by FileReader after parsing to sync positions.
        /// </summary>
        public void UpdateUnderlyingStreamPosition(Stream underlyingStream)
        {
            var currentPos = GetCurrentPosition();
            if (underlyingStream.CanSeek && underlyingStream.Position != currentPos)
            {
                underlyingStream.Position = currentPos;
            }
        }

        private void EnsureReaderInitialized(Stream stream)
        {
            if (_reader != null)
            {
                return; // Already initialized
            }

            // Wrap the stream with our position tracking stream
            _trackingStream = new PositionTrackingStream(stream);

            _logger?.LogDebug("Initializing Sep reader from position {Position}", _trackingStream.CurrentPosition);

            // Create the Sep reader with tracking stream
            _reader = Sep.Reader(opts => opts with 
            {
                HasHeader = _csvConfig.HasHeader,
                Sep = new Sep(_csvConfig.Delimiter[0])
            })
            .From(_trackingStream);

            // If we have a header and haven't processed it yet, we need to handle it
            if (_csvConfig.HasHeader && !_headerProcessed)
            {
                // The reader will handle the header automatically if HasHeader is true
                // We just need to track when we've seen it
                _headers = _reader.Header.ColNames.ToArray();
                BuildColumnIndexMap();
                _headerProcessed = true;
                
                _logger?.LogDebug("Processed CSV header with {ColumnCount} columns: {Headers}", 
                    _headers.Length, string.Join(", ", _headers));
            }
        }

        private void ProcessHeader(SepReader.Row row)
        {
            // Extract header names from the row
            _headers = new string[row.ColCount];
            for (int i = 0; i < row.ColCount; i++)
            {
                _headers[i] = row[i].ToString();
            }
            
            BuildColumnIndexMap();
            
            _logger?.LogDebug("Processed CSV header with {ColumnCount} columns: {Headers}", 
                _headers.Length, string.Join(", ", _headers));
        }

        private void BuildColumnIndexMap()
        {
            if (_headers == null) return;
            
            _columnIndexMap.Clear();
            for (int i = 0; i < _headers.Length; i++)
            {
                _columnIndexMap[_headers[i]] = i;
            }
        }

        private (LogEntry? entry, ParsingFailure? failure) ParseCsvRecord(SepReader.Row row)
        {
            try
            {
                if (_headers != null && row.ColCount != _headers.Length)
                {
                    var rowText = string.Join(_csvConfig.Delimiter, GetRowValues(row));
                    return (null, CreateFailure($"Column count mismatch. Expected {_headers.Length}, got {row.ColCount}", rowText));
                }

                var timestamp = ExtractTimestamp(row);
                var level = ExtractLevel(row);
                var message = ExtractMessage(row);
                var additionalFields = ExtractAdditionalFields(row);

                var entry = CreateLogEntry(timestamp, level, message, additionalFields);
                return (entry, null);
            }
            catch (Exception ex)
            {
                var rowText = string.Join(_csvConfig.Delimiter, GetRowValues(row));
                _logger?.LogWarning(ex, "Failed to parse CSV record: {Row}", rowText);
                return (null, CreateFailure($"CSV record parsing failed: {ex.Message}", rowText));
            }
        }

        private string[] GetRowValues(SepReader.Row row)
        {
            var values = new string[row.ColCount];
            for (int i = 0; i < row.ColCount; i++)
            {
                values[i] = row[i].ToString();
            }
            return values;
        }

        private DateTime ExtractTimestamp(SepReader.Row row)
        {
            if (string.IsNullOrEmpty(_csvConfig.TimestampColumn))
            {
                return DateTime.UtcNow;
            }

            if (!_columnIndexMap.TryGetValue(_csvConfig.TimestampColumn, out int columnIndex) || 
                columnIndex >= row.ColCount)
            {
                throw new InvalidOperationException($"Timestamp column '{_csvConfig.TimestampColumn}' not found");
            }

            var timestampText = row[columnIndex].ToString();
            
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

        private string ExtractLevel(SepReader.Row row)
        {
            if (string.IsNullOrEmpty(_csvConfig.LevelColumn))
            {
                return "INFO";
            }

            if (!_columnIndexMap.TryGetValue(_csvConfig.LevelColumn, out int columnIndex) || 
                columnIndex >= row.ColCount)
            {
                return "INFO";
            }

            return row[columnIndex].ToString() ?? "INFO";
        }

        private string ExtractMessage(SepReader.Row row)
        {
            if (string.IsNullOrEmpty(_csvConfig.MessageColumn))
            {
                // Use entire record as message
                return string.Join(_csvConfig.Delimiter, GetRowValues(row));
            }

            if (!_columnIndexMap.TryGetValue(_csvConfig.MessageColumn, out int columnIndex) || 
                columnIndex >= row.ColCount)
            {
                return string.Join(_csvConfig.Delimiter, GetRowValues(row));
            }

            return row[columnIndex].ToString() ?? "";
        }

        private Dictionary<string, object> ExtractAdditionalFields(SepReader.Row row)
        {
            var additionalFields = new Dictionary<string, object>();

            foreach (var mapping in _csvConfig.FieldMappings)
            {
                if (!_columnIndexMap.TryGetValue(mapping.ColumnName, out int columnIndex) || 
                    columnIndex >= row.ColCount)
                {
                    if (mapping.IsRequired)
                    {
                        _logger?.LogWarning("Required field '{FieldName}' (column '{ColumnName}') not found", 
                            mapping.FieldName, mapping.ColumnName);
                    }
                    continue;
                }

                var value = row[columnIndex].ToString();
                if (string.IsNullOrEmpty(value) && mapping.IsRequired)
                {
                    _logger?.LogWarning("Required field '{FieldName}' is empty", mapping.FieldName);
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

        private object? ConvertFieldValue(string? value, string type)
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
                _logger?.LogWarning(ex, "Failed to convert field value '{Value}' to type '{Type}'", value, type);
                return value; // Fall back to string representation
            }
        }

        private LogEntry CreateLogEntry(DateTime timestamp, string level, string message, 
            IReadOnlyDictionary<string, object>? fields = null)
        {
            return new LogEntry(
                timestamp, 
                level, 
                message, 
                _sourceFile, 
                0, // TemplateHash - could be computed based on message pattern
                fields ?? new Dictionary<string, object>()
            );
        }

        private ParsingFailure CreateFailure(string errorMessage, string? problematicText = null)
        {
            return new ParsingFailure(errorMessage, _sourceFile, problematicText);
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                if (_trackingStream != null)
                {
                    await _trackingStream.DisposeAsync();
                    _trackingStream = null;
                }

                _reader?.Dispose();
                _reader = null;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Error during SepCsvLogParser disposal");
            }
        }
    }
}