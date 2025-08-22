using LogTapestry.Core;
using LogTapestry.Core.Interfaces;

using Microsoft.Extensions.Logging;

using Parquet;
using Parquet.Data;
using Parquet.Schema;

using System.Diagnostics;

namespace LogTapestry.Ingester.Sinks;

/// <summary>
/// Data sink that writes log entries to Parquet files with the same schema,
/// partitioning strategy, and file operations as the original CheckpointDataSink.
/// 
/// This sink extracts and preserves all existing Parquet functionality:
/// - Same Parquet schema with nested fields support
/// - Time-based partitioning (year/month/day/landing)
/// - Atomic file operations (write to temp, then move)
/// - Field schema tracking in SQLite
/// - Parquet file indexing for query optimization
/// </summary>
public class ParquetSink : IMultiDataSink
{
    public string Name => "parquet";

    private readonly ILogger<ParquetSink> _logger;
    private readonly IStateProvider _stateProvider;
    private readonly ParquetSinkConfiguration _config;

    // Parquet schema - identical to original CheckpointDataSink
    private static readonly ParquetSchema Schema = new(
        new DataField<DateTime>("Timestamp"),
        new DataField<string>("Level"),
        new DataField<string>("Message"),
        new DataField<string>("Source"),
        new DataField<long>("TemplateHash"),
        new DataField<byte[]>("Ulid"), // 16-byte ULID
        new ListField("Fields",
            new StructField("FieldElement",
                new DataField<string>("Key"),
                new DataField<string>("StringValue", true),
                new DataField<long?>("LongValue", true),
                new DataField<double?>("DoubleValue", true),
                new DataField<bool?>("BoolValue", true)
            )
        )
    );

    public ParquetSink(ILogger<ParquetSink> logger, IStateProvider stateProvider, ParquetSinkConfiguration? config = null)
    {
        _logger = logger;
        _stateProvider = stateProvider;
        _config = config ?? new ParquetSinkConfiguration();
    }

    public async Task<SinkResult> WriteBatchAsync(IList<DataBlock> batch, SinkContext context, CancellationToken cancellationToken = default)
    {
        if (batch.Count == 0) {
            return SinkResult.CreateSuccess();
        }

        var stopwatch = Stopwatch.StartNew();
        long totalEntriesProcessed = 0;
        long totalBytesProcessed = 0;

        try {
            _logger.LogDebug("ParquetSink processing batch {BatchId} with {Count} data blocks", context.BatchId, batch.Count);

            // Group entries by partition - same logic as original CheckpointDataSink
            var partitionGroups = batch
                .SelectMany(db => db.Entries.Select(e => (Entry: e, DataBlock: db)))
                .GroupBy(x => GetPartitionKey(x.Entry))
                .ToDictionary(g => g.Key, g => g.ToList());

            foreach (var (partitionPath, entries) in partitionGroups) {
                var logEntries = entries.Select(x => x.Entry).ToArray();
                
                _logger.LogTrace("Writing {EntryCount} entries to partition {PartitionPath}", logEntries.Length, partitionPath);

                var bytesWritten = await WriteToParquetAsync(logEntries, partitionPath);
                
                totalEntriesProcessed += logEntries.Length;
                totalBytesProcessed += bytesWritten;
            }

            stopwatch.Stop();

            _logger.LogDebug("ParquetSink completed batch {BatchId}: {Entries} entries, {Bytes} bytes in {Duration}ms", 
                context.BatchId, totalEntriesProcessed, totalBytesProcessed, stopwatch.ElapsedMilliseconds);

            return SinkResult.CreateSuccess(
                entriesProcessed: totalEntriesProcessed,
                bytesProcessed: totalBytesProcessed,
                processingTime: stopwatch.Elapsed,
                metrics: new Dictionary<string, object> {
                    ["partitions_written"] = partitionGroups.Count,
                    ["files_created"] = partitionGroups.Count
                });

        } catch (Exception ex) {
            stopwatch.Stop();
            _logger.LogError(ex, "ParquetSink failed to process batch {BatchId} after {Duration}ms", context.BatchId, stopwatch.ElapsedMilliseconds);
            
            return SinkResult.CreateFailure(
                errorMessage: $"Parquet write failed: {ex.Message}",
                metrics: new Dictionary<string, object> {
                    ["processing_time_ms"] = stopwatch.ElapsedMilliseconds,
                    ["entries_processed"] = totalEntriesProcessed,
                    ["bytes_processed"] = totalBytesProcessed
                });
        }
    }

    public async Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default)
    {
        try {
            // Check if data root directory exists and is writable
            var dataRoot = _config.DataRoot;
            if (!Directory.Exists(dataRoot)) {
                Directory.CreateDirectory(dataRoot);
            }

            // Test write access
            var testFile = Path.Combine(dataRoot, $"health_check_{Guid.NewGuid()}.tmp");
            await File.WriteAllTextAsync(testFile, "health_check", cancellationToken);
            File.Delete(testFile);

            _logger.LogTrace("ParquetSink health check passed");
            return true;
        } catch (Exception ex) {
            _logger.LogError(ex, "ParquetSink health check failed");
            return false;
        }
    }

    private async Task<long> WriteToParquetAsync(LogEntry[] batch, string partitionPath)
    {
        Directory.CreateDirectory(partitionPath);
        var tempFilePath = Path.Combine(partitionPath, $"part-{Guid.NewGuid()}.parquet_tmp");
        var finalFilePath = Path.ChangeExtension(tempFilePath, ".parquet");

        long bytesWritten = 0;
        try {
            using (var stream = File.Create(tempFilePath)) {
                await WriteParquetDataAsync(batch, stream);
                bytesWritten = stream.Length;
            }

            // Stream is now closed, safe to move the file
            File.Move(tempFilePath, finalFilePath);

            // Populate field schema and index the file - preserving existing functionality
            await PopulateFieldSchemaAsync(batch);
            await IndexParquetFileAsync(batch, finalFilePath, partitionPath, bytesWritten);

            return bytesWritten;
        } catch {
            // Clean up temp file on error
            if (File.Exists(tempFilePath)) {
                File.Delete(tempFilePath);
            }
            throw;
        }
    }

    private async Task WriteParquetDataAsync(LogEntry[] batch, Stream targetStream)
    {
        if (batch.Length > 0) {
            _logger.LogTrace("Writing batch of {length} log entries. First entry: {entry}", batch.Length, batch[0]);
        }

        using var parquetWriter = await ParquetWriter.CreateAsync(Schema, targetStream);
        using var groupWriter = parquetWriter.CreateRowGroup();

        // Write top-level columns - exact same logic as original
        await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[0], batch.Select(e => e.Timestamp).ToArray()));
        await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[1], batch.Select(e => e.Level).ToArray()));
        await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[2], batch.Select(e => e.Message).ToArray()));
        await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[3], batch.Select(e => e.Source).ToArray()));
        await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[4], batch.Select(e => e.TemplateHash).ToArray()));
        await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[5], batch.Select(e => e.Ulid).ToArray()));

        // Process nested fields using the same shredder logic
        var listField = (ListField)Schema.Fields[6];
        var shredder = new NestedListShredder(listField);
        shredder.Shred(batch);

        foreach (var dataColumn in shredder.GetDataColumns()) {
            await groupWriter.WriteColumnAsync(dataColumn);
        }
    }

    private string GetPartitionKey(LogEntry entry)
    {
        var timestamp = entry.Timestamp.ToUniversalTime();
        return Path.Combine(
            _config.DataRoot,
            $"year={timestamp.Year}",
            $"month={timestamp.Month:D2}",
            $"day={timestamp.Day:D2}",
            "landing"
        );
    }

    private async Task PopulateFieldSchemaAsync(LogEntry[] batch)
    {
        if (_stateProvider is SqliteStateProvider sqliteProvider) {
            var conn = sqliteProvider.GetConnection();
            var allFields = batch.SelectMany(e => e.Fields).GroupBy(f => f.Key).Select(g => g.First());
            
            foreach (var field in allFields) {
                var fieldName = field.Key;
                var value = field.Value;
                string fieldType = value switch {
                    long => "long",
                    double => "double",
                    bool => "bool",
                    _ => "string"
                };
                
                var cmdCheck = conn.CreateCommand();
                cmdCheck.CommandText = "SELECT COUNT(*) FROM FieldSchema WHERE FieldName = @fieldName";
                cmdCheck.Parameters.AddWithValue("@fieldName", fieldName);
                var exists = (long)await cmdCheck.ExecuteScalarAsync() > 0;
                
                if (!exists) {
                    var cmdInsert = conn.CreateCommand();
                    cmdInsert.CommandText = "INSERT INTO FieldSchema (FieldName, FieldType) VALUES (@fieldName, @fieldType)";
                    cmdInsert.Parameters.AddWithValue("@fieldName", fieldName);
                    cmdInsert.Parameters.AddWithValue("@fieldType", fieldType);
                    await cmdInsert.ExecuteNonQueryAsync();
                }
            }
        }
    }

    private async Task IndexParquetFileAsync(LogEntry[] batch, string parquetFilePath, string partitionPath, long sizeBytes)
    {
        if (_stateProvider is SqliteStateProvider sqliteProvider) {
            var cmd = sqliteProvider.GetConnection().CreateCommand();
            cmd.CommandText = @"INSERT INTO ParquetFiles (FilePath, Partition, RowCount, SizeBytes, CreatedUtc) VALUES (@filePath, @partition, @rowCount, @sizeBytes, @createdUtc);";
            cmd.Parameters.AddWithValue("@filePath", parquetFilePath);
            cmd.Parameters.AddWithValue("@partition", partitionPath);
            cmd.Parameters.AddWithValue("@rowCount", batch.Length);
            cmd.Parameters.AddWithValue("@sizeBytes", sizeBytes);
            cmd.Parameters.AddWithValue("@createdUtc", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            await cmd.ExecuteNonQueryAsync();
        }
    }

    #region Nested List Shredding (Identical to original)

    private class NestedListShredder
    {
        // Column Builders
        private readonly ReferenceTypeColumnBuilder<string> _keyBuilder;
        private readonly ReferenceTypeColumnBuilder<string> _stringBuilder;
        private readonly ValueTypeColumnBuilder<long> _longBuilder;
        private readonly ValueTypeColumnBuilder<double> _doubleBuilder;
        private readonly ValueTypeColumnBuilder<bool> _boolBuilder;
        private readonly List<ColumnBuilder> _allBuilders;

        // Shared State
        private readonly List<int> _repetitionLevels = [];

        public NestedListShredder(ListField listField)
        {
            var structField = (StructField)listField.Item;
            _keyBuilder = new ReferenceTypeColumnBuilder<string>((DataField)structField.Fields[0]);
            _stringBuilder = new ReferenceTypeColumnBuilder<string>((DataField)structField.Fields[1]);
            _longBuilder = new ValueTypeColumnBuilder<long>((DataField)structField.Fields[2]);
            _doubleBuilder = new ValueTypeColumnBuilder<double>((DataField)structField.Fields[3]);
            _boolBuilder = new ValueTypeColumnBuilder<bool>((DataField)structField.Fields[4]);

            _allBuilders = [_keyBuilder, _stringBuilder, _longBuilder, _doubleBuilder, _boolBuilder];
        }

        public void Shred(IEnumerable<LogEntry> batch)
        {
            var fieldsList = batch.Select(ShredEntry).ToList();

            foreach (var list in fieldsList) {
                if (list.Count == 0) {
                    _repetitionLevels.Add(0);
                    _allBuilders.ForEach(b => b.AddEmptyListEntry());
                } else {
                    for (int j = 0; j < list.Count; j++) {
                        var element = list[j];
                        _repetitionLevels.Add(j == 0 ? 0 : 1);

                        _keyBuilder.Add(element.Key);
                        _stringBuilder.Add(element.Value.StringValue);
                        _longBuilder.Add(element.Value.LongValue);
                        _doubleBuilder.Add(element.Value.DoubleValue);
                        _boolBuilder.Add(element.Value.BoolValue);
                    }
                }
            }
        }

        public IEnumerable<DataColumn> GetDataColumns()
        {
            var repLevelsArray = _repetitionLevels.ToArray();
            return _allBuilders.Select(b => b.ToDataColumn(repLevelsArray));
        }

        private List<FieldElement> ShredEntry(LogEntry entry)
        {
            return [.. entry.Fields.Select(kv => kv.Value switch
                {
                    long l => new FieldElement(kv.Key, new FieldValue(LongValue: l)),
                    double d => new FieldElement(kv.Key, new FieldValue(DoubleValue: d)),
                    bool b => new FieldElement(kv.Key, new FieldValue(BoolValue: b)),
                    _ => new FieldElement(kv.Key, new FieldValue(StringValue: kv.Value?.ToString()))
                })];
        }
    }

    private abstract class ColumnBuilder(DataField field)
    {
        protected const int MaxDefLevel = 4;
        protected const int StructExistsDefLevel = 3;
        protected const int ListExistsDefLevel = 1;

        protected readonly DataField Field = field;
        protected readonly List<int> DefLevels = [];

        public void AddEmptyListEntry() => DefLevels.Add(ListExistsDefLevel);

        public abstract DataColumn ToDataColumn(int[] repetitionLevels);
    }

    private class ValueTypeColumnBuilder<T>(DataField field) : ColumnBuilder(field) where T : struct
    {
        private readonly List<T> _values = [];

        public void Add(T? value)
        {
            if (value.HasValue) {
                DefLevels.Add(MaxDefLevel);
                _values.Add(value.Value);
            } else {
                DefLevels.Add(StructExistsDefLevel);
            }
        }

        public override DataColumn ToDataColumn(int[] repetitionLevels) =>
            new(Field, _values.ToArray(), [.. DefLevels], repetitionLevels);
    }

    private class ReferenceTypeColumnBuilder<T>(DataField field) : ColumnBuilder(field) where T : class
    {
        private readonly List<T> _values = [];

        public void Add(T? value)
        {
            if (value != null) {
                DefLevels.Add(MaxDefLevel);
                _values.Add(value);
            } else {
                DefLevels.Add(StructExistsDefLevel);
            }
        }

        public override DataColumn ToDataColumn(int[] repetitionLevels) =>
            new(Field, _values.ToArray(), [.. DefLevels], repetitionLevels);
    }

    #endregion
}

/// <summary>
/// Configuration for the Parquet sink.
/// </summary>
public class ParquetSinkConfiguration
{
    /// <summary>
    /// Root directory for Parquet data files.
    /// </summary>
    public string DataRoot { get; set; } = "data";

    /// <summary>
    /// Compression level for Parquet files.
    /// </summary>
    public string CompressionLevel { get; set; } = "Snappy";

    /// <summary>
    /// Maximum file size before creating a new file (future enhancement).
    /// </summary>
    public long MaxFileSizeBytes { get; set; } = 128 * 1024 * 1024; // 128MB
}

// Supporting types from original CheckpointDataSink
public record FieldValue(string? StringValue = null, long? LongValue = null, double? DoubleValue = null, bool? BoolValue = null);
public record FieldElement(string Key, FieldValue Value);