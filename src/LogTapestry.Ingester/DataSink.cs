using LogTapestry.Core;

using Microsoft.Extensions.Logging;

using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace LogTapestry.Ingester;

public class DataSink
{
  // Metrics instrumentation
  public static class Metrics
  {
    public static long LogEntriesIngested = 0;
    public static long BytesProcessed = 0;
  }

  // Helper records and Schema remain the same...
  public record FieldValue(string? StringValue = null, long? LongValue = null, double? DoubleValue = null, bool? BoolValue = null);
  public record FieldElement(string Key, FieldValue Value);

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

  private readonly ILogger<DataSink> _logger;
  private readonly IStateProvider? _stateProvider;

  public DataSink(ILogger<DataSink> logger, IStateProvider? stateProvider = null)
  {
    _logger = logger;
    _stateProvider = stateProvider;
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

  private async Task IndexParquetFileAsync(LogEntry[] batch, Stream targetStream, string? parquetFilePath, string? partitionPath)
  {
    if (_stateProvider != null && parquetFilePath != null && partitionPath != null && targetStream.CanSeek) {
      var cmd = ((SqliteStateProvider)_stateProvider).GetConnection().CreateCommand();
      cmd.CommandText = @"INSERT INTO ParquetFiles (FilePath, Partition, RowCount, SizeBytes, CreatedUtc) VALUES (@filePath, @partition, @rowCount, @sizeBytes, @createdUtc);";
      cmd.Parameters.AddWithValue("@filePath", parquetFilePath);
      cmd.Parameters.AddWithValue("@partition", partitionPath);
      cmd.Parameters.AddWithValue("@rowCount", batch.Length);
      cmd.Parameters.AddWithValue("@sizeBytes", targetStream.Length);
      cmd.Parameters.AddWithValue("@createdUtc", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
      await cmd.ExecuteNonQueryAsync();
    }
  }

  public async Task WriteBatchAsync(LogEntry[] batch,
    Stream targetStream,
    string? parquetFilePath = null, string? partitionPath = null)
  {
    if (batch.Length > 0)
      _logger.LogDebug("Writing batch of {length} log entries. First entry: {entry}",
        batch.Length, batch[0]);

    using var parquetWriter = await ParquetWriter.CreateAsync(Schema, targetStream);
    using var groupWriter = parquetWriter.CreateRowGroup();

    // Write top-level columns
    await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[0], batch.Select(e => e.Timestamp).ToArray()));
    await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[1], batch.Select(e => e.Level).ToArray()));
    await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[2], batch.Select(e => e.Message).ToArray()));
    await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[3], batch.Select(e => e.Source).ToArray()));
    await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[4], batch.Select(e => e.TemplateHash).ToArray()));
    await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[5], batch.Select(e => e.Ulid).ToArray()));

    var listField = (ListField)Schema.Fields[6];
    var shredder = new NestedListShredder(listField);
    shredder.Shred(batch);

    foreach (var dataColumn in shredder.GetDataColumns()) {
      await groupWriter.WriteColumnAsync(dataColumn);
    }

    // Metrics instrumentation
    Metrics.LogEntriesIngested += batch.Length;
    if (targetStream.CanSeek) {
      Metrics.BytesProcessed += targetStream.Length;
    }

    // --- FieldSchema population logic ---
    await PopulateFieldSchemaAsync(batch);

    // --- Parquet file indexing logic ---
    await IndexParquetFileAsync(batch, targetStream, parquetFilePath, partitionPath);
  }

  private class NestedListShredder
  {
    // --- Column Builders ---
    // We now instantiate the specific builder types
    private readonly ReferenceTypeColumnBuilder<string> _keyBuilder;
    private readonly ReferenceTypeColumnBuilder<string> _stringBuilder;
    private readonly ValueTypeColumnBuilder<long> _longBuilder;
    private readonly ValueTypeColumnBuilder<double> _doubleBuilder;
    private readonly ValueTypeColumnBuilder<bool> _boolBuilder;
    private readonly List<ColumnBuilder> _allBuilders;

    // --- Shared State ---
    private readonly List<int> _repetitionLevels = [];

    public NestedListShredder(ListField listField)
    {
      var structField = (StructField)listField.Item;

      // Use the new, specialized classes
      _keyBuilder = new ReferenceTypeColumnBuilder<string>((DataField)structField.Fields[0]);
      _stringBuilder = new ReferenceTypeColumnBuilder<string>((DataField)structField.Fields[1]);
      _longBuilder = new ValueTypeColumnBuilder<long>((DataField)structField.Fields[2]);
      _doubleBuilder = new ValueTypeColumnBuilder<double>((DataField)structField.Fields[3]);
      _boolBuilder = new ValueTypeColumnBuilder<bool>((DataField)structField.Fields[4]);

      _allBuilders = [_keyBuilder, _stringBuilder, _longBuilder, _doubleBuilder, _boolBuilder];
    }

    public void Shred(IEnumerable<LogEntry> batch)
    {
      // This logic remains the same, but the .Add calls are now unambiguous
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

  // ====================================================================
  // STEP 3: The New Class Hierarchy for Column Builders
  // ====================================================================

  private abstract class ColumnBuilder
  {
    protected const int MaxDefLevel = 4;
    protected const int StructExistsDefLevel = 3;
    protected const int ListExistsDefLevel = 1;

    protected readonly DataField Field;
    protected readonly List<int> DefLevels = [];

    protected ColumnBuilder(DataField field) { Field = field; }

    public void AddEmptyListEntry() => DefLevels.Add(ListExistsDefLevel);

    public abstract DataColumn ToDataColumn(int[] repetitionLevels);
  }

  private class ValueTypeColumnBuilder<T> : ColumnBuilder where T : struct
  {
    private readonly List<T> _values = [];
    public ValueTypeColumnBuilder(DataField field) : base(field) { }

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

  private class ReferenceTypeColumnBuilder<T> : ColumnBuilder where T : class
  {
    private readonly List<T> _values = [];
    public ReferenceTypeColumnBuilder(DataField field) : base(field) { }

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
}
