using DuckDB.NET.Data;

using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace LogTapestry.Compactor
{
  public class CompactionTask
  {
    private readonly string _dataPath;
    private readonly string _compactOlderThan;

    public CompactionTask(string dataPath, string compactOlderThan)
    {
      _dataPath = dataPath;
      _compactOlderThan = compactOlderThan;
    }

    public async Task RunAsync()
    {
      Console.WriteLine($"Compaction started for data path: {_dataPath}, compacting partitions older than: {_compactOlderThan}");

      var candidates = FindCompactionCandidates();
      foreach (var partitionPath in candidates) {
        Console.WriteLine($"Compaction candidate: {partitionPath}");
        await ExecuteCompaction(partitionPath);
      }

      await Task.CompletedTask;
    }

    public IEnumerable<string> FindCompactionCandidates()
    {
      var candidates = new List<string>();
      var threshold = ParseTimeSpan(_compactOlderThan);

      foreach (var dir in Directory.GetDirectories(_dataPath, "*", SearchOption.AllDirectories)) {
        var dirInfo = new DirectoryInfo(dir);

        // Check age
        if (DateTime.UtcNow - dirInfo.LastWriteTimeUtc < threshold)
          continue;

        // Must contain landing/
        var landingPath = Path.Combine(dir, "landing");
        if (!Directory.Exists(landingPath))
          continue;

        // Must NOT contain _COMPACTION_COMPLETE
        var markerPath = Path.Combine(dir, "_COMPACTION_COMPLETE");
        if (File.Exists(markerPath))
          continue;

        candidates.Add(dir);
      }

      return candidates;
    }

    public async Task ExecuteCompaction(string partitionPath)
    {
      var landingPath = Path.Combine(partitionPath, "landing");
      var markerPath = Path.Combine(partitionPath, "_COMPACTION_COMPLETE");
      const long targetChunkSize = 128 * 1024 * 1024; // 128MB

      try {
        using var duckDbConn = new DuckDBConnection("DataSource=:memory:");
        duckDbConn.Open();
        var cmd = duckDbConn.CreateCommand();
        var parquetFiles = Directory.GetFiles(landingPath, "*.parquet");
        foreach (var file in parquetFiles) {
          cmd.CommandText = $"CREATE TABLE tmp AS SELECT * FROM parquet_scan('{file}');";
          cmd.ExecuteNonQuery();
        }

        cmd.CommandText = "SELECT * FROM tmp;";
        using var reader = cmd.ExecuteReader();
        var schemaFields = new List<DataField>();
        for (int i = 0; i < reader.FieldCount; i++) {
          schemaFields.Add(CreateDataField(reader.GetName(i), reader.GetFieldType(i)));
        }
        var parquetSchema = new ParquetSchema(schemaFields);

        int fileIndex = 0;
        long currentSize = 0;
        var batchRows = new List<object[]>();
        const int batchSize = 10000;
        while (reader.Read()) {
          var row = new object[reader.FieldCount];
          reader.GetValues(row);
          batchRows.Add(row);
          currentSize += EstimateRowSize(row);
          if (batchRows.Count >= batchSize || currentSize >= targetChunkSize) {
            var chunkPath = Path.Combine(partitionPath, $"compacted.{fileIndex}.parquet");
            using (var fs = File.Create(chunkPath)) {
              var parquetWriter = await ParquetWriter.CreateAsync(parquetSchema, fs);
              await WriteRowGroupAsync(parquetWriter, parquetSchema, batchRows);
            }
            batchRows.Clear();
            currentSize = 0;
            fileIndex++;
          }
        }
        if (batchRows.Count > 0) {
          var chunkPath = Path.Combine(partitionPath, $"compacted.{fileIndex}.parquet");
          using var fs = File.Create(chunkPath);
          var parquetWriter = await ParquetWriter.CreateAsync(parquetSchema, fs);
          await WriteRowGroupAsync(parquetWriter, parquetSchema, batchRows);
        }

        // 4. Delete landing/ directory
        Directory.Delete(landingPath, true);

        // 5. Create _COMPACTION_COMPLETE marker file
        File.WriteAllText(markerPath, "done");
      } catch (Exception ex) {
        Console.Error.WriteLine($"Compaction error for {partitionPath}: {ex}");
      }

      await Task.CompletedTask;
    }

    private async Task WriteRowGroupAsync(ParquetWriter parquetWriter, ParquetSchema schema, List<object[]> rows)
    {
      using var groupWriter = parquetWriter.CreateRowGroup();
      foreach (var colIdx in Enumerable.Range(0, schema.DataFields.Length)) {
        var colData = rows.Select(r => r[colIdx]).ToArray();
        var column = new DataColumn(schema.DataFields[colIdx], colData);
        await groupWriter.WriteColumnAsync(column);
      }
    }
    private TimeSpan ParseTimeSpan(string input)
    {
      // Simple parser: "1h", "2d", "30m"
      if (input.EndsWith("h"))
        return TimeSpan.FromHours(double.Parse(input.TrimEnd('h')));
      if (input.EndsWith("d"))
        return TimeSpan.FromDays(double.Parse(input.TrimEnd('d')));
      if (input.EndsWith("m"))
        return TimeSpan.FromMinutes(double.Parse(input.TrimEnd('m')));
      throw new ArgumentException("Invalid timespan format. Use '1h', '2d', or '30m'.");
    }

    private DataField CreateDataField(string name, Type type)
    {
      // Map .NET types to Parquet.Data.DataField<T>
      if (type == typeof(string)) return new DataField<string>(name);
      if (type == typeof(long)) return new DataField<long>(name);
      if (type == typeof(int)) return new DataField<int>(name);
      if (type == typeof(double)) return new DataField<double>(name);
      if (type == typeof(float)) return new DataField<float>(name);
      if (type == typeof(bool)) return new DataField<bool>(name);
      if (type == typeof(DateTime)) return new DataField<DateTime>(name);
      // Fallback to string for unknown types
      return new DataField<string>(name);
    }

    private long EstimateRowSize(object[] row)
    {
      long size = 0;
      foreach (var val in row) {
        if (val is string s) size += s.Length;
        else if (val is long || val is int || val is double || val is float) size += 8;
        else if (val is bool) size += 1;
        else if (val is DateTime) size += 8;
        else if (val != null) size += 16;
      }
      return size;
    }
  }
}
