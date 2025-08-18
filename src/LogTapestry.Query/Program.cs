using DuckDB.NET.Data;

using Spectre.Console;

namespace LogTapestry.Query
{
  class Program
  {
    static void Main(string[] args)
    {
      if (args.Length < 2 || args[0] != "--data") {
        Console.WriteLine("Usage: LogTapestry.Query.exe --data <path_to_data_dir> \"SQL_QUERY\"");
        return;
      }

      var dataPath = args[1];
      var query = args[2];

      // UNNEST-based query rewriting for PoC
      var parquetSource = $"read_parquet('{dataPath.Replace("\\", "/")}/output.parquet')";
      string rewrittenQuery;
      if (query.Contains("WHERE")) {
        // Example: SELECT * FROM logs WHERE user_id > 100
        var whereIndex = query.IndexOf("WHERE");
        var selectPart = query[..whereIndex];
        var wherePart = query[(whereIndex + "WHERE".Length)..].Trim();
        rewrittenQuery = $"{selectPart}FROM {parquetSource} AS t WHERE EXISTS (SELECT 1 FROM UNNEST(t.Fields) AS f WHERE f.Key = 'user_id' AND f.LongValue > 100)";
      } else {
        rewrittenQuery = query.Replace("FROM logs", $"FROM {parquetSource}");
      }

      using var duckDBConnection = new DuckDBConnection("Data Source=:memory:");
      duckDBConnection.Open();

      using var command = duckDBConnection.CreateCommand();
      command.CommandText = rewrittenQuery;
      using var reader = command.ExecuteReader();
      var table = new Table();

      for (int i = 0; i < reader.FieldCount; i++) {
        table.AddColumn(reader.GetName(i));
      }

      while (reader.Read()) {
        var row = new List<string>();
        for (int i = 0; i < reader.FieldCount; i++) {
          row.Add(reader.GetValue(i)?.ToString() ?? "NULL");
        }
        table.AddRow(row.ToArray());
      }

      AnsiConsole.Write(table);
    }
  }
}
