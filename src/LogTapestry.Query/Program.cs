// LogTapestry.Query/Program.cs

using DuckDB.NET.Data;

using Spectre.Console;
using Spectre.Console.Rendering;

using System.Diagnostics;
using System.Text;

namespace LogTapestry.Query
{
  class Program
  {
    static void Main(string[] args)
    {
      // --- Argument Parsing ---
      if (args.Length < 3 || args[0] != "--data") {
        Console.WriteLine("Usage: LogTapestry.Query.exe --data <path_to_data_dir> \"SQL_QUERY\"");
        Console.WriteLine("Example: ... --data C:\\data \"SELECT timestamp, user_id FROM logs WHERE user_id > 100\"");
        return;
      }

      var dataPath = args[1];
      var userQuery = args[2];

      var stopwatch = Stopwatch.StartNew();

      AnsiConsole.MarkupLine($"[grey]Original Query:[/] {userQuery}");
      AnsiConsole.MarkupLine($"[grey]Rewritten Query:[/] {new QueryRewriter().Rewrite(userQuery, $"read_parquet('{dataPath.Replace("\\", "/")}/output.parquet')")}");
      AnsiConsole.WriteLine();

      // --- Database Execution ---
      try {
        using var duckDBConnection = new DuckDBConnection("Data Source=:memory:");
        duckDBConnection.Open();
        using var command = duckDBConnection.CreateCommand();
        command.CommandText = new QueryRewriter().Rewrite(userQuery, $"read_parquet('{dataPath.Replace("\\", "/")}/output.parquet')");
        using var reader = command.ExecuteReader();

        // --- Display Results ---
        var table = new Table().Expand();
        table.Title = new TableTitle("Query Results");

        for (int i = 0; i < reader.FieldCount; i++) {
          table.AddColumn(new TableColumn(reader.GetName(i)).LeftAligned());
        }

        int rowCount = 0;
        while (reader.Read()) {
          var renderableRow = new IRenderable[reader.FieldCount];
          for (int i = 0; i < reader.FieldCount; i++) {
            var columnName = reader.GetName(i);
            var rawValue = reader.GetValue(i);
            string displayValue;

            // ============================================================================== //
            // ==                            THE FIX IS HERE                               == //
            // ============================================================================== //
            if (columnName.Equals("Fields", StringComparison.OrdinalIgnoreCase)) {
              // This value IS markup, so we create the Markup object directly.
              displayValue = FormatFieldsColumn(rawValue);
              renderableRow[i] = new Markup(displayValue);
            } else {
              // This value is raw data, so we MUST escape it before creating the Markup.
              displayValue = rawValue?.ToString() ?? "[grey]NULL[/]";
              renderableRow[i] = new Markup(Markup.Escape(displayValue));
            }
          }
          table.AddRow(renderableRow);
          rowCount++;
        }

        stopwatch.Stop();
        table.Caption = new TableTitle($"[green]{rowCount} row(s) returned in {stopwatch.ElapsedMilliseconds}ms[/]");
        AnsiConsole.Write(table);
      } catch (Exception ex) {
        stopwatch.Stop();
        AnsiConsole.MarkupLine($"[red]An error occurred after {stopwatch.ElapsedMilliseconds}ms:[/]");
        AnsiConsole.WriteException(ex);
      }
    }

    private static string FormatFieldsColumn(object? rawValue)
    {
      if (rawValue is not List<Dictionary<string, object>> fieldsList || fieldsList.Count == 0) {
        return "[grey]NULL[/]";
      }

      var builder = new StringBuilder();
      foreach (var element in fieldsList) {
        if (!element.TryGetValue("Key", out var keyObj) || keyObj == null) continue;
        string key = keyObj.ToString();

        object? value = null;
        if (element.TryGetValue("StringValue", out var val) && val != null) value = val;
        else if (element.TryGetValue("LongValue", out val) && val != null) value = val;
        else if (element.TryGetValue("DoubleValue", out val) && val != null) value = val;
        else if (element.TryGetValue("BoolValue", out val) && val != null) value = val;

        if (value != null) {
          string valueStr = value.ToString();
          if (value is string && valueStr.Contains(' ')) {
            // Important: Escape the content *before* wrapping it in markup tags
            valueStr = $"'{Markup.Escape(valueStr)}'";
          }

          builder.Append($"[blue]{Markup.Escape(key)}[/]=");
          builder.Append(value is string ? $"[green]{valueStr}[/]" : $"[cyan]{valueStr}[/]");
          builder.Append(' ');
        }
      }

      return builder.ToString().Trim();
    }
  }
}
