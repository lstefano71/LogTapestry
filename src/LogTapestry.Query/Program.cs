using DuckDB.NET.Data;

using LogTapestry.Core;

using Spectre.Console;
using Spectre.Console.Rendering;

using System.CommandLine;
using System.Diagnostics;
using System.Text;

namespace LogTapestry.Query;

class Program
{
  static int Main(string[] args)
  {
    var databasePathOption = new Option<string>("--database-path") { Arity = ArgumentArity.ExactlyOne };
    databasePathOption.Description = "Path to state.sqlite";
    var dataPathOption = new Option<string>("--data-path") { Arity = ArgumentArity.ExactlyOne };
    dataPathOption.Description = "Path to data directory";
    var outputOption = new Option<string>("--output") { Arity = ArgumentArity.ExactlyOne };
    outputOption.Description = "Output format: table, json, csv";
    var queryArgument = new Argument<string>("query") { Arity = ArgumentArity.ExactlyOne };
    queryArgument.Description = "SQL query to execute";

    var rootCommand = new RootCommand("LogTapestry Query Tool")
    {
      databasePathOption,
      dataPathOption,
      outputOption,
      queryArgument
    };

    rootCommand.SetAction(async parseResult => {
      var databasePath = parseResult.GetValue(databasePathOption);
      var dataPath = parseResult.GetValue(dataPathOption);
      var output = parseResult.GetValue(outputOption);
      var query = parseResult.GetValue(queryArgument);
      if (string.IsNullOrEmpty(output)) output = "table";

      var stopwatch = Stopwatch.StartNew();
      AnsiConsole.MarkupLine($"[grey]Original Query:[/] {query}");
      try {
        using var stateProvider = new SqliteStateProvider(databasePath);
        var queryRewriter = new QueryRewriter(stateProvider);
        var rewrittenQuery = await queryRewriter.RewriteQueryAsync(query.Replace("{data_path}", dataPath.Replace("\\", "/")));
        AnsiConsole.MarkupLine($"[grey]Rewritten Query:[/] {rewrittenQuery}");

        // Use DuckDB in-memory for Parquet queries
        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = rewrittenQuery;
        using var reader = cmd.ExecuteReader();

        if (output == "table") {
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
              renderableRow[i] = new Markup(rawValue?.ToString() ?? "");
            }
            table.AddRow(renderableRow);
            rowCount++;
          }
          stopwatch.Stop();
          AnsiConsole.Write(table);
          AnsiConsole.MarkupLine($"[green]{rowCount} row(s) returned in {stopwatch.ElapsedMilliseconds}ms[/]");
        } else if (output == "json") {
          var rows = new List<Dictionary<string, object?>>();
          int rowCount = 0;
          while (reader.Read()) {
            var row = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++) {
              row[reader.GetName(i)] = reader.GetValue(i);
            }
            rows.Add(row);
            rowCount++;
          }
          stopwatch.Stop();
          var json = System.Text.Json.JsonSerializer.Serialize(rows, new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
          AnsiConsole.WriteLine(json);
          AnsiConsole.MarkupLine($"[green]{rowCount} row(s) returned in {stopwatch.ElapsedMilliseconds}ms[/]");
        } else if (output == "csv") {
          var sb = new StringBuilder();
          for (int i = 0; i < reader.FieldCount; i++) {
            sb.Append(reader.GetName(i));
            if (i < reader.FieldCount - 1) sb.Append(',');
          }
          sb.AppendLine();
          int rowCount = 0;
          while (reader.Read()) {
            for (int i = 0; i < reader.FieldCount; i++) {
              var val = reader.GetValue(i)?.ToString()?.Replace("\"", "\"\"");
              sb.Append($"\"{val}\"");
              if (i < reader.FieldCount - 1) sb.Append(',');
            }
            sb.AppendLine();
            rowCount++;
          }
          stopwatch.Stop();
          AnsiConsole.WriteLine(sb.ToString());
          AnsiConsole.MarkupLine($"[green]{rowCount} row(s) returned in {stopwatch.ElapsedMilliseconds}ms[/]");
        } else {
          AnsiConsole.MarkupLine("[red]Unknown output format specified. Use 'table', 'json', or 'csv'.[/]");
        }
      } catch (Exception ex) {
        stopwatch.Stop();
        AnsiConsole.MarkupLine($"[red]An error occurred after {stopwatch.ElapsedMilliseconds}ms:[/]");
        AnsiConsole.WriteException(ex);
      }
    });

    return rootCommand.Parse(args).Invoke();
  }
}