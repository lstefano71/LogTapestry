// LogTapestry.Query/Program.cs

using DuckDB.NET.Data;

using Spectre.Console;
using Spectre.Console.Rendering;

using System.CommandLine;
using System.Diagnostics;
using System.Text;

namespace LogTapestry.Query
{
  class Program
  {
    static async Task<int> Main(string[] args)
    {
      var rootCommand = new System.CommandLine.RootCommand("LogTapestry Query Tool");

      var databasePathOption = new Option<string>("--database-path") { Arity = ArgumentArity.ExactlyOne };
      var dataPathOption = new Option<string>("--data-path") { Arity = ArgumentArity.ExactlyOne };
      var outputOption = new Option<string>("--output", "table") { Arity = ArgumentArity.ExactlyOne };
      var queryArgument = new Argument<string>("query") { Arity = ArgumentArity.ExactlyOne };

      rootCommand.Add(databasePathOption);
      rootCommand.Add(dataPathOption);
      rootCommand.Add(outputOption);
      rootCommand.Add(queryArgument);

      rootCommand.Handler = async context => {
        var databasePath = context.ParseResult.GetValueForOption(databasePathOption);
        var dataPath = context.ParseResult.GetValueForOption(dataPathOption);
        var output = context.ParseResult.GetValueForOption(outputOption);
        var query = context.ParseResult.GetValueForArgument(queryArgument);

        var stopwatch = Stopwatch.StartNew();

        AnsiConsole.MarkupLine($"[grey]Original Query:[/] {query}");

        // TODO: Instantiate SqliteStateProvider and QueryRewriter
        // var stateProvider = new SqliteStateProvider(databasePath);
        // var queryRewriter = new QueryRewriter(stateProvider);

        // For now, use placeholder QueryRewriter
        var rewrittenQuery = new QueryRewriter().Rewrite(query, $"read_parquet('{dataPath.Replace("\\", "/")}/output.parquet')");
        AnsiConsole.MarkupLine($"[grey]Rewritten Query:[/] {rewrittenQuery}");
        AnsiConsole.WriteLine();

        try {
          using var duckDBConnection = new DuckDBConnection("Data Source=:memory:");
          duckDBConnection.Open();
          using var command = duckDBConnection.CreateCommand();
          command.CommandText = rewrittenQuery;
          using var reader = command.ExecuteReader();

          // TODO: Select output formatter based on 'output'
          // For now, always use table output
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

              if (columnName.Equals("Fields", StringComparison.OrdinalIgnoreCase)) {
                displayValue = FormatFieldsColumn(rawValue);
                renderableRow[i] = new Markup(displayValue);
              } else {
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
        await Task.CompletedTask;
      };

      return await rootCommand.InvokeAsync(args);
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
