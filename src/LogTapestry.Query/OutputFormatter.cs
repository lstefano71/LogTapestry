using Spectre.Console;

namespace LogTapestry.Query
{
  public interface IOutputFormatter
  {
    void WriteHeader(IEnumerable<string> columnNames);
    void WriteRow(object[] values);
    void WriteFooter();
  }

  public class TableOutputFormatter : IOutputFormatter
  {
    private Table _table;

    public void WriteHeader(IEnumerable<string> columnNames)
    {
      _table = new Table();
      foreach (var col in columnNames)
        _table.AddColumn(col);
    }

    public void WriteRow(object[] values)
    {
      _table.AddRow(values.Select(v => v?.ToString() ?? "").ToArray());
    }

    public void WriteFooter()
    {
      AnsiConsole.Write(_table);
    }
  }

  public class JsonOutputFormatter : IOutputFormatter
  {
    private List<Dictionary<string, object>> _jsonRows;
    private string[] _jsonColumns;

    public void WriteHeader(IEnumerable<string> columnNames)
    {
      _jsonRows = new List<Dictionary<string, object>>();
      _jsonColumns = columnNames.ToArray();
    }

    public void WriteRow(object[] values)
    {
      var row = new Dictionary<string, object>();
      for (int i = 0; i < _jsonColumns.Length; i++)
        row[_jsonColumns[i]] = values[i];
      _jsonRows.Add(row);
    }

    public void WriteFooter()
    {
      var json = System.Text.Json.JsonSerializer.Serialize(_jsonRows, new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
      Console.Out.WriteLine(json);
    }
  }

  public class CsvOutputFormatter : IOutputFormatter
  {
    private string[] _csvColumns;

    public void WriteHeader(IEnumerable<string> columnNames)
    {
      _csvColumns = columnNames.ToArray();
      Console.Out.WriteLine(string.Join(",", _csvColumns));
    }

    public void WriteRow(object[] values)
    {
      var row = values.Select(v => v?.ToString()?.Replace("\"", "\"\"") ?? "").Select(v => $"\"{v}\"");
      Console.Out.WriteLine(string.Join(",", row));
    }

    public void WriteFooter()
    {
      // No footer needed for CSV
    }
  }
}
