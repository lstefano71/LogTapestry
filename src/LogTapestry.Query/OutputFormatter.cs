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
    public void WriteHeader(IEnumerable<string> columnNames)
    {
      // TODO: Implement table header using Spectre.Console
    }

    public void WriteRow(object[] values)
    {
      // TODO: Implement table row using Spectre.Console
    }

    public void WriteFooter()
    {
      // TODO: Implement table footer using Spectre.Console
    }
  }

  public class JsonOutputFormatter : IOutputFormatter
  {
    public void WriteHeader(IEnumerable<string> columnNames)
    {
      // TODO: Implement JSON header
    }

    public void WriteRow(object[] values)
    {
      // TODO: Implement JSON row
    }

    public void WriteFooter()
    {
      // TODO: Implement JSON footer
    }
  }

  public class CsvOutputFormatter : IOutputFormatter
  {
    public void WriteHeader(IEnumerable<string> columnNames)
    {
      // TODO: Implement CSV header
    }

    public void WriteRow(object[] values)
    {
      // TODO: Implement CSV row
    }

    public void WriteFooter()
    {
      // TODO: Implement CSV footer
    }
  }
}
