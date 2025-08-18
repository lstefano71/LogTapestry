namespace LogTapestry.Core
{
  public interface ILogParser
  {
    IEnumerable<ParsingResult> Parse(string[] lines);
    ParsingResult? Flush();
  }
}
