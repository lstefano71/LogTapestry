namespace LogTapestry.Core
{
  public interface ILogParser
  {
    IEnumerable<ParsingResult> Parse(IList<string> lines);
    ParsingResult? Flush();
  }
}
