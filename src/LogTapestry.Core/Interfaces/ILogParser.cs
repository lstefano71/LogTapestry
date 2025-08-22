namespace LogTapestry.Core
{
  /// <summary>
  /// A parser instance represents an ongoing parsing session for a single file.
  /// </summary>
  public interface ILogParser : IAsyncDisposable
  {
    /// <summary>
    /// Reads from the stream and parses the next available, complete set of log entries.
    /// The parser is responsible for handling its own internal buffering for partial entries.
    /// </summary>
    /// <param name="stream">The input stream, positioned where the last read left off.</param>
    /// <param name="token">A cancellation token.</param>
    /// <returns>A chunk of successfully parsed entries and any failures encountered.</returns>
    Task<ParseChunkResult> ParseNextChunkAsync(Stream stream, CancellationToken token);
  }

  /// <summary>
  /// Legacy interface for backward compatibility during transition.
  /// </summary>
  public interface ILegacyLogParser
  {
    IEnumerable<ParsingResult> Parse(IList<string> lines);
    ParsingResult? Flush();
  }
}