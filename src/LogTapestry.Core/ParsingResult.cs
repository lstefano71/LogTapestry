
namespace LogTapestry.Core
{
  /// <summary>
  /// Represents the outcome of a parsing attempt.
  /// </summary>
  public record ParsingResult(
      bool IsSuccess,
      LogEntry? Entry,
      string? ErrorMessage,
      string? UnparseableText,
      string Source
  )
  {
    public static ParsingResult Success(LogEntry entry) =>
        new(true, entry, null, null, entry.Source);

    public static ParsingResult Failure(string errorMessage, string unparseableText, string source) =>
        new(false, null, errorMessage, unparseableText, source);
  }

  /// <summary>
  /// The result of a single parsing iteration.
  /// </summary>
  public record ParseChunkResult(
      IReadOnlyList<LogEntry> SuccessfulEntries,
      IReadOnlyList<ParsingFailure> Failures
      // BytesConsumed is no longer needed here, as the stream position is the source of truth.
  );

  /// <summary>
  /// Represents a parsing failure with detailed error information.
  /// </summary>
  public record ParsingFailure(string ErrorMessage, string Source, string? ProblematicText = null);
}
