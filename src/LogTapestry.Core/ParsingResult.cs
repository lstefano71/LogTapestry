
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
}
