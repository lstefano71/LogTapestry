
namespace LogTapestry.Core
{
    /// <summary>
    /// Represents the outcome of a parsing attempt.
    /// </summary>
    public record ParsingResult
    {
        public bool IsSuccess { get; init; }
        public LogEntry? Entry { get; init; }
        public string? ErrorMessage { get; init; }
        public string? UnparseableText { get; init; }
        public string Source { get; init; } = "";

        public static ParsingResult Success(LogEntry entry)
        {
            return new ParsingResult { IsSuccess = true, Entry = entry, Source = entry.Source };
        }

        public static ParsingResult Failure(string errorMessage, string unparseableText, string source)
        {
            return new ParsingResult { IsSuccess = false, ErrorMessage = errorMessage, UnparseableText = unparseableText, Source = source };
        }
    }
}
