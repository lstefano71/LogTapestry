namespace LogTapestry.Core;

/// <summary>
/// Represents a single, structured log event after successful parsing.
/// This is the canonical object that flows to the data sink.
/// </summary>
public record LogEntry(DateTime Timestamp,
string Level, string Message, string Source,
long TemplateHash,
IReadOnlyDictionary<string, object> Fields,
byte[]? Ulid = null)
{
  public byte[] Ulid { get; init; } = Ulid ?? System.Ulid.NewUlid().ToByteArray();
}
