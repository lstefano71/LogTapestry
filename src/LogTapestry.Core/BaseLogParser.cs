using Microsoft.Extensions.Logging;
using System.Text;

namespace LogTapestry.Core
{
  /// <summary>
  /// Abstract base class for log parsers providing common functionality.
  /// </summary>
  public abstract class BaseLogParser : ILogParser
  {
    protected readonly PluginSettings Settings;
    protected readonly string SourceFile;
    protected readonly ILogger? Logger;
    protected readonly byte[] Buffer;
    protected int BufferLength;
    protected const int DefaultBufferSize = 8192;

    protected BaseLogParser(PluginSettings settings, string sourceFile, ILogger? logger = null)
    {
      Settings = settings;
      SourceFile = sourceFile;
      Logger = logger;
      Buffer = new byte[DefaultBufferSize];
      BufferLength = 0;
    }

    /// <summary>
    /// Reads data from the stream into the internal buffer.
    /// </summary>
    /// <param name="stream">The input stream.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>Number of bytes read.</returns>
    protected async Task<int> ReadIntoBufferAsync(Stream stream, CancellationToken token)
    {
      // If buffer is full, we need to process it or expand it
      if (BufferLength >= Buffer.Length)
      {
        throw new InvalidOperationException("Buffer overflow - need to process buffered data first");
      }

      var bytesRead = await stream.ReadAsync(Buffer.AsMemory(BufferLength), token);
      BufferLength += bytesRead;
      return bytesRead;
    }

    /// <summary>
    /// Converts a portion of the buffer to a string.
    /// </summary>
    /// <param name="start">Start position in buffer.</param>
    /// <param name="length">Number of bytes to convert.</param>
    /// <returns>The converted string.</returns>
    protected string BufferToString(int start, int length)
    {
      return Encoding.UTF8.GetString(Buffer, start, length);
    }

    /// <summary>
    /// Removes processed data from the beginning of the buffer.
    /// </summary>
    /// <param name="bytesToRemove">Number of bytes to remove from the beginning.</param>
    protected void ConsumeBuffer(int bytesToRemove)
    {
      if (bytesToRemove > BufferLength)
      {
        throw new ArgumentException("Cannot consume more bytes than available in buffer");
      }

      if (bytesToRemove == BufferLength)
      {
        // Consumed everything
        BufferLength = 0;
      }
      else
      {
        // Shift remaining data to the beginning
        Array.Copy(Buffer, bytesToRemove, Buffer, 0, BufferLength - bytesToRemove);
        BufferLength -= bytesToRemove;
      }
    }

    /// <summary>
    /// Creates a LogEntry with default values and common fields.
    /// </summary>
    /// <param name="timestamp">The parsed timestamp.</param>
    /// <param name="level">The log level.</param>
    /// <param name="message">The log message.</param>
    /// <param name="fields">Additional fields.</param>
    /// <returns>A new LogEntry instance.</returns>
    protected LogEntry CreateLogEntry(DateTime timestamp, string level, string message, 
      IReadOnlyDictionary<string, object>? fields = null)
    {
      return new LogEntry(
        timestamp, 
        level, 
        message, 
        SourceFile, 
        0, // TemplateHash - could be computed based on message pattern
        fields ?? new Dictionary<string, object>()
      );
    }

    /// <summary>
    /// Creates a parsing failure record.
    /// </summary>
    /// <param name="errorMessage">The error message.</param>
    /// <param name="problematicText">The text that caused the failure.</param>
    /// <returns>A ParsingFailure instance.</returns>
    protected ParsingFailure CreateFailure(string errorMessage, string? problematicText = null)
    {
      return new ParsingFailure(errorMessage, SourceFile, problematicText);
    }

    /// <summary>
    /// Abstract method that derived classes must implement for actual parsing logic.
    /// </summary>
    /// <param name="stream">The input stream.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>Parse chunk result.</returns>
    public abstract Task<ParseChunkResult> ParseNextChunkAsync(Stream stream, CancellationToken token);

    /// <summary>
    /// Default dispose implementation.
    /// </summary>
    public virtual ValueTask DisposeAsync()
    {
      return ValueTask.CompletedTask;
    }
  }
}