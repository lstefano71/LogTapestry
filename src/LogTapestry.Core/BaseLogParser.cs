using Microsoft.Extensions.Logging;
using System.Buffers;
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
    protected const int DefaultBufferSize = 16384; // Increased for better I/O efficiency
    protected const int MinimumWorkingBuffer = 1024; // Minimum space before triggering shift
    
    // Reusable StringBuilder with initial capacity to reduce allocations
    protected readonly StringBuilder WorkingStringBuilder;
    
    // UTF-8 decoder for efficient string conversion
    private readonly Decoder _utf8Decoder;
    
    // Character buffer for efficient string building
    private readonly char[] _charBuffer;
    private const int CharBufferSize = 4096;

    protected BaseLogParser(PluginSettings settings, string sourceFile, ILogger? logger = null)
    {
      Settings = settings;
      SourceFile = sourceFile;
      Logger = logger;
      Buffer = new byte[DefaultBufferSize];
      BufferLength = 0;
      WorkingStringBuilder = new StringBuilder(1024); // Pre-allocate capacity
      _utf8Decoder = Encoding.UTF8.GetDecoder();
      _charBuffer = new char[CharBufferSize];
    }

    /// <summary>
    /// Reads data from the stream into the internal buffer with smart space management.
    /// </summary>
    /// <param name="stream">The input stream.</param>
    /// <param name="token">Cancellation token.</param>
    /// <returns>Number of bytes read.</returns>
    protected async Task<int> ReadIntoBufferAsync(Stream stream, CancellationToken token)
    {
      // If we don't have enough working space, compact the buffer first
      if (Buffer.Length - BufferLength < MinimumWorkingBuffer)
      {
        // Option 1: If buffer is mostly consumed, just shift remaining data
        if (BufferLength < Buffer.Length / 4)
        {
          // Small amount of data, worth shifting
          if (BufferLength > 0)
          {
            Buffer.AsSpan(0, BufferLength).CopyTo(Buffer.AsSpan());
          }
        }
        else
        {
          // Buffer is too full, caller needs to consume more data first
          throw new InvalidOperationException("Buffer overflow - need to process buffered data first");
        }
      }

      var bytesRead = await stream.ReadAsync(Buffer.AsMemory(BufferLength), token);
      BufferLength += bytesRead;
      return bytesRead;
    }

    /// <summary>
    /// Gets a span view of the current buffer data.
    /// </summary>
    /// <returns>ReadOnlySpan of the buffered data.</returns>
    protected ReadOnlySpan<byte> BufferSpan => Buffer.AsSpan(0, BufferLength);

    /// <summary>
    /// Converts a portion of the buffer to a string using efficient decoding.
    /// </summary>
    /// <param name="start">Start position in buffer.</param>
    /// <param name="length">Number of bytes to convert.</param>
    /// <returns>The converted string.</returns>
    protected string BufferToString(int start, int length)
    {
      if (length == 0) return string.Empty;
      
      // Use Span for bounds checking and efficiency
      var span = Buffer.AsSpan(start, length);
      
      // For small strings, use direct conversion
      if (length <= 256)
      {
        return Encoding.UTF8.GetString(span);
      }
      
      // For larger strings, use the decoder with char buffer for efficiency
      WorkingStringBuilder.Clear();
      var decoder = _utf8Decoder;
      decoder.Reset();
      
      int bytesUsed = 0;
      while (bytesUsed < length)
      {
        var remainingBytes = length - bytesUsed;
        var bytesToProcess = Math.Min(remainingBytes, 1024); // Process in chunks
        
        var charsUsed = decoder.GetChars(span.Slice(bytesUsed, bytesToProcess), _charBuffer, false);
        WorkingStringBuilder.Append(_charBuffer, 0, charsUsed);
        bytesUsed += bytesToProcess;
      }
      
      return WorkingStringBuilder.ToString();
    }

    /// <summary>
    /// Efficiently appends buffer data to a StringBuilder.
    /// </summary>
    /// <param name="sb">The StringBuilder to append to.</param>
    /// <param name="start">Start position in buffer.</param>
    /// <param name="length">Number of bytes to convert and append.</param>
    protected void AppendBufferToStringBuilder(StringBuilder sb, int start, int length)
    {
      if (length == 0) return;
      
      var span = Buffer.AsSpan(start, length);
      var decoder = _utf8Decoder;
      decoder.Reset();
      
      int bytesUsed = 0;
      while (bytesUsed < length)
      {
        var remainingBytes = length - bytesUsed;
        var bytesToProcess = Math.Min(remainingBytes, 1024);
        
        var charsUsed = decoder.GetChars(span.Slice(bytesUsed, bytesToProcess), _charBuffer, false);
        sb.Append(_charBuffer, 0, charsUsed);
        bytesUsed += bytesToProcess;
      }
    }

    /// <summary>
    /// Removes processed data from the beginning of the buffer using efficient memory operations.
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
        // Consumed everything - just reset length
        BufferLength = 0;
      }
      else if (bytesToRemove > 0)
      {
        // Shift remaining data to the beginning using Span for efficiency
        var remainingLength = BufferLength - bytesToRemove;
        Buffer.AsSpan(bytesToRemove, remainingLength).CopyTo(Buffer.AsSpan());
        BufferLength = remainingLength;
      }
    }

    /// <summary>
    /// Finds the next occurrence of a byte in the buffer.
    /// </summary>
    /// <param name="value">The byte to search for.</param>
    /// <param name="startIndex">Starting position for search.</param>
    /// <returns>Index of the byte, or -1 if not found.</returns>
    protected int IndexOfByte(byte value, int startIndex = 0)
    {
      if (startIndex >= BufferLength) return -1;
      
      var span = Buffer.AsSpan(startIndex, BufferLength - startIndex);
      var index = span.IndexOf(value);
      return index >= 0 ? startIndex + index : -1;
    }

    /// <summary>
    /// Checks if the buffer contains a specific byte sequence at the given position.
    /// </summary>
    /// <param name="sequence">The sequence to check for.</param>
    /// <param name="position">Position to check at.</param>
    /// <returns>True if the sequence matches.</returns>
    protected bool BufferContainsAt(ReadOnlySpan<byte> sequence, int position)
    {
      if (position + sequence.Length > BufferLength) return false;
      
      var bufferSlice = Buffer.AsSpan(position, sequence.Length);
      return bufferSlice.SequenceEqual(sequence);
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