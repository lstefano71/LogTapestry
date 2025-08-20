using LogTapestry.Core;

using Microsoft.Extensions.Logging;

using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// Wrapper for processor-specific channels that handle file events.
  /// Each processor has its own channel to ensure events for the same file
  /// are processed sequentially by the same worker.
  /// </summary>
  public class ProcessorChannel(ILogger<ProcessorChannel> logger, int processorId)
  {
    private readonly ILogger<ProcessorChannel> _logger = logger;

    public int ProcessorId { get; } = processorId;
    public Channel<FileEvent> InputChannel { get; } = Channel.CreateUnbounded<FileEvent>();
    public ChannelWriter<FileEvent> Writer => InputChannel.Writer;
    public ChannelReader<FileEvent> Reader => InputChannel.Reader;

    /// <summary>
    /// Attempts to write a file event to this processor's channel.
    /// </summary>
    public bool TryWrite(FileEvent fileEvent)
    {
      if (Writer.TryWrite(fileEvent)) {
        _logger.LogTrace("Processor {ProcessorId}: Successfully queued {EventType} event for {FilePath}",
            ProcessorId, fileEvent.Type, fileEvent.FilePath);
        return true;
      } else {
        _logger.LogWarning("Processor {ProcessorId}: Failed to queue {EventType} event for {FilePath}",
            ProcessorId, fileEvent.Type, fileEvent.FilePath);
        return false;
      }
    }

    /// <summary>
    /// Asynchronously writes a file event to this processor's channel.
    /// </summary>
    public async ValueTask WriteAsync(FileEvent fileEvent, CancellationToken token = default)
    {
      await Writer.WriteAsync(fileEvent, token);
      _logger.LogInformation("Processor {ProcessorId}: Successfully queued {EventType} event for {FilePath}",
          ProcessorId, fileEvent.Type, fileEvent.FilePath);
    }

    /// <summary>
    /// Gets the current count of pending events in the channel.
    /// </summary>
    public static int GetPendingEventCount()
    {
      // Note: Channel doesn't provide a direct count, but we could track this
      // if needed in the future by maintaining a counter
      return 0; // For now, return 0 as placeholder
    }

    /// <summary>
    /// Completes the channel, preventing further writes.
    /// </summary>
    public void Complete()
    {
      Writer.Complete();
      _logger.LogDebug("Processor {ProcessorId}: Channel completed", ProcessorId);
    }
  }
}
