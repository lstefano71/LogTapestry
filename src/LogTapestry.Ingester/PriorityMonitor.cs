using LogTapestry.Core;

using Microsoft.Extensions.Logging;

using System.Collections.Concurrent;
using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// Merges multiple file discovery channels with priority handling.
  /// Fast channels (real-time) take precedence over slow channels (initial scan).
  /// </summary>
  public class PriorityMonitor
  {
    private readonly ILogger<PriorityMonitor> _logger;
    private readonly Channel<FileEvent> _outputChannel;
    private readonly ConcurrentBag<(ulong FileId, long VolumeSerial)> _seenEvents = new();

    public PriorityMonitor(ILogger<PriorityMonitor> logger)
    {
      _logger = logger;
      _outputChannel = Channel.CreateUnbounded<FileEvent>();
    }

    public ChannelWriter<FileEvent> Writer => _outputChannel.Writer;
    public ChannelReader<FileEvent> Reader => _outputChannel.Reader;

    /// <summary>
    /// Processes events from multiple channels with priority.
    /// Fast channels (watcher) are preferred over slow channels (initial scan).
    /// </summary>
    public async Task ProcessEventsAsync(
      ChannelReader<FileEvent> fastChannel,
      ChannelReader<FileEvent> slowChannel,
      CancellationToken token)
    {
      var fastTask = ProcessChannelAsync(fastChannel, isFastChannel: true, token);
      var slowTask = ProcessChannelAsync(slowChannel, isFastChannel: false, token);

      await Task.WhenAll(fastTask, slowTask);
      _outputChannel.Writer.Complete();
    }

    private async Task ProcessChannelAsync(
      ChannelReader<FileEvent> channel,
      bool isFastChannel,
      CancellationToken token)
    {
      try {
        await foreach (var fileEvent in channel.ReadAllAsync(token)) {
          var key = (fileEvent.FileId, fileEvent.VolumeSerial);

          // Skip if we've already processed this file from a fast channel
          if (!isFastChannel && _seenEvents.Contains(key)) {
            _logger.LogDebug("Skipping duplicate event for file {FilePath} (already processed from fast channel)",
              fileEvent.FilePath);
            continue;
          }

          // Track that we've seen this file
          _seenEvents.Add(key);

          await Writer.WriteAsync(fileEvent, token);

          _logger.LogDebug("Processed {ChannelType} event: {Type} {FilePath}",
            isFastChannel ? "fast" : "slow", fileEvent.Type, fileEvent.FilePath);
        }
      } catch (OperationCanceledException) {
        _logger.LogInformation("Channel processing cancelled for {ChannelType}", isFastChannel ? "fast" : "slow");
        // Optionally: perform any cleanup here
      }
    }
  }
}
