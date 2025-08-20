using LogTapestry.Core;

using Microsoft.Extensions.Logging;

using System.Collections.Concurrent;
using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// Merges multiple file discovery channels with priority handling and routes events
  /// to appropriate processor channels using consistent hashing.
  /// Fast channels (real-time) take precedence over slow channels (initial scan).
  /// </summary>
  public class PriorityMonitor(
    ILogger<PriorityMonitor> logger,
    ConsistentHashRouter hashRouter,
    List<ProcessorChannel> processorChannels)
  {
    private readonly ILogger<PriorityMonitor> _logger = logger;
    private readonly ConsistentHashRouter _hashRouter = hashRouter;
    private readonly List<ProcessorChannel> _processorChannels = processorChannels;
    private readonly ConcurrentBag<(ulong FileId, long VolumeSerial)> _seenEvents = [];

    /// <summary>
    /// Processes events from multiple channels with priority and routes them to processor channels.
    /// Fast channels (watcher) are preferred over slow channels (initial scan).
    /// </summary>
    public async Task ProcessEventsAsync(
      ChannelReader<FileEvent> fastChannel,
      ChannelReader<FileEvent> slowChannel,
      CancellationToken token)
    {
      _logger.LogInformation("Starting priority-based event processing with {ProcessorCount} processors",
        _processorChannels.Count);

      var fastTask = ProcessChannelAsync(fastChannel, isFastChannel: true, token);
      var slowTask = ProcessChannelAsync(slowChannel, isFastChannel: false, token);

      await Task.WhenAll(fastTask, slowTask);

      // Complete all processor channels
      foreach (var processorChannel in _processorChannels) {
        processorChannel.Complete();
      }

      _logger.LogInformation("Priority-based event processing completed");
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
            _logger.LogInformation("Skipping duplicate event for file {FilePath} (already processed from fast channel)",
                fileEvent.FilePath);
            continue;
          }

          // Track that we've seen this file
          _seenEvents.Add(key);

          // Route event to the appropriate processor channel using consistent hashing
          await RouteToProcessorAsync(fileEvent, token);

          _logger.LogInformation("Routed {ChannelType} event: {Type} {FilePath}",
            isFastChannel ? "fast" : "slow", fileEvent.Type, fileEvent.FilePath);
        }
      } catch (OperationCanceledException) {
        _logger.LogInformation("Channel processing cancelled for {ChannelType}", isFastChannel ? "fast" : "slow");
      }
    }

    /// <summary>
    /// Routes a file event to the appropriate processor channel using consistent hashing.
    /// </summary>
    private async Task RouteToProcessorAsync(FileEvent fileEvent, CancellationToken token)
    {
      var processorId = _hashRouter.GetProcessorForFile(fileEvent.FileId, fileEvent.VolumeSerial);

      if (processorId >= 0 && processorId < _processorChannels.Count) {
        var processorChannel = _processorChannels[processorId];
        await processorChannel.WriteAsync(fileEvent, token);

        _logger.LogInformation("Routed event for {FilePath} to processor {ProcessorId}",
          fileEvent.FilePath, processorId);
      } else {
        _logger.LogError("Invalid processor ID {ProcessorId} for file {FilePath}",
          processorId, fileEvent.FilePath);
      }
    }
  }
}
