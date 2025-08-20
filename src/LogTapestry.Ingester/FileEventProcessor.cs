using LogTapestry.Core;

using Microsoft.Extensions.Logging;

using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// Processes file events sequentially for a specific processor.
  /// Since all events for the same file are routed to the same processor,
  /// the existing FileReader logic will naturally handle deduplication.
  /// </summary>
  public class FileEventProcessor(
      ILogger<FileEventProcessor> logger,
      FileReader fileReader,
      Channel<DataBlock> dataBlockChannel,
      int processorId)
  {
    private readonly ILogger<FileEventProcessor> _logger = logger;
    private readonly FileReader _fileReader = fileReader;
    private readonly Channel<DataBlock> _dataBlockChannel = dataBlockChannel;
    private readonly int _processorId = processorId;

    /// <summary>
    /// Processes events from the processor's channel.
    /// Events are processed sequentially, ensuring that for any given file,
    /// events are handled in order and the FileReader can naturally deduplicate.
    /// </summary>
    public async Task ProcessEventsAsync(ChannelReader<FileEvent> eventReader, CancellationToken token)
    {
      _logger.LogInformation("Processor {ProcessorId}: Starting event processing", _processorId);

      try {
        _logger.LogInformation("Processor {ProcessorId}: Beginning channel read loop", _processorId);

        await foreach (var fileEvent in eventReader.ReadAllAsync(token)) {
          _logger.LogTrace("Processor {ProcessorId}: Received {EventType} event for {FilePath}",
              _processorId, fileEvent.Type, fileEvent.FilePath);

          try {
            _logger.LogTrace("Processor {ProcessorId}: About to process event for {FilePath}",
                _processorId, fileEvent.FilePath);

            await ProcessFileEventAsync(fileEvent, token);

            _logger.LogTrace("Processor {ProcessorId}: Successfully processed event for {FilePath}",
                _processorId, fileEvent.FilePath);
          } catch (Exception ex) {
            _logger.LogError(ex, "Processor {ProcessorId}: Error processing {EventType} event for {FilePath}",
                _processorId, fileEvent.Type, fileEvent.FilePath);
            // Continue processing other events even if one fails
          }
        }

        _logger.LogInformation("Processor {ProcessorId}: Channel read loop ended", _processorId);
      } catch (OperationCanceledException) {
        _logger.LogInformation("Processor {ProcessorId}: Event processing cancelled", _processorId);
      } catch (Exception ex) {
        _logger.LogError(ex, "Processor {ProcessorId}: Fatal error in event processing", _processorId);
      }

      _logger.LogInformation("Processor {ProcessorId}: Event processing completed", _processorId);
    }

    /// <summary>
    /// Processes a single file event by delegating to the FileReader.
    /// The FileReader will naturally handle deduplication since:
    /// 1. Events for the same file are processed sequentially
    /// 2. The first event advances the file position
    /// 3. Subsequent events find no new content to process
    /// </summary>
    private async Task ProcessFileEventAsync(FileEvent fileEvent, CancellationToken token)
    {
      _logger.LogTrace("Processor {ProcessorId}: Processing {EventType} event for {FilePath}",
          _processorId, fileEvent.Type, fileEvent.FilePath);

      // Transform FileEvent to FileCheckRequest
      var fileRequest = new FileCheckRequest {
        FileId = fileEvent.FileId,
        VolumeSerial = fileEvent.VolumeSerial,
        FilePath = fileEvent.FilePath,
        LastWriteTimeUtc = fileEvent.LastWriteTimeUtc,
        Type = fileEvent.Type
      };

      _logger.LogTrace("Processor {ProcessorId}: Created FileCheckRequest for {FilePath}",
          _processorId, fileEvent.FilePath);

      try {
        // Process the file using the existing FileReader
        // This will naturally handle deduplication since events are processed sequentially
        await foreach (var dataBlock in _fileReader.ReadAndCreateDataBlocksAsync(fileRequest, token)) {
          _logger.LogTrace("Processor {ProcessorId}: ReadAndCreateDataBlocksAsync returned a DataBlock for {FilePath}",
              _processorId, fileEvent.FilePath);

          if (dataBlock != null) {
            // Send DataBlock to the data block channel for batch processing
            //_logger.LogTrace("Processor {ProcessorId}: Queueing DataBlock for {FilePath}: {EntryCount} entries, {Position}...",
            //    _processorId, fileEvent.FilePath, dataBlock.Entries.Count, dataBlock.EndPosition);

            await _dataBlockChannel.Writer.WriteAsync(dataBlock, token);

            _logger.LogTrace("Processor {ProcessorId}: Queued DataBlock for {FilePath}: {EntryCount} entries, {Position}",
                _processorId, fileEvent.FilePath, dataBlock.Entries.Count, dataBlock.EndPosition);
          } else {
            _logger.LogWarning("Processor {ProcessorId}: DataBlock was null for {FilePath}",
                _processorId, fileEvent.FilePath);
          }
        }

        _logger.LogTrace("Processor {ProcessorId}: Finished processing ReadAndCreateDataBlocksAsync for {FilePath}",
            _processorId, fileEvent.FilePath);
      } catch (OperationCanceledException) {
        _logger.LogInformation("Processor {ProcessorId}: Processing cancelled for {FilePath}", _processorId, fileEvent.FilePath);
      } catch (Exception ex) {
        _logger.LogError(ex, "Processor {ProcessorId}: Error reading file {FilePath}", _processorId, fileEvent.FilePath);
        throw;
      }
    }
  }
}
