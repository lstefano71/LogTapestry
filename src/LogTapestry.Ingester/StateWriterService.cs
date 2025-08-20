using LogTapestry.Core;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  public class StateWriterService : IHostedService
  {
    private readonly ILogger<StateWriterService> _logger;
    private readonly IStateProvider _stateProvider;
    private readonly IngesterSettings _settings;
    private readonly Channel<PositionUpdate> _updateChannel;
    private Task? _processingTask;
    private CancellationTokenSource? _cts;
    private readonly PeriodicTimer _batchTimer;

    public StateWriterService(
      ILogger<StateWriterService> logger,
      IStateProvider stateProvider,
      Microsoft.Extensions.Options.IOptions<IngesterSettings> options)
    {
      _logger = logger;
      _stateProvider = stateProvider;
      _settings = options.Value;
      _updateChannel = Channel.CreateUnbounded<PositionUpdate>();
      _batchTimer = new PeriodicTimer(TimeSpan.FromSeconds(_settings.StateWriterIntervalSeconds));
    }

    public ChannelWriter<PositionUpdate> Writer => _updateChannel.Writer;

    public Task StartAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("StateWriterService starting with batch size {BatchSize} and interval {Interval}s",
        _settings.StateWriterBatchSize, _settings.StateWriterIntervalSeconds);

      _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

      _processingTask = Task.Run(async () => await StartPipeline(_cts.Token), cancellationToken);

      return Task.CompletedTask;
    }

    private async Task StartPipeline(CancellationToken token)
    {
      try {
        var batch = new List<PositionUpdate>();

        while (await _batchTimer.WaitForNextTickAsync(token)) {
          try {
            // Drain all available updates into the batch
            while (_updateChannel.Reader.TryRead(out var update)) {
              batch.Add(update);

              // If batch is full, write it immediately
              if (batch.Count >= _settings.StateWriterBatchSize) {
                await WriteBatch(batch);
                batch.Clear();
              }
            }

            // Write remaining batch items
            if (batch.Count > 0) {
              await WriteBatch(batch);
              batch.Clear();
            }
          } catch (Exception ex) {
            _logger.LogError(ex, "Error in StateWriterService batch processing iteration");
          }
        }
      } catch (OperationCanceledException) {
        _logger.LogInformation("StateWriterService pipeline cancellation requested");
      }

      _logger.LogInformation("StateWriterService pipeline has shut down");
    }

    private async Task WriteBatch(List<PositionUpdate> batch)
    {
      if (batch.Count == 0) return;

      try {
        // Use the new batch update method
        await _stateProvider.UpdateTrackedFilesBatchAsync(batch.ToArray());

        _logger.LogDebug("Wrote batch of {Count} position updates", batch.Count);
      } catch (Exception ex) {
        _logger.LogError(ex, "Failed to write batch of {Count} position updates", batch.Count);
        // Continue processing - don't let one batch failure stop the service
      }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("StateWriterService stopping");

      if (_cts != null) {
        _cts.Cancel();
      }

      // Process any remaining items in the channel
      var remainingBatch = new List<PositionUpdate>();
      while (_updateChannel.Reader.TryRead(out var update)) {
        remainingBatch.Add(update);
      }

      if (remainingBatch.Count > 0) {
        await WriteBatch(remainingBatch);
      }

      if (_processingTask != null) {
        await _processingTask;
      }

      _batchTimer.Dispose();
    }
  }
}
