using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace LogTapestry.Ingester
{
  public class StateWriterService : IHostedService
  {
    private readonly ILogger<StateWriterService> _logger;
    private readonly CheckpointDataSink _checkpointDataSink;
    private readonly LiveStateService _liveStateService;
    private Task? _checkpointProcessingTask;
    private CancellationTokenSource? _cts;

    public StateWriterService(
      ILogger<StateWriterService> logger,
      CheckpointDataSink checkpointDataSink,
      LiveStateService liveStateService)
    {
      _logger = logger;
      _checkpointDataSink = checkpointDataSink;
      _liveStateService = liveStateService;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("StateWriterService starting - monitoring checkpoint updates");

      _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
      _checkpointProcessingTask = Task.Run(() => ProcessCheckpointUpdatesAsync(_cts.Token), cancellationToken);

      return Task.CompletedTask;
    }

    /// <summary>
    /// Process checkpoint updates from the CheckpointDataSink.
    /// In the checkpointing architecture, this is mainly for monitoring and logging.
    /// The actual state updates are handled by LiveStateService internally.
    /// </summary>
    private async Task ProcessCheckpointUpdatesAsync(CancellationToken token)
    {
      try {
        await foreach (var checkpointUpdate in _checkpointDataSink.CheckpointReader.ReadAllAsync(token)) {
          try {
            _logger.LogDebug("Checkpoint processed for {FilePath}: position {Position} at {CheckpointTime:u}",
              checkpointUpdate.FilePath,
              checkpointUpdate.Position,
              checkpointUpdate.CheckpointTime);

            // The LiveStateService has already handled the actual state update
            // This service now serves as a monitoring/logging component for checkpoints
          } catch (Exception ex) {
            _logger.LogError(ex, "Error processing checkpoint update for {FilePath}",
              checkpointUpdate.FilePath);
          }
        }
      } catch (OperationCanceledException) {
        _logger.LogInformation("Checkpoint processing cancelled");
      } catch (Exception ex) {
        _logger.LogError(ex, "Error in checkpoint processing");
      }

      _logger.LogInformation("Checkpoint processing has shut down");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("StateWriterService stopping");

      if (_cts != null) {
        _cts.Cancel();
      }

      if (_checkpointProcessingTask != null) {
        await _checkpointProcessingTask;
      }

      _logger.LogInformation("StateWriterService stopped");
    }
  }
}
