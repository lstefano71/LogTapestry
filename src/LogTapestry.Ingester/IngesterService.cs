using LogTapestry.Core;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// Checkpointing IngesterService using coordinated state update model.
  /// Implements atomic data and state persistence to prevent race conditions.
  /// </summary>
  public class IngesterService : IHostedService
  {
    private readonly ILogger<IngesterService> _logger;
    private readonly LogTapestrySettings _settings;
    private readonly DirectoryMonitor _directoryMonitor;
    private readonly StateWriterService _stateWriterService;
    private readonly CheckpointDataSink _checkpointDataSink;
    private readonly LiveStateService _liveStateService;
    private readonly FileReader _fileReader;

    // Checkpointing pipeline components
    private readonly Channel<DataBlock> _dataBlockChannel;

    private Task? _directoryMonitorTask;
    private Task? _stateWriterTask;
    private Task? _workerPoolTask;
    private Task? _dataSinkTask;
    private CancellationTokenSource? _cts;

    public IngesterService(
      ILogger<IngesterService> logger,
      Microsoft.Extensions.Options.IOptions<LogTapestrySettings> options,
      DirectoryMonitor directoryMonitor,
      StateWriterService stateWriterService,
      CheckpointDataSink checkpointDataSink,
      LiveStateService liveStateService,
      FileReader fileReader)
    {
      _logger = logger;
      _settings = options.Value;
      _directoryMonitor = directoryMonitor;
      _stateWriterService = stateWriterService;
      _checkpointDataSink = checkpointDataSink;
      _liveStateService = liveStateService;
      _fileReader = fileReader;

      // Initialize checkpointing pipeline channels
      _dataBlockChannel = Channel.CreateBounded<DataBlock>(10);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("Starting checkpointing architecture");

      _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

      // Start directory monitoring
      _directoryMonitorTask = _directoryMonitor.RunAsync(_cts.Token);

      // Start state writer service
      _stateWriterTask = _stateWriterService.StartAsync(_cts.Token);

      // Start worker pool for file processing
      _workerPoolTask = StartWorkerPoolAsync(_cts.Token);

      // Start data sink pipeline
      _dataSinkTask = StartDataSinkPipelineAsync(_cts.Token);

      return Task.CompletedTask;
    }

    /// <summary>
    /// Starts the worker pool that processes file events from directory monitoring.
    /// </summary>
    private async Task StartWorkerPoolAsync(CancellationToken token)
    {
      _logger.LogInformation("Starting worker pool with {WorkerCount} workers",
        _settings.Ingester.FileReaderThreadPoolSize);

      // Start multiple worker tasks
      var workerTasks = new List<Task>();
      for (int i = 0; i < _settings.Ingester.FileReaderThreadPoolSize; i++) {
        workerTasks.Add(Task.Run(() => ProcessFileEventsAsync(token), token));
      }

      await Task.WhenAll(workerTasks);
    }

    /// <summary>
    /// Sets up the checkpointing pipeline.
    /// Transforms file events into DataBlocks and processes them atomically.
    /// </summary>
    private async Task SetupCheckpointingPipelineAsync(CancellationToken token)
    {
      // Pipeline: FileEvent -> FileCheckRequest -> DataBlock -> CheckpointDataSink
      await foreach (var fileEvent in _directoryMonitor.FileEvents.ReadAllAsync(token)) {
        try {
          // Transform FileEvent to FileCheckRequest
          var fileRequest = new FileCheckRequest {
            FileId = fileEvent.FileId,
            VolumeSerial = fileEvent.VolumeSerial,
            FilePath = fileEvent.FilePath,
            LastWriteTimeUtc = fileEvent.LastWriteTimeUtc,
            Type = fileEvent.Type
          };

          // Process the file and get DataBlock
          await foreach (var dataBlock in _fileReader.ReadAndCreateDataBlocksAsync(fileRequest, token)) {

            if (dataBlock != null) {
              // Send DataBlock to checkpointing data sink
              await _dataBlockChannel.Writer.WriteAsync(dataBlock, token);
              _logger.LogDebug("Sent DataBlock for {FilePath}: {EntryCount} entries",
                fileEvent.FilePath, dataBlock.Entries.Count);
            }
          }
        } catch (Exception ex) {
          _logger.LogError(ex, "Error processing file event for {FilePath}", fileEvent.FilePath);
        }
      }
    }

    /// <summary>
    /// Worker task that runs the checkpointing pipeline.
    /// </summary>
    private async Task ProcessFileEventsAsync(CancellationToken token)
    {
      try {
        _logger.LogInformation("Checkpointing pipeline started");

        await SetupCheckpointingPipelineAsync(token);
      } catch (OperationCanceledException) {
        _logger.LogInformation("Checkpointing pipeline cancelled");
      } catch (Exception ex) {
        _logger.LogError(ex, "Error in checkpointing pipeline");
      }
    }

    /// <summary>
    /// Starts the data sink pipeline that processes DataBlocks with checkpointing.
    /// </summary>
    private async Task StartDataSinkPipelineAsync(CancellationToken token)
    {
      _logger.LogInformation("Starting checkpointing data sink pipeline");

      var batch = new List<DataBlock>();
      using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));

      try {
        while (await timer.WaitForNextTickAsync(token)) {
          try {
            // Drain DataBlocks into batch
            while (_dataBlockChannel.Reader.TryRead(out var dataBlock)) {
              batch.Add(dataBlock);

              // Process batch if it reaches the size limit
              if (batch.Count >= _settings.Ingester.BatchSize) {
                await ProcessDataBlockBatch(batch);
                batch.Clear();
              }
            }

            // Process remaining batch items
            if (batch.Count > 0) {
              await ProcessDataBlockBatch(batch);
              batch.Clear();
            }
          } catch (Exception ex) {
            _logger.LogError(ex, "Error in data sink pipeline iteration");
          }
        }
      } catch (OperationCanceledException) {
        _logger.LogInformation("Data sink pipeline cancelled");
      }
    }

    private async Task ProcessDataBlockBatch(List<DataBlock> batch)
    {
      if (batch.Count == 0) return;

      try {
        // Use checkpointing data sink to ensure atomic data and state persistence
        await _checkpointDataSink.WriteBatchAndCheckpointStateAsync(batch.ToArray());
        _logger.LogInformation("Checkpointed batch of {Count} data blocks", batch.Count);
      } catch (Exception ex) {
        _logger.LogError(ex, "Failed to checkpoint batch of {Count} data blocks", batch.Count);
        // In checkpointing model, we don't continue processing on failure
        // The atomic nature means partial failures require investigation
        throw;
      }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("Stopping declarative worker pool architecture");

      if (_cts != null) {
        _cts.Cancel();
      }

      // Stop directory monitor
      if (_directoryMonitorTask != null) {
        await _directoryMonitor.StopAsync();
      }

      // Stop state writer service
      if (_stateWriterTask != null) {
        await _stateWriterService.StopAsync(cancellationToken);
      }

      // Wait for all tasks to complete
      var tasks = new List<Task>();
      if (_directoryMonitorTask != null) tasks.Add(_directoryMonitorTask);
      if (_stateWriterTask != null) tasks.Add(_stateWriterTask);
      if (_workerPoolTask != null) tasks.Add(_workerPoolTask);
      if (_dataSinkTask != null) tasks.Add(_dataSinkTask);

      if (tasks.Any()) {
        await Task.WhenAll(tasks);
      }

      _logger.LogInformation("Checkpointing architecture stopped");
    }
  }
}
