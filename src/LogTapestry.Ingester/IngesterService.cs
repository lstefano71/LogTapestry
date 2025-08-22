using LogTapestry.Core;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// Multi-sink IngesterService using coordinated state update model with consistent hashing.
  /// Implements atomic data and state persistence across multiple configurable sinks to prevent race conditions.
  /// Routes file events to processor-specific channels to ensure sequential processing per file.
  /// </summary>
  public class IngesterService(
    ILogger<IngesterService> logger,
    Microsoft.Extensions.Options.IOptions<LogTapestrySettings> options,
    DirectoryMonitor directoryMonitor,
    StateWriterService stateWriterService,
    MultiSinkProcessor multiSinkProcessor,
    FileReader fileReader,
    ConsistentHashRouter hashRouter,
    List<ProcessorChannel> processorChannels,
    ILoggerFactory loggerFactory) : IHostedService
  {
    private readonly ILogger<IngesterService> _logger = logger;
    private readonly LogTapestrySettings _settings = options.Value;
    private readonly DirectoryMonitor _directoryMonitor = directoryMonitor;
    private readonly StateWriterService _stateWriterService = stateWriterService;
    private readonly MultiSinkProcessor _multiSinkProcessor = multiSinkProcessor;
    private readonly FileReader _fileReader = fileReader;
    private readonly ConsistentHashRouter _hashRouter = hashRouter;
    private readonly List<ProcessorChannel> _processorChannels = processorChannels; // Injected from DI
    private readonly ILoggerFactory _loggerFactory = loggerFactory;

    private List<FileEventProcessor>? _fileEventProcessors;
    private List<Task>? _processorTasks;

    // Checkpointing pipeline components
    private readonly Channel<DataBlock> _dataBlockChannel = Channel.CreateBounded<DataBlock>(10);

    private Task? _directoryMonitorTask;
    private Task? _stateWriterTask;
    private Task? _dataSinkTask;
    private CancellationTokenSource? _cts;

    public Task StartAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("Starting consistent hashing architecture");

      _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

      // Initialize processor workers (channels are already injected)
      InitializeProcessorChannels();

      // Start directory monitoring
      _directoryMonitorTask = _directoryMonitor.RunAsync(_cts.Token);

      // Start state writer service
      _stateWriterTask = _stateWriterService.StartAsync(_cts.Token);

      // Start processor workers
      _processorTasks = StartProcessorWorkers(_cts.Token);

      // Start data sink pipeline
      _dataSinkTask = StartDataSinkPipelineAsync(_cts.Token);

      return Task.CompletedTask;
    }

    /// <summary>
    /// Initializes processor workers for consistent hashing using injected channels.
    /// </summary>
    private void InitializeProcessorChannels()
    {
      var processorCount = _settings.Ingester.FileReaderThreadPoolSize;
      _logger.LogInformation("Initializing {ProcessorCount} processor channels (from DI)", processorCount);

      _fileEventProcessors = [];

      for (int i = 0; i < processorCount; i++) {
        var processorLogger = _loggerFactory.CreateLogger<FileEventProcessor>();
        var fileEventProcessor = new FileEventProcessor(
          processorLogger,
          _fileReader,
          _dataBlockChannel,
          i);

        _logger.LogDebug("Created FileEventProcessor {ProcessorId} with FileReader {FileReaderType}",
          i, _fileReader.GetType().Name);

        _fileEventProcessors.Add(fileEventProcessor);
      }
    }

    /// <summary>
    /// Starts processor workers that handle file events from their respective channels.
    /// </summary>
    private List<Task> StartProcessorWorkers(CancellationToken token)
    {
      _logger.LogInformation("Starting {ProcessorCount} processor workers", _fileEventProcessors?.Count ?? 0);

      var tasks = new List<Task>();

      if (_fileEventProcessors != null && _processorChannels != null) {
        for (int i = 0; i < _fileEventProcessors.Count; i++) {
          var processor = _fileEventProcessors[i];
          var channel = _processorChannels[i];

          var task = Task.Run(() => processor.ProcessEventsAsync(channel.Reader, token), token);
          tasks.Add(task);
        }
      }

      return tasks;
    }

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
              if (batch.Sum(dblock => dblock.Entries.Count) >= _settings.Ingester.BatchSize) {
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
        // Use multi-sink processor to ensure atomic data and state persistence across all sinks
        await _multiSinkProcessor.WriteBatchAndCheckpointStateAsync(batch);
        _logger.LogInformation("Multi-sink checkpointed batch of {Count} data blocks", batch.Count);
      } catch (Exception ex) {
        _logger.LogError(ex, "Failed to checkpoint batch of {Count} data blocks with multi-sink processor", batch.Count);
        // In checkpointing model, we don't continue processing on failure
        // The atomic nature means partial failures require investigation
        throw;
      }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("Stopping consistent hashing architecture");

      _cts?.Cancel();

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
      if (_processorTasks != null) tasks.AddRange(_processorTasks);
      if (_dataSinkTask != null) tasks.Add(_dataSinkTask);

      if (tasks.Count != 0) {
        await Task.WhenAll(tasks);
      }

      _logger.LogInformation("Consistent hashing architecture stopped");
    }
  }
}
