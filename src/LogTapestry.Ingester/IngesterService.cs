// C#
using LogTapestry.Core;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Open.ChannelExtensions;

using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// Refactored IngesterService using declarative worker pool architecture.
  /// Replaces the old TailingManager-based model with a scalable channel-based pipeline.
  /// </summary>
  public class IngesterService : IHostedService
  {
    private readonly ILogger<IngesterService> _logger;
    private readonly LogTapestrySettings _settings;
    private readonly DirectoryMonitor _directoryMonitor;
    private readonly StateWriterService _stateWriterService;
    private readonly DataSink _dataSink;
    private readonly IStateProvider _stateProvider;
    private readonly FileReader _fileReader;

    // New declarative pipeline components
    private readonly Channel<FileCheckRequest> _fileRequestChannel;
    private readonly Channel<PositionUpdate> _positionUpdateChannel;
    private readonly Channel<ParsingResult> _parsingResultChannel;

    private Task? _directoryMonitorTask;
    private Task? _stateWriterTask;
    private Task? _workerPoolTask;
    private Task? _consumerTask;
    private CancellationTokenSource? _cts;

    public IngesterService(
      ILogger<IngesterService> logger,
      Microsoft.Extensions.Options.IOptions<LogTapestrySettings> options,
      DirectoryMonitor directoryMonitor,
      StateWriterService stateWriterService,
      DataSink dataSink,
      IStateProvider stateProvider,
      FileReader fileReader)
    {
      _logger = logger;
      _settings = options.Value;
      _directoryMonitor = directoryMonitor;
      _stateWriterService = stateWriterService;
      _dataSink = dataSink;
      _stateProvider = stateProvider;
      _fileReader = fileReader;

      // Initialize declarative pipeline channels
      _fileRequestChannel = Channel.CreateUnbounded<FileCheckRequest>();
      _positionUpdateChannel = Channel.CreateUnbounded<PositionUpdate>();
      _parsingResultChannel = Channel.CreateBounded<ParsingResult>(10000);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("Starting declarative worker pool architecture");

      _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

      // Start directory monitoring pipeline
      _directoryMonitorTask = _directoryMonitor.RunAsync(_cts.Token);

      // Start state writer service
      _stateWriterTask = _stateWriterService.StartAsync(_cts.Token);

      // Start worker pool for file processing
      _workerPoolTask = StartWorkerPoolAsync(_cts.Token);

      // Start consumer pipeline for parsing results
      _consumerTask = StartConsumerPipelineAsync(_cts.Token);

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
    /// Sets up the declarative pipeline using Open.ChannelExtensions.
    /// Transforms file events into file requests, processes them, and handles results.
    /// </summary>
    private async Task SetupDeclarativePipelineAsync(CancellationToken token)
    {
      // Pipeline: FileEvent -> FileCheckRequest -> PositionUpdate -> Batched DB Update
      var pipeline = _directoryMonitor.FileEvents
        .Transform(async fileEvent => {
          // Transform FileEvent to FileCheckRequest
          var fileRequest = new FileCheckRequest {
            FileId = fileEvent.FileId,
            VolumeSerial = fileEvent.VolumeSerial,
            FilePath = fileEvent.FilePath,
            LastWriteTimeUtc = fileEvent.LastWriteTimeUtc,
            Type = fileEvent.Type
          };

          // Process the file and get position update
          await _fileReader.ReadAndParseFileAsync(
            fileRequest,
            _positionUpdateChannel.Writer,
            _parsingResultChannel.Writer,
            token);

          return fileRequest;
        })
        .Batch(_settings.Ingester.StateWriterBatchSize)  // Batch file requests
        .WithTimeout(_settings.Ingester.PollingIntervalMs)
        .Transform(async batch => {
          // Process batch of position updates
          var positionUpdates = new List<PositionUpdate>();
          foreach (var _ in batch) {
            if (_positionUpdateChannel.Reader.TryRead(out var update)) {
              positionUpdates.Add(update);
            }
          }

          if (positionUpdates.Any()) {
            // Send batch to state writer service
            foreach (var update in positionUpdates) {
              await _stateWriterService.Writer.WriteAsync(update, token);
            }
          }

          return positionUpdates;
        });

      // Consume the pipeline
      await foreach (var resultTask in pipeline.ReadAllAsync(token)) {
        // Pipeline results are consumed here
        var result = await resultTask;
        _logger.LogDebug("Processed batch of {Count} file requests", result.Count);
      }
    }

    /// <summary>
    /// Worker task that runs the declarative pipeline.
    /// </summary>
    private async Task ProcessFileEventsAsync(CancellationToken token)
    {
      try {
        await SetupDeclarativePipelineAsync(token);
      } catch (OperationCanceledException) {
        _logger.LogInformation("Pipeline processing cancelled");
      } catch (Exception ex) {
        _logger.LogError(ex, "Error in declarative pipeline");
      }
    }

    /// <summary>
    /// Starts the consumer pipeline that processes parsing results.
    /// </summary>
    private async Task StartConsumerPipelineAsync(CancellationToken token)
    {
      _logger.LogInformation("Starting consumer pipeline");

      var batch = new List<LogEntry>();
      using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));

      try {
        while (await timer.WaitForNextTickAsync(token)) {
          try {
            // Drain parsing results
            while (_parsingResultChannel.Reader.TryRead(out var result)) {
              if (result.IsSuccess && result.Entry != null) {
                batch.Add(result.Entry);
              } else {
                _logger.LogWarning("Log parsing failed: {ErrorMessage}. Source: {Source}",
                  result.ErrorMessage, result.Source);
              }

              // Write batch if it reaches the size limit
              if (batch.Count >= _settings.Ingester.BatchSize) {
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
            _logger.LogError(ex, "Error in consumer pipeline iteration");
          }
        }
      } catch (OperationCanceledException) {
        _logger.LogInformation("Consumer pipeline cancelled");
      }
    }

    private async Task WriteBatch(List<LogEntry> batch)
    {
      if (batch.Count == 0) return;
      try {
        // Assign ULID to each entry
        var ulidBatch = batch.Select(e =>
          e with { Ulid = Ulid.NewUlid().ToByteArray() }
        ).ToList();
        // Use the timestamp of the first entry for partitioning
        var firstEntryTimestamp = ulidBatch[0].Timestamp.ToUniversalTime();
        var dataRoot = _settings.Ingester.DataRoot ?? "data";
        var partitionPath = Path.Combine(
          dataRoot,
          $"year={firstEntryTimestamp.Year}",
          $"month={firstEntryTimestamp.Month:D2}",
          $"day={firstEntryTimestamp.Day:D2}",
          "landing"
        );
        Directory.CreateDirectory(partitionPath);
        var tempFilePath = Path.Combine(partitionPath, $"part-{Guid.NewGuid()}.parquet_tmp");
        var finalFilePath = Path.ChangeExtension(tempFilePath, ".parquet");
        await using (var stream = File.Create(tempFilePath)) {
          await _dataSink.WriteBatchAsync([.. ulidBatch], stream, finalFilePath, partitionPath);
        }
        File.Move(tempFilePath, finalFilePath);
        _logger.LogInformation(
          "Wrote batch of {Count} entries to {Path}",
          ulidBatch.Count,
          finalFilePath
        );
      } catch (Exception ex) {
        _logger.LogError(ex, "Failed to write batch of {Count} log entries. Data may be lost.", batch.Count);
        // Optionally implement retry logic here
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
      if (_consumerTask != null) tasks.Add(_consumerTask);

      if (tasks.Any()) {
        await Task.WhenAll(tasks);
      }

      _logger.LogInformation("Declarative worker pool architecture stopped");
    }
  }
}
