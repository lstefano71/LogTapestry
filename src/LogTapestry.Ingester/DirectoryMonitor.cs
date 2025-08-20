// LogTapestry.Ingester/DirectoryMonitor.cs
using LogTapestry.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// Refactored DirectoryMonitor using priority-aware pipeline architecture.
  /// Coordinates initial scan and real-time file watching through PriorityMonitor.
  /// </summary>
  public class DirectoryMonitor
  {
    private readonly ILogger<DirectoryMonitor> _logger;
    private readonly IngesterSettings _settings;
    private readonly IStateProvider _stateProvider;
    private readonly PriorityMonitor _priorityMonitor;
    private readonly InitialScanProducer _initialScanProducer;
    private readonly WatcherProducer _watcherProducer;
    private Task? _priorityTask;
    private Task? _initialScanTask;
    private Task? _watcherTask;
    private CancellationTokenSource? _cts;

    public DirectoryMonitor(
      ILogger<DirectoryMonitor> logger,
      IOptions<IngesterSettings> options,
      IStateProvider stateProvider,
      PriorityMonitor priorityMonitor,
      InitialScanProducer initialScanProducer,
      WatcherProducer watcherProducer)
    {
      _logger = logger;
      _settings = options.Value;
      _stateProvider = stateProvider;
      _priorityMonitor = priorityMonitor;
      _initialScanProducer = initialScanProducer;
      _watcherProducer = watcherProducer;
    }

    /// <summary>
    /// Returns a channel reader for consuming prioritized file events.
    /// </summary>
    public ChannelReader<FileEvent> FileEvents => _priorityMonitor.Reader;

    /// <summary>
    /// Starts the priority-based file monitoring pipeline.
    /// </summary>
    public async Task RunAsync(CancellationToken token)
    {
      _logger.LogInformation("Starting priority-based file monitoring pipeline");

      _cts = CancellationTokenSource.CreateLinkedTokenSource(token);

      try {
        // Start the priority monitor
        _priorityTask = _priorityMonitor.ProcessEventsAsync(
          _watcherProducer.Reader,
          _initialScanProducer.Reader,
          _cts.Token);

        // Start the watcher producer
        _watcherTask = _watcherProducer.RunAsync(_cts.Token);

        // Start the initial scan producer
        _initialScanTask = _initialScanProducer.RunAsync(_cts.Token);

        // Wait for all tasks to complete
        await Task.WhenAll(_priorityTask, _watcherTask, _initialScanTask);

        _logger.LogInformation("Priority-based file monitoring pipeline completed");
      } catch (OperationCanceledException) {
        _logger.LogInformation("File monitoring pipeline cancelled");
      } catch (Exception ex) {
        _logger.LogError(ex, "Error in file monitoring pipeline");
        throw;
      }
    }

    /// <summary>
    /// Stops the file monitoring pipeline.
    /// </summary>
    public async Task StopAsync()
    {
      _logger.LogInformation("Stopping file monitoring pipeline");

      if (_cts != null) {
        _cts.Cancel();
      }

      var tasks = new List<Task>();
      if (_priorityTask != null) tasks.Add(_priorityTask);
      if (_watcherTask != null) tasks.Add(_watcherTask);
      if (_initialScanTask != null) tasks.Add(_initialScanTask);

      if (tasks.Any()) {
        await Task.WhenAll(tasks);
      }
    }
  }
}
