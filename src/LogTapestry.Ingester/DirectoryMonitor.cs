// LogTapestry.Ingester/DirectoryMonitor.cs
using LogTapestry.Core;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// Refactored DirectoryMonitor using priority-aware pipeline architecture with consistent hashing.
  /// Coordinates initial scan and real-time file watching through PriorityMonitor.
  /// Routes events to processor-specific channels for sequential processing per file.
  /// </summary>
  public class DirectoryMonitor(
    ILogger<DirectoryMonitor> logger,
    IOptions<IngesterSettings> options,
    IStateProvider stateProvider,
    PriorityMonitor priorityMonitor,
    InitialScanProducer initialScanProducer,
    WatcherProducer watcherProducer)
  {
    private readonly ILogger<DirectoryMonitor> _logger = logger;
    private readonly IngesterSettings _settings = options.Value;
    private readonly IStateProvider _stateProvider = stateProvider;
    private readonly PriorityMonitor _priorityMonitor = priorityMonitor;
    private readonly InitialScanProducer _initialScanProducer = initialScanProducer;
    private readonly WatcherProducer _watcherProducer = watcherProducer;
    private Task? _priorityTask;
    private Task? _initialScanTask;
    private Task? _watcherTask;
    private CancellationTokenSource? _cts;

    /// <summary>
    /// Returns a channel reader for consuming prioritized file events.
    /// This is kept for backward compatibility but the new architecture routes
    /// events directly to processor channels via PriorityMonitor.
    /// </summary>
    [Obsolete("Use processor channels directly via PriorityMonitor for new consistent hashing architecture")]
    public ChannelReader<FileEvent> FileEvents => throw new NotSupportedException(
      "Use processor channels directly via PriorityMonitor for consistent hashing architecture");

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

      _cts?.Cancel();

      var tasks = new List<Task>();
      if (_priorityTask != null) tasks.Add(_priorityTask);
      if (_watcherTask != null) tasks.Add(_watcherTask);
      if (_initialScanTask != null) tasks.Add(_initialScanTask);

      if (tasks.Count != 0) {
        await Task.WhenAll(tasks);
      }
    }
  }
}
