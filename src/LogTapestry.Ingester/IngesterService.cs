// C#
using LogTapestry.Core;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  public class IngesterService : IHostedService
  {
    private readonly ILogger<IngesterService> _logger;
    private readonly LogTapestrySettings _settings;
    private readonly DirectoryMonitor _directoryMonitor;
    private readonly TailingManager _tailingManager;
    private readonly DataSink _dataSink; // Injected DataSink
    private Task? _monitorTask;
    private Task? _tailingTask;
    private Task? _consumerTask; // Consumer pipeline task
    private Channel<FileWorkItem>? _workChannel;
    private Channel<ParsingResult>? _outputChannel;
    private CancellationTokenSource? _cts;

    public IngesterService(
      ILogger<IngesterService> logger,
      Microsoft.Extensions.Options.IOptions<LogTapestrySettings> options,
      DirectoryMonitor directoryMonitor,
      TailingManager tailingManager,
      DataSink dataSink)
    {
      _logger = logger;
      _settings = options.Value;
      _directoryMonitor = directoryMonitor;
      _tailingManager = tailingManager;
      _dataSink = dataSink;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("IngesterService starting.");

      _workChannel = Channel.CreateUnbounded<FileWorkItem>();
      _outputChannel = Channel.CreateBounded<ParsingResult>(10000); // Bounded for safety
      _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

      _monitorTask = _directoryMonitor.RunAsync(_workChannel.Writer, _cts.Token);
      _tailingTask = _tailingManager.RunAsync(_workChannel.Reader, _outputChannel.Writer, _cts.Token);

      // Start consumer pipeline
      _consumerTask = Task.Run(async () => {
        var batch = new List<LogEntry>();
        var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        while (!_cts!.Token.IsCancellationRequested) {
          try {
            await Task.WhenAny(
              _outputChannel!.Reader.WaitToReadAsync(_cts.Token).AsTask(),
              timer.WaitForNextTickAsync(_cts.Token).AsTask()
            );
            while (_outputChannel.Reader.TryRead(out var result)) {
              if (result.IsSuccess && result.Entry != null) {
                batch.Add(result.Entry);
              } else {
                _logger.LogWarning("Log parsing failed: {ErrorMessage}. Source: {Source}, Text: {UnparseableText}", result.ErrorMessage, result.Source, result.UnparseableText);
              }
              if (batch.Count >= 1000) {
                await WriteBatch(batch);
                batch.Clear();
              }
            }
            if (batch.Count > 0) {
              await WriteBatch(batch);
              batch.Clear();
            }
          } catch (OperationCanceledException) {
            break;
          } catch (Exception ex) {
            _logger.LogError(ex, "An error occurred in the consumer pipeline.");
          }
        }
        _logger.LogInformation("Consumer pipeline shutting down.");
      }, _cts.Token);

      return Task.CompletedTask;
    }

    private async Task WriteBatch(List<LogEntry> batch)
    {
      if (batch.Count == 0) return;
      var path = Path.Combine("data", $"log_{DateTime.UtcNow:yyyyMMdd_HHmmss}_{Guid.NewGuid()}.parquet");
      Directory.CreateDirectory("data");
      await using var stream = File.Create(path);
      await _dataSink.WriteBatchAsync(batch.ToArray(), stream);
      _logger.LogInformation("Wrote batch of {Count} entries to {Path}", batch.Count, path);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("IngesterService stopping.");
      _cts?.Cancel();
      Task?[] tasks = [_monitorTask, _tailingTask, _consumerTask];
      foreach (var t in tasks) {
        if (t != null) await t;
      }
    }
  }
}
