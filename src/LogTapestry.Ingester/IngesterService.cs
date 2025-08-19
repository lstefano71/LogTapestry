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
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));

        try {
          // The idiomatic and race-free way to use a PeriodicTimer.
          // This loop will execute approximately every 5 seconds.
          while (await timer.WaitForNextTickAsync(_cts.Token)) {
            try {
              // After each tick, drain whatever is currently in the channel.
              // This is a non-blocking loop. If the channel is empty, it does nothing.
              while (_outputChannel.Reader.TryRead(out var result)) {
                if (result.IsSuccess && result.Entry != null) {
                  batch.Add(result.Entry);
                } else {
                  _logger.LogWarning("Log parsing failed: {ErrorMessage}. Source: {Source}, Text: {UnparseableText}", result.ErrorMessage, result.Source, result.UnparseableText);
                }

                // To avoid letting the batch grow too large during a high-volume burst
                // that lasts longer than the timer's interval, we still check the batch size here.
                if (batch.Count >= 1000) {
                  await WriteBatch(batch);
                  batch.Clear();
                }
              }

              // After draining the channel, if there's anything left in the batch, write it.
              // This handles the case where log volume is low and the batch never reaches 1000.
              if (batch.Count > 0) {
                await WriteBatch(batch);
                batch.Clear();
              }
            } catch (Exception ex) {
              // Catching exceptions inside the loop makes the consumer resilient to transient errors.
              _logger.LogError(ex, "An error occurred in a single consumer pipeline iteration.");
            }
          }
        } catch (OperationCanceledException) {
          // This is the expected way to exit the loop when shutdown is requested.
          _logger.LogInformation("Consumer pipeline cancellation requested.");
        }

        _logger.LogInformation("Consumer pipeline has shut down.");

      }, _cts.Token);

      return Task.CompletedTask;
    }

    private async Task WriteBatch(List<LogEntry> batch)
    {
      if (batch.Count == 0) return;
      try {
        // Use the timestamp of the first entry for partitioning
        var firstEntryTimestamp = batch[0].Timestamp.ToUniversalTime();
        var dataRoot = _settings.Ingester.DataRoot ?? "data";
        var partitionPath = Path.Combine(
          dataRoot,
          $"year={firstEntryTimestamp.Year}",
          $"month={firstEntryTimestamp.Month:D2}",
          $"day={firstEntryTimestamp.Day:D2}",
          "landing"
        );
        Directory.CreateDirectory(partitionPath);
        var tempFilePath = Path.Combine(partitionPath, $"part-{Guid.NewGuid()}.parquet.tmp");
        var finalFilePath = Path.ChangeExtension(tempFilePath, ".parquet");
        await using (var stream = File.Create(tempFilePath)) {
          await _dataSink.WriteBatchAsync([.. batch], stream);
        }
        File.Move(tempFilePath, finalFilePath);
        _logger.LogInformation(
          "Wrote batch of {Count} entries to {Path}",
          batch.Count,
          finalFilePath
        );
      } catch (Exception ex) {
        _logger.LogError(ex, "Failed to write batch of {Count} log entries. Data may be lost.", batch.Count);
        // Optionally implement retry logic here
      }
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
