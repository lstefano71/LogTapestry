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
    private Task? _monitorTask;
    private Task? _tailingTask;
    private Channel<FileWorkItem>? _workChannel;
    private Channel<ParsingResult>? _outputChannel;
    private CancellationTokenSource? _cts;

    public IngesterService(
      ILogger<IngesterService> logger,
      Microsoft.Extensions.Options.IOptions<LogTapestrySettings> options,
      DirectoryMonitor directoryMonitor,
      TailingManager tailingManager)
    {
      _logger = logger;
      _settings = options.Value;
      _directoryMonitor = directoryMonitor;
      _tailingManager = tailingManager;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("IngesterService starting.");

      _workChannel = Channel.CreateUnbounded<FileWorkItem>();
      _outputChannel = Channel.CreateUnbounded<ParsingResult>();
      _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

      _monitorTask = _directoryMonitor.RunAsync(_workChannel.Writer, _cts.Token);
      _tailingTask = _tailingManager.RunAsync(_workChannel.Reader, _outputChannel.Writer, _cts.Token);

      return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("IngesterService stopping.");
      _cts?.Cancel();

      if (_monitorTask != null)
        await _monitorTask;
      if (_tailingTask != null)
        await _tailingTask;
    }
  }
}
