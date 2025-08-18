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
    private DirectoryMonitor _directoryMonitor;
    private TailingManager _tailingManager;
    private Task _monitorTask;
    private Task _tailingTask;
    private Channel<FileWorkItem> _workChannel;
    private Channel<ParsingResult> _outputChannel;
    private CancellationTokenSource _cts;

    public IngesterService(ILogger<IngesterService> logger)
    {
      _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("IngesterService starting.");

      // Load configuration from appsettings.json
      var configText = File.ReadAllText("src/LogTapestry.Ingester/appsettings.json");
      var configDoc = System.Text.Json.JsonDocument.Parse(configText);

      var settings = new IngesterSettings {
        Directory = configDoc.RootElement.GetProperty("Ingester").GetProperty("Directory").GetString() ?? "logs",
        IncludePatterns = configDoc.RootElement.GetProperty("Ingester").GetProperty("IncludePatterns").EnumerateArray().Select(x => x.GetString() ?? "*.log").ToList()
      };

      var pluginElem = configDoc.RootElement.GetProperty("Plugins")[0];
      var pluginSettings = System.Text.Json.JsonSerializer.Deserialize<PluginSettings>(pluginElem.GetRawText());

      var stateProvider = new SqliteStateProvider("state.db");

      _directoryMonitor = new DirectoryMonitor(settings, stateProvider);
      _tailingManager = new TailingManager(stateProvider, pluginSettings);

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
