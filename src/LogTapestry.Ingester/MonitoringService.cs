// C#
using LogTapestry.Core;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using static Microsoft.AspNetCore.Http.Results;

namespace LogTapestry.Ingester
{
  public class MonitoringService : IHostedService
  {
    private readonly ILogger<MonitoringService> _logger;
    private readonly IStateProvider _stateProvider;
    private readonly IngesterSettings _settings;

    public MonitoringService(ILogger<MonitoringService> logger, IStateProvider stateProvider, Microsoft.Extensions.Options.IOptions<IngesterSettings> options)
    {
      _logger = logger;
      _stateProvider = stateProvider;
      _settings = options.Value;
    }

    private WebApplication? _webApp;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("MonitoringService starting.");

      var builder = WebApplication.CreateBuilder();
      builder.WebHost.UseUrls("http://localhost:8080");

      var app = builder.Build();

      app.MapGet("/health", () => {
        bool dbHealthy;
        bool dirHealthy;
        try {
          dbHealthy = _stateProvider.CheckHealth();
        } catch {
          dbHealthy = false;
        }
        dirHealthy = Directory.Exists(_settings.Directory);

        if (dbHealthy && dirHealthy) {
          return Results.Json(new { status = "Healthy", timestamp = DateTime.UtcNow });
        } else {
          var errors = new List<string>();
          if (!dbHealthy) errors.Add("Database check failed.");
          if (!dirHealthy) errors.Add("Directory check failed.");
          return Results.Json(new { status = "Unhealthy", errors }, statusCode: 503);
        }
      });

      app.MapGet("/metrics", () => {
        var ingested = DataSink.Metrics.LogEntriesIngested;
        var bytes = DataSink.Metrics.BytesProcessed;
        var metrics = "# HELP log_entries_ingested_total Total log entries ingested\n" +
                      "# TYPE log_entries_ingested_total counter\n" +
                      $"log_entries_ingested_total {ingested}\n" +
                      "# HELP bytes_processed_total Total bytes processed\n" +
                      "# TYPE bytes_processed_total counter\n" +
                      $"bytes_processed_total {bytes}\n";
        return Text(metrics);
      });

      _webApp = app;
      await app.StartAsync(cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("MonitoringService stopping.");
      if (_webApp != null) {
        await _webApp.StopAsync(cancellationToken);
      }
    }
  }
}
