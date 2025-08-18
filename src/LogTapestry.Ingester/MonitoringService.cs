// C#
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

    public MonitoringService(ILogger<MonitoringService> logger)
    {
      _logger = logger;
    }

    private WebApplication? _webApp;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("MonitoringService starting.");

      var builder = WebApplication.CreateBuilder();
      builder.WebHost.UseUrls("http://localhost:8080");

      var app = builder.Build();

      app.MapGet("/health", () => {
        // TODO: Inject IStateProvider and IngesterSettings for real checks
        var dbHealthy = true; // Replace with actual DB check
        var dirHealthy = true; // Replace with actual directory check
        if (dbHealthy && dirHealthy) {
          return Results.Json(new { status = "Healthy", timestamp = DateTime.UtcNow });
        } else {
          return Results.Json(new { status = "Unhealthy", errors = new[] { "Database or directory check failed." } }, statusCode: 503);
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
