// C#
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace LogTapestry.Ingester
{
  public class IngesterService : IHostedService
  {
    private readonly ILogger<IngesterService> _logger;

    public IngesterService(ILogger<IngesterService> logger)
    {
      _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("IngesterService starting.");
      // TODO: Start DirectoryMonitor and TailingManager pipeline here
      return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
      _logger.LogInformation("IngesterService stopping.");
      // TODO: Graceful shutdown logic here
      return Task.CompletedTask;
    }
  }
}
