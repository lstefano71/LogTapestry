// C#
using LogTapestry.Core;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Serilog;

namespace LogTapestry.Ingester;

public class Program
{
  public static async Task Main(string[] args)
  {
    bool validate = args.Contains("--validate");

    var builder = Host.CreateApplicationBuilder(args);

    // Configure Serilog
    builder.Services.AddSerilog((_, logConfig) => {
      logConfig
          .WriteTo.Console()
          .WriteTo.File("logs/ingester-.log", rollingInterval: Serilog.RollingInterval.Day)
          .WriteTo.File("logs/ingester-errors-.json", rollingInterval: Serilog.RollingInterval.Day, restrictedToMinimumLevel: Serilog.Events.LogEventLevel.Warning, formatProvider: null);
    });

    // Bind configuration
    builder.Services.Configure<LogTapestrySettings>(builder.Configuration.GetSection(""));

    // Compute database path from Ingester.DataRoot
    var configSection = builder.Configuration.GetSection("Ingester");
    var dataRoot = configSection["DataRoot"] ?? "data";
    var dbPath = Path.Combine(dataRoot, "state.sqlite");
    builder.Services.AddSingleton<IStateProvider>(_ => new SqliteStateProvider(dbPath));
    builder.Services.AddSingleton<DirectoryMonitor>();
    builder.Services.AddSingleton<TailingManager>();
    builder.Services.AddSingleton<DataSink>();

    // Register hosted services
    builder.Services.AddHostedService<IngesterService>();
    builder.Services.AddHostedService<MonitoringService>();

    // Enable Windows Service
    builder.Services.AddWindowsService(options => options.ServiceName = "LogTapestry Ingester");

    var host = builder.Build();

    if (validate) {
      try {
        var config = builder.Configuration;
        var directory = config.GetSection("Ingester:Directory").Value;
        if (string.IsNullOrEmpty(directory) || !Directory.Exists(directory)) {
          Console.WriteLine($"Validation failed: Directory '{directory}' does not exist.");
          Environment.Exit(1);
        }
        // Add more validation as needed
        Console.WriteLine("Configuration validation succeeded.");
        Environment.Exit(0);
      } catch (Exception ex) {
        Console.WriteLine($"Validation failed: {ex.Message}");
        Environment.Exit(1);
      }
    } else {
      await host.RunAsync();
    }
  }
}
