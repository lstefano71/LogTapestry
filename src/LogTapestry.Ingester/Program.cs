// C#
using LogTapestry.Core;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Serilog;

using System.Reflection;

namespace LogTapestry.Ingester;

public class Program
{
  public static async Task Main(string[] args)
  {
    bool validate = args.Contains("--validate");

    var exeDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

    var builder = Host.CreateApplicationBuilder(args);

    // Clear default configuration sources and add appsettings.json from exe directory
    builder.Configuration.Sources.Clear();
    builder.Configuration.AddJsonFile(
      Path.Combine(exeDir!, "appsettings.json"),
      optional: true,
      reloadOnChange: true
    );

    // Configure Serilog
    var configSection = builder.Configuration.GetSection("Ingester");
    var logLevelStr = configSection["LogLevel"] ?? "Information";
    var logLevel = logLevelStr switch {
      "Verbose" => Serilog.Events.LogEventLevel.Verbose,
      "Debug" => Serilog.Events.LogEventLevel.Debug,
      "Information" => Serilog.Events.LogEventLevel.Information,
      "Warning" => Serilog.Events.LogEventLevel.Warning,
      "Error" => Serilog.Events.LogEventLevel.Error,
      "Fatal" => Serilog.Events.LogEventLevel.Fatal,
      _ => Serilog.Events.LogEventLevel.Information
    };
    builder.Services.AddSerilog((_, logConfig) => {
      logConfig
          .MinimumLevel.Is(logLevel)
          .Enrich.With(new UtcTimestampEnricher())
          .WriteTo.Console(restrictedToMinimumLevel: logLevel, outputTemplate: "{UtcTimestamp} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
          .WriteTo.File("logs/ingester-.log", rollingInterval: Serilog.RollingInterval.Day)
          .WriteTo.File("logs/ingester-errors-.json", rollingInterval: Serilog.RollingInterval.Day, restrictedToMinimumLevel: Serilog.Events.LogEventLevel.Warning, formatProvider: null);
    });

    // Bind configuration
    builder.Services.Configure<LogTapestrySettings>(builder.Configuration.GetSection(""));
    builder.Services.Configure<IngesterSettings>(builder.Configuration.GetSection("Ingester"));
    builder.Services.Configure<PluginSettings>(builder.Configuration.GetSection("Plugins:0"));
    builder.Services.AddOptions<IngesterSettings>()
        .Bind(builder.Configuration.GetSection("Ingester"))
        .ValidateDataAnnotations();
    builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<PluginSettings>>().Value);

    // Compute database path from Ingester.DataRoot
    var dataRoot = configSection["DataRoot"] ?? "data";
    var dbPath = Path.Combine(dataRoot, "state.sqlite");
    builder.Services.AddSingleton<IStateProvider>(_ => new SqliteStateProvider(dbPath));
    builder.Services.AddSingleton<DirectoryMonitor>(sp => new DirectoryMonitor(
      sp.GetRequiredService<IOptions<IngesterSettings>>(),
      sp.GetRequiredService<IStateProvider>(),
      sp.GetRequiredService<ILoggerFactory>()
    ));
    builder.Services.AddSingleton<TailingManager>(sp => new TailingManager(
      sp.GetRequiredService<IStateProvider>(),
      builder.Configuration.GetSection("Plugins").Get<List<PluginSettings>>() ?? [],
      sp.GetRequiredService<ILoggerFactory>(),
      sp.GetRequiredService<IOptions<IngesterSettings>>().Value
    ));
    builder.Services.AddSingleton<DataSink>(sp => new DataSink(sp.GetRequiredService<ILogger<DataSink>>()));

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
