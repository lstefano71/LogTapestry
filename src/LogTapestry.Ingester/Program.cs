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
    bool zapDb = args.Contains("--zap-db");

    var exeDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

    // Early configuration for Serilog so zap-db logs are visible
    var configBuilder = new ConfigurationBuilder()
      .SetBasePath(exeDir!)
      .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
    var configuration = configBuilder.Build();
    var configSection = configuration.GetSection("Ingester");
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
    Log.Logger = new LoggerConfiguration()
      .MinimumLevel.Is(logLevel)
      .Enrich.With(new UtcTimestampEnricher())
      .Enrich.With(new ManagedThreadIdEnricher())
      .WriteTo.Console(restrictedToMinimumLevel: logLevel, outputTemplate: "{UtcTimestamp} [{Level:u3}] [T{ManagedThreadId}] {Message:lj}{NewLine}{Exception}")
      .WriteTo.File("logs/ingester-.log", rollingInterval: RollingInterval.Day, outputTemplate: "{UtcTimestamp} [{Level:u3}] [T{ManagedThreadId}] {Message:lj}{NewLine}{Exception}")
      .WriteTo.File("logs/ingester-errors-.json", rollingInterval: RollingInterval.Day, restrictedToMinimumLevel: Serilog.Events.LogEventLevel.Warning, formatProvider: null)
      .CreateLogger();

    // Compute database path from Ingester.DataRoot
    var dataRoot = configSection["DataRoot"] ?? "data";
    var dbPath = Path.Combine(dataRoot, "state.sqlite");

    // Zap DB directory if requested
    if (zapDb) {
      ZapDbDirectory(dataRoot);
    }

    var builder = Host.CreateApplicationBuilder(args);

    // Clear default configuration sources and add appsettings.json from exe directory
    builder.Configuration.Sources.Clear();
    builder.Configuration.AddJsonFile(
      Path.Combine(exeDir!, "appsettings.json"),
      optional: true,
      reloadOnChange: true
    );

    // Configure Serilog for DI
    builder.Services.AddSerilog();

    // Bind configuration
    builder.Services.Configure<LogTapestrySettings>(builder.Configuration); // Bind to root
    builder.Services.Configure<IngesterSettings>(builder.Configuration.GetSection("Ingester"));

    builder.Services.AddOptions<IngesterSettings>()
        .Bind(builder.Configuration.GetSection("Ingester"))
        .ValidateDataAnnotations();
    builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<PluginSettings>>().Value);

    // Register state providers
    builder.Services.AddSingleton<SqliteStateProvider>(_ => new SqliteStateProvider(dbPath));
    builder.Services.AddSingleton<IStateProvider>(sp => sp.GetRequiredService<SqliteStateProvider>());
    builder.Services.AddSingleton<LiveStateService>();

    // Register PriorityMonitor, InitialScanProducer, and WatcherProducer for DI
    builder.Services.AddSingleton<PriorityMonitor>(sp =>
        new PriorityMonitor(sp.GetRequiredService<ILogger<PriorityMonitor>>()));
    builder.Services.AddSingleton<InitialScanProducer>(sp =>
        new InitialScanProducer(
            sp.GetRequiredService<ILogger<InitialScanProducer>>(),
            sp.GetRequiredService<IOptions<IngesterSettings>>(),
            sp.GetRequiredService<IStateProvider>()));
    builder.Services.AddSingleton<WatcherProducer>(sp =>
        new WatcherProducer(
            sp.GetRequiredService<ILogger<WatcherProducer>>(),
            sp.GetRequiredService<IOptions<IngesterSettings>>()));

    // Register directory monitor with injected dependencies
    builder.Services.AddSingleton<DirectoryMonitor>(sp => new DirectoryMonitor(
        sp.GetRequiredService<ILogger<DirectoryMonitor>>(),
        sp.GetRequiredService<IOptions<IngesterSettings>>(),
        sp.GetRequiredService<IStateProvider>(),
        sp.GetRequiredService<PriorityMonitor>(),
        sp.GetRequiredService<InitialScanProducer>(),
        sp.GetRequiredService<WatcherProducer>()
    ));

    // Register checkpointing data sink
    builder.Services.AddSingleton<CheckpointDataSink>();

    // Register hosted services
    builder.Services.AddHostedService<IngesterService>();
    builder.Services.AddHostedService<MonitoringService>();
    builder.Services.AddHostedService<LiveStateService>();
    builder.Services.AddHostedService<StateWriterService>();
    builder.Services.AddSingleton<StateWriterService>();

    // Enable Windows Service
    builder.Services.AddWindowsService(options => options.ServiceName = "LogTapestry Ingester");

    // Register other services
    builder.Services.AddSingleton<FileReader>();

    var host = builder.Build();

    if (validate) {
      try {
        var config = builder.Configuration;
        var directory = config.GetSection("Ingester:Directory").Value;
        if (string.IsNullOrEmpty(directory) || !Directory.Exists(directory)) {
          Log.Error("Validation failed: Directory '{Directory}' does not exist.", directory);
          Environment.Exit(1);
        }
        // Add more validation as needed
        Log.Information("Configuration validation succeeded.");
        Environment.Exit(0);
      } catch (Exception ex) {
        Log.Error(ex, "Validation failed: {Message}", ex.Message);
        Environment.Exit(1);
      }
    } else {
      await host.RunAsync();
    }
  }

  private static void ZapDbDirectory(string dataRoot)
  {
    try {
      if (Directory.Exists(dataRoot)) {
        foreach (var file in Directory.GetFiles(dataRoot)) {
          File.Delete(file);
        }
        foreach (var dir in Directory.GetDirectories(dataRoot)) {
          Directory.Delete(dir, true);
        }
        Log.Information("[--zap-db] Deleted all contents of data DB directory: {DataRoot}", dataRoot);
      } else {
        Log.Warning("[--zap-db] Data DB directory does not exist: {DataRoot}", dataRoot);
      }
    } catch (Exception ex) {
      Log.Error(ex, "[--zap-db] Error deleting data DB directory contents: {Message}", ex.Message);
    }
  }
}
