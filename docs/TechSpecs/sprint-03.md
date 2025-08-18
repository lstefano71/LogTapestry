## **LogTapestry: Technical Specification - Sprint 3**

**Version:** 1.0
**Date:** August 18, 2025
**Author:** Implementation Team Lead

### **1. Sprint Goal: Configuration and Operability**

The primary objective of this sprint is to make the LogTapestry Ingester a fully configurable, deployable, and monitorable background service. We will replace all hardcoded settings with a robust configuration system, implement the necessary hooks to run as a Windows Service, and add HTTP endpoints for health checks and performance metrics.

### **2. Scope and Objectives**

*   **Implement Configuration Loading:** The ingester must load all its settings from an external `appsettings.json` file.
*   **Integrate Dependency Injection (DI):** Refactor the application to use the standard .NET `IHostBuilder` and its DI container to manage object lifecycles and settings.
*   **Implement as a Hosted Service:** Create a main service class that implements `IHostedService` to control the application's startup and shutdown logic.
*   **Enable Windows Service Deployment:** Add the necessary components to allow the executable to be installed and run as a native Windows Service.
*   **Implement Health and Metrics Endpoints:** Expose two HTTP endpoints (`/health` and `/metrics`) for external monitoring.
*   **Finalize Logging Implementation:** Configure Serilog with structured, rolling file sinks based on the application's configuration.

### **3. Architectural Changes**

The application's entry point (`Program.cs`) will be completely refactored to use the Generic Host (`IHostBuilder`). This is a significant shift from the manual object creation in previous sprints.

*   **From:** Manual instantiation (`var monitor = new DirectoryMonitor(...)`)
*   **To:** A managed lifecycle using Dependency Injection (`builder.Services.AddSingleton<DirectoryMonitor>()`) and Hosted Services (`builder.Services.AddHostedService<IngesterService>()`).

This change provides a standard, robust framework for managing configuration, logging, and application lifetime.

### **4. Detailed Component Specifications**

#### **4.1. Configuration (`LogTapestry.Ingester`)**

*   **File:** `appsettings.json` will be the primary configuration source.
*   **Loading:** The `Host.CreateDefaultBuilder(args)` method will be used in `Program.cs`. This automatically sets up configuration loading from `appsettings.json`, environment variables, and command-line arguments.
*   **Model:** The C# classes defined in Sprint 2 (`LogTapestrySettings`, `IngesterSettings`, `PluginSettings`, etc.) will be bound to the configuration sections. This will be configured in `Program.cs`:
    ```csharp
    builder.Services.Configure<LogTapestrySettings>(builder.Configuration);
    ```
*   **Validation (`--validate` flag):**
    *   The `System.CommandLine` library will be used to parse command-line arguments.
    *   If the `--validate` flag is present, the application will:
        1.  Build the host configuration and DI container (`host.Build()`).
        2.  Attempt to resolve the main services from the container.
        3.  Perform explicit validation checks (e.g., `Directory.Exists(settings.Directory)`).
        4.  Print a success or failure message and exit immediately without running the host.

#### **4.2. Main Service and Hosting (`LogTapestry.Ingester`)**

*   **`Program.cs` Refactoring:**
    ```csharp
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var builder = Host.CreateApplicationBuilder(args);

            // 1. Configure Serilog
            builder.Logging.ClearProviders();
            builder.Services.AddSerilog(logConfig => { /* ... */ });

            // 2. Configure Settings
            builder.Services.Configure<LogTapestrySettings>(builder.Configuration);

            // 3. Register Services with DI Container
            builder.Services.AddSingleton<IStateProvider, SqliteStateProvider>();
            builder.Services.AddSingleton<DirectoryMonitor>();
            builder.Services.AddSingleton<TailingManager>();
            // ... other services ...

            // 4. Register Hosted Services
            builder.Services.AddHostedService<IngesterService>();
            builder.Services.AddHostedService<MonitoringService>(); // For HTTP endpoints

            // 5. Enable Windows Service
            builder.Services.AddWindowsService(options => options.ServiceName = "LogTapestry Ingester");

            var host = builder.Build();
            await host.RunAsync();
        }
    }
    ```
*   **Class:** `IngesterService : IHostedService`
    *   **Constructor:** Injects all major components (`DirectoryMonitor`, `TailingManager`, `ILogger`, etc.).
    *   **`StartAsync(CancellationToken token)`:**
        1.  Logs that the service is starting.
        2.  Creates the central `Channel<ParsingResult>`.
        3.  Starts the `DirectoryMonitor.RunAsync` and `TailingManager.RunAsync` tasks without awaiting them. These tasks will run for the lifetime of the application.
    *   **`StopAsync(CancellationToken token)`:**
        1.  Logs that the service is stopping.
        2.  Triggers the graceful shutdown sequence by cancelling the main `CancellationTokenSource` and awaiting the completion of the main pipeline tasks.

#### **4.3. Monitoring Endpoints (`LogTapestry.Ingester`)**

*   **Class:** `MonitoringService : IHostedService`
    *   **Responsibility:** Runs a minimal Kestrel web server in the background.
    *   **Implementation:**
        *   **`StartAsync`:** Builds and runs a `WebApplication` instance.
        *   **`StopAsync`:** Stops the `WebApplication`.
    *   **Endpoint:** `/health`
        *   **Logic:** The endpoint handler will inject the `IStateProvider` and `IOptions<IngesterSettings>`. It will perform the following checks:
            1.  Can it connect to the SQLite database and run a simple query (`PRAGMA quick_check`)?
            2.  Does the configured log directory exist (`Directory.Exists`)?
        *   **Response:** Returns a JSON object with `200 OK` on success or `503 Service Unavailable` on failure.
            *   *Success:* `{ "status": "Healthy", "timestamp": "..." }`
            *   *Failure:* `{ "status": "Unhealthy", "errors": ["Database check failed."] }`
    *   **Endpoint:** `/metrics`
        *   **Metrics Registration:** A static `Metrics` class will be created to define the `Meter` and `Counter<T>` objects from `System.Diagnostics.Metrics`.
            *   `public static readonly Counter<long> LogEntriesIngested;`
            *   `public static readonly Counter<long> BytesProcessed;`
        *   **Instrumentation:** The `DataSink` will be updated to call `Metrics.LogEntriesIngested.Add(batch.Length)` after each successful write.
        *   **Endpoint Logic:** The endpoint will be configured to use the `prometheus-net.AspNetCore` library to automatically scrape the registered metrics and return them in the Prometheus text exposition format.

#### **4.4. Logging Implementation**

*   **NuGet Packages:** `Serilog.Extensions.Hosting`, `Serilog.Sinks.File`, `Serilog.Sinks.Console`.
*   **Configuration:** The Serilog configuration will be done in `Program.cs`, reading parameters (like file paths and log levels) from the `appsettings.json` file.
*   **Sinks:**
    1.  **Console Sink:** For interactive debugging.
    2.  **Main File Sink:** A rolling file sink (`logs/ingester-.log`) writing `Information` level and above in a structured text format.
    3.  **Error File Sink:** A rolling file sink (`logs/ingester-errors-.json`) writing `Warning` level and above in a structured JSON format for machine-parseable error analysis.

### **5. Testing Strategy for Sprint 3**

*   **Unit Tests:**
    *   Test configuration model validation logic.
*   **Integration Tests:**
    *   **Test Case:** `Host_Should_Build_And_Start_Successfully_With_Valid_Config`
        *   **Setup:** Create a valid `appsettings.json` in the test output directory.
        *   **Act:** Build and run the `IHost` for a short duration (e.g., 2 seconds).
        *   **Assert:** Ensure no exceptions are thrown during startup.
    *   **Test Case:** `Health_Endpoint_Should_Return_Healthy_On_Success`
        *   **Setup:** Start the host in the background using `WebApplicationFactory`.
        *   **Act:** Use an `HttpClient` to send a `GET` request to the `/health` endpoint.
        *   **Assert:** Assert the status code is `200 OK` and the response body is as expected.
    *   **Test Case:** `Metrics_Endpoint_Should_Expose_Ingested_Count`
        *   **Setup:** Start the host. Manually push a few `LogEntry` objects through the `DataSink`.
        *   **Act:** Send a `GET` request to `/metrics`.
        *   **Assert:** Assert the response body is in the Prometheus format and contains a line like `log_entries_ingested_total 10`.

### **6. Definition of Done**

The sprint is complete when:
1.  All hardcoded settings are removed from the ingester and are loaded from `appsettings.json`.
2.  The application is fully managed by the .NET Generic Host, with all major components registered for Dependency Injection.
3.  The `IngesterService` correctly starts and stops the monitoring and tailing components.
4.  The application can be successfully installed and run as a Windows Service.
5.  A `GET` request to the `/health` endpoint returns a `200 OK` status and a valid JSON response in a healthy state.
6.  A `GET` request to the `/metrics` endpoint returns a valid Prometheus exposition, including the `log_entries_ingested_total` metric.
7.  Structured logs are correctly written to both the main and error rolling log files as configured.
8.  The new integration tests for configuration loading and the monitoring endpoints pass successfully.
