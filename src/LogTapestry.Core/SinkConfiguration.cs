using System.Text.Json.Serialization;

namespace LogTapestry.Core;

/// <summary>
/// Configuration for the multi-sink ingester system.
/// Defines which sinks are active, their settings, and filtering rules.
/// </summary>
public class SinkConfiguration
{
    /// <summary>
    /// Default sink assignment for entries that don't match any specific filters.
    /// </summary>
    public DefaultSinkConfig Default { get; set; } = new();

    /// <summary>
    /// Configuration definitions for all available sinks.
    /// </summary>
    public Dictionary<string, SinkDefinition> Configurations { get; set; } = new();

    /// <summary>
    /// Filtering rules to route specific entries to specific sinks.
    /// </summary>
    public List<SinkFilterRule> Filters { get; set; } = new();

    /// <summary>
    /// Global sink settings that apply to all sinks.
    /// </summary>
    public GlobalSinkSettings Global { get; set; } = new();
}

/// <summary>
/// Default configuration for entries that don't match any specific filters.
/// </summary>
public class DefaultSinkConfig
{
    /// <summary>
    /// Filter expression for the default sink assignment. "*" means all entries.
    /// </summary>
    public string FilterExpression { get; set; } = "*";

    /// <summary>
    /// Names of sinks that should receive entries by default.
    /// </summary>
    public List<string> Sinks { get; set; } = new() { "parquet" };

    /// <summary>
    /// Whether the default assignment is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;
}

/// <summary>
/// Definition of a sink including its type, settings, and operational parameters.
/// </summary>
public class SinkDefinition
{
    /// <summary>
    /// Type of the sink (e.g., "Parquet", "OpenTelemetry", "Custom").
    /// </summary>
    public string Type { get; set; } = "";

    /// <summary>
    /// Type-specific settings for the sink.
    /// </summary>
    public Dictionary<string, object> Settings { get; set; } = new();

    /// <summary>
    /// Whether this sink is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Priority of this sink (higher numbers = higher priority).
    /// Used for ordering when multiple sinks are configured.
    /// </summary>
    public int Priority { get; set; } = 0;

    /// <summary>
    /// Maximum number of retries for this sink.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Timeout for sink operations in seconds.
    /// </summary>
    public int TimeoutSeconds { get; set; } = 30;

    /// <summary>
    /// Whether this sink should participate in health checks.
    /// </summary>
    public bool HealthCheckEnabled { get; set; } = true;
}

/// <summary>
/// Filter rule that routes specific entries to specific sinks based on expressions.
/// </summary>
public class SinkFilterRule
{
    /// <summary>
    /// Unique name for this filter rule.
    /// </summary>
    public string Name { get; set; } = "";

    /// <summary>
    /// Filter expression that determines which entries match this rule.
    /// Supports field-based filtering (e.g., "Level == 'ERROR'", "Source.Contains('HighVolumeApp')").
    /// </summary>
    public string Expression { get; set; } = "";

    /// <summary>
    /// Names of sinks that should receive entries matching this filter.
    /// </summary>
    public List<string> Sinks { get; set; } = new();

    /// <summary>
    /// Whether this filter rule is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Priority of this filter rule (higher numbers = higher priority).
    /// Rules are evaluated in priority order.
    /// </summary>
    public int Priority { get; set; } = 0;

    /// <summary>
    /// Whether processing should stop after this rule matches (default: false).
    /// If true, subsequent filter rules are not evaluated for matching entries.
    /// </summary>
    public bool StopProcessing { get; set; } = false;
}

/// <summary>
/// Global settings that apply to all sinks.
/// </summary>
public class GlobalSinkSettings
{
    /// <summary>
    /// Maximum batch size for sink operations.
    /// </summary>
    public int MaxBatchSize { get; set; } = 1000;

    /// <summary>
    /// Maximum time to wait for a batch to fill before processing (seconds).
    /// </summary>
    public int BatchTimeoutSeconds { get; set; } = 5;

    /// <summary>
    /// Maximum number of concurrent sink operations.
    /// </summary>
    public int MaxConcurrency { get; set; } = 10;

    /// <summary>
    /// Whether to enable detailed sink metrics.
    /// </summary>
    public bool EnableMetrics { get; set; } = true;

    /// <summary>
    /// Circuit breaker failure threshold (applies to all sinks).
    /// </summary>
    public int CircuitBreakerThreshold { get; set; } = 5;

    /// <summary>
    /// Circuit breaker timeout in seconds.
    /// </summary>
    public int CircuitBreakerTimeoutSeconds { get; set; } = 60;
}

/// <summary>
/// Strongly-typed configuration classes for specific sink types.
/// </summary>
public static class SinkConfigurations
{
    /// <summary>
    /// Configuration for Parquet sinks.
    /// </summary>
    public class ParquetSinkConfig
    {
        public string DataRoot { get; set; } = "data";
        public string CompressionLevel { get; set; } = "Snappy";
        public long MaxFileSizeBytes { get; set; } = 128 * 1024 * 1024; // 128MB
        public string PartitionStrategy { get; set; } = "timestamp";
    }

    /// <summary>
    /// Configuration for OpenTelemetry sinks.
    /// </summary>
    public class OpenTelemetrySinkConfig
    {
        public string Endpoint { get; set; } = "http://localhost:4317";
        public string Protocol { get; set; } = "http";
        public int BatchSize { get; set; } = 1000;
        public int ExportTimeoutSeconds { get; set; } = 30;
        public Dictionary<string, string> Headers { get; set; } = new();
        public string ServiceName { get; set; } = "LogTapestry";
        public string ServiceVersion { get; set; } = "1.0.0";
    }

    /// <summary>
    /// Configuration for HTTP webhook sinks.
    /// </summary>
    public class HttpSinkConfig
    {
        public string Endpoint { get; set; } = "";
        public string Method { get; set; } = "POST";
        public Dictionary<string, string> Headers { get; set; } = new();
        public string ContentType { get; set; } = "application/json";
        public int TimeoutSeconds { get; set; } = 30;
        public bool VerifySsl { get; set; } = true;
    }
}

/// <summary>
/// Extension methods for working with sink configurations.
/// </summary>
public static class SinkConfigurationExtensions
{
    /// <summary>
    /// Gets a strongly-typed configuration for a specific sink.
    /// </summary>
    public static T GetSinkConfig<T>(this SinkDefinition sinkDefinition) where T : new()
    {
        var config = new T();
        
        if (sinkDefinition.Settings.Count == 0) {
            return config;
        }

        // Simple property mapping - in production, you'd use a proper mapper
        var configType = typeof(T);
        foreach (var setting in sinkDefinition.Settings) {
            var property = configType.GetProperty(setting.Key, System.Reflection.BindingFlags.IgnoreCase | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
            if (property != null && property.CanWrite) {
                try {
                    var convertedValue = Convert.ChangeType(setting.Value, property.PropertyType);
                    property.SetValue(config, convertedValue);
                } catch {
                    // Ignore conversion errors - use default value
                }
            }
        }

        return config;
    }

    /// <summary>
    /// Gets all enabled sink definitions ordered by priority.
    /// </summary>
    public static List<SinkDefinition> GetEnabledSinks(this SinkConfiguration config)
    {
        return config.Configurations
            .Where(kvp => kvp.Value.Enabled)
            .OrderByDescending(kvp => kvp.Value.Priority)
            .Select(kvp => kvp.Value)
            .ToList();
    }

    /// <summary>
    /// Gets all enabled filter rules ordered by priority.
    /// </summary>
    public static List<SinkFilterRule> GetEnabledFilters(this SinkConfiguration config)
    {
        return config.Filters
            .Where(f => f.Enabled)
            .OrderByDescending(f => f.Priority)
            .ToList();
    }

    /// <summary>
    /// Validates the sink configuration for common issues.
    /// </summary>
    public static List<string> Validate(this SinkConfiguration config)
    {
        var errors = new List<string>();

        // Check if at least one sink is configured
        if (config.Configurations.Count == 0) {
            errors.Add("No sinks configured");
        }

        // Check if default sinks exist
        foreach (var sinkName in config.Default.Sinks) {
            if (!config.Configurations.ContainsKey(sinkName)) {
                errors.Add($"Default sink '{sinkName}' is not defined in configurations");
            }
        }

        // Check filter rule references
        foreach (var filter in config.Filters) {
            foreach (var sinkName in filter.Sinks) {
                if (!config.Configurations.ContainsKey(sinkName)) {
                    errors.Add($"Filter '{filter.Name}' references undefined sink '{sinkName}'");
                }
            }

            if (string.IsNullOrWhiteSpace(filter.Expression)) {
                errors.Add($"Filter '{filter.Name}' has empty expression");
            }
        }

        // Check for duplicate filter names
        var duplicateFilters = config.Filters
            .GroupBy(f => f.Name)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key);

        foreach (var duplicate in duplicateFilters) {
            errors.Add($"Duplicate filter name: '{duplicate}'");
        }

        return errors;
    }
}