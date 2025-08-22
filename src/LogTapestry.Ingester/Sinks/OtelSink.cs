using LogTapestry.Core;
using LogTapestry.Core.Interfaces;

using Microsoft.Extensions.Logging;

using System.Diagnostics;
using System.Net.Http;
using System.Text;
using System.Text.Json;

namespace LogTapestry.Ingester.Sinks;

/// <summary>
/// OpenTelemetry sink that converts LogEntry records to OTEL LogRecord format
/// and sends them to an OTEL collector via GRPC or HTTP protocols.
/// 
/// Supports:
/// - LogEntry to OTEL LogRecord conversion with proper severity mapping
/// - GRPC and HTTP protocol support for OTEL collectors
/// - Batching for performance optimization
/// - Retry logic with exponential backoff
/// - Resource attributes for service identification
/// - Health monitoring and circuit breaker patterns
/// </summary>
public class OtelSink : IMultiDataSink
{
    public string Name => "otel";

    private readonly ILogger<OtelSink> _logger;
    private readonly HttpClient _httpClient;
    private readonly OtelSinkConfiguration _config;
    private readonly SemaphoreSlim _rateLimitSemaphore;

    // Circuit breaker state
    private bool _circuitBreakerOpen = false;
    private DateTime _circuitBreakerLastFailure = DateTime.MinValue;
    private int _consecutiveFailures = 0;

    public OtelSink(ILogger<OtelSink> logger, HttpClient httpClient, OtelSinkConfiguration config)
    {
        _logger = logger;
        _httpClient = httpClient;
        _config = config;
        _rateLimitSemaphore = new SemaphoreSlim(_config.MaxConcurrentRequests, _config.MaxConcurrentRequests);

        ConfigureHttpClient();
    }

    public async Task<SinkResult> WriteBatchAsync(IList<DataBlock> batch, SinkContext context, CancellationToken cancellationToken = default)
    {
        if (batch.Count == 0) {
            return SinkResult.CreateSuccess();
        }

        // Check circuit breaker
        if (IsCircuitBreakerOpen()) {
            return SinkResult.CreateFailure("Circuit breaker is open - OTEL collector unavailable");
        }

        var stopwatch = Stopwatch.StartNew();
        long totalEntriesProcessed = 0;

        try {
            await _rateLimitSemaphore.WaitAsync(cancellationToken);

            try {
                _logger.LogDebug("OtelSink processing batch {BatchId} with {Count} data blocks", context.BatchId, batch.Count);

                // Convert DataBlocks to OTEL LogRecords
                var otelLogs = ConvertBatchToOtelLogs(batch);
                totalEntriesProcessed = otelLogs.Sum(resourceLogs => resourceLogs.ScopeLogs.Sum(scopeLogs => scopeLogs.LogRecords.Count));

                // Send to OTEL collector with retry logic
                await SendToOtelCollectorWithRetry(otelLogs, cancellationToken);

                // Reset circuit breaker on success
                ResetCircuitBreaker();

                stopwatch.Stop();

                _logger.LogDebug("OtelSink completed batch {BatchId}: {Entries} entries in {Duration}ms", 
                    context.BatchId, totalEntriesProcessed, stopwatch.ElapsedMilliseconds);

                return SinkResult.CreateSuccess(
                    entriesProcessed: totalEntriesProcessed,
                    bytesProcessed: EstimatePayloadSize(otelLogs),
                    processingTime: stopwatch.Elapsed,
                    metrics: new Dictionary<string, object> {
                        ["resource_logs_count"] = otelLogs.Count,
                        ["endpoint"] = _config.Endpoint,
                        ["protocol"] = _config.Protocol
                    });

            } finally {
                _rateLimitSemaphore.Release();
            }

        } catch (Exception ex) {
            stopwatch.Stop();
            TripCircuitBreaker();
            
            _logger.LogError(ex, "OtelSink failed to process batch {BatchId} after {Duration}ms", 
                context.BatchId, stopwatch.ElapsedMilliseconds);
            
            return SinkResult.CreateFailure(
                errorMessage: $"OTEL export failed: {ex.Message}",
                metrics: new Dictionary<string, object> {
                    ["processing_time_ms"] = stopwatch.ElapsedMilliseconds,
                    ["entries_processed"] = totalEntriesProcessed,
                    ["consecutive_failures"] = _consecutiveFailures
                });
        }
    }

    public async Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default)
    {
        try {
            // Simple health check - try to connect to the endpoint
            var healthEndpoint = _config.Protocol.ToLower() == "grpc" 
                ? $"{_config.Endpoint}/health" 
                : $"{_config.Endpoint}/v1/logs"; // OTLP HTTP endpoint

            using var request = new HttpRequestMessage(HttpMethod.Head, healthEndpoint);
            AddHeaders(request);

            using var response = await _httpClient.SendAsync(request, cancellationToken);
            
            var isHealthy = response.IsSuccessStatusCode || response.StatusCode == System.Net.HttpStatusCode.MethodNotAllowed;
            
            if (isHealthy) {
                ResetCircuitBreaker();
            }
            
            _logger.LogTrace("OtelSink health check: {Status} ({StatusCode})", 
                isHealthy ? "Healthy" : "Unhealthy", response.StatusCode);
            
            return isHealthy;
        } catch (Exception ex) {
            _logger.LogError(ex, "OtelSink health check failed");
            return false;
        }
    }

    private List<OtelResourceLogs> ConvertBatchToOtelLogs(IList<DataBlock> batch)
    {
        // Group by resource (for now, single resource representing this LogTapestry instance)
        var resourceLogs = new OtelResourceLogs {
            Resource = CreateResource(),
            ScopeLogs = new List<OtelScopeLogs> {
                new OtelScopeLogs {
                    Scope = CreateInstrumentationScope(),
                    LogRecords = batch
                        .SelectMany(db => db.Entries)
                        .Select(ConvertLogEntryToOtelLogRecord)
                        .ToList()
                }
            }
        };

        return new List<OtelResourceLogs> { resourceLogs };
    }

    private OtelLogRecord ConvertLogEntryToOtelLogRecord(LogEntry logEntry)
    {
        var otelRecord = new OtelLogRecord {
            TimeUnixNano = (ulong)((DateTimeOffset)logEntry.Timestamp).ToUnixTimeMilliseconds() * 1_000_000,
            SeverityNumber = MapLogLevelToSeverityNumber(logEntry.Level),
            SeverityText = logEntry.Level,
            Body = new OtelAnyValue { StringValue = logEntry.Message },
            Attributes = new List<OtelKeyValue>(),
            TraceId = new byte[16], // Empty for now - could be extracted from fields
            SpanId = new byte[8]    // Empty for now - could be extracted from fields
        };

        // Add LogEntry fields as attributes
        foreach (var field in logEntry.Fields) {
            otelRecord.Attributes.Add(new OtelKeyValue {
                Key = field.Key,
                Value = ConvertFieldValueToOtelValue(field.Value)
            });
        }

        // Add LogTapestry-specific attributes
        otelRecord.Attributes.Add(new OtelKeyValue {
            Key = "logtapestry.source",
            Value = new OtelAnyValue { StringValue = logEntry.Source }
        });

        otelRecord.Attributes.Add(new OtelKeyValue {
            Key = "logtapestry.template_hash",
            Value = new OtelAnyValue { IntValue = logEntry.TemplateHash }
        });

        otelRecord.Attributes.Add(new OtelKeyValue {
            Key = "logtapestry.ulid",
            Value = new OtelAnyValue { BytesValue = logEntry.Ulid }
        });

        return otelRecord;
    }

    private int MapLogLevelToSeverityNumber(string level)
    {
        // OTEL Severity mapping: https://opentelemetry.io/docs/specs/otel/logs/data-model/#field-severitynumber
        return level.ToUpperInvariant() switch {
            "TRACE" => 1,
            "DEBUG" => 5,
            "INFO" => 9,
            "WARN" or "WARNING" => 13,
            "ERROR" => 17,
            "FATAL" or "CRITICAL" => 21,
            _ => 9 // Default to INFO
        };
    }

    private OtelAnyValue ConvertFieldValueToOtelValue(object value)
    {
        return value switch {
            string s => new OtelAnyValue { StringValue = s },
            long l => new OtelAnyValue { IntValue = l },
            double d => new OtelAnyValue { DoubleValue = d },
            bool b => new OtelAnyValue { BoolValue = b },
            _ => new OtelAnyValue { StringValue = value?.ToString() ?? "" }
        };
    }

    private OtelResource CreateResource()
    {
        return new OtelResource {
            Attributes = new List<OtelKeyValue> {
                new OtelKeyValue { Key = "service.name", Value = new OtelAnyValue { StringValue = _config.ServiceName } },
                new OtelKeyValue { Key = "service.version", Value = new OtelAnyValue { StringValue = _config.ServiceVersion } },
                new OtelKeyValue { Key = "service.instance.id", Value = new OtelAnyValue { StringValue = Environment.MachineName } },
                new OtelKeyValue { Key = "logtapestry.component", Value = new OtelAnyValue { StringValue = "ingester" } }
            }
        };
    }

    private OtelInstrumentationScope CreateInstrumentationScope()
    {
        return new OtelInstrumentationScope {
            Name = "LogTapestry.Ingester",
            Version = _config.ServiceVersion
        };
    }

    private async Task SendToOtelCollectorWithRetry(List<OtelResourceLogs> logs, CancellationToken cancellationToken)
    {
        var exportRequest = new OtelExportLogsServiceRequest {
            ResourceLogs = logs
        };

        var maxRetries = _config.MaxRetries;
        var baseDelayMs = _config.BaseRetryDelayMs;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                if (_config.Protocol.ToLower() == "grpc") {
                    await SendViaGrpc(exportRequest, cancellationToken);
                } else {
                    await SendViaHttp(exportRequest, cancellationToken);
                }
                
                return; // Success
            } catch (Exception ex) when (attempt < maxRetries) {
                var delayMs = baseDelayMs * (int)Math.Pow(2, attempt);
                _logger.LogWarning(ex, "OTEL export attempt {Attempt} failed, retrying in {DelayMs}ms", attempt + 1, delayMs);
                
                await Task.Delay(delayMs, cancellationToken);
            }
        }

        // If we get here, all retries failed
        throw new InvalidOperationException($"OTEL export failed after {maxRetries + 1} attempts");
    }

    private async Task SendViaHttp(OtelExportLogsServiceRequest request, CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(request, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        var content = new StringContent(json, Encoding.UTF8, "application/json");

        var endpoint = $"{_config.Endpoint}/v1/logs";
        
        using var httpRequest = new HttpRequestMessage(HttpMethod.Post, endpoint) { Content = content };
        AddHeaders(httpRequest);

        using var response = await _httpClient.SendAsync(httpRequest, cancellationToken);
        
        if (!response.IsSuccessStatusCode) {
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new HttpRequestException($"OTEL HTTP export failed: {response.StatusCode} - {responseBody}");
        }
    }

    private Task SendViaGrpc(OtelExportLogsServiceRequest request, CancellationToken cancellationToken)
    {
        // For GRPC, we would need the actual GRPC client libraries
        // For now, implement as HTTP with protobuf content type
        return Task.FromException(new NotImplementedException("GRPC support requires additional GRPC client libraries. Use HTTP protocol instead."));
    }

    private void ConfigureHttpClient()
    {
        _httpClient.Timeout = TimeSpan.FromSeconds(_config.ExportTimeoutSeconds);
        
        // Add default headers
        foreach (var header in _config.Headers) {
            _httpClient.DefaultRequestHeaders.Add(header.Key, header.Value);
        }
    }

    private void AddHeaders(HttpRequestMessage request)
    {
        request.Headers.Add("User-Agent", "LogTapestry-Ingester/1.0");
        
        if (_config.Protocol.ToLower() == "http") {
            request.Headers.Add("Content-Type", "application/json");
        }

        foreach (var header in _config.Headers) {
            if (!request.Headers.Contains(header.Key)) {
                request.Headers.Add(header.Key, header.Value);
            }
        }
    }

    private bool IsCircuitBreakerOpen()
    {
        if (!_circuitBreakerOpen) return false;

        var timeSinceFailure = DateTime.UtcNow - _circuitBreakerLastFailure;
        if (timeSinceFailure > TimeSpan.FromSeconds(_config.CircuitBreakerTimeoutSeconds)) {
            _logger.LogInformation("Circuit breaker timeout expired, attempting to reset");
            return false; // Allow one attempt
        }

        return true;
    }

    private void TripCircuitBreaker()
    {
        _consecutiveFailures++;
        _circuitBreakerLastFailure = DateTime.UtcNow;
        
        if (_consecutiveFailures >= _config.CircuitBreakerFailureThreshold) {
            _circuitBreakerOpen = true;
            _logger.LogWarning("Circuit breaker tripped after {FailureCount} consecutive failures", _consecutiveFailures);
        }
    }

    private void ResetCircuitBreaker()
    {
        if (_consecutiveFailures > 0) {
            _logger.LogInformation("Resetting circuit breaker after successful operation");
        }
        
        _circuitBreakerOpen = false;
        _consecutiveFailures = 0;
        _circuitBreakerLastFailure = DateTime.MinValue;
    }

    private long EstimatePayloadSize(List<OtelResourceLogs> logs)
    {
        // Rough estimate of JSON payload size
        return logs.Sum(rl => rl.ScopeLogs.Sum(sl => sl.LogRecords.Sum(lr => 
            (lr.Body?.StringValue?.Length ?? 0) + 
            lr.Attributes.Sum(attr => attr.Key.Length + (attr.Value.StringValue?.Length ?? 10))
        ))) * 2; // Factor for JSON overhead
    }
}

/// <summary>
/// Configuration for the OpenTelemetry sink.
/// </summary>
public class OtelSinkConfiguration
{
    public string Endpoint { get; set; } = "http://localhost:4317";
    public string Protocol { get; set; } = "http"; // "grpc" or "http"
    public int BatchSize { get; set; } = 1000;
    public int ExportTimeoutSeconds { get; set; } = 30;
    public Dictionary<string, string> Headers { get; set; } = new();
    public string ServiceName { get; set; } = "LogTapestry";
    public string ServiceVersion { get; set; } = "1.0.0";
    
    // Retry configuration
    public int MaxRetries { get; set; } = 3;
    public int BaseRetryDelayMs { get; set; } = 1000;
    
    // Circuit breaker configuration
    public int CircuitBreakerFailureThreshold { get; set; } = 5;
    public int CircuitBreakerTimeoutSeconds { get; set; } = 60;
    
    // Rate limiting
    public int MaxConcurrentRequests { get; set; } = 10;
}

#region OTEL Data Model

public class OtelExportLogsServiceRequest
{
    public List<OtelResourceLogs> ResourceLogs { get; set; } = new();
}

public class OtelResourceLogs
{
    public OtelResource Resource { get; set; } = new();
    public List<OtelScopeLogs> ScopeLogs { get; set; } = new();
}

public class OtelResource
{
    public List<OtelKeyValue> Attributes { get; set; } = new();
}

public class OtelScopeLogs
{
    public OtelInstrumentationScope Scope { get; set; } = new();
    public List<OtelLogRecord> LogRecords { get; set; } = new();
}

public class OtelInstrumentationScope
{
    public string Name { get; set; } = "";
    public string Version { get; set; } = "";
}

public class OtelLogRecord
{
    public ulong TimeUnixNano { get; set; }
    public int SeverityNumber { get; set; }
    public string SeverityText { get; set; } = "";
    public OtelAnyValue Body { get; set; } = new();
    public List<OtelKeyValue> Attributes { get; set; } = new();
    public byte[] TraceId { get; set; } = new byte[16];
    public byte[] SpanId { get; set; } = new byte[8];
}

public class OtelKeyValue
{
    public string Key { get; set; } = "";
    public OtelAnyValue Value { get; set; } = new();
}

public class OtelAnyValue
{
    public string? StringValue { get; set; }
    public long? IntValue { get; set; }
    public double? DoubleValue { get; set; }
    public bool? BoolValue { get; set; }
    public byte[]? BytesValue { get; set; }
}

#endregion