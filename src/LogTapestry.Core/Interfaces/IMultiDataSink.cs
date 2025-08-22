using LogTapestry.Core;

namespace LogTapestry.Core.Interfaces;

/// <summary>
/// Enhanced data sink interface for the multi-sink architecture.
/// Replaces the legacy IDataSink interface with support for batching,
/// proper error handling, and health monitoring.
/// </summary>
public interface IMultiDataSink
{
    /// <summary>
    /// Unique name identifying this sink instance.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Processes a batch of DataBlocks asynchronously.
    /// This is the primary method for data ingestion.
    /// </summary>
    /// <param name="batch">Collection of DataBlocks to process</param>
    /// <param name="context">Sink context with metadata and configuration</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>SinkResult indicating success/failure with metrics</returns>
    Task<SinkResult> WriteBatchAsync(
        IList<DataBlock> batch, 
        SinkContext context,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Performs a health check on the sink to verify it's operational.
    /// Used for circuit breaker patterns and monitoring.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if sink is healthy, false otherwise</returns>
    Task<bool> HealthCheckAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Result returned by sink operations indicating success/failure and metrics.
/// </summary>
public class SinkResult
{
    /// <summary>
    /// Whether the operation was successful.
    /// </summary>
    public bool Success { get; init; }

    /// <summary>
    /// Error message if the operation failed.
    /// </summary>
    public string? ErrorMessage { get; init; }

    /// <summary>
    /// Optional metrics collected during the operation.
    /// Can include timing, byte counts, record counts, etc.
    /// </summary>
    public Dictionary<string, object>? Metrics { get; init; }

    /// <summary>
    /// Number of entries successfully processed.
    /// </summary>
    public long EntriesProcessed { get; init; }

    /// <summary>
    /// Number of bytes processed.
    /// </summary>
    public long BytesProcessed { get; init; }

    /// <summary>
    /// Duration of the operation.
    /// </summary>
    public TimeSpan ProcessingTime { get; init; }

    /// <summary>
    /// Creates a successful result.
    /// </summary>
    public static SinkResult CreateSuccess(long entriesProcessed = 0, long bytesProcessed = 0, TimeSpan processingTime = default, Dictionary<string, object>? metrics = null)
    {
        return new SinkResult
        {
            Success = true,
            EntriesProcessed = entriesProcessed,
            BytesProcessed = bytesProcessed,
            ProcessingTime = processingTime,
            Metrics = metrics
        };
    }

    /// <summary>
    /// Creates a failed result.
    /// </summary>
    public static SinkResult CreateFailure(string errorMessage, Dictionary<string, object>? metrics = null)
    {
        return new SinkResult
        {
            Success = false,
            ErrorMessage = errorMessage,
            Metrics = metrics
        };
    }
}

/// <summary>
/// Context information passed to sinks during processing.
/// Contains metadata and configuration specific to the current operation.
/// </summary>
public class SinkContext
{
    /// <summary>
    /// Partition path for this batch (used by Parquet sink).
    /// </summary>
    public string? PartitionPath { get; init; }

    /// <summary>
    /// Additional properties that can be used by sink implementations.
    /// </summary>
    public Dictionary<string, object> Properties { get; init; } = new();

    /// <summary>
    /// Batch identifier for tracking purposes.
    /// </summary>
    public string? BatchId { get; init; }

    /// <summary>
    /// Timestamp when the batch was created.
    /// </summary>
    public DateTime BatchTimestamp { get; init; } = DateTime.UtcNow;
}

/// <summary>
/// Assignment of a DataBlock batch to specific sinks after filtering.
/// Used by the SinkFilter to determine which sinks should process which data.
/// </summary>
public class SinkAssignment
{
    /// <summary>
    /// The DataBlocks to be processed.
    /// </summary>
    public IList<DataBlock> DataBlocks { get; init; } = new List<DataBlock>();

    /// <summary>
    /// Names of sinks that should process this batch.
    /// </summary>
    public IList<string> SinkNames { get; init; } = new List<string>();

    /// <summary>
    /// Context for this specific assignment.
    /// </summary>
    public SinkContext Context { get; init; } = new();
}