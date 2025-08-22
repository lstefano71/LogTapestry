using LogTapestry.Core;
using LogTapestry.Core.Interfaces;

using Microsoft.Extensions.Logging;

using System.Threading.Channels;

namespace LogTapestry.Ingester;

/// <summary>
/// Multi-sink processor that coordinates data persistence across multiple configured sinks
/// while preserving the critical checkpointing guarantees from the original CheckpointDataSink.
/// 
/// This replaces CheckpointDataSink as the main coordinator but maintains the same atomicity
/// guarantees: file positions are only updated after ALL applicable sinks successfully process the data.
/// </summary>
public class MultiSinkProcessor(
    ILogger<MultiSinkProcessor> logger,
    LiveStateService liveStateService,
    SinkExecutor sinkExecutor,
    CheckpointManager checkpointManager,
    SinkFilter sinkFilter) : IDataSink
{
    // Metrics instrumentation (preserves existing interface)
    public static class Metrics
    {
        public static long LogEntriesIngested = 0;
        public static long BytesProcessed = 0;
        public static long CheckpointsCompleted = 0;
        public static long SinkExecutions = 0;
        public static long FailedBatches = 0;
    }

    private readonly ILogger<MultiSinkProcessor> _logger = logger;
    private readonly LiveStateService _liveStateService = liveStateService;
    private readonly SinkExecutor _sinkExecutor = sinkExecutor;
    private readonly CheckpointManager _checkpointManager = checkpointManager;
    private readonly SinkFilter _sinkFilter = sinkFilter;

    // Channel for emitting checkpoint position updates after successful data persistence
    // This preserves the existing interface for compatibility with StateWriterService
    private readonly Channel<CheckpointPositionUpdate> _checkpointChannel = Channel.CreateBounded<CheckpointPositionUpdate>(10);

    public ChannelReader<CheckpointPositionUpdate> CheckpointReader => _checkpointChannel.Reader;
    ChannelWriter<CheckpointPositionUpdate> CheckpointWriter => _checkpointChannel.Writer;

    /// <summary>
    /// Main entry point for coordinated multi-sink batch write and checkpoint update.
    /// This is the atomic operation that ensures data and state consistency across all sinks.
    /// 
    /// CRITICAL: File positions are only updated after ALL applicable sinks successfully process the data.
    /// </summary>
    public async Task WriteBatchAndCheckpointStateAsync(IList<DataBlock> batch, CancellationToken token = default)
    {
        if (batch.Count == 0) return;

        var batchId = Guid.NewGuid().ToString();
        _logger.LogDebug("Starting multi-sink processing for batch {BatchId} with {Count} data blocks", batchId, batch.Count);

        try {
            // Step 1: Apply sink filtering to determine which sinks should process this batch
            var sinkAssignments = _sinkFilter.FilterBatch(batch, batchId);
            
            if (sinkAssignments.Count == 0) {
                _logger.LogWarning("No sink assignments for batch {BatchId} - no sinks configured or all filtered out", batchId);
                return;
            }

            _logger.LogDebug("Batch {BatchId} assigned to {AssignmentCount} sink assignments", batchId, sinkAssignments.Count);

            // Step 2: Execute all sink assignments in parallel
            // This is where multiple sinks process the data concurrently
            var allSinkResults = new List<SinkResult>();
            
            foreach (var assignment in sinkAssignments) {
                _logger.LogTrace("Executing sink assignment for batch {BatchId} with {SinkCount} sinks: {SinkNames}", 
                    batchId, assignment.SinkNames.Count, string.Join(", ", assignment.SinkNames));

                var sinkResults = await _sinkExecutor.ExecuteAssignmentAsync(assignment, token);
                allSinkResults.AddRange(sinkResults);
                
                Metrics.SinkExecutions += sinkResults.Count;
            }

            // Step 3: Verify ALL sinks succeeded before checkpointing
            var failedSinks = allSinkResults.Where(r => !r.Success).ToList();
            
            if (failedSinks.Count > 0) {
                var failedSinkMessages = failedSinks.Select(f => f.ErrorMessage).ToList();
                var errorMessage = $"Batch {batchId} failed: {failedSinks.Count} sinks failed - {string.Join(", ", failedSinkMessages)}";
                
                _logger.LogError(errorMessage);
                Metrics.FailedBatches++;
                
                throw new InvalidOperationException(errorMessage);
            }

            // Step 4: ALL sinks succeeded - now we can safely checkpoint
            // This preserves the critical atomicity guarantee
            await _checkpointManager.CommitCheckpointAsync(batch, _liveStateService, CheckpointWriter, token);

            // Step 5: Update metrics
            var totalEntriesProcessed = allSinkResults.Sum(r => r.EntriesProcessed);
            var totalBytesProcessed = allSinkResults.Sum(r => r.BytesProcessed);
            
            Metrics.LogEntriesIngested += totalEntriesProcessed;
            Metrics.BytesProcessed += totalBytesProcessed;
            Metrics.CheckpointsCompleted++;

            _logger.LogInformation("Successfully processed batch {BatchId} with {SinkCount} sink executions. " +
                "Entries: {Entries}, Bytes: {Bytes}, Checkpoints: {Checkpoints}", 
                batchId, allSinkResults.Count, totalEntriesProcessed, totalBytesProcessed, 1);

        } catch (Exception ex) {
            _logger.LogError(ex, "Failed to process batch {BatchId} with multi-sink coordination", batchId);
            Metrics.FailedBatches++;
            throw; // Re-throw to let caller handle the error
        }
    }

    #region Legacy IDataSink Support

    /// <summary>
    /// Legacy support for the old IDataSink interface.
    /// This method is maintained for backward compatibility but should not be used
    /// in the multi-sink architecture. It delegates to a default Parquet sink behavior.
    /// </summary>
    public async Task WriteBatchAsync(LogEntry[] batch, Stream targetStream, string? parquetFilePath = null, string? partitionPath = null)
    {
        _logger.LogWarning("Legacy WriteBatchAsync called - this bypasses multi-sink processing and checkpointing guarantees");
        
        // For legacy compatibility, we could delegate to a Parquet sink
        // However, this bypasses the multi-sink coordination and checkpointing
        // It's better to throw an exception to force migration to the new API
        throw new NotSupportedException(
            "Legacy WriteBatchAsync is not supported in multi-sink mode. " +
            "Use WriteBatchAndCheckpointStateAsync with DataBlock batches instead.");
    }

    #endregion
}

/// <summary>
/// Manages sink filtering logic to determine which entries go to which sinks.
/// Supports configurable filtering expressions and routing rules.
/// </summary>
public class SinkFilter
{
    private readonly ILogger<SinkFilter> _logger;
    private readonly SinkConfiguration _config;

    public SinkFilter(ILogger<SinkFilter> logger, SinkConfiguration config)
    {
        _logger = logger;
        _config = config;
    }

    /// <summary>
    /// Applies filtering logic to create sink assignments based on configured rules.
    /// </summary>
    public List<SinkAssignment> FilterBatch(IList<DataBlock> batch, string batchId)
    {
        var assignments = new List<SinkAssignment>();
        
        // Group entries by file to maintain context
        var entriesByFile = batch
            .SelectMany(db => db.Entries.Select(e => (Entry: e, DataBlock: db)))
            .GroupBy(x => x.DataBlock.FilePath)
            .ToList();

        foreach (var fileGroup in entriesByFile) {
            var fileEntries = fileGroup.ToList();
            ProcessFileEntries(fileEntries, assignments, batchId);
        }

        if (assignments.Count == 0) {
            _logger.LogWarning("No sink assignments created for batch {BatchId} - check sink configuration", batchId);
        } else {
            _logger.LogDebug("Created {AssignmentCount} sink assignments for batch {BatchId}", assignments.Count, batchId);
        }

        return assignments;
    }

    private void ProcessFileEntries(
        List<(LogEntry Entry, DataBlock DataBlock)> fileEntries, 
        List<SinkAssignment> assignments, 
        string batchId)
    {
        var processedEntries = new HashSet<LogEntry>();
        
        // Apply filter rules in priority order
        var enabledFilters = _config.GetEnabledFilters();
        
        foreach (var filter in enabledFilters) {
            var matchingEntries = fileEntries
                .Where(x => !processedEntries.Contains(x.Entry) && EvaluateFilterExpression(x.Entry, filter.Expression))
                .ToList();

            if (matchingEntries.Count > 0) {
                CreateAssignment(matchingEntries, filter.Sinks, assignments, batchId, filter.Name);
                
                // Mark entries as processed
                foreach (var entry in matchingEntries) {
                    processedEntries.Add(entry.Entry);
                }
                
                // Stop processing if this filter has StopProcessing = true
                if (filter.StopProcessing) {
                    break;
                }
            }
        }

        // Apply default assignment to remaining unprocessed entries
        if (_config.Default.Enabled) {
            var remainingEntries = fileEntries
                .Where(x => !processedEntries.Contains(x.Entry))
                .ToList();

            if (remainingEntries.Count > 0) {
                CreateAssignment(remainingEntries, _config.Default.Sinks, assignments, batchId, "default");
            }
        }
    }

    private void CreateAssignment(
        List<(LogEntry Entry, DataBlock DataBlock)> entries, 
        List<string> sinkNames, 
        List<SinkAssignment> assignments, 
        string batchId, 
        string ruleName)
    {
        if (sinkNames.Count == 0) {
            _logger.LogWarning("No sinks configured for rule {RuleName} in batch {BatchId}", ruleName, batchId);
            return;
        }

        // Recreate DataBlocks from the filtered entries
        var dataBlocksByFile = entries
            .GroupBy(x => x.DataBlock.FilePath)
            .Select(g => {
                var firstEntry = g.First();
                return new DataBlock(
                    firstEntry.DataBlock.FileId,
                    firstEntry.DataBlock.VolumeSerial,
                    firstEntry.DataBlock.FilePath,
                    firstEntry.DataBlock.EndPosition,
                    firstEntry.DataBlock.LastWriteTime,
                    g.Select(x => x.Entry).ToList()
                );
            })
            .ToList();

        var assignment = new SinkAssignment {
            DataBlocks = dataBlocksByFile,
            SinkNames = sinkNames.Where(name => _config.Configurations.ContainsKey(name) && _config.Configurations[name].Enabled).ToList(),
            Context = new SinkContext {
                BatchId = batchId,
                BatchTimestamp = DateTime.UtcNow,
                Properties = new Dictionary<string, object> {
                    ["rule_name"] = ruleName,
                    ["entry_count"] = entries.Count
                }
            }
        };

        if (assignment.SinkNames.Count > 0) {
            assignments.Add(assignment);
            
            _logger.LogTrace("Created assignment for rule {RuleName}: {EntryCount} entries → {SinkNames}", 
                ruleName, entries.Count, string.Join(", ", assignment.SinkNames));
        } else {
            _logger.LogWarning("No enabled sinks available for rule {RuleName} in batch {BatchId}", ruleName, batchId);
        }
    }

    private bool EvaluateFilterExpression(LogEntry entry, string expression)
    {
        if (string.IsNullOrWhiteSpace(expression) || expression == "*") {
            return true;
        }

        try {
            // Simple expression evaluation - in production, use a proper expression evaluator
            return EvaluateSimpleExpression(entry, expression);
        } catch (Exception ex) {
            _logger.LogWarning(ex, "Failed to evaluate filter expression: {Expression}", expression);
            return false;
        }
    }

    private bool EvaluateSimpleExpression(LogEntry entry, string expression)
    {
        // Simple expression evaluator for common patterns
        // For production use, consider using a library like System.Linq.Dynamic.Core
        
        expression = expression.Trim();
        
        // Level-based filters
        if (expression.StartsWith("Level ==")) {
            var targetLevel = ExtractQuotedValue(expression);
            return string.Equals(entry.Level, targetLevel, StringComparison.OrdinalIgnoreCase);
        }
        
        if (expression.StartsWith("Level !=")) {
            var targetLevel = ExtractQuotedValue(expression);
            return !string.Equals(entry.Level, targetLevel, StringComparison.OrdinalIgnoreCase);
        }

        // Source-based filters
        if (expression.Contains("Source.Contains(")) {
            var searchText = ExtractQuotedValue(expression);
            return entry.Source.Contains(searchText, StringComparison.OrdinalIgnoreCase);
        }
        
        // Message-based filters
        if (expression.Contains("Message.Contains(")) {
            var searchText = ExtractQuotedValue(expression);
            return entry.Message.Contains(searchText, StringComparison.OrdinalIgnoreCase);
        }

        // Field-based filters
        if (expression.Contains("Fields[")) {
            return EvaluateFieldExpression(entry, expression);
        }

        // Default: treat as field name existence check
        return entry.Fields.ContainsKey(expression);
    }

    private bool EvaluateFieldExpression(LogEntry entry, string expression)
    {
        // Extract field name from expressions like Fields['fieldName'] == 'value'
        var fieldNameStart = expression.IndexOf("Fields[") + 7;
        var fieldNameEnd = expression.IndexOf(']', fieldNameStart);
        
        if (fieldNameEnd == -1) return false;
        
        var fieldName = expression.Substring(fieldNameStart, fieldNameEnd - fieldNameStart).Trim('\'', '"');
        
        if (!entry.Fields.TryGetValue(fieldName, out var fieldValue)) {
            return false;
        }

        if (expression.Contains(" == ")) {
            var targetValue = ExtractQuotedValue(expression.Substring(fieldNameEnd + 1));
            return string.Equals(fieldValue?.ToString(), targetValue, StringComparison.OrdinalIgnoreCase);
        }

        return true; // Field exists
    }

    private string ExtractQuotedValue(string expression)
    {
        var quoteStart = expression.IndexOfAny(new[] { '\'', '"' });
        if (quoteStart == -1) return "";
        
        var quoteChar = expression[quoteStart];
        var quoteEnd = expression.IndexOf(quoteChar, quoteStart + 1);
        
        if (quoteEnd == -1) return "";
        
        return expression.Substring(quoteStart + 1, quoteEnd - quoteStart - 1);
    }
}

/// <summary>
/// Executes sink assignments in parallel with proper error handling.
/// </summary>
public class SinkExecutor
{
    private readonly ILogger<SinkExecutor> _logger;
    private readonly Dictionary<string, IMultiDataSink> _sinks;

    public SinkExecutor(ILogger<SinkExecutor> logger)
    {
        _logger = logger;
        _sinks = new Dictionary<string, IMultiDataSink>();
    }

    /// <summary>
    /// Registers a sink with the executor.
    /// </summary>
    public void RegisterSink(IMultiDataSink sink)
    {
        _sinks[sink.Name] = sink;
        _logger.LogInformation("Registered sink: {SinkName}", sink.Name);
    }

    /// <summary>
    /// Executes all sinks in an assignment in parallel.
    /// </summary>
    public async Task<List<SinkResult>> ExecuteAssignmentAsync(SinkAssignment assignment, CancellationToken token = default)
    {
        var results = new List<SinkResult>();
        var tasks = new List<Task<SinkResult>>();

        // Create tasks for all sinks in the assignment
        foreach (var sinkName in assignment.SinkNames) {
            if (_sinks.TryGetValue(sinkName, out var sink)) {
                var task = ExecuteSinkAsync(sink, assignment, token);
                tasks.Add(task);
            } else {
                _logger.LogError("Sink {SinkName} not found in assignment for batch {BatchId}", sinkName, assignment.Context.BatchId);
                results.Add(SinkResult.CreateFailure($"Sink {sinkName} not found"));
            }
        }

        // Execute all sinks in parallel
        if (tasks.Count > 0) {
            var sinkResults = await Task.WhenAll(tasks);
            results.AddRange(sinkResults);
        }

        return results;
    }

    private async Task<SinkResult> ExecuteSinkAsync(IMultiDataSink sink, SinkAssignment assignment, CancellationToken token)
    {
        try {
            _logger.LogTrace("Executing sink {SinkName} for batch {BatchId} with {BlockCount} blocks", 
                sink.Name, assignment.Context.BatchId, assignment.DataBlocks.Count);

            var result = await sink.WriteBatchAsync(assignment.DataBlocks, assignment.Context, token);
            
            if (result.Success) {
                _logger.LogTrace("Sink {SinkName} succeeded for batch {BatchId}: {Entries} entries, {Bytes} bytes", 
                    sink.Name, assignment.Context.BatchId, result.EntriesProcessed, result.BytesProcessed);
            } else {
                _logger.LogError("Sink {SinkName} failed for batch {BatchId}: {Error}", 
                    sink.Name, assignment.Context.BatchId, result.ErrorMessage);
            }

            return result;
        } catch (Exception ex) {
            _logger.LogError(ex, "Exception in sink {SinkName} for batch {BatchId}", sink.Name, assignment.Context.BatchId);
            return SinkResult.CreateFailure($"Exception in sink {sink.Name}: {ex.Message}");
        }
    }
}

/// <summary>
/// Manages checkpoint operations to maintain atomicity guarantees.
/// Only commits checkpoints after all sinks successfully process data.
/// </summary>
public class CheckpointManager
{
    private readonly ILogger<CheckpointManager> _logger;

    public CheckpointManager(ILogger<CheckpointManager> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Commits checkpoint updates after all sinks have successfully processed the data.
    /// This preserves the critical atomicity guarantee from the original CheckpointDataSink.
    /// </summary>
    public async Task CommitCheckpointAsync(
        IList<DataBlock> batch, 
        LiveStateService liveStateService, 
        ChannelWriter<CheckpointPositionUpdate> checkpointWriter, 
        CancellationToken token = default)
    {
        _logger.LogTrace("Committing checkpoint for {BlockCount} data blocks", batch.Count);

        try {
            foreach (var dataBlock in batch) {
                // Create checkpoint update (same logic as original CheckpointDataSink)
                var checkpointUpdate = new CheckpointPositionUpdate(
                    dataBlock.FileId,
                    dataBlock.VolumeSerial,
                    dataBlock.FilePath,
                    dataBlock.EndPosition,
                    dataBlock.LastWriteTime,
                    DateTime.UtcNow
                );

                // Emit to checkpoint channel for StateWriterService monitoring
                await checkpointWriter.WriteAsync(checkpointUpdate, token);

                // Update the live state service (this will queue SQLite persistence)
                // This is the critical step that maintains atomicity
                await liveStateService.UpdatePosition(
                    dataBlock.FileId,
                    dataBlock.VolumeSerial,
                    dataBlock.EndPosition,
                    dataBlock.FilePath,
                    dataBlock.LastWriteTime,
                    PositionUpdateMode.InMemoryAndPersist
                );

                _logger.LogTrace("Checkpoint committed for {FilePath}: position {Position}",
                    dataBlock.FilePath, dataBlock.EndPosition);
            }

            _logger.LogDebug("Successfully committed checkpoint for {BlockCount} data blocks", batch.Count);
        } catch (Exception ex) {
            _logger.LogError(ex, "Failed to commit checkpoint for {BlockCount} data blocks", batch.Count);
            throw;
        }
    }
}