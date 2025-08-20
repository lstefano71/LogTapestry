using Microsoft.Extensions.Logging;

namespace LogTapestry.Ingester
{
  /// <summary>
  /// Implements consistent hashing to assign files to specific processors.
  /// Ensures all events for the same file are routed to the same processor.
  /// </summary>
  public class ConsistentHashRouter
  {
    private readonly ILogger<ConsistentHashRouter> _logger;
    private readonly int _processorCount;
    private readonly Dictionary<(ulong FileId, long VolumeSerial), int> _fileAssignments = [];
    private readonly Lock _lock = new();

    public ConsistentHashRouter(ILogger<ConsistentHashRouter> logger, int processorCount)
    {
      _logger = logger;
      _processorCount = processorCount;

      if (processorCount <= 0) {
        throw new ArgumentException("Processor count must be greater than 0", nameof(processorCount));
      }
    }

    /// <summary>
    /// Gets the assigned processor for a file using consistent hashing.
    /// Uses the file ID and volume serial to create a stable hash.
    /// </summary>
    public int GetProcessorForFile(ulong fileId, long volumeSerial)
    {
      lock (_lock) {
        var key = (fileId, volumeSerial);

        if (_fileAssignments.TryGetValue(key, out var processorId)) {
          return processorId;
        }

        // Create a stable hash from file ID and volume serial
        var hashCode = HashCode.Combine(fileId, volumeSerial);
        var processor = Math.Abs(hashCode) % _processorCount;

        _fileAssignments[key] = processor;

        _logger.LogDebug("Assigned file (ID: {FileId}, Volume: {VolumeSerial}) to processor {ProcessorId}",
            fileId, volumeSerial, processor);

        return processor;
      }
    }

    /// <summary>
    /// Removes the assignment for a file (useful for cleanup).
    /// </summary>
    public void RemoveFileAssignment(ulong fileId, long volumeSerial)
    {
      lock (_lock) {
        var key = (fileId, volumeSerial);
        if (_fileAssignments.Remove(key)) {
          _logger.LogDebug("Removed assignment for file (ID: {FileId}, Volume: {VolumeSerial})",
              fileId, volumeSerial);
        }
      }
    }

    /// <summary>
    /// Gets the current assignment count for debugging purposes.
    /// </summary>
    public int GetAssignmentCount()
    {
      lock (_lock) {
        return _fileAssignments.Count;
      }
    }

    /// <summary>
    /// Clears all assignments (useful for testing or reset scenarios).
    /// </summary>
    public void ClearAssignments()
    {
      lock (_lock) {
        _fileAssignments.Clear();
        _logger.LogInformation("Cleared all file-to-processor assignments");
      }
    }
  }
}
