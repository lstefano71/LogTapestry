namespace LogTapestry.Core;

/// <summary>
/// Rich data structure carrying both log data and position context for the checkpointing pipeline.
/// This replaces the separate PositionUpdate and ParsingResult objects to ensure transactional consistency.
/// </summary>
public record DataBlock(
    ulong FileId,
    long VolumeSerial,
    string FilePath,
    long EndPosition, // The new byte offset after this block was read
    DateTime LastWriteTime,
    IReadOnlyList<LogEntry> Entries
);

/// <summary>
/// Enhanced position update with additional metadata for durability tracking.
/// Used by the checkpointing system to ensure state updates are committed after data persistence.
/// </summary>
public record CheckpointPositionUpdate(
    ulong FileId,
    long VolumeSerial,
    string FilePath,
    long Position,
    DateTime LastWriteTime,
    DateTime CheckpointTime // When this update was verified as durable
);
