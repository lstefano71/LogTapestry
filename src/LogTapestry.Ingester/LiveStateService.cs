using LogTapestry.Core;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using System.Collections.Concurrent;
using System.Threading.Channels;

namespace LogTapestry.Ingester;

public enum PositionUpdateMode
{
  InMemoryOnly,
  InMemoryAndPersist
}

/// <summary>
/// In-memory source of truth for file positions with SQLite persistence.
/// Implements checkpointing by ensuring position updates are only committed after data persistence.
/// </summary>
public class LiveStateService(
    ILogger<LiveStateService> logger,
    IStateProvider persistentStateProvider) : IStateProvider, IHostedService
{
  private readonly ILogger<LiveStateService> _logger = logger;
  private readonly IStateProvider _persistentStateProvider = persistentStateProvider;

  // In-memory position cache - the source of truth
  private readonly ConcurrentDictionary<(ulong FileId, long VolumeSerial), TrackedFileInfo> _positionCache = new();

  // Channel for queuing position updates to SQLite
  private readonly Channel<CheckpointPositionUpdate> _checkpointChannel = Channel.CreateUnbounded<CheckpointPositionUpdate>();
  private Task? _persistenceTask;
  private CancellationTokenSource? _cts;

  /// <summary>
  /// Fast in-memory position lookup - this is the source of truth.
  /// </summary>
  public async Task<long?> GetPosition(ulong fileId, long volumeSerial)
  {
    if (_positionCache.TryGetValue((fileId, volumeSerial), out var trackedFile)) {
      return trackedFile.Position;
    }

    // If not in cache, try to load from persistent storage
    var persistentFile = await _persistentStateProvider.GetTrackedFileAsync(fileId, volumeSerial);
    if (persistentFile != null) {
      _positionCache[(fileId, volumeSerial)] = persistentFile;
      return persistentFile.Position;
    }

    return null;
  }

  /// <summary>
  /// Update position in memory and queue for SQLite persistence.
  /// This implements the checkpointing principle: memory is source of truth.
  /// Thread-safe to prevent duplicate updates from multiple workers.
  /// </summary>
  public async Task UpdatePosition(ulong fileId, long volumeSerial, long position, string filePath, DateTime lastWriteTime, PositionUpdateMode mode = PositionUpdateMode.InMemoryAndPersist)
  {
    var key = (fileId, volumeSerial);

    // Use thread-safe update to prevent duplicate processing
    var updated = false;
    _positionCache.AddOrUpdate(key,
        // Add new entry
        _ => {
          updated = true;
          return new TrackedFileInfo {
            FileId = fileId,
            VolumeSerial = volumeSerial,
            FilePath = filePath,
            Position = position,
            LastWriteTimeUtc = lastWriteTime
          };
        },
        // Update existing entry only if new position is greater
        (existingKey, existingFile) => {
          if (position > existingFile.Position) {
            updated = true;
            return new TrackedFileInfo {
              FileId = fileId,
              VolumeSerial = volumeSerial,
              FilePath = filePath,
              Position = position,
              LastWriteTimeUtc = lastWriteTime
            };
          }
          return existingFile;
        });

    // Only queue checkpoint update if position was actually updated
    if (updated) {
      var checkpointUpdate = new CheckpointPositionUpdate(
          fileId,
          volumeSerial,
          filePath,
          position,
          lastWriteTime,
          DateTime.UtcNow
      );

      if (mode == PositionUpdateMode.InMemoryAndPersist) {
        await _checkpointChannel.Writer.WriteAsync(checkpointUpdate, _cts?.Token ?? CancellationToken.None);
        _logger.LogTrace("Queued checkpoint update for {FilePath}: position {Position}", filePath, position);
      } else {
        _logger.LogTrace("In-memory update for {FilePath}: position {Position}", filePath, position);
      }
    } else {
      _logger.LogTrace("Skipped duplicate checkpoint update for {FilePath}: position {Position}", filePath, position);
    }
  }

  /// <summary>
  /// Process checkpoint updates and persist to SQLite.
  /// This runs continuously to ensure durability.
  /// </summary>
  private async Task ProcessCheckpointUpdatesAsync(CancellationToken token)
  {
    try {
      while (await _checkpointChannel.Reader.WaitToReadAsync(token)) {
        var batch = new List<CheckpointPositionUpdate>();

        // Drain available updates into a batch
        while (_checkpointChannel.Reader.TryRead(out var update)) {
          batch.Add(update);

          // Process in smaller batches to avoid holding locks too long
          if (batch.Count >= 100) {
            await PersistBatch(batch);
            batch.Clear();
          }
        }

        // Process remaining batch
        if (batch.Count > 0) {
          await PersistBatch(batch);
        }
      }
    } catch (OperationCanceledException) {
      _logger.LogInformation("Checkpoint processing cancelled");
    } catch (Exception ex) {
      _logger.LogError(ex, "Error in checkpoint processing");
    }
  }

  private async Task PersistBatch(List<CheckpointPositionUpdate> batch)
  {
    try {
      // Convert to PositionUpdate for compatibility with existing IStateProvider
      var positionUpdates = batch.Select(update => new PositionUpdate {
        FileId = update.FileId,
        VolumeSerial = update.VolumeSerial,
        Position = update.Position,
        FilePath = update.FilePath,
        LastWriteTimeUtc = update.LastWriteTime.Ticks
      }).ToArray();

      await _persistentStateProvider.UpdateTrackedFilesBatchAsync(positionUpdates);
      _logger.LogDebug("Persisted batch of {Count} checkpoint updates", batch.Count);
    } catch (Exception ex) {
      _logger.LogError(ex, "Failed to persist batch of {Count} checkpoint updates", batch.Count);
      // Continue processing - don't let one batch failure stop the service
    }
  }

  #region IStateProvider Implementation

  public async Task<TrackedFileInfo?> GetTrackedFileAsync(ulong fileId, long volumeSerial)
  {
    // Check in-memory cache first (source of truth)
    if (_positionCache.TryGetValue((fileId, volumeSerial), out var cachedFile)) {
      return cachedFile;
    }

    // Fall back to persistent storage
    var persistentFile = await _persistentStateProvider.GetTrackedFileAsync(fileId, volumeSerial);
    if (persistentFile != null) {
      // Cache it for future use
      _positionCache[(fileId, volumeSerial)] = persistentFile;
    }

    return persistentFile;
  }

  public async Task UpdateTrackedFileAsync(TrackedFileInfo info)
  {
    await UpdatePosition(info.FileId, info.VolumeSerial, info.Position, info.FilePath, info.LastWriteTimeUtc, PositionUpdateMode.InMemoryAndPersist);
  }

  public async Task RemoveTrackedFileAsync(ulong fileId, long volumeSerial)
  {
    _positionCache.TryRemove((fileId, volumeSerial), out _);
    await _persistentStateProvider.RemoveTrackedFileAsync(fileId, volumeSerial);
  }

  public async Task<Dictionary<ulong, TrackedFileInfo>> GetAllTrackedFilesAsync()
  {
    var allFiles = await _persistentStateProvider.GetAllTrackedFilesAsync();

    // Update cache with any files not already cached
    foreach (var file in allFiles) {
      var key = (file.Value.FileId, file.Value.VolumeSerial);
      _positionCache[key] = file.Value;
    }

    return allFiles;
  }

  public async Task<string?> GetFieldTypeAsync(string fieldName)
  {
    return await _persistentStateProvider.GetFieldTypeAsync(fieldName);
  }

  public async Task UpdateTrackedFilesBatchAsync(PositionUpdate[] updates)
  {
    foreach (var update in updates) {
      await UpdatePosition(update.FileId, update.VolumeSerial, update.Position,
                         update.FilePath, new DateTime(update.LastWriteTimeUtc), PositionUpdateMode.InMemoryAndPersist);
    }
  }

  public bool CheckHealth()
  {
    return _persistentStateProvider.CheckHealth();
  }

  #endregion

  #region IHostedService Implementation

  public Task StartAsync(CancellationToken cancellationToken)
  {
    _logger.LogInformation("LiveStateService starting with in-memory position cache");

    _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    _persistenceTask = Task.Run(() => ProcessCheckpointUpdatesAsync(_cts.Token), cancellationToken);

    return Task.CompletedTask;
  }

  public async Task StopAsync(CancellationToken cancellationToken)
  {
    _logger.LogInformation("LiveStateService stopping");

    _cts?.Cancel();

    // Process any remaining checkpoint updates
    if (_checkpointChannel.Reader.Count > 0) {
      var remainingUpdates = new List<CheckpointPositionUpdate>();
      while (_checkpointChannel.Reader.TryRead(out var update)) {
        remainingUpdates.Add(update);
      }

      if (remainingUpdates.Count > 0) {
        await PersistBatch(remainingUpdates);
      }
    }

    if (_persistenceTask != null) {
      await _persistenceTask;
    }

    _logger.LogInformation("LiveStateService stopped");
  }

  #endregion
}
