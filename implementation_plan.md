# Implementation Plan: Sprint 4.5 - Scale and Robustness Refactoring

## Overview
This sprint implements a fundamental architectural refactoring of the LogTapestry ingestion pipeline to address critical scaling issues identified with monitoring 15,000+ files. The current "one-task-per-file" model will be replaced with a scalable, declarative, worker-based architecture using System.Threading.Channels and Open.ChannelExtensions.

## Types
New data structures and types to support the refactored architecture:

```csharp
// New data structures for the worker pool model
public class FileCheckRequest
{
    public ulong FileId { get; set; }
    public long VolumeSerial { get; set; }
    public string FilePath { get; set; }
    public long LastWriteTimeUtc { get; set; }
    public FileWorkType Type { get; set; }
}

public class PositionUpdate
{
    public ulong FileId { get; set; }
    public long VolumeSerial { get; set; }
    public long Position { get; set; }
    public string FilePath { get; set; }
    public long LastWriteTimeUtc { get; set; }
}

public class FileEvent
{
    public ulong FileId { get; set; }
    public long VolumeSerial { get; set; }
    public string FilePath { get; set; }
    public long LastWriteTimeUtc { get; set; }
    public FileWorkType Type { get; set; }
}
```

## Files
### New Files
- `src/LogTapestry.Ingester/PriorityMonitor.cs` - Priority-aware merger for file discovery pipeline
- `src/LogTapestry.Ingester/InitialScanProducer.cs` - Parallelized initial directory scan producer
- `src/LogTapestry.Ingester/WatcherProducer.cs` - File system watcher producer
- `src/LogTapestry.Ingester/StateWriterService.cs` - IHostedService for batched state updates
- `src/LogTapestry.Ingester/FileReader.cs` - Static utility class for file reading logic
- `src/LogTapestry.Core/Interfaces/IStateProviderExtensions.cs` - Extension methods for batch operations

### Modified Files
- `src/LogTapestry.Ingester/TailingManager.cs` - **REMOVE ENTIRELY** (replaced by worker pool)
- `src/LogTapestry.Ingester/DirectoryMonitor.cs` - Refactor to use priority pipeline architecture
- `src/LogTapestry.Ingester/IngesterService.cs` - Complete rewrite to declarative pipeline
- `src/LogTapestry.Core/Interfaces/IStateProvider.cs` - Add batch update method
- `src/LogTapestry.Core/SqliteStateProvider.cs` - Implement batch update with transactions
- `src/LogTapestry.Ingester/appsettings.json` - Add FileReaderThreadPoolSize configuration

### Configuration Changes
Add to appsettings.json:
```json
{
  "Ingester": {
    "FileReaderThreadPoolSize": 8,
    "StateWriterBatchSize": 1000,
    "StateWriterIntervalSeconds": 2,
    "FileSystemWatcherBufferSize": 65536
  }
}
```

## Functions
### New Functions
- `FileReader.ReadAndParseFileAsync(FileCheckRequest, ChannelWriter<PositionUpdate>)` - Core file reading logic
- `PriorityMonitor.ProcessEventsAsync()` - Priority-aware event processing
- `StateWriterService.StartPipeline()` - Batched state update pipeline
- `IStateProvider.UpdateTrackedFilesBatchAsync(PositionUpdate[])` - Batch database updates

### Removed Functions
- `TailingManager.TailingTask()` - Replaced by worker pool model
- `TailingManager.RunAsync()` - Replaced by declarative pipeline
- `DirectoryMonitor.InitialScanAsync()` - Replaced by parallel InitialScanProducer

## Classes
### New Classes
- `StateWriterService : IHostedService` - Manages batched state updates
- `PriorityMonitor` - Merges slow and fast file discovery channels
- `InitialScanProducer` - Parallelized directory scanning
- `WatcherProducer` - File system watcher abstraction

### Modified Classes
- `IngesterService` - Complete architectural rewrite to declarative pipeline
- `DirectoryMonitor` - Refactored to priority-aware pipeline architecture
- `SqliteStateProvider` - Added batch transaction support

### Removed Classes
- `TailingManager` - **ENTIRE CLASS REMOVED** (replaced by worker pool)

## Dependencies
### New Dependencies
- `Open.ChannelExtensions` (nuget) - For advanced channel operations (.Transform, .Batch, etc.)
- `System.Threading.Channels` (already present) - Enhanced usage patterns

### Configuration Updates
Update `src/LogTapestry.Ingester/LogTapestry.Ingester.csproj`:
```xml
<PackageReference Include="Open.ChannelExtensions" Version="8.0.0" />
```

## Testing
### New Integration Tests
- `PriorityMonitor_Should_Prioritize_Fast_Channel_And_Deduplicate` - Verify priority merging logic
- `Ingester_Should_Maintain_Low_File_Handle_Count_Under_Load` - Performance test for resource usage
- `StateWriterService_Should_Batch_Updates_Efficiently` - Verify batching behavior

### Modified Tests
- Update existing tests to work with new architecture
- Add tests for new configuration parameters

## Implementation Order
1. **Setup Dependencies** - Add Open.ChannelExtensions package
2. **Create New Types** - FileCheckRequest, PositionUpdate, FileEvent
3. **Implement StateWriterService** - Batched state update functionality
4. **Update IStateProvider Interface** - Add batch method signature
5. **Implement SqliteStateProvider Batch Updates** - Transaction-based batching
6. **Create Priority Monitor Pipeline** - PriorityMonitor, InitialScanProducer, WatcherProducer
7. **Implement FileReader Utility** - Core file reading logic
8. **Refactor DirectoryMonitor** - Priority-aware file discovery
9. **Refactor IngesterService** - Declarative pipeline architecture
10. **Remove TailingManager** - Complete removal of old architecture
11. **Update Configuration** - Add new settings to appsettings.json
12. **Add Integration Tests** - Verify new functionality
13. **Performance Testing** - Ensure scaling improvements
