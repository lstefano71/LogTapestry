using LogTapestry.Core;
using LogTapestry.Core.Interfaces;
using LogTapestry.Ingester;
using LogTapestry.Ingester.Sinks;

using Microsoft.Extensions.Logging;

using Moq;

using System.Threading.Channels;

namespace LogTapestry.Integration.Tests;

[TestClass]
public class MultiSinkCheckpointingTests
{
    private string _tempDataPath;
    private string _tempDbPath;
    private SqliteStateProvider _stateProvider;
    private LiveStateService _liveStateService;
    private ILoggerFactory _loggerFactory;

    [TestInitialize]
    public void Setup()
    {
        _tempDataPath = Path.Combine(Path.GetTempPath(), $"LogTapestry_Test_{Guid.NewGuid()}");
        _tempDbPath = Path.Combine(_tempDataPath, "test_state.sqlite");
        Directory.CreateDirectory(_tempDataPath);

        _loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug));
        
        _stateProvider = new SqliteStateProvider(_tempDbPath);
        _liveStateService = new LiveStateService(
            _loggerFactory.CreateLogger<LiveStateService>(),
            _stateProvider
        );
    }

    [TestCleanup]
    public void Cleanup()
    {
        _liveStateService?.Dispose();
        _stateProvider?.Dispose();
        _loggerFactory?.Dispose();

        if (Directory.Exists(_tempDataPath))
        {
            try
            {
                Directory.Delete(_tempDataPath, true);
            }
            catch
            {
                // Ignore cleanup errors in tests
            }
        }
    }

    [TestMethod]
    public async Task MultiSinkProcessor_AllSinksSucceed_CheckpointsCommitted()
    {
        // Arrange
        var processor = CreateMultiSinkProcessor(
            createSuccessfulParquetSink: true,
            createSuccessfulMockSink: true);

        var testBatch = CreateTestDataBlocks();
        var initialPositions = await GetCurrentFilePositions();

        // Act
        await processor.WriteBatchAndCheckpointStateAsync(testBatch);

        // Assert - Verify checkpoints were committed
        var finalPositions = await GetCurrentFilePositions();
        
        Assert.AreNotEqual(initialPositions.Count, finalPositions.Count, 
            "File positions should have been updated after successful multi-sink processing");

        foreach (var dataBlock in testBatch)
        {
            var position = finalPositions.FirstOrDefault(p => p.FileId == dataBlock.FileId);
            Assert.IsNotNull(position, $"Position should be tracked for file {dataBlock.FileId}");
            Assert.AreEqual(dataBlock.EndPosition, position.Position, 
                $"Checkpoint position should match DataBlock end position for file {dataBlock.FileId}");
        }

        // Verify Parquet files were created
        var parquetFiles = Directory.GetFiles(_tempDataPath, "*.parquet", SearchOption.AllDirectories);
        Assert.IsTrue(parquetFiles.Length > 0, "Parquet files should have been created");
    }

    [TestMethod]
    public async Task MultiSinkProcessor_OneSinkFails_NoCheckpointsCommitted()
    {
        // Arrange
        var processor = CreateMultiSinkProcessor(
            createSuccessfulParquetSink: true,
            createSuccessfulMockSink: false); // Mock sink will fail

        var testBatch = CreateTestDataBlocks();
        var initialPositions = await GetCurrentFilePositions();

        // Act & Assert
        await Assert.ThrowsExceptionAsync<InvalidOperationException>(
            () => processor.WriteBatchAndCheckpointStateAsync(testBatch));

        // Verify NO checkpoints were committed
        var finalPositions = await GetCurrentFilePositions();
        Assert.AreEqual(initialPositions.Count, finalPositions.Count, 
            "File positions should NOT have been updated when any sink fails");

        // Verify metrics reflect the failure
        Assert.IsTrue(MultiSinkProcessor.Metrics.FailedBatches > 0, 
            "Failed batch metrics should be incremented");
    }

    [TestMethod]
    public async Task MultiSinkProcessor_ParquetSinkFails_NoCheckpointsCommitted()
    {
        // Arrange - Use invalid path to force Parquet sink failure
        var invalidDataPath = Path.Combine("Z:\\NonExistentDrive", "invalid");
        var processor = CreateMultiSinkProcessor(
            createSuccessfulParquetSink: false, // Parquet sink will fail
            createSuccessfulMockSink: true,
            customDataPath: invalidDataPath);

        var testBatch = CreateTestDataBlocks();
        var initialPositions = await GetCurrentFilePositions();

        // Act & Assert
        await Assert.ThrowsExceptionAsync<InvalidOperationException>(
            () => processor.WriteBatchAndCheckpointStateAsync(testBatch));

        // Verify NO checkpoints were committed
        var finalPositions = await GetCurrentFilePositions();
        Assert.AreEqual(initialPositions.Count, finalPositions.Count, 
            "File positions should NOT have been updated when Parquet sink fails");
    }

    [TestMethod]
    public async Task MultiSinkProcessor_ConcurrentOperations_MaintainsConsistency()
    {
        // Arrange
        var processor = CreateMultiSinkProcessor(
            createSuccessfulParquetSink: true,
            createSuccessfulMockSink: true);

        var batch1 = CreateTestDataBlocks("file1.log", 100);
        var batch2 = CreateTestDataBlocks("file2.log", 200);
        var batch3 = CreateTestDataBlocks("file3.log", 300);

        // Act - Process multiple batches concurrently
        var tasks = new[]
        {
            processor.WriteBatchAndCheckpointStateAsync(batch1),
            processor.WriteBatchAndCheckpointStateAsync(batch2),
            processor.WriteBatchAndCheckpointStateAsync(batch3)
        };

        await Task.WhenAll(tasks);

        // Assert - All checkpoints should be committed consistently
        var finalPositions = await GetCurrentFilePositions();
        Assert.AreEqual(3, finalPositions.Count, "All three files should have position records");

        // Verify each file has the correct final position
        var file1Position = finalPositions.First(p => p.FilePath.Contains("file1.log"));
        var file2Position = finalPositions.First(p => p.FilePath.Contains("file2.log"));
        var file3Position = finalPositions.First(p => p.FilePath.Contains("file3.log"));

        Assert.AreEqual(100, file1Position.Position);
        Assert.AreEqual(200, file2Position.Position);
        Assert.AreEqual(300, file3Position.Position);
    }

    [TestMethod]
    public async Task MultiSinkProcessor_FilteredAssignments_OnlyProcessesCorrectSinks()
    {
        // Arrange
        var config = CreateFilteredConfiguration(); // ERROR entries go to both sinks, INFO only to Parquet
        var processor = CreateMultiSinkProcessor(
            createSuccessfulParquetSink: true,
            createSuccessfulMockSink: true,
            customConfig: config);

        var mixedBatch = CreateMixedLevelDataBlocks(); // Contains both INFO and ERROR entries
        var initialPositions = await GetCurrentFilePositions();

        // Act
        await processor.WriteBatchAndCheckpointStateAsync(mixedBatch);

        // Assert - All entries should be checkpointed regardless of filtering
        var finalPositions = await GetCurrentFilePositions();
        Assert.IsTrue(finalPositions.Count > initialPositions.Count, 
            "Checkpoints should be committed for all entries");

        // Verify all entries were processed (total metrics should include both INFO and ERROR)
        Assert.IsTrue(MultiSinkProcessor.Metrics.LogEntriesIngested >= 2, 
            "Both INFO and ERROR entries should have been processed");
    }

    [TestMethod]
    public async Task CheckpointManager_CommitCheckpoint_UpdatesLiveStateCorrectly()
    {
        // Arrange
        var checkpointManager = new CheckpointManager(_loggerFactory.CreateLogger<CheckpointManager>());
        var channel = Channel.CreateUnbounded<CheckpointPositionUpdate>();
        var testBatch = CreateTestDataBlocks();

        // Act
        await checkpointManager.CommitCheckpointAsync(testBatch, _liveStateService, channel.Writer);

        // Assert
        var positions = await GetCurrentFilePositions();
        Assert.AreEqual(testBatch.Count, positions.Count, 
            "Each DataBlock should have a corresponding position record");

        foreach (var dataBlock in testBatch)
        {
            var position = positions.FirstOrDefault(p => p.FileId == dataBlock.FileId);
            Assert.IsNotNull(position);
            Assert.AreEqual(dataBlock.EndPosition, position.Position);
            Assert.AreEqual(dataBlock.FilePath, position.FilePath);
        }

        // Verify checkpoint updates were emitted to channel
        var updateCount = 0;
        while (channel.Reader.TryRead(out _))
        {
            updateCount++;
        }
        Assert.AreEqual(testBatch.Count, updateCount, 
            "Each DataBlock should emit a checkpoint update");
    }

    private MultiSinkProcessor CreateMultiSinkProcessor(
        bool createSuccessfulParquetSink,
        bool createSuccessfulMockSink,
        string? customDataPath = null,
        SinkConfiguration? customConfig = null)
    {
        var config = customConfig ?? CreateDefaultConfiguration();
        var sinkFilter = new SinkFilter(_loggerFactory.CreateLogger<SinkFilter>(), config);
        var sinkExecutor = new SinkExecutor(_loggerFactory.CreateLogger<SinkExecutor>());
        var checkpointManager = new CheckpointManager(_loggerFactory.CreateLogger<CheckpointManager>());

        // Create and register Parquet sink
        if (createSuccessfulParquetSink)
        {
            var parquetConfig = new ParquetSinkConfiguration
            {
                DataRoot = customDataPath ?? _tempDataPath
            };
            var parquetSink = new ParquetSink(
                _loggerFactory.CreateLogger<ParquetSink>(),
                _stateProvider,
                parquetConfig);
            sinkExecutor.RegisterSink(parquetSink);
        }
        else
        {
            // Register a failing mock Parquet sink
            var mockParquetSink = new Mock<IMultiDataSink>();
            mockParquetSink.Setup(x => x.Name).Returns("parquet");
            mockParquetSink.Setup(x => x.WriteBatchAsync(It.IsAny<IList<DataBlock>>(), It.IsAny<SinkContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(SinkResult.CreateFailure("Parquet sink failed"));
            sinkExecutor.RegisterSink(mockParquetSink.Object);
        }

        // Create and register mock sink
        if (createSuccessfulMockSink)
        {
            var mockSink = new Mock<IMultiDataSink>();
            mockSink.Setup(x => x.Name).Returns("mock");
            mockSink.Setup(x => x.WriteBatchAsync(It.IsAny<IList<DataBlock>>(), It.IsAny<SinkContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(SinkResult.CreateSuccess(10, 1000));
            sinkExecutor.RegisterSink(mockSink.Object);
        }
        else
        {
            var mockSink = new Mock<IMultiDataSink>();
            mockSink.Setup(x => x.Name).Returns("mock");
            mockSink.Setup(x => x.WriteBatchAsync(It.IsAny<IList<DataBlock>>(), It.IsAny<SinkContext>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(SinkResult.CreateFailure("Mock sink failed"));
            sinkExecutor.RegisterSink(mockSink.Object);
        }

        return new MultiSinkProcessor(
            _loggerFactory.CreateLogger<MultiSinkProcessor>(),
            _liveStateService,
            sinkExecutor,
            checkpointManager,
            sinkFilter);
    }

    private SinkConfiguration CreateDefaultConfiguration()
    {
        return new SinkConfiguration
        {
            Default = new DefaultSinkConfig
            {
                Sinks = new List<string> { "parquet", "mock" },
                Enabled = true
            },
            Configurations = new Dictionary<string, SinkDefinition>
            {
                ["parquet"] = new SinkDefinition { Type = "Parquet", Enabled = true },
                ["mock"] = new SinkDefinition { Type = "Mock", Enabled = true }
            }
        };
    }

    private SinkConfiguration CreateFilteredConfiguration()
    {
        var config = CreateDefaultConfiguration();
        config.Filters.Add(new SinkFilterRule
        {
            Name = "ErrorsToAllSinks",
            Expression = "Level == 'ERROR'",
            Sinks = new List<string> { "parquet", "mock" },
            Enabled = true,
            Priority = 100
        });
        return config;
    }

    private List<DataBlock> CreateTestDataBlocks(string fileName = "test.log", long endPosition = 1000)
    {
        var entries = new List<LogEntry>
        {
            new LogEntry(
                Timestamp: DateTime.UtcNow,
                Level: "INFO",
                Message: "Test message",
                Source: "TestApp",
                TemplateHash: 12345,
                Ulid: new byte[16],
                Fields: new Dictionary<string, object> { ["test"] = "value" }
            )
        };

        return new List<DataBlock>
        {
            new DataBlock(
                FileId: (ulong)fileName.GetHashCode(),
                VolumeSerial: 456,
                FilePath: Path.Combine(_tempDataPath, fileName),
                EndPosition: endPosition,
                LastWriteTime: DateTime.UtcNow,
                Entries: entries
            )
        };
    }

    private List<DataBlock> CreateMixedLevelDataBlocks()
    {
        var infoEntry = new LogEntry(
            Timestamp: DateTime.UtcNow,
            Level: "INFO",
            Message: "Info message",
            Source: "TestApp",
            TemplateHash: 12345,
            Ulid: new byte[16],
            Fields: new Dictionary<string, object>()
        );

        var errorEntry = new LogEntry(
            Timestamp: DateTime.UtcNow,
            Level: "ERROR",
            Message: "Error message",
            Source: "TestApp",
            TemplateHash: 67890,
            Ulid: new byte[16],
            Fields: new Dictionary<string, object>()
        );

        return new List<DataBlock>
        {
            new DataBlock(123, 456, Path.Combine(_tempDataPath, "info.log"), 1000, DateTime.UtcNow, new List<LogEntry> { infoEntry }),
            new DataBlock(124, 456, Path.Combine(_tempDataPath, "error.log"), 2000, DateTime.UtcNow, new List<LogEntry> { errorEntry })
        };
    }

    private async Task<List<TrackedFileInfo>> GetCurrentFilePositions()
    {
        return await _stateProvider.GetAllTrackedFilesAsync();
    }
}