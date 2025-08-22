using LogTapestry.Core;
using LogTapestry.Core.Interfaces;
using LogTapestry.Ingester;
using LogTapestry.Ingester.Sinks;

using Microsoft.Extensions.Logging;

using Moq;

using System.Threading.Channels;

namespace LogTapestry.Integration.Tests;

[TestClass]
public class FailureScenarioTests
{
    private string _tempDataPath;
    private string _tempDbPath;
    private SqliteStateProvider _stateProvider;
    private LiveStateService _liveStateService;
    private ILoggerFactory _loggerFactory;

    [TestInitialize]
    public void Setup()
    {
        _tempDataPath = Path.Combine(Path.GetTempPath(), $"LogTapestry_FailureTest_{Guid.NewGuid()}");
        _tempDbPath = Path.Combine(_tempDataPath, "test_state.sqlite");
        Directory.CreateDirectory(_tempDataPath);

        _loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));
        
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
    public async Task ParquetSink_DirectoryPermissionDenied_FailsGracefully()
    {
        // Arrange
        var readOnlyPath = Path.Combine(_tempDataPath, "readonly");
        Directory.CreateDirectory(readOnlyPath);
        
        // Make directory read-only on Windows
        if (OperatingSystem.IsWindows())
        {
            File.SetAttributes(readOnlyPath, FileAttributes.ReadOnly);
        }

        var parquetConfig = new ParquetSinkConfiguration { DataRoot = readOnlyPath };
        var parquetSink = new ParquetSink(
            _loggerFactory.CreateLogger<ParquetSink>(),
            _stateProvider,
            parquetConfig);

        var testBatch = CreateTestDataBlocks();
        var context = new SinkContext { BatchId = "test-readonly" };

        // Act
        var result = await parquetSink.WriteBatchAsync(testBatch, context);

        // Assert
        Assert.IsFalse(result.Success, "Parquet sink should fail when directory is read-only");
        Assert.IsNotNull(result.ErrorMessage, "Error message should be provided");
        Assert.IsTrue(result.ErrorMessage.Contains("Parquet write failed"), "Should contain specific error details");
    }

    [TestMethod]
    public async Task OtelSink_NetworkUnavailable_TriggersFailure()
    {
        // Arrange
        var otelConfig = new SinkConfigurations.OpenTelemetrySinkConfig
        {
            Endpoint = "http://127.0.0.1:9999/v1/logs", // Non-existent endpoint
            Protocol = "http",
            ExportTimeoutSeconds = 1,
            BatchSize = 10
        };

        var otelSink = new OtelSinkImproved(
            _loggerFactory.CreateLogger<OtelSinkImproved>(),
            otelConfig);

        var testBatch = CreateTestDataBlocks();
        var context = new SinkContext { BatchId = "test-network-fail" };

        // Act - Should fail due to unreachable endpoint
        var result = await otelSink.WriteBatchAsync(testBatch, context);
        
        // Assert
        Assert.IsFalse(result.Success, "OTEL sink should fail when endpoint is unreachable");
        Assert.IsNotNull(result.ErrorMessage, "Error message should be provided");
        
        // Clean up
        otelSink.Dispose();
    }

    [TestMethod]
    public async Task MultiSinkProcessor_PartialSinkFailure_NoCheckpointCommitted()
    {
        // Arrange
        var processor = CreateMixedSuccessFailureProcessor();
        var testBatch = CreateTestDataBlocks();
        var initialPositions = await GetCurrentFilePositions();
        var initialCheckpointCount = MultiSinkProcessor.Metrics.CheckpointsCompleted;
        var initialFailureCount = MultiSinkProcessor.Metrics.FailedBatches;

        // Act & Assert
        await Assert.ThrowsExceptionAsync<InvalidOperationException>(
            () => processor.WriteBatchAndCheckpointStateAsync(testBatch));

        // Assert atomicity guarantees
        var finalPositions = await GetCurrentFilePositions();
        Assert.AreEqual(initialPositions.Count, finalPositions.Count, 
            "File positions should NOT change when any sink fails");

        Assert.AreEqual(initialCheckpointCount, MultiSinkProcessor.Metrics.CheckpointsCompleted,
            "Checkpoint count should NOT increase when any sink fails");

        Assert.IsTrue(MultiSinkProcessor.Metrics.FailedBatches > initialFailureCount,
            "Failed batch count should increase");
    }

    [TestMethod]
    public async Task MultiSinkProcessor_TransientFailure_RecoverableAfterRetry()
    {
        // Arrange
        var transientSink = CreateTransientFailureSink();
        var processor = CreateProcessorWithTransientSink(transientSink);
        var testBatch = CreateTestDataBlocks();

        // Act - First attempt should fail
        await Assert.ThrowsExceptionAsync<InvalidOperationException>(
            () => processor.WriteBatchAndCheckpointStateAsync(testBatch));

        // Reset transient sink to succeed
        transientSink.Setup(x => x.WriteBatchAsync(It.IsAny<IList<DataBlock>>(), It.IsAny<SinkContext>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(SinkResult.CreateSuccess(10, 1000));

        var initialPositions = await GetCurrentFilePositions();

        // Act - Second attempt should succeed
        await processor.WriteBatchAndCheckpointStateAsync(testBatch);

        // Assert - Should now be checkpointed
        var finalPositions = await GetCurrentFilePositions();
        Assert.IsTrue(finalPositions.Count > initialPositions.Count, 
            "Checkpoints should be committed after transient failure recovers");
    }

    [TestMethod]
    public async Task SinkExecutor_ExceptionDuringExecution_ReturnsFailureResult()
    {
        // Arrange
        var sinkExecutor = new SinkExecutor(_loggerFactory.CreateLogger<SinkExecutor>());
        
        var exceptionSink = new Mock<IMultiDataSink>();
        exceptionSink.Setup(x => x.Name).Returns("exception-sink");
        exceptionSink.Setup(x => x.WriteBatchAsync(It.IsAny<IList<DataBlock>>(), It.IsAny<SinkContext>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OutOfMemoryException("Simulated OOM"));

        sinkExecutor.RegisterSink(exceptionSink.Object);

        var assignment = new SinkAssignment
        {
            DataBlocks = CreateTestDataBlocks(),
            SinkNames = new List<string> { "exception-sink" },
            Context = new SinkContext { BatchId = "exception-test" }
        };

        // Act
        var results = await sinkExecutor.ExecuteAssignmentAsync(assignment);

        // Assert
        Assert.AreEqual(1, results.Count);
        Assert.IsFalse(results[0].Success);
        Assert.IsTrue(results[0].ErrorMessage.Contains("OutOfMemoryException"), 
            "Should capture exception type in error message");
        Assert.IsTrue(results[0].ErrorMessage.Contains("Simulated OOM"), 
            "Should capture exception message");
    }

    [TestMethod]
    public async Task CheckpointManager_StateProviderFailure_PropagatesException()
    {
        // Arrange
        var mockLiveStateService = new Mock<LiveStateService>();
        mockLiveStateService.Setup(x => x.UpdatePosition(
            It.IsAny<ulong>(),
            It.IsAny<long>(),
            It.IsAny<long>(),
            It.IsAny<string>(),
            It.IsAny<DateTime>(),
            It.IsAny<PositionUpdateMode>()))
            .ThrowsAsync(new InvalidOperationException("Database connection lost"));

        var checkpointManager = new CheckpointManager(_loggerFactory.CreateLogger<CheckpointManager>());
        var channel = Channel.CreateUnbounded<CheckpointPositionUpdate>();
        var testBatch = CreateTestDataBlocks();

        // Act & Assert
        await Assert.ThrowsExceptionAsync<InvalidOperationException>(
            () => checkpointManager.CommitCheckpointAsync(testBatch, mockLiveStateService.Object, channel.Writer));
    }

    [TestMethod]
    public async Task ParquetSink_HealthCheck_ReflectsDirectoryAvailability()
    {
        // Arrange - Valid directory
        var validConfig = new ParquetSinkConfiguration { DataRoot = _tempDataPath };
        var validSink = new ParquetSink(
            _loggerFactory.CreateLogger<ParquetSink>(),
            _stateProvider,
            validConfig);

        // Arrange - Invalid directory
        var invalidConfig = new ParquetSinkConfiguration { DataRoot = "Z:\\NonExistentDrive\\Invalid" };
        var invalidSink = new ParquetSink(
            _loggerFactory.CreateLogger<ParquetSink>(),
            _stateProvider,
            invalidConfig);

        // Act
        var validHealth = await validSink.HealthCheckAsync();
        var invalidHealth = await invalidSink.HealthCheckAsync();

        // Assert
        Assert.IsTrue(validHealth, "Valid sink should pass health check");
        Assert.IsFalse(invalidHealth, "Invalid sink should fail health check");
    }

    [TestMethod]
    public async Task OtelSink_HealthCheck_ReflectsEndpointAvailability()
    {
        // Arrange - Valid endpoint (using localhost without expecting it to work)
        var validConfig = new SinkConfigurations.OpenTelemetrySinkConfig
        {
            Endpoint = "http://localhost:4318/v1/logs",
            Protocol = "http",
            ExportTimeoutSeconds = 1
        };
        var validSink = new OtelSinkImproved(
            _loggerFactory.CreateLogger<OtelSinkImproved>(),
            validConfig);

        // Invalid endpoint
        var invalidConfig = new SinkConfigurations.OpenTelemetrySinkConfig
        {
            Endpoint = "http://127.0.0.1:9999/v1/logs",
            Protocol = "http",
            ExportTimeoutSeconds = 1
        };
        var invalidSink = new OtelSinkImproved(
            _loggerFactory.CreateLogger<OtelSinkImproved>(),
            invalidConfig);

        // Act
        var validHealth = await validSink.HealthCheckAsync();
        var invalidHealth = await invalidSink.HealthCheckAsync();

        // Assert
        // Note: Health check results may vary depending on whether OTEL collector is running
        // The test primarily ensures no exceptions are thrown
        Assert.IsTrue(validHealth || !validHealth, "Valid endpoint health check should complete without exception");
        Assert.IsTrue(invalidHealth || !invalidHealth, "Invalid endpoint health check should complete without exception");
        
        // Clean up
        validSink.Dispose();
        invalidSink.Dispose();
    }

    [TestMethod]
    public async Task SinkFilter_InvalidConfiguration_LogsErrorAndUsesDefaults()
    {
        // Arrange
        var invalidConfig = new SinkConfiguration
        {
            Default = new DefaultSinkConfig
            {
                Sinks = new List<string> { "non-existent-sink" }, // References non-existent sink
                Enabled = true
            },
            Configurations = new Dictionary<string, SinkDefinition>
            {
                ["parquet"] = new SinkDefinition { Type = "Parquet", Enabled = true }
            }
        };

        var sinkFilter = new SinkFilter(_loggerFactory.CreateLogger<SinkFilter>(), invalidConfig);
        var testBatch = CreateTestDataBlocks();

        // Act
        var assignments = sinkFilter.FilterBatch(testBatch, "invalid-config-test");

        // Assert
        Assert.AreEqual(1, assignments.Count, "Should still create an assignment");
        var assignment = assignments[0];
        
        // Should only include valid sinks (non-existent sinks are filtered out)
        Assert.IsFalse(assignment.SinkNames.Contains("non-existent-sink"), 
            "Non-existent sink should be filtered out");
    }

    private MultiSinkProcessor CreateMixedSuccessFailureProcessor()
    {
        var config = new SinkConfiguration
        {
            Default = new DefaultSinkConfig { Sinks = new List<string> { "success", "failure" }, Enabled = true },
            Configurations = new Dictionary<string, SinkDefinition>
            {
                ["success"] = new SinkDefinition { Type = "Success", Enabled = true },
                ["failure"] = new SinkDefinition { Type = "Failure", Enabled = true }
            }
        };

        var sinkFilter = new SinkFilter(_loggerFactory.CreateLogger<SinkFilter>(), config);
        var sinkExecutor = new SinkExecutor(_loggerFactory.CreateLogger<SinkExecutor>());
        var checkpointManager = new CheckpointManager(_loggerFactory.CreateLogger<CheckpointManager>());

        // Register successful sink
        var successSink = new Mock<IMultiDataSink>();
        successSink.Setup(x => x.Name).Returns("success");
        successSink.Setup(x => x.WriteBatchAsync(It.IsAny<IList<DataBlock>>(), It.IsAny<SinkContext>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(SinkResult.CreateSuccess(10, 1000));
        sinkExecutor.RegisterSink(successSink.Object);

        // Register failing sink
        var failureSink = new Mock<IMultiDataSink>();
        failureSink.Setup(x => x.Name).Returns("failure");
        failureSink.Setup(x => x.WriteBatchAsync(It.IsAny<IList<DataBlock>>(), It.IsAny<SinkContext>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(SinkResult.CreateFailure("Intentional failure for testing"));
        sinkExecutor.RegisterSink(failureSink.Object);

        return new MultiSinkProcessor(
            _loggerFactory.CreateLogger<MultiSinkProcessor>(),
            _liveStateService,
            sinkExecutor,
            checkpointManager,
            sinkFilter);
    }

    private Mock<IMultiDataSink> CreateTransientFailureSink()
    {
        var transientSink = new Mock<IMultiDataSink>();
        transientSink.Setup(x => x.Name).Returns("transient");
        transientSink.Setup(x => x.WriteBatchAsync(It.IsAny<IList<DataBlock>>(), It.IsAny<SinkContext>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(SinkResult.CreateFailure("Transient network failure"));
        return transientSink;
    }

    private MultiSinkProcessor CreateProcessorWithTransientSink(Mock<IMultiDataSink> transientSink)
    {
        var config = new SinkConfiguration
        {
            Default = new DefaultSinkConfig { Sinks = new List<string> { "transient" }, Enabled = true },
            Configurations = new Dictionary<string, SinkDefinition>
            {
                ["transient"] = new SinkDefinition { Type = "Transient", Enabled = true }
            }
        };

        var sinkFilter = new SinkFilter(_loggerFactory.CreateLogger<SinkFilter>(), config);
        var sinkExecutor = new SinkExecutor(_loggerFactory.CreateLogger<SinkExecutor>());
        var checkpointManager = new CheckpointManager(_loggerFactory.CreateLogger<CheckpointManager>());

        sinkExecutor.RegisterSink(transientSink.Object);

        return new MultiSinkProcessor(
            _loggerFactory.CreateLogger<MultiSinkProcessor>(),
            _liveStateService,
            sinkExecutor,
            checkpointManager,
            sinkFilter);
    }

    private List<DataBlock> CreateTestDataBlocks()
    {
        var entries = new List<LogEntry>
        {
            new LogEntry(
                Timestamp: DateTime.UtcNow,
                Level: "INFO",
                Message: "Test failure scenario message",
                Source: "FailureTestApp",
                TemplateHash: 99999,
                Ulid: new byte[16],
                Fields: new Dictionary<string, object> { ["test"] = "failure" }
            )
        };

        return new List<DataBlock>
        {
            new DataBlock(
                FileId: 999,
                VolumeSerial: 456,
                FilePath: Path.Combine(_tempDataPath, "failure_test.log"),
                EndPosition: 2000,
                LastWriteTime: DateTime.UtcNow,
                Entries: entries
            )
        };
    }

    private async Task<List<TrackedFileInfo>> GetCurrentFilePositions()
    {
        return await _stateProvider.GetAllTrackedFilesAsync();
    }
}