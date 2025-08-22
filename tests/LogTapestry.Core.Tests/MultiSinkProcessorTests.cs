using LogTapestry.Core;
using LogTapestry.Core.Interfaces;
using LogTapestry.Ingester;

using Microsoft.Extensions.Logging;

using Moq;

using System.Threading.Channels;

namespace LogTapestry.Core.Tests;

[TestClass]
public class MultiSinkProcessorTests
{
    private Mock<ILogger<MultiSinkProcessor>> _mockLogger;
    private Mock<IStateProvider> _mockStateProvider;
    private LiveStateService _liveStateService;
    private Mock<SinkExecutor> _mockSinkExecutor;
    private Mock<CheckpointManager> _mockCheckpointManager;
    private Mock<SinkFilter> _mockSinkFilter;
    private MultiSinkProcessor _processor;
    private string _tempDbPath;
    private ILoggerFactory _loggerFactory;

    [TestInitialize]
    public void Setup()
    {
        _tempDbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.sqlite");
        _loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
        
        _mockLogger = new Mock<ILogger<MultiSinkProcessor>>();
        _mockStateProvider = new Mock<IStateProvider>();
        
        // Create a real LiveStateService since it's hard to mock
        var sqliteProvider = new SqliteStateProvider(_tempDbPath);
        _liveStateService = new LiveStateService(
            _loggerFactory.CreateLogger<LiveStateService>(),
            sqliteProvider);
        
        _mockSinkExecutor = new Mock<SinkExecutor>(Mock.Of<ILogger<SinkExecutor>>());
        _mockCheckpointManager = new Mock<CheckpointManager>(Mock.Of<ILogger<CheckpointManager>>());
        _mockSinkFilter = new Mock<SinkFilter>(Mock.Of<ILogger<SinkFilter>>(), new SinkConfiguration());

        _processor = new MultiSinkProcessor(
            _mockLogger.Object,
            _liveStateService,
            _mockSinkExecutor.Object,
            _mockCheckpointManager.Object,
            _mockSinkFilter.Object
        );
    }

    [TestCleanup]
    public void Cleanup()
    {
        // LiveStateService doesn't implement IDisposable, so we don't dispose it
        _loggerFactory?.Dispose();
        
        if (File.Exists(_tempDbPath))
        {
            try { File.Delete(_tempDbPath); } catch { }
        }
    }

    [TestMethod]
    public async Task WriteBatchAndCheckpointStateAsync_EmptyBatch_ReturnsImmediately()
    {
        // Arrange
        var emptyBatch = new List<DataBlock>();

        // Act
        await _processor.WriteBatchAndCheckpointStateAsync(emptyBatch);

        // Assert
        _mockSinkFilter.Verify(x => x.FilterBatch(It.IsAny<IList<DataBlock>>(), It.IsAny<string>()), Times.Never);
        _mockSinkExecutor.Verify(x => x.ExecuteAssignmentAsync(It.IsAny<SinkAssignment>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [TestMethod]
    public async Task WriteBatchAndCheckpointStateAsync_AllSinksSucceed_CheckpointsCommitted()
    {
        // Arrange
        var testBatch = CreateTestDataBlocks();
        var testAssignment = CreateTestSinkAssignment(testBatch);
        var successResults = new List<SinkResult> 
        { 
            SinkResult.CreateSuccess(10, 1000),
            SinkResult.CreateSuccess(10, 800)
        };

        _mockSinkFilter.Setup(x => x.FilterBatch(It.IsAny<IList<DataBlock>>(), It.IsAny<string>()))
            .Returns(new List<SinkAssignment> { testAssignment });

        _mockSinkExecutor.Setup(x => x.ExecuteAssignmentAsync(testAssignment, It.IsAny<CancellationToken>()))
            .ReturnsAsync(successResults);

        // Act
        await _processor.WriteBatchAndCheckpointStateAsync(testBatch);

        // Assert
        _mockCheckpointManager.Verify(x => x.CommitCheckpointAsync(
            testBatch,
            _liveStateService,
            It.IsAny<ChannelWriter<CheckpointPositionUpdate>>(),
            It.IsAny<CancellationToken>()), Times.Once);

        // Verify metrics are updated
        Assert.AreEqual(1, MultiSinkProcessor.Metrics.CheckpointsCompleted);
        Assert.AreEqual(20, MultiSinkProcessor.Metrics.LogEntriesIngested);
    }

    [TestMethod]
    public async Task WriteBatchAndCheckpointStateAsync_SomeSinksFail_ThrowsException()
    {
        // Arrange
        var testBatch = CreateTestDataBlocks();
        var testAssignment = CreateTestSinkAssignment(testBatch);
        var mixedResults = new List<SinkResult>
        {
            SinkResult.CreateSuccess(10, 1000),
            SinkResult.CreateFailure("Sink failed")
        };

        _mockSinkFilter.Setup(x => x.FilterBatch(It.IsAny<IList<DataBlock>>(), It.IsAny<string>()))
            .Returns(new List<SinkAssignment> { testAssignment });

        _mockSinkExecutor.Setup(x => x.ExecuteAssignmentAsync(testAssignment, It.IsAny<CancellationToken>()))
            .ReturnsAsync(mixedResults);

        // Act & Assert
        var exception = await Assert.ThrowsExceptionAsync<InvalidOperationException>(
            () => _processor.WriteBatchAndCheckpointStateAsync(testBatch));

        Assert.IsTrue(exception.Message.Contains("1 sinks failed"));

        // Verify checkpoint was NOT committed
        _mockCheckpointManager.Verify(x => x.CommitCheckpointAsync(
            It.IsAny<IList<DataBlock>>(),
            It.IsAny<LiveStateService>(),
            It.IsAny<ChannelWriter<CheckpointPositionUpdate>>(),
            It.IsAny<CancellationToken>()), Times.Never);

        // Verify failure metrics
        Assert.AreEqual(1, MultiSinkProcessor.Metrics.FailedBatches);
    }

    [TestMethod]
    public async Task WriteBatchAndCheckpointStateAsync_NoSinkAssignments_ReturnsWithoutError()
    {
        // Arrange
        var testBatch = CreateTestDataBlocks();

        _mockSinkFilter.Setup(x => x.FilterBatch(It.IsAny<IList<DataBlock>>(), It.IsAny<string>()))
            .Returns(new List<SinkAssignment>());

        // Act
        await _processor.WriteBatchAndCheckpointStateAsync(testBatch);

        // Assert
        _mockSinkExecutor.Verify(x => x.ExecuteAssignmentAsync(It.IsAny<SinkAssignment>(), It.IsAny<CancellationToken>()), Times.Never);
        _mockCheckpointManager.Verify(x => x.CommitCheckpointAsync(It.IsAny<IList<DataBlock>>(), It.IsAny<LiveStateService>(), It.IsAny<ChannelWriter<CheckpointPositionUpdate>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [TestMethod]
    public void LegacyWriteBatchAsync_ThrowsNotSupportedException()
    {
        // Arrange
        var testEntries = new LogEntry[] { CreateTestLogEntry() };

        // Act & Assert
        Assert.ThrowsException<NotSupportedException>(
            () => _processor.WriteBatchAsync(testEntries, Stream.Null));
    }

    private List<DataBlock> CreateTestDataBlocks()
    {
        var entries = new List<LogEntry> { CreateTestLogEntry(), CreateTestLogEntry() };
        
        return new List<DataBlock>
        {
            new DataBlock(
                FileId: 123,
                VolumeSerial: 456,
                FilePath: "test.log",
                EndPosition: 1000,
                LastWriteTime: DateTime.UtcNow,
                Entries: entries
            )
        };
    }

    private LogEntry CreateTestLogEntry()
    {
        return new LogEntry(
            Timestamp: DateTime.UtcNow,
            Level: "INFO",
            Message: "Test message",
            Source: "TestApp",
            TemplateHash: 12345,
            Ulid: new byte[16],
            Fields: new Dictionary<string, object> { ["test"] = "value" }
        );
    }

    private SinkAssignment CreateTestSinkAssignment(List<DataBlock> dataBlocks)
    {
        return new SinkAssignment
        {
            DataBlocks = dataBlocks,
            SinkNames = new List<string> { "parquet", "otel" },
            Context = new SinkContext
            {
                BatchId = "test-batch-123",
                BatchTimestamp = DateTime.UtcNow
            }
        };
    }
}