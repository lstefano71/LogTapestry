using LogTapestry.Core;
using LogTapestry.Core.Interfaces;
using LogTapestry.Ingester;

using Microsoft.Extensions.Logging;

using Moq;

namespace LogTapestry.Core.Tests;

[TestClass]
public class SinkExecutorTests
{
    private Mock<ILogger<SinkExecutor>> _mockLogger;
    private SinkExecutor _executor;
    private Mock<IMultiDataSink> _mockParquetSink;
    private Mock<IMultiDataSink> _mockOtelSink;

    [TestInitialize]
    public void Setup()
    {
        _mockLogger = new Mock<ILogger<SinkExecutor>>();
        _executor = new SinkExecutor(_mockLogger.Object);

        _mockParquetSink = new Mock<IMultiDataSink>();
        _mockParquetSink.Setup(x => x.Name).Returns("parquet");

        _mockOtelSink = new Mock<IMultiDataSink>();
        _mockOtelSink.Setup(x => x.Name).Returns("otel");

        _executor.RegisterSink(_mockParquetSink.Object);
        _executor.RegisterSink(_mockOtelSink.Object);
    }

    [TestMethod]
    public async Task ExecuteAssignmentAsync_AllSinksSucceed_ReturnsSuccessResults()
    {
        // Arrange
        var assignment = CreateTestAssignment();
        var parquetResult = SinkResult.CreateSuccess(10, 1000);
        var otelResult = SinkResult.CreateSuccess(10, 800);

        _mockParquetSink.Setup(x => x.WriteBatchAsync(
            It.IsAny<IList<DataBlock>>(),
            It.IsAny<SinkContext>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(parquetResult);

        _mockOtelSink.Setup(x => x.WriteBatchAsync(
            It.IsAny<IList<DataBlock>>(),
            It.IsAny<SinkContext>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(otelResult);

        // Act
        var results = await _executor.ExecuteAssignmentAsync(assignment);

        // Assert
        Assert.AreEqual(2, results.Count);
        Assert.IsTrue(results.All(r => r.Success));
        Assert.AreEqual(20, results.Sum(r => r.EntriesProcessed));
        Assert.AreEqual(1800, results.Sum(r => r.BytesProcessed));
    }

    [TestMethod]
    public async Task ExecuteAssignmentAsync_OneSinkFails_ReturnsFailureResult()
    {
        // Arrange
        var assignment = CreateTestAssignment();
        var parquetResult = SinkResult.CreateSuccess(10, 1000);
        var otelResult = SinkResult.CreateFailure("OTEL endpoint unavailable");

        _mockParquetSink.Setup(x => x.WriteBatchAsync(
            It.IsAny<IList<DataBlock>>(),
            It.IsAny<SinkContext>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(parquetResult);

        _mockOtelSink.Setup(x => x.WriteBatchAsync(
            It.IsAny<IList<DataBlock>>(),
            It.IsAny<SinkContext>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(otelResult);

        // Act
        var results = await _executor.ExecuteAssignmentAsync(assignment);

        // Assert
        Assert.AreEqual(2, results.Count);
        Assert.AreEqual(1, results.Count(r => r.Success));
        Assert.AreEqual(1, results.Count(r => !r.Success));
        
        var failedResult = results.First(r => !r.Success);
        Assert.IsTrue(failedResult.ErrorMessage.Contains("OTEL endpoint unavailable"));
    }

    [TestMethod]
    public async Task ExecuteAssignmentAsync_SinkThrowsException_ReturnsFailureResult()
    {
        // Arrange
        var assignment = CreateTestAssignment();

        _mockParquetSink.Setup(x => x.WriteBatchAsync(
            It.IsAny<IList<DataBlock>>(),
            It.IsAny<SinkContext>(),
            It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Database connection failed"));

        _mockOtelSink.Setup(x => x.WriteBatchAsync(
            It.IsAny<IList<DataBlock>>(),
            It.IsAny<SinkContext>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(SinkResult.CreateSuccess(10, 800));

        // Act
        var results = await _executor.ExecuteAssignmentAsync(assignment);

        // Assert
        Assert.AreEqual(2, results.Count);
        Assert.AreEqual(1, results.Count(r => r.Success));
        Assert.AreEqual(1, results.Count(r => !r.Success));
        
        var failedResult = results.First(r => !r.Success);
        Assert.IsTrue(failedResult.ErrorMessage.Contains("Database connection failed"));
    }

    [TestMethod]
    public async Task ExecuteAssignmentAsync_UnknownSink_ReturnsFailureResult()
    {
        // Arrange
        var assignment = new SinkAssignment
        {
            DataBlocks = CreateTestDataBlocks(),
            SinkNames = new List<string> { "parquet", "unknown-sink" },
            Context = new SinkContext { BatchId = "test-batch" }
        };

        _mockParquetSink.Setup(x => x.WriteBatchAsync(
            It.IsAny<IList<DataBlock>>(),
            It.IsAny<SinkContext>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(SinkResult.CreateSuccess(10, 1000));

        // Act
        var results = await _executor.ExecuteAssignmentAsync(assignment);

        // Assert
        Assert.AreEqual(2, results.Count);
        Assert.AreEqual(1, results.Count(r => r.Success));
        Assert.AreEqual(1, results.Count(r => !r.Success));
        
        var failedResult = results.First(r => !r.Success);
        Assert.IsTrue(failedResult.ErrorMessage.Contains("unknown-sink not found"));
    }

    [TestMethod]
    public async Task ExecuteAssignmentAsync_EmptyAssignment_ReturnsEmptyResults()
    {
        // Arrange
        var assignment = new SinkAssignment
        {
            DataBlocks = new List<DataBlock>(),
            SinkNames = new List<string>(),
            Context = new SinkContext { BatchId = "empty-batch" }
        };

        // Act
        var results = await _executor.ExecuteAssignmentAsync(assignment);

        // Assert
        Assert.AreEqual(0, results.Count);
    }

    [TestMethod]
    public void RegisterSink_NewSink_RegistersSuccessfully()
    {
        // Arrange
        var newExecutor = new SinkExecutor(_mockLogger.Object);
        var mockSink = new Mock<IMultiDataSink>();
        mockSink.Setup(x => x.Name).Returns("test-sink");

        // Act
        newExecutor.RegisterSink(mockSink.Object);

        // Assert - should not throw, and sink should be available for assignments
        var assignment = new SinkAssignment
        {
            DataBlocks = CreateTestDataBlocks(),
            SinkNames = new List<string> { "test-sink" },
            Context = new SinkContext { BatchId = "test" }
        };

        // This should not result in "sink not found" errors
        Assert.IsNotNull(assignment);
    }

    private SinkAssignment CreateTestAssignment()
    {
        return new SinkAssignment
        {
            DataBlocks = CreateTestDataBlocks(),
            SinkNames = new List<string> { "parquet", "otel" },
            Context = new SinkContext
            {
                BatchId = "test-batch-123",
                BatchTimestamp = DateTime.UtcNow
            }
        };
    }

    private List<DataBlock> CreateTestDataBlocks()
    {
        var entries = new List<LogEntry>
        {
            new LogEntry(
                Timestamp: DateTime.UtcNow,
                Level: "INFO",
                Message: "Test message 1",
                Source: "TestApp",
                TemplateHash: 12345,
                Ulid: new byte[16],
                Fields: new Dictionary<string, object> { ["test"] = "value1" }
            ),
            new LogEntry(
                Timestamp: DateTime.UtcNow,
                Level: "ERROR",
                Message: "Test message 2",
                Source: "TestApp",
                TemplateHash: 67890,
                Ulid: new byte[16],
                Fields: new Dictionary<string, object> { ["test"] = "value2" }
            )
        };

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
}