using LogTapestry.Core;
using LogTapestry.Ingester;

using Microsoft.Extensions.Logging;

using Moq;

namespace LogTapestry.Core.Tests;

[TestClass]
public class SinkFilterTests
{
    private Mock<ILogger<SinkFilter>> _mockLogger;
    private SinkConfiguration _config;
    private SinkFilter _filter;

    [TestInitialize]
    public void Setup()
    {
        _mockLogger = new Mock<ILogger<SinkFilter>>();
        _config = CreateTestConfiguration();
        _filter = new SinkFilter(_mockLogger.Object, _config);
    }

    [TestMethod]
    public void FilterBatch_DefaultConfigOnly_AssignsToDefaultSinks()
    {
        // Arrange
        var testBatch = CreateTestDataBlocks();
        var batchId = "test-batch-123";

        // Act
        var assignments = _filter.FilterBatch(testBatch, batchId);

        // Assert
        Assert.AreEqual(1, assignments.Count);
        var assignment = assignments[0];
        Assert.AreEqual(1, assignment.DataBlocks.Count);
        CollectionAssert.AreEqual(new[] { "parquet" }, assignment.SinkNames.ToArray());
        Assert.AreEqual(batchId, assignment.Context.BatchId);
    }

    [TestMethod]
    public void FilterBatch_WithErrorFilter_RoutesErrorsCorrectly()
    {
        // Arrange
        _config.Filters.Add(new SinkFilterRule
        {
            Name = "ErrorsToOtel",
            Expression = "Level == 'ERROR'",
            Sinks = new List<string> { "otel" },
            Enabled = true,
            Priority = 100
        });

        // Enable OTEL sink
        _config.Configurations["otel"].Enabled = true;

        var testBatch = CreateMixedLevelDataBlocks(); // Contains both INFO and ERROR entries
        var batchId = "test-batch-456";

        // Act
        var assignments = _filter.FilterBatch(testBatch, batchId);

        // Assert
        Assert.AreEqual(2, assignments.Count);

        // Check ERROR assignment
        var errorAssignment = assignments.FirstOrDefault(a => a.SinkNames.Contains("otel"));
        Assert.IsNotNull(errorAssignment);
        Assert.AreEqual(1, errorAssignment.DataBlocks.Count);
        Assert.AreEqual(1, errorAssignment.DataBlocks[0].Entries.Count);
        Assert.AreEqual("ERROR", errorAssignment.DataBlocks[0].Entries[0].Level);

        // Check INFO assignment (goes to default)
        var infoAssignment = assignments.FirstOrDefault(a => a.SinkNames.Contains("parquet") && !a.SinkNames.Contains("otel"));
        Assert.IsNotNull(infoAssignment);
        Assert.AreEqual(1, infoAssignment.DataBlocks.Count);
        Assert.AreEqual(1, infoAssignment.DataBlocks[0].Entries.Count);
        Assert.AreEqual("INFO", infoAssignment.DataBlocks[0].Entries[0].Level);
    }

    [TestMethod]
    public void FilterBatch_WithSourceFilter_RoutesSourcesCorrectly()
    {
        // Arrange
        _config.Filters.Add(new SinkFilterRule
        {
            Name = "HighVolumeToParquetOnly",
            Expression = "Source.Contains('HighVolumeApp')",
            Sinks = new List<string> { "parquet" },
            Enabled = true,
            Priority = 50
        });

        var testBatch = CreateMixedSourceDataBlocks(); // Contains both TestApp and HighVolumeApp entries
        var batchId = "test-batch-789";

        // Act
        var assignments = _filter.FilterBatch(testBatch, batchId);

        // Assert
        Assert.AreEqual(2, assignments.Count);

        // Check HighVolumeApp assignment
        var highVolumeAssignment = assignments.FirstOrDefault(a => a.Context.Properties.ContainsKey("rule_name") && 
            a.Context.Properties["rule_name"].ToString() == "HighVolumeToParquetOnly");
        Assert.IsNotNull(highVolumeAssignment);
        Assert.IsTrue(highVolumeAssignment.DataBlocks[0].Entries.All(e => e.Source.Contains("HighVolumeApp")));

        // Check default assignment for remaining entries
        var defaultAssignment = assignments.FirstOrDefault(a => a.Context.Properties.ContainsKey("rule_name") && 
            a.Context.Properties["rule_name"].ToString() == "default");
        Assert.IsNotNull(defaultAssignment);
        Assert.IsTrue(defaultAssignment.DataBlocks[0].Entries.All(e => !e.Source.Contains("HighVolumeApp")));
    }

    [TestMethod]
    public void FilterBatch_WithStopProcessingRule_StopsAfterFirstMatch()
    {
        // Arrange
        _config.Filters.Add(new SinkFilterRule
        {
            Name = "StopAfterError",
            Expression = "Level == 'ERROR'",
            Sinks = new List<string> { "otel" },
            Enabled = true,
            Priority = 100,
            StopProcessing = true
        });

        _config.Filters.Add(new SinkFilterRule
        {
            Name = "AllToParquet",
            Expression = "*",
            Sinks = new List<string> { "parquet" },
            Enabled = true,
            Priority = 50
        });

        // Enable OTEL sink
        _config.Configurations["otel"].Enabled = true;

        var testBatch = CreateErrorDataBlocks();
        var batchId = "test-batch-stop";

        // Act
        var assignments = _filter.FilterBatch(testBatch, batchId);

        // Assert
        Assert.AreEqual(1, assignments.Count); // Only the first rule should apply
        Assert.IsTrue(assignments[0].SinkNames.Contains("otel"));
        Assert.IsFalse(assignments[0].SinkNames.Contains("parquet"));
    }

    [TestMethod]
    public void FilterBatch_DisabledFilter_IgnoresFilter()
    {
        // Arrange
        _config.Filters.Add(new SinkFilterRule
        {
            Name = "DisabledRule",
            Expression = "Level == 'ERROR'",
            Sinks = new List<string> { "otel" },
            Enabled = false, // Disabled
            Priority = 100
        });

        var testBatch = CreateErrorDataBlocks();
        var batchId = "test-batch-disabled";

        // Act
        var assignments = _filter.FilterBatch(testBatch, batchId);

        // Assert
        Assert.AreEqual(1, assignments.Count);
        // Should only get default assignment since the filter is disabled
        var assignment = assignments[0];
        CollectionAssert.AreEqual(new[] { "parquet" }, assignment.SinkNames.ToArray());
    }

    [TestMethod]
    public void FilterBatch_EmptyBatch_ReturnsEmptyAssignments()
    {
        // Arrange
        var emptyBatch = new List<DataBlock>();
        var batchId = "empty-batch";

        // Act
        var assignments = _filter.FilterBatch(emptyBatch, batchId);

        // Assert
        Assert.AreEqual(0, assignments.Count);
    }

    [TestMethod]
    public void FilterBatch_InvalidExpression_LogsWarningAndSkipsFilter()
    {
        // Arrange
        _config.Filters.Add(new SinkFilterRule
        {
            Name = "InvalidExpression",
            Expression = "Invalid.Syntax.Here()",
            Sinks = new List<string> { "otel" },
            Enabled = true,
            Priority = 100
        });

        var testBatch = CreateTestDataBlocks();
        var batchId = "test-batch-invalid";

        // Act
        var assignments = _filter.FilterBatch(testBatch, batchId);

        // Assert
        // Should still get default assignment
        Assert.AreEqual(1, assignments.Count);
        CollectionAssert.AreEqual(new[] { "parquet" }, assignments[0].SinkNames.ToArray());
    }

    private SinkConfiguration CreateTestConfiguration()
    {
        return new SinkConfiguration
        {
            Default = new DefaultSinkConfig
            {
                FilterExpression = "*",
                Sinks = new List<string> { "parquet" },
                Enabled = true
            },
            Configurations = new Dictionary<string, SinkDefinition>
            {
                ["parquet"] = new SinkDefinition
                {
                    Type = "Parquet",
                    Enabled = true,
                    Priority = 100
                },
                ["otel"] = new SinkDefinition
                {
                    Type = "OpenTelemetry",
                    Enabled = false, // Disabled by default for tests
                    Priority = 90
                }
            },
            Filters = new List<SinkFilterRule>()
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
                Level: "INFO",
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
            new DataBlock(123, 456, "test1.log", 1000, DateTime.UtcNow, new List<LogEntry> { infoEntry }),
            new DataBlock(124, 456, "test2.log", 2000, DateTime.UtcNow, new List<LogEntry> { errorEntry })
        };
    }

    private List<DataBlock> CreateMixedSourceDataBlocks()
    {
        var normalAppEntry = new LogEntry(
            Timestamp: DateTime.UtcNow,
            Level: "INFO",
            Message: "Normal app message",
            Source: "TestApp",
            TemplateHash: 12345,
            Ulid: new byte[16],
            Fields: new Dictionary<string, object>()
        );

        var highVolumeAppEntry = new LogEntry(
            Timestamp: DateTime.UtcNow,
            Level: "INFO",
            Message: "High volume app message",
            Source: "HighVolumeApp.Service",
            TemplateHash: 67890,
            Ulid: new byte[16],
            Fields: new Dictionary<string, object>()
        );

        return new List<DataBlock>
        {
            new DataBlock(123, 456, "normal.log", 1000, DateTime.UtcNow, new List<LogEntry> { normalAppEntry }),
            new DataBlock(124, 456, "highvolume.log", 2000, DateTime.UtcNow, new List<LogEntry> { highVolumeAppEntry })
        };
    }

    private List<DataBlock> CreateErrorDataBlocks()
    {
        var errorEntry = new LogEntry(
            Timestamp: DateTime.UtcNow,
            Level: "ERROR",
            Message: "Critical error occurred",
            Source: "TestApp",
            TemplateHash: 99999,
            Ulid: new byte[16],
            Fields: new Dictionary<string, object>()
        );

        return new List<DataBlock>
        {
            new DataBlock(125, 456, "error.log", 3000, DateTime.UtcNow, new List<LogEntry> { errorEntry })
        };
    }
}