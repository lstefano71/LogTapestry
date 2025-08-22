using System.Text;
using LogTapestry.Core;

namespace LogTapestry.Core.Tests;

[TestClass]
public sealed class SepCsvLogParserTests
{
    [TestMethod]
    public async Task SepCsvLogParser_ParsesSimpleCsvRecord()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            Name = "test_csv",
            IncludePatterns = ["*.csv"],
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                TimestampColumn = "timestamp",
                LevelColumn = "level",
                MessageColumn = "message",
                TimestampFormat = "yyyy-MM-dd HH:mm:ss",
                FieldMappings = [
                    new CsvFieldMapping { ColumnName = "user_id", FieldName = "user_id", Type = "long" },
                    new CsvFieldMapping { ColumnName = "session_id", FieldName = "session_id", Type = "string" }
                ]
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "timestamp,level,message,user_id,session_id\n2025-08-18 16:00:01,INFO,User login succeeded,101,abc123\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);

        Assert.AreEqual(1, result.SuccessfulEntries.Count);
        Assert.AreEqual(0, result.Failures.Count);

        var entry = result.SuccessfulEntries[0];
        Assert.AreEqual("INFO", entry.Level);
        Assert.AreEqual("User login succeeded", entry.Message);
        Assert.AreEqual(101L, entry.Fields["user_id"]);
        Assert.AreEqual("abc123", entry.Fields["session_id"]);
        Assert.AreEqual("test.csv", entry.Source);
    }

    [TestMethod]
    public async Task SepCsvLogParser_HandlesQuotedFields()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            Name = "test_csv",
            IncludePatterns = ["*.csv"],
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                MessageColumn = "message",
                FieldMappings = [
                    new CsvFieldMapping { ColumnName = "description", FieldName = "description", Type = "string" }
                ]
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "message,description\n\"Error occurred\",\"This is a \"\"quoted\"\" description\"\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);

        Assert.AreEqual(1, result.SuccessfulEntries.Count);
        var entry = result.SuccessfulEntries[0];
        Assert.AreEqual("Error occurred", entry.Message);
        Assert.AreEqual("This is a \"quoted\" description", entry.Fields["description"]);
    }

    [TestMethod]
    public async Task SepCsvLogParser_HandlesMultipleRecords()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            Name = "test_csv",
            IncludePatterns = ["*.csv"],
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                LevelColumn = "level",
                MessageColumn = "message",
                MaxRecordsPerChunk = 10
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "level,message\nINFO,First log\nWARN,Second log\nERROR,Third log\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);

        Assert.AreEqual(3, result.SuccessfulEntries.Count);
        Assert.AreEqual("INFO", result.SuccessfulEntries[0].Level);
        Assert.AreEqual("WARN", result.SuccessfulEntries[1].Level);
        Assert.AreEqual("ERROR", result.SuccessfulEntries[2].Level);
    }

    [TestMethod]
    public async Task SepCsvLogParser_HandlesMultiLineFields()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            Name = "test_csv",
            IncludePatterns = ["*.csv"],
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                MessageColumn = "message",
                FieldMappings = [
                    new CsvFieldMapping { ColumnName = "description", FieldName = "description", Type = "string" }
                ]
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        // CSV with multi-line field containing actual newlines
        var csvData = "message,description\n\"Single line\",\"This is a\nmulti-line\ndescription\"\n\"Another record\",\"Simple description\"\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);

        Assert.AreEqual(2, result.SuccessfulEntries.Count);
        Assert.AreEqual(0, result.Failures.Count);

        var firstEntry = result.SuccessfulEntries[0];
        Assert.AreEqual("Single line", firstEntry.Message);
        Assert.AreEqual("This is a\nmulti-line\ndescription", firstEntry.Fields["description"]);

        var secondEntry = result.SuccessfulEntries[1];
        Assert.AreEqual("Another record", secondEntry.Message);
        Assert.AreEqual("Simple description", secondEntry.Fields["description"]);
    }

    [TestMethod]
    public async Task SepCsvLogParser_TracksPositionCorrectly()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            Name = "test_csv",
            IncludePatterns = ["*.csv"],
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                LevelColumn = "level",
                MessageColumn = "message",
                MaxRecordsPerChunk = 2
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "level,message\nINFO,First log\nWARN,Second log\nERROR,Third log\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        // Parse first chunk
        var result1 = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        
        Assert.AreEqual(2, result1.SuccessfulEntries.Count);
        Assert.IsTrue(parser.GetCurrentPosition() > 0);
        
        // Verify position tracking
        var position1 = parser.GetCurrentPosition();
        
        // Parse second chunk
        var result2 = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        
        Assert.AreEqual(1, result2.SuccessfulEntries.Count);
        Assert.IsTrue(parser.GetCurrentPosition() > position1);
        
        // Should have read all data
        var finalPosition = parser.GetCurrentPosition();
        Assert.AreEqual(csvData.Length, finalPosition);
    }

    [TestMethod]
    public async Task SepCsvLogParser_HandlesNoHeaderMode()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            Name = "test_csv",
            IncludePatterns = ["*.csv"],
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = false,
                Delimiter = ",",
                MessageColumn = "1" // Use column index when no header
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "INFO,First log entry\nWARN,Second log entry\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);

        Assert.AreEqual(2, result.SuccessfulEntries.Count);
        Assert.AreEqual("First log entry", result.SuccessfulEntries[0].Message);
        Assert.AreEqual("Second log entry", result.SuccessfulEntries[1].Message);
    }

    [TestMethod]
    public async Task SepCsvLogParser_HandlesSemicolonDelimiter()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            Name = "test_csv",
            IncludePatterns = ["*.csv"],
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ";",
                LevelColumn = "level",
                MessageColumn = "message"
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "level;message\nINFO;First log\nWARN;Second log\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);

        Assert.AreEqual(2, result.SuccessfulEntries.Count);
        Assert.AreEqual("INFO", result.SuccessfulEntries[0].Level);
        Assert.AreEqual("First log", result.SuccessfulEntries[0].Message);
        Assert.AreEqual("WARN", result.SuccessfulEntries[1].Level);
        Assert.AreEqual("Second log", result.SuccessfulEntries[1].Message);
    }

    [TestMethod]
    public async Task SepCsvLogParser_HandlesFieldTypeConversions()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            Name = "test_csv",
            IncludePatterns = ["*.csv"],
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                MessageColumn = "message",
                FieldMappings = [
                    new CsvFieldMapping { ColumnName = "user_id", FieldName = "user_id", Type = "long" },
                    new CsvFieldMapping { ColumnName = "score", FieldName = "score", Type = "double" },
                    new CsvFieldMapping { ColumnName = "active", FieldName = "active", Type = "bool" },
                    new CsvFieldMapping { ColumnName = "count", FieldName = "count", Type = "int" }
                ]
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "message,user_id,score,active,count\nTest message,12345,95.5,true,42\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);

        Assert.AreEqual(1, result.SuccessfulEntries.Count);
        var entry = result.SuccessfulEntries[0];
        
        Assert.AreEqual(12345L, entry.Fields["user_id"]);
        Assert.AreEqual(95.5, entry.Fields["score"]);
        Assert.AreEqual(true, entry.Fields["active"]);
        Assert.AreEqual(42, entry.Fields["count"]);
    }

    [TestMethod]
    public async Task SepCsvLogParser_HandlesColumnCountMismatch()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            Name = "test_csv",
            IncludePatterns = ["*.csv"],
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                MessageColumn = "message"
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "message,level\nValid message,INFO\nIncomplete row\n"; // Second data row missing a column
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);

        Assert.AreEqual(1, result.SuccessfulEntries.Count); // Only the valid row
        Assert.AreEqual(1, result.Failures.Count); // One parsing failure
        
        Assert.AreEqual("Valid message", result.SuccessfulEntries[0].Message);
        Assert.IsTrue(result.Failures[0].ErrorMessage.Contains("Column count mismatch"));
    }

    [TestMethod]
    public async Task SepCsvLogParser_UpdatesUnderlyingStreamPosition()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            Name = "test_csv",
            IncludePatterns = ["*.csv"],
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                MessageColumn = "message"
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "message\nFirst log\nSecond log\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        // Parse data
        await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        
        // Update underlying stream position
        parser.UpdateUnderlyingStreamPosition(stream);
        
        // Verify stream position matches parser position
        Assert.AreEqual(parser.GetCurrentPosition(), stream.Position);
    }

    [TestMethod]
    public async Task SepCsvLogParser_ComparisonWithOriginalCsvParser()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "csv", // Will be ignored by constructors but kept for consistency
            Name = "test_csv",
            IncludePatterns = ["*.csv"],
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                TimestampColumn = "timestamp",
                LevelColumn = "level",
                MessageColumn = "message",
                TimestampFormat = "yyyy-MM-dd HH:mm:ss",
                FieldMappings = [
                    new CsvFieldMapping { ColumnName = "user_id", FieldName = "user_id", Type = "long" },
                    new CsvFieldMapping { ColumnName = "session_id", FieldName = "session_id", Type = "string" }
                ]
            }
        };

        var csvData = "timestamp,level,message,user_id,session_id\n" +
                      "2025-08-18 16:00:01,INFO,User login succeeded,101,abc123\n" +
                      "2025-08-18 16:00:02,WARN,Invalid password attempt,102,def456\n" +
                      "2025-08-18 16:00:03,ERROR,Account locked,103,ghi789\n";

        // Test original parser
        await using var originalParser = new CsvLogParser(pluginSettings, "test.csv");
        var originalStream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));
        var originalResult = await originalParser.ParseNextChunkAsync(originalStream, CancellationToken.None);

        // Test Sep-based parser
        await using var sepParser = new SepCsvLogParser(pluginSettings, "test.csv");
        var sepStream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));
        var sepResult = await sepParser.ParseNextChunkAsync(sepStream, CancellationToken.None);

        // Compare results
        Assert.AreEqual(originalResult.SuccessfulEntries.Count, sepResult.SuccessfulEntries.Count);
        Assert.AreEqual(originalResult.Failures.Count, sepResult.Failures.Count);

        for (int i = 0; i < originalResult.SuccessfulEntries.Count; i++)
        {
            var originalEntry = originalResult.SuccessfulEntries[i];
            var sepEntry = sepResult.SuccessfulEntries[i];

            Assert.AreEqual(originalEntry.Level, sepEntry.Level);
            Assert.AreEqual(originalEntry.Message, sepEntry.Message);
            Assert.AreEqual(originalEntry.Source, sepEntry.Source);
            
            // Compare fields
            Assert.AreEqual(originalEntry.Fields.Count, sepEntry.Fields.Count);
            foreach (var kvp in originalEntry.Fields)
            {
                Assert.IsTrue(sepEntry.Fields.ContainsKey(kvp.Key));
                Assert.AreEqual(kvp.Value, sepEntry.Fields[kvp.Key]);
            }
        }
    }
}