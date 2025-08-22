using System.Text;
using LogTapestry.Core;

namespace LogTapestry.Core.Tests;

[TestClass]
public sealed class SepEnhancementValidationTests
{
    /// <summary>
    /// Validates that quote unescaping is properly enabled and working correctly
    /// </summary>
    [TestMethod]
    public async Task ValidateQuoteUnescaping_HandlesEscapedQuotesCorrectly()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
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
        
        // Test various quote escaping scenarios
        var csvData = "message,description\n" +
                     "\"Simple message\",\"No quotes here\"\n" +
                     "\"Message with \"\"escaped\"\" quotes\",\"Desc with \"\"quotes\"\"\"\n" +
                     "\"\"\"Starting quote\"\"\",\"\"\"Ending quote\"\"\"\n" +
                     "\"Complex \"\"inner\"\" text\",\"Multi \"\"quote\"\" scenario\"\n";
        
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));
        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);

        Assert.AreEqual(4, result.SuccessfulEntries.Count);
        Assert.AreEqual(0, result.Failures.Count);

        // Verify proper quote unescaping
        Assert.AreEqual("Simple message", result.SuccessfulEntries[0].Message);
        Assert.AreEqual("No quotes here", result.SuccessfulEntries[0].Fields["description"]);

        Assert.AreEqual("Message with \"escaped\" quotes", result.SuccessfulEntries[1].Message);
        Assert.AreEqual("Desc with \"quotes\"", result.SuccessfulEntries[1].Fields["description"]);

        Assert.AreEqual("\"Starting quote\"", result.SuccessfulEntries[2].Message);
        Assert.AreEqual("\"Ending quote\"", result.SuccessfulEntries[2].Fields["description"]);

        Assert.AreEqual("Complex \"inner\" text", result.SuccessfulEntries[3].Message);
        Assert.AreEqual("Multi \"quote\" scenario", result.SuccessfulEntries[3].Fields["description"]);
    }

    /// <summary>
    /// Validates that chunking works correctly with complex multi-line data
    /// </summary>
    [TestMethod]
    public async Task ValidateChunking_HandlesMultiLineFieldsCorrectly()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                MessageColumn = "message",
                MaxRecordsPerChunk = 2,
                FieldMappings = [
                    new CsvFieldMapping { ColumnName = "description", FieldName = "description", Type = "string" }
                ]
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        
        // Create data with multi-line fields within quotes
        var csvData = "message,description\n" +
                     "\"First message\",\"Simple description\"\n" +
                     "\"Second message\",\"This is a\nmulti-line\ndescription\"\n" +
                     "\"Third message\",\"Another\nmulti-line field\"\n" +
                     "\"Fourth message\",\"Final description\"\n";
        
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));
        var allEntries = new List<LogEntry>();

        // Process all chunks
        while (true)
        {
            var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
            if (result.SuccessfulEntries.Count == 0)
                break;

            allEntries.AddRange(result.SuccessfulEntries);
            Assert.AreEqual(0, result.Failures.Count, "Should have no parsing failures");
            
            // Update stream position for next chunk
            parser.UpdateUnderlyingStreamPosition(stream);
        }

        // Verify all records were processed correctly
        Assert.AreEqual(4, allEntries.Count);
        
        Assert.AreEqual("First message", allEntries[0].Message);
        Assert.AreEqual("Simple description", allEntries[0].Fields["description"]);

        Assert.AreEqual("Second message", allEntries[1].Message);
        Assert.IsTrue(((string)allEntries[1].Fields["description"]).Contains("\n"));
        Assert.AreEqual("This is a\nmulti-line\ndescription", allEntries[1].Fields["description"]);

        Assert.AreEqual("Third message", allEntries[2].Message);
        Assert.AreEqual("Another\nmulti-line field", allEntries[2].Fields["description"]);

        Assert.AreEqual("Fourth message", allEntries[3].Message);
        Assert.AreEqual("Final description", allEntries[3].Fields["description"]);
    }

    /// <summary>
    /// Validates that position tracking provides meaningful checkpointing information
    /// even with Sep's buffering behavior
    /// </summary>
    [TestMethod]
    public async Task ValidatePositionTracking_ProvidesReliableCheckpoints()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                MessageColumn = "message",
                MaxRecordsPerChunk = 3
            }
        };

        // Create larger dataset to test position tracking
        var sb = new StringBuilder("message\n");
        for (int i = 1; i <= 10; i++)
        {
            sb.AppendLine($"Message {i}");
        }
        var csvData = sb.ToString();
        var csvBytes = Encoding.UTF8.GetBytes(csvData);

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var stream = new MemoryStream(csvBytes);
        
        var allEntries = new List<LogEntry>();
        var positions = new List<long>();
        var chunkSizes = new List<int>();

        // Process in chunks and track positions
        while (true)
        {
            var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
            if (result.SuccessfulEntries.Count == 0)
                break;

            allEntries.AddRange(result.SuccessfulEntries);
            chunkSizes.Add(result.SuccessfulEntries.Count);
            
            var position = parser.GetCurrentPosition();
            positions.Add(position);
            
            parser.UpdateUnderlyingStreamPosition(stream);
        }

        // Verify that we processed all records
        Assert.AreEqual(10, allEntries.Count);
        
        // Verify that position progresses meaningfully
        Assert.IsTrue(positions.Count > 1, "Should have multiple chunks");
        
        // Position should end at or near the total file size
        var finalPosition = positions.Last();
        Assert.IsTrue(finalPosition >= csvBytes.Length * 0.8, 
            $"Final position {finalPosition} should be close to file size {csvBytes.Length}");
        
        // Verify chunk sizes respect the limit
        foreach (var chunkSize in chunkSizes)
        {
            Assert.IsTrue(chunkSize <= 3, $"Chunk size {chunkSize} should not exceed limit of 3");
        }
        
        Console.WriteLine($"Processed {allEntries.Count} records in {chunkSizes.Count} chunks");
        Console.WriteLine($"Chunk sizes: {string.Join(", ", chunkSizes)}");
        Console.WriteLine($"Final position: {finalPosition} / {csvBytes.Length}");
    }

    /// <summary>
    /// Validates that Unicode and special characters are handled correctly
    /// </summary>
    [TestMethod]
    public async Task ValidateUnicodeHandling_ProcessesInternationalCharacters()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
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
        
        var csvData = "message,description\n" +
                     "\"Hello 世界\",\"Unicode test\"\n" +
                     "\"Café ñoño\",\"Español characters\"\n" +
                     "\"Emoji 🚀 test\",\"With emojis 🎉 ✨\"\n" +
                     "\"Symbols €£¥\",\"Currency symbols\"\n";
        
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));
        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);

        Assert.AreEqual(4, result.SuccessfulEntries.Count);
        Assert.AreEqual(0, result.Failures.Count);

        Assert.AreEqual("Hello 世界", result.SuccessfulEntries[0].Message);
        Assert.AreEqual("Café ñoño", result.SuccessfulEntries[1].Message);
        Assert.AreEqual("Emoji 🚀 test", result.SuccessfulEntries[2].Message);
        Assert.AreEqual("Symbols €£¥", result.SuccessfulEntries[3].Message);

        // Verify Unicode in fields
        Assert.AreEqual("With emojis 🎉 ✨", result.SuccessfulEntries[2].Fields["description"]);
        Assert.AreEqual("Currency symbols", result.SuccessfulEntries[3].Fields["description"]);
    }
}