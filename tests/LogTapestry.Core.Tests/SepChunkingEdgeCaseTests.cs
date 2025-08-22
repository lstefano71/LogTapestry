using System.Text;
using LogTapestry.Core;

namespace LogTapestry.Core.Tests;

[TestClass]
public sealed class SepChunkingEdgeCaseTests
{
    /// <summary>
    /// Test chunking when a record boundary falls exactly at the chunk limit
    /// </summary>
    [TestMethod]
    public async Task ChunkingEdgeCase_RecordBoundaryAtChunkLimit()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                MessageColumn = "message",
                MaxRecordsPerChunk = 2 // Small chunk to trigger boundary conditions
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "message\nRecord 1\nRecord 2\nRecord 3\nRecord 4\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        // First chunk should have exactly 2 records
        var result1 = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        Assert.AreEqual(2, result1.SuccessfulEntries.Count);
        Assert.AreEqual("Record 1", result1.SuccessfulEntries[0].Message);
        Assert.AreEqual("Record 2", result1.SuccessfulEntries[1].Message);

        var position1 = parser.GetCurrentPosition();
        parser.UpdateUnderlyingStreamPosition(stream);

        // Second chunk should have the remaining 2 records
        var result2 = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        Assert.AreEqual(2, result2.SuccessfulEntries.Count);
        Assert.AreEqual("Record 3", result2.SuccessfulEntries[0].Message);
        Assert.AreEqual("Record 4", result2.SuccessfulEntries[1].Message);

        var position2 = parser.GetCurrentPosition();
        Assert.IsTrue(position2 > position1);
        Assert.AreEqual(csvData.Length, position2);
    }

    /// <summary>
    /// Test chunking with multi-line quoted fields that span across potential chunk boundaries
    /// </summary>
    [TestMethod]
    public async Task ChunkingEdgeCase_MultiLineQuotedFieldsAcrossChunkBoundary()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                MessageColumn = "message",
                MaxRecordsPerChunk = 1 // Force each record into its own chunk
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "message,description\n" +
                     "\"Simple message\",\"Simple desc\"\n" +
                     "\"Complex message\",\"This is a\nmulti-line\ndescription with\nquoted \"\"inner\"\" text\"\n" +
                     "\"Another message\",\"Final desc\"\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        var allEntries = new List<LogEntry>();
        var allFailures = new List<ParsingFailure>();

        // Parse all chunks
        while (true)
        {
            var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
            if (result.SuccessfulEntries.Count == 0)
                break;

            allEntries.AddRange(result.SuccessfulEntries);
            allFailures.AddRange(result.Failures);
            parser.UpdateUnderlyingStreamPosition(stream);
        }

        // Verify all records were parsed correctly
        Assert.AreEqual(3, allEntries.Count);
        Assert.AreEqual(0, allFailures.Count);

        Assert.AreEqual("Simple message", allEntries[0].Message);
        Assert.AreEqual("Complex message", allEntries[1].Message);
        Assert.AreEqual("Another message", allEntries[2].Message);

        // Verify the multi-line field was handled correctly
        if (allEntries[1].Fields.ContainsKey("description"))
        {
            var multiLineDesc = (string)allEntries[1].Fields["description"];
            Assert.IsTrue(multiLineDesc.Contains("\n"));
            Assert.IsTrue(multiLineDesc.Contains("\"inner\""));
        }
    }

    /// <summary>
    /// Test chunking with very large individual records
    /// </summary>
    [TestMethod]
    public async Task ChunkingEdgeCase_VeryLargeIndividualRecords()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                MessageColumn = "message",
                MaxRecordsPerChunk = 2
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        
        // Create a very large message field
        var largeMessage = new string('A', 10000); // 10KB message
        var csvData = $"message\nSmall message\n\"{largeMessage}\"\nAnother small message\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        
        Assert.AreEqual(2, result.SuccessfulEntries.Count);
        Assert.AreEqual(0, result.Failures.Count);
        
        Assert.AreEqual("Small message", result.SuccessfulEntries[0].Message);
        Assert.AreEqual(largeMessage, result.SuccessfulEntries[1].Message);
    }

    /// <summary>
    /// Test chunking when stream ends mid-record (simulating truncated file)
    /// </summary>
    [TestMethod]
    public async Task ChunkingEdgeCase_StreamEndsMidRecord()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                MessageColumn = "message",
                MaxRecordsPerChunk = 10
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        
        // Create truncated CSV (missing closing quote and newline)
        var csvData = "message\nComplete record\n\"Incomplete rec";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        
        // Should handle the complete record and either handle or fail gracefully on incomplete one
        Assert.IsTrue(result.SuccessfulEntries.Count >= 1);
        Assert.AreEqual("Complete record", result.SuccessfulEntries[0].Message);
        
        // The incomplete record might be in failures or might be ignored
        // The key is that it doesn't crash
    }

    /// <summary>
    /// Test chunking with empty fields and various quote combinations
    /// </summary>
    [TestMethod]
    public async Task ChunkingEdgeCase_EmptyFieldsAndQuoteCombinations()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                MessageColumn = "message",
                MaxRecordsPerChunk = 5
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "message,level\n" +
                     "\"\",INFO\n" +  // Empty quoted field
                     ",WARN\n" +      // Empty unquoted field
                     "\"Normal message\",ERROR\n" +
                     "\"Message with \"\"quotes\"\"\",DEBUG\n" + // Escaped quotes
                     "\"\"\"Just quotes\"\"\",INFO\n";  // Quote at start and end
        
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        
        Assert.AreEqual(5, result.SuccessfulEntries.Count);
        Assert.AreEqual(0, result.Failures.Count);

        // Verify empty fields are handled
        Assert.AreEqual("", result.SuccessfulEntries[0].Message);
        Assert.AreEqual("", result.SuccessfulEntries[1].Message);
        Assert.AreEqual("Normal message", result.SuccessfulEntries[2].Message);
        Assert.AreEqual("Message with \"quotes\"", result.SuccessfulEntries[3].Message);
        Assert.AreEqual("\"Just quotes\"", result.SuccessfulEntries[4].Message);
    }

    /// <summary>
    /// Test chunking with position tracking across multiple chunks with complex data
    /// </summary>
    [TestMethod]
    public async Task ChunkingEdgeCase_PositionTrackingWithComplexData()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                MessageColumn = "message",
                MaxRecordsPerChunk = 2
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "message,data\n" +
                     "\"Simple\",\"data1\"\n" +
                     "\"Multi\nline\",\"data2\"\n" +
                     "\"With \"\"quotes\"\"\",\"data3\"\n" +
                     "\"Final\",\"data4\"\n";
        
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));
        var totalEntries = 0;
        var positions = new List<long>();

        // Parse in chunks and track positions
        while (true)
        {
            var positionBefore = parser.GetCurrentPosition();
            var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
            var positionAfter = parser.GetCurrentPosition();
            
            if (result.SuccessfulEntries.Count == 0)
                break;

            totalEntries += result.SuccessfulEntries.Count;
            positions.Add(positionAfter);
            
            // Verify position always increases
            Assert.IsTrue(positionAfter > positionBefore, 
                $"Position should increase: {positionBefore} -> {positionAfter}");
            
            parser.UpdateUnderlyingStreamPosition(stream);
            
            // Verify stream position matches parser position
            Assert.AreEqual(parser.GetCurrentPosition(), stream.Position);
        }

        Assert.AreEqual(4, totalEntries);
        Assert.AreEqual(csvData.Length, positions.Last());
        
        // Verify positions are monotonically increasing
        for (int i = 1; i < positions.Count; i++)
        {
            Assert.IsTrue(positions[i] > positions[i-1]);
        }
    }

    /// <summary>
    /// Test chunking with Unicode and special characters
    /// </summary>
    [TestMethod]
    public async Task ChunkingEdgeCase_UnicodeAndSpecialCharacters()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                MessageColumn = "message",
                MaxRecordsPerChunk = 2
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "message\n" +
                     "\"Hello 世界\"\n" +  // Unicode
                     "\"Emoji 🚀 test\"\n" +  // Emoji
                     "\"Símböls & spëcîal chärs\"\n" +  // Special chars
                     "\"Mixed: 中文 🎉 café\"\n";  // Mixed
        
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));
        
        var result1 = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        parser.UpdateUnderlyingStreamPosition(stream);
        
        var result2 = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        
        Assert.AreEqual(2, result1.SuccessfulEntries.Count);
        Assert.AreEqual(2, result2.SuccessfulEntries.Count);
        Assert.AreEqual(0, result1.Failures.Count);
        Assert.AreEqual(0, result2.Failures.Count);

        Assert.AreEqual("Hello 世界", result1.SuccessfulEntries[0].Message);
        Assert.AreEqual("Emoji 🚀 test", result1.SuccessfulEntries[1].Message);
        Assert.AreEqual("Símböls & spëcîal chärs", result2.SuccessfulEntries[0].Message);
        Assert.AreEqual("Mixed: 中文 🎉 café", result2.SuccessfulEntries[1].Message);
    }

    /// <summary>
    /// Test chunking when chunk size exactly matches record boundaries vs when it doesn't
    /// </summary>
    [TestMethod]
    public async Task ChunkingEdgeCase_ChunkSizeVsRecordBoundaryAlignment()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                MessageColumn = "message",
                MaxRecordsPerChunk = 3 // Odd number to create misalignment
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        
        // Create 10 records to see how they get chunked
        var sb = new StringBuilder("message\n");
        for (int i = 1; i <= 10; i++)
        {
            sb.AppendLine($"Message {i}");
        }
        var csvData = sb.ToString();
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        var chunkSizes = new List<int>();
        var totalProcessed = 0;

        while (totalProcessed < 10)
        {
            var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
            if (result.SuccessfulEntries.Count == 0)
                break;

            chunkSizes.Add(result.SuccessfulEntries.Count);
            totalProcessed += result.SuccessfulEntries.Count;
            parser.UpdateUnderlyingStreamPosition(stream);
        }

        Assert.AreEqual(10, totalProcessed);
        
        // Verify chunk sizes: should be [3, 3, 3, 1] or similar
        Assert.IsTrue(chunkSizes.Count >= 3);
        Assert.IsTrue(chunkSizes.Take(chunkSizes.Count - 1).All(size => size == 3));
        Assert.AreEqual(10, chunkSizes.Sum());
    }
}