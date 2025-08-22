using System.Text;
using LogTapestry.Core;

namespace LogTapestry.Core.Tests;

[TestClass]
public sealed class SepIntegrationTests
{
    [TestMethod]
    public async Task Integration_SepCsvParser_ChunkedProcessing()
    {
        // Create test data that will require multiple chunks
        var csvData = GenerateLargeCsvData(3000); // 3000 rows
        var csvBytes = Encoding.UTF8.GetBytes(csvData);
        
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                LevelColumn = "level",
                MessageColumn = "message",
                MaxRecordsPerChunk = 500 // Force multiple chunks
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var stream = new MemoryStream(csvBytes);
        
        var totalEntries = 0;
        var chunkCount = 0;
        var lastPosition = 0L;
        
        // Process in chunks
        while (stream.Position < stream.Length)
        {
            var positionBefore = parser.GetCurrentPosition();
            var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
            var positionAfter = parser.GetCurrentPosition();
            
            if (result.SuccessfulEntries.Count == 0)
            {
                break; // No more data
            }
            
            chunkCount++;
            totalEntries += result.SuccessfulEntries.Count;
            
            // Verify position tracking
            Assert.IsTrue(positionAfter > positionBefore, $"Position should increase: {positionBefore} -> {positionAfter}");
            
            // Sync stream position with parser
            parser.UpdateUnderlyingStreamPosition(stream);
            Assert.AreEqual(parser.GetCurrentPosition(), stream.Position, "Stream and parser positions should match after sync");
            
            lastPosition = positionAfter;
            
            Console.WriteLine($"Chunk {chunkCount}: {result.SuccessfulEntries.Count} entries, position: {positionAfter}");
        }
        
        // Verify results
        Assert.IsTrue(chunkCount > 1, "Should have processed multiple chunks");
        Assert.AreEqual(3000, totalEntries, "Should have parsed all 3000 data rows");
        Assert.AreEqual(csvBytes.Length, lastPosition, "Final position should match total data length");
        
        Console.WriteLine($"Successfully processed {totalEntries} entries across {chunkCount} chunks");
        Console.WriteLine($"Data size: {csvBytes.Length} bytes, final position: {lastPosition}");
    }

    [TestMethod]
    public async Task Integration_PositionTracking_SeekAndResume()
    {
        var csvData = "level,message\nINFO,Message 1\nWARN,Message 2\nERROR,Message 3\nDEBUG,Message 4\n";
        var csvBytes = Encoding.UTF8.GetBytes(csvData);
        
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                LevelColumn = "level",
                MessageColumn = "message",
                MaxRecordsPerChunk = 2 // Small chunks
            }
        };

        // First pass - parse first chunk
        await using (var parser1 = new SepCsvLogParser(pluginSettings, "test.csv"))
        {
            var stream1 = new MemoryStream(csvBytes);
            var result1 = await parser1.ParseNextChunkAsync(stream1, CancellationToken.None);
            
            Assert.AreEqual(2, result1.SuccessfulEntries.Count);
            var checkpoint1 = parser1.GetCurrentPosition();
            parser1.UpdateUnderlyingStreamPosition(stream1);
            
            // Second pass - start from where we left off
            await using var parser2 = new SepCsvLogParser(pluginSettings, "test.csv");
            var stream2 = new MemoryStream(csvBytes);
            stream2.Seek(checkpoint1, SeekOrigin.Begin); // Resume from checkpoint
            
            var result2 = await parser2.ParseNextChunkAsync(stream2, CancellationToken.None);
            
            Assert.AreEqual(2, result2.SuccessfulEntries.Count);
            Assert.AreNotEqual(result1.SuccessfulEntries[0].Message, result2.SuccessfulEntries[0].Message, "Should parse different records");
            
            // Verify we can resume correctly
            var checkpoint2 = parser2.GetCurrentPosition();
            Assert.IsTrue(checkpoint2 > checkpoint1, "Second checkpoint should be further in the file");
            
            Console.WriteLine($"First chunk: {result1.SuccessfulEntries.Count} entries, checkpoint: {checkpoint1}");
            Console.WriteLine($"Second chunk: {result2.SuccessfulEntries.Count} entries, checkpoint: {checkpoint2}");
            Console.WriteLine($"Messages from first chunk: {string.Join(", ", result1.SuccessfulEntries.Select(e => e.Message))}");
            Console.WriteLine($"Messages from second chunk: {string.Join(", ", result2.SuccessfulEntries.Select(e => e.Message))}");
        }
    }

    [TestMethod]
    public async Task Integration_CompareWithOriginalParser()
    {
        var csvData = GenerateLargeCsvData(1000);
        var csvBytes = Encoding.UTF8.GetBytes(csvData);
        
        var pluginSettings = new PluginSettings
        {
            Type = "csv", // Original parser
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                LevelColumn = "level",
                MessageColumn = "message"
            }
        };

        // Parse with original parser
        await using var originalParser = new CsvLogParser(pluginSettings, "test.csv");
        var originalStream = new MemoryStream(csvBytes);
        var originalResult = await originalParser.ParseNextChunkAsync(originalStream, CancellationToken.None);
        
        // Parse with Sep parser
        await using var sepParser = new SepCsvLogParser(pluginSettings, "test.csv");
        var sepStream = new MemoryStream(csvBytes);
        var sepResult = await sepParser.ParseNextChunkAsync(sepStream, CancellationToken.None);
        
        // Compare results
        Assert.AreEqual(originalResult.SuccessfulEntries.Count, sepResult.SuccessfulEntries.Count, "Both parsers should find same number of entries");
        Assert.AreEqual(originalResult.Failures.Count, sepResult.Failures.Count, "Both parsers should have same number of failures");
        
        // Verify position tracking gives us the full file length
        Assert.AreEqual(csvBytes.Length, sepParser.GetCurrentPosition(), "Sep parser should track position to end of file");
        
        Console.WriteLine($"Original parser: {originalResult.SuccessfulEntries.Count} entries");
        Console.WriteLine($"Sep parser: {sepResult.SuccessfulEntries.Count} entries");
        Console.WriteLine($"File size: {csvBytes.Length}, Sep position: {sepParser.GetCurrentPosition()}");
    }

    private static string GenerateLargeCsvData(int rowCount)
    {
        var sb = new StringBuilder();
        sb.AppendLine("level,message,user_id");

        var levels = new[] { "INFO", "WARN", "ERROR", "DEBUG" };
        var messages = new[] { "Test message", "Another message", "System event", "User action" };
        var random = new Random(42);

        for (int i = 0; i < rowCount; i++)
        {
            var level = levels[random.Next(levels.Length)];
            var message = $"{messages[random.Next(messages.Length)]} {i}";
            var userId = random.Next(1000, 9999);
            
            sb.AppendLine($"{level},{message},{userId}");
        }

        return sb.ToString();
    }
}