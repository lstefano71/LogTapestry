using System.Text;
using LogTapestry.Core;

namespace LogTapestry.Core.Tests;

[TestClass]
public sealed class SepPositionDebuggingTests
{
    [TestMethod]
    public async Task Debug_PositionTracking_SimpleCase()
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
        var csvData = "message\nRecord 1\nRecord 2\nRecord 3\nRecord 4\n";
        Console.WriteLine($"Total CSV data length: {csvData.Length}");
        Console.WriteLine($"CSV data: {csvData.Replace("\n", "\\n")}");
        
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));

        // First chunk
        Console.WriteLine($"\n=== FIRST CHUNK ===");
        Console.WriteLine($"Stream position before: {stream.Position}");
        var position1Before = parser.GetCurrentPosition();
        Console.WriteLine($"Parser position before: {position1Before}");
        
        var result1 = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        
        var position1After = parser.GetCurrentPosition();
        Console.WriteLine($"Parser position after: {position1After}");
        Console.WriteLine($"Stream position after: {stream.Position}");
        Console.WriteLine($"Records parsed: {result1.SuccessfulEntries.Count}");
        foreach (var entry in result1.SuccessfulEntries)
        {
            Console.WriteLine($"  - {entry.Message}");
        }
        
        parser.UpdateUnderlyingStreamPosition(stream);
        Console.WriteLine($"Stream position after sync: {stream.Position}");

        // Second chunk
        Console.WriteLine($"\n=== SECOND CHUNK ===");
        Console.WriteLine($"Stream position before: {stream.Position}");
        var position2Before = parser.GetCurrentPosition();
        Console.WriteLine($"Parser position before: {position2Before}");
        
        var result2 = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        
        var position2After = parser.GetCurrentPosition();
        Console.WriteLine($"Parser position after: {position2After}");
        Console.WriteLine($"Stream position after: {stream.Position}");
        Console.WriteLine($"Records parsed: {result2.SuccessfulEntries.Count}");
        foreach (var entry in result2.SuccessfulEntries)
        {
            Console.WriteLine($"  - {entry.Message}");
        }

        // Final assertions
        Console.WriteLine($"\n=== ASSERTIONS ===");
        Console.WriteLine($"Position1: {position1After}, Position2: {position2After}");
        Console.WriteLine($"Position increased: {position2After > position1After}");
        Console.WriteLine($"Final position matches data length: {position2After == csvData.Length}");
        
        Assert.AreEqual(2, result1.SuccessfulEntries.Count);
        Assert.AreEqual(2, result2.SuccessfulEntries.Count);
    }
}