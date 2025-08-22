using System.Text;
using LogTapestry.Core;

namespace LogTapestry.Core.Tests;

[TestClass]
public sealed class SepDebugTests
{
    [TestMethod]
    public async Task Debug_SepPositionTracking()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            Name = "test_csv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                MessageColumn = "message"
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "message\nFirst\nSecond\nThird\n";
        Console.WriteLine($"CSV Data length: {csvData.Length}");
        
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));
        var initialPosition = stream.Position;
        Console.WriteLine($"Initial stream position: {initialPosition}");
        
        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        
        Console.WriteLine($"Final stream position: {stream.Position}");
        Console.WriteLine($"Parser current position: {parser.GetCurrentPosition()}");
        Console.WriteLine($"Entries parsed: {result.SuccessfulEntries.Count}");
        
        Assert.AreEqual(csvData.Length, parser.GetCurrentPosition());
    }
    
    [TestMethod]
    public async Task Debug_SepQuoteHandling()
    {
        var pluginSettings = new PluginSettings
        {
            Type = "sepcsv",
            Name = "test_csv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                MessageColumn = "message"
            }
        };

        await using var parser = new SepCsvLogParser(pluginSettings, "test.csv");
        var csvData = "message\n\"Quoted value\"\n";
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));
        
        var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        
        Console.WriteLine($"Parsed message: '{result.SuccessfulEntries[0].Message}'");
        Assert.AreEqual("Quoted value", result.SuccessfulEntries[0].Message);
    }
}