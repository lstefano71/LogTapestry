using System.Diagnostics;
using System.Text;
using LogTapestry.Core;

namespace LogTapestry.Core.Tests;

[TestClass]
public sealed class CsvParserBenchmarkTests
{
    private const int TestDataRows = 10000;
    private const int WarmupIterations = 3;
    private const int BenchmarkIterations = 10;

    [TestMethod]
    public async Task Benchmark_CsvParserComparison()
    {
        // Generate test data
        var testData = GenerateTestCsvData(TestDataRows);
        var testDataBytes = Encoding.UTF8.GetBytes(testData);
        
        Console.WriteLine($"Test data size: {testDataBytes.Length:N0} bytes");
        Console.WriteLine($"Test data rows: {TestDataRows:N0}");
        Console.WriteLine($"Warmup iterations: {WarmupIterations}");
        Console.WriteLine($"Benchmark iterations: {BenchmarkIterations}");
        Console.WriteLine();

        var pluginSettings = new PluginSettings
        {
            Type = "csv",
            Name = "benchmark_csv",
            CsvConfig = new CsvPluginConfig
            {
                HasHeader = true,
                Delimiter = ",",
                TimestampColumn = "timestamp",
                LevelColumn = "level", 
                MessageColumn = "message",
                FieldMappings = [
                    new CsvFieldMapping { ColumnName = "user_id", FieldName = "user_id", Type = "long" },
                    new CsvFieldMapping { ColumnName = "session_id", FieldName = "session_id", Type = "string" },
                    new CsvFieldMapping { ColumnName = "score", FieldName = "score", Type = "double" }
                ]
            }
        };

        // Warmup both parsers
        Console.WriteLine("Warming up parsers...");
        await WarmupParser(() => new CsvLogParser(pluginSettings, "test.csv"), testDataBytes, WarmupIterations);
        await WarmupParser(() => new SepCsvLogParser(pluginSettings, "test.csv"), testDataBytes, WarmupIterations);

        // Benchmark original CSV parser
        Console.WriteLine("Benchmarking CsvLogParser...");
        var originalResults = await BenchmarkParser(() => new CsvLogParser(pluginSettings, "test.csv"), testDataBytes, BenchmarkIterations);

        // Benchmark Sep CSV parser  
        Console.WriteLine("Benchmarking SepCsvLogParser...");
        var sepResults = await BenchmarkParser(() => new SepCsvLogParser(pluginSettings, "test.csv"), testDataBytes, BenchmarkIterations);

        // Display results
        Console.WriteLine();
        Console.WriteLine("=== BENCHMARK RESULTS ===");
        Console.WriteLine($"CsvLogParser    - Avg: {originalResults.averageMs:F2}ms, Min: {originalResults.minMs:F2}ms, Max: {originalResults.maxMs:F2}ms");
        Console.WriteLine($"SepCsvLogParser - Avg: {sepResults.averageMs:F2}ms, Min: {sepResults.minMs:F2}ms, Max: {sepResults.maxMs:F2}ms");
        Console.WriteLine();

        var speedupFactor = originalResults.averageMs / sepResults.averageMs;
        var speedupPercent = ((originalResults.averageMs - sepResults.averageMs) / originalResults.averageMs) * 100;

        Console.WriteLine($"Performance improvement: {speedupFactor:F2}x faster ({speedupPercent:+F1}%)");
        Console.WriteLine();

        var originalThroughput = (testDataBytes.Length / 1024.0 / 1024.0) / (originalResults.averageMs / 1000.0);
        var sepThroughput = (testDataBytes.Length / 1024.0 / 1024.0) / (sepResults.averageMs / 1000.0);

        Console.WriteLine($"Throughput:");
        Console.WriteLine($"CsvLogParser    - {originalThroughput:F2} MB/s");
        Console.WriteLine($"SepCsvLogParser - {sepThroughput:F2} MB/s");

        // Verify both parsers produce same entry count
        Assert.AreEqual(originalResults.entryCount, sepResults.entryCount, "Both parsers should produce the same number of entries");
        
        // Sep should be faster (this might not always be true for small datasets, but should be for large ones)
        if (TestDataRows >= 1000)
        {
            Assert.IsTrue(sepResults.averageMs <= originalResults.averageMs * 1.1, "Sep parser should be competitive with original parser");
        }
    }

    private static string GenerateTestCsvData(int rowCount)
    {
        var sb = new StringBuilder();
        sb.AppendLine("timestamp,level,message,user_id,session_id,score");

        var random = new Random(42); // Fixed seed for reproducible results
        var levels = new[] { "INFO", "WARN", "ERROR", "DEBUG" };
        var messages = new[] 
        {
            "User login successful",
            "Database connection established",
            "Cache miss for key user_preferences", 
            "File uploaded successfully",
            "Authentication failed",
            "Transaction completed",
            "System health check passed",
            "Background job started"
        };

        for (int i = 0; i < rowCount; i++)
        {
            var timestamp = DateTime.Now.AddMinutes(-random.Next(10000)).ToString("yyyy-MM-dd HH:mm:ss");
            var level = levels[random.Next(levels.Length)];
            var message = messages[random.Next(messages.Length)];
            var userId = random.Next(1000, 9999);
            var sessionId = Guid.NewGuid().ToString("N")[..8];
            var score = random.NextDouble() * 100;

            sb.AppendLine($"{timestamp},{level},{message},{userId},{sessionId},{score:F2}");
        }

        return sb.ToString();
    }

    private static async Task WarmupParser(Func<ILogParser> parserFactory, byte[] testData, int iterations)
    {
        for (int i = 0; i < iterations; i++)
        {
            await using var parser = parserFactory();
            var stream = new MemoryStream(testData);
            var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
        }
    }

    private static async Task<(double averageMs, double minMs, double maxMs, int entryCount)> BenchmarkParser(
        Func<ILogParser> parserFactory, byte[] testData, int iterations)
    {
        var times = new List<double>();
        int totalEntries = 0;

        for (int i = 0; i < iterations; i++)
        {
            await using var parser = parserFactory();
            var stream = new MemoryStream(testData);
            
            var stopwatch = Stopwatch.StartNew();
            var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
            stopwatch.Stop();

            times.Add(stopwatch.Elapsed.TotalMilliseconds);
            totalEntries = result.SuccessfulEntries.Count; // All iterations should have same count
        }

        return (times.Average(), times.Min(), times.Max(), totalEntries);
    }
}