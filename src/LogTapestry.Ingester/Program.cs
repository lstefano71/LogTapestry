using LogTapestry.Core;

using System.Threading.Channels;

namespace LogTapestry.Ingester
{
  class Program
  {
    static async Task Main(string[] args)
    {
      var settings = new LogTapestrySettings {
        Ingester = new IngesterSettings {
          Directory = "d:\\devel\\LogTapestry\\sample-logs",
          IncludePatterns = ["*.log"]
        },
        Plugins =
          [
            new PluginSettings {
              Type = "regex",
              Name = "default",
              IncludePatterns = ["*.log"],
              Config = new RegexPluginConfig
              {
                  StartOfEntryRegex = @"^\d{4}-\d{2}-\d{2}",
                  TimestampRegex = @"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)",
                  TimestampIsUtc = true,
                  LevelRegex = @"\b(INFO|WARN|ERROR)\b",
                  FieldsRegexes =
                  [
                      new FieldRegex { Regex = @"user_id=(\d+)", FieldName = "user_id", Type = "long" },
                      new FieldRegex { Regex = @"order_id=(\d+)", FieldName = "order_id", Type = "long" },
                      new FieldRegex { Regex = @"free_space=(\d+)", FieldName = "free_space", Type = "long" },
                      new FieldRegex { Regex = @"username=""([^""]+)""", FieldName = "username" },
                      new FieldRegex { Regex = @"reason=""([^""]+)""", FieldName = "reason" },
                      new FieldRegex { Regex = @"details=""([^""]+)""", FieldName = "details" }

                  ]
              }
           }
         ]
      };

      var channel = Channel.CreateBounded<ParsingResult>(settings.Ingester.PipelineBufferCapacity);

      var consumer = Task.Run(async () => {
        var batch = new List<LogEntry>();
        await foreach (var result in channel.Reader.ReadAllAsync()) {
          if (result.IsSuccess && result.Entry != null) {
            batch.Add(result.Entry);
            if (batch.Count >= 100) // Arbitrary batch size
            {
              await WriteBatch(batch);
              batch.Clear();
            }
          } else {
            Console.WriteLine($"Error parsing line: {result.ErrorMessage} - {result.UnparseableText}");
          }
        }
        if (batch.Count > 0) {
          await WriteBatch(batch);
        }
      });

      var logFilePath = Path.Combine(settings.Ingester.Directory, "test.log");
      var plugin = settings.Plugins[0];
      var parser = new RegexLogParser(plugin, logFilePath);

      var lines = await File.ReadAllLinesAsync(logFilePath);
      foreach (var result in parser.Parse(lines)) {
        await channel.Writer.WriteAsync(result);
      }

      var finalResult = parser.Flush();
      if (finalResult != null) {
        await channel.Writer.WriteAsync(finalResult);
      }

      channel.Writer.Complete();
      await consumer;
    }

    private static async Task WriteBatch(List<LogEntry> batch)
    {
      Directory.CreateDirectory("data");
      foreach (var entry in batch) {
        Console.WriteLine($"Timestamp: {entry.Timestamp}\nLevel: {entry.Level}\nMessage: {entry.Message}\nSource: {entry.Source}\nTemplateHash: {entry.TemplateHash}");
        Console.WriteLine("Fields:");
        foreach (var field in entry.Fields) {
          var valueType = field.Value?.GetType().Name ?? "null";
          Console.WriteLine($"  {field.Key}: {field.Value} (Type: {valueType})");
        }
        Console.WriteLine(new string('-', 40));
      }
      var dataSink = new DataSink();
      using var stream = new FileStream("data/output.parquet", FileMode.Append);
      await dataSink.WriteBatchAsync([.. batch], stream);
    }
  }
}