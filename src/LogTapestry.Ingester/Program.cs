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
          IncludePatterns = new List<string> { "*.log" }
        },
        Plugins = new List<PluginSettings>
          {
            new PluginSettings
            {
                Type = "regex",
                Name = "default",
                IncludePatterns = new List<string> { "*.log" },
                Config = new RegexPluginConfig
                {
                    StartOfEntryRegex = @"^\d{4}-\d{2}-\d{2}",
                    TimestampRegex = @"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)",
                    TimestampIsUtc = true,
                    LevelRegex = @"\b(INFO|WARN|ERROR)\b",
                    FieldsRegexes = new List<FieldRegex>
                    {
                        new FieldRegex { Regex = @"user_id=(\d+)", FieldName = "user_id", Type = "long" },
                        new FieldRegex { Regex = @"order_id=(\d+)", FieldName = "order_id", Type = "long" },
                        new FieldRegex { Regex = @"free_space=(\d+)", FieldName = "free_space", Type = "long" },
                        new FieldRegex { Regex = @"username=""([^""]+)""", FieldName = "username" },
                        new FieldRegex { Regex = @"reason=""([^""]+)""", FieldName = "reason" },
                        new FieldRegex { Regex = @"details=""([^""]+)""", FieldName = "details" }
                    }
                }
            }
        }
      };

      var stateProvider = new SqliteStateProvider("state.sqlite");
      var fileWorkChannel = Channel.CreateUnbounded<FileWorkItem>();
      var parsingResultChannel = Channel.CreateUnbounded<ParsingResult>();

      var directoryMonitor = new DirectoryMonitor(settings.Ingester, stateProvider);
      var tailingManager = new TailingManager(stateProvider);

      var cts = new CancellationTokenSource();

      var monitorTask = directoryMonitor.RunAsync(fileWorkChannel.Writer, cts.Token);
      var tailingTask = tailingManager.RunAsync(fileWorkChannel.Reader, parsingResultChannel.Writer, cts.Token);

      var consumer = Task.Run(async () => {
        var batch = new List<LogEntry>();
        await foreach (var result in parsingResultChannel.Reader.ReadAllAsync(cts.Token)) {
          if (result.IsSuccess && result.Entry != null) {
            batch.Add(result.Entry);
            if (batch.Count >= 100) {
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

      await Task.WhenAll(monitorTask, tailingTask, consumer);
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
      using var stream = new FileStream("data/output.parquet", FileMode.Create);
      await dataSink.WriteBatchAsync([.. batch], stream);
    }
  }
}
