namespace LogTapestry.Core;

public class LogTapestrySettings
{
  public IngesterSettings Ingester { get; set; } = new();
  public List<PluginSettings> Plugins { get; set; } = [];
}

public class IngesterSettings
{
  public string Directory { get; set; } = "";
  public List<string> IncludePatterns { get; set; } = [];
  public List<string> ExcludePatterns { get; set; } = [];
  public int PipelineBufferCapacity { get; set; } = 10000;
  public int MaxParsingParallelism { get; set; } = Environment.ProcessorCount;
  public string DataRoot { get; set; } = "data";
  public string DatabasePath { get; set; } = "data/state.sqlite";
  public string LogLevel { get; set; } = "Information"; // Added log level config

  public int BatchSize { get; set; } = 50000;
  public int PollingIntervalMs { get; set; } = 1000;
  public string MonitoringUrl { get; set; } = "";

  // New configuration for worker pool architecture
  public int FileReaderThreadPoolSize { get; set; } = 8;
  public int StateWriterBatchSize { get; set; } = 1000;
  public int StateWriterIntervalSeconds { get; set; } = 2;
  public int FileSystemWatcherBufferSize { get; set; } = 65536;

  // Checkpointing configuration
  public int BatchSizeInBlocks { get; set; } = 100; // DataBlock batch size
  public TimeSpan BatchTimeout { get; set; } = TimeSpan.FromSeconds(30);
  public bool EnableCheckpointing { get; set; } = true; // Enable/disable checkpointing
}

public class PluginSettings
{
  public string Type { get; set; } = "";
  public string Name { get; set; } = "";
  public List<string> IncludePatterns { get; set; } = [];
  public List<string> ExcludePatterns { get; set; } = [];
  public RegexPluginConfig Config { get; set; } = new();
}

public class RegexPluginConfig
{
  public string StartOfEntryRegex { get; set; } = "";
  public string TimestampRegex { get; set; } = "";
  public string LevelRegex { get; set; } = "";
  public List<FieldRegex> FieldsRegexes { get; set; } = [];
  public bool TimestampIsUtc { get; set; } = false;
}

public class FieldRegex
{
  public string Regex { get; set; } = "";
  public string FieldName { get; set; } = "";
  public string Type { get; set; } = "string"; // "long", "double", etc.
}
