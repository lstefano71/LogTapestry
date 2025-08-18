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
