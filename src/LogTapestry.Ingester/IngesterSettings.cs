public class IngesterSettings
{
  public string LogLevel { get; set; } = "Information";
  public string Directory { get; set; } = string.Empty;
  public string[] IncludePatterns { get; set; } = [];
  public string[] ExcludePatterns { get; set; } = [];
  public string DataRoot { get; set; } = "data";
  public int PollingIntervalMs { get; set; } = 1000;
  public string MonitoringUrl { get; set; } = "http://0.0.0.0:8080";
}
