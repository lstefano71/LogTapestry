using Serilog.Core;
using Serilog.Events;

namespace LogTapestry.Ingester
{
  public class UtcTimestampEnricher : ILogEventEnricher
  {
    public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
    {
      var utcTimestamp = logEvent.Timestamp.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.fffZ");
      logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("UtcTimestamp", utcTimestamp));
    }
  }
}
