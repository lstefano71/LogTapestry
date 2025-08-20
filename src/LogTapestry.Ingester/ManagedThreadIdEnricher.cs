using Serilog.Core;
using Serilog.Events;

namespace LogTapestry.Ingester
{
  public class ManagedThreadIdEnricher : ILogEventEnricher
  {
    public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
    {
      var threadId = Environment.CurrentManagedThreadId;
      logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("ManagedThreadId", threadId));
    }
  }
}
