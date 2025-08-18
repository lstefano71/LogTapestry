// C#
namespace LogTapestry.Integration.Tests
{
  [TestClass]
  public class TestHealthAndMetrics
  {
    [TestMethod]
    public async Task HealthEndpoint_Should_Return_Healthy()
    {
      using var client = new HttpClient();
      var response = await client.GetAsync("http://localhost:8080/health");
      var content = await response.Content.ReadAsStringAsync();
      Assert.AreEqual(200, (int)response.StatusCode);
      StringAssert.Contains(content, "\"status\":\"Healthy\"");
    }

    [TestMethod]
    public async Task MetricsEndpoint_Should_Expose_Metrics()
    {
      using var client = new HttpClient();
      var response = await client.GetAsync("http://localhost:8080/metrics");
      var content = await response.Content.ReadAsStringAsync();
      Assert.AreEqual(200, (int)response.StatusCode);
      StringAssert.Contains(content, "log_entries_ingested_total");
      StringAssert.Contains(content, "bytes_processed_total");
    }
  }
}
