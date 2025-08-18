namespace LogTapestry.Integration.Tests
{
  [TestClass]
  public class TestEndToEnd
  {
    [TestMethod]
    public async Task EndToEnd_Ingestion_Compaction_Querying()
    {
      // Arrange: Setup temp directory and test log files
      string tempDir = Path.Combine(Path.GetTempPath(), "LogTapestryE2E");
      Directory.CreateDirectory(tempDir);

      // TODO: Start Ingester, generate logs, wait, stop Ingester

      // TODO: Run Compactor
      // var compactionTask = new LogTapestry.Compactor.CompactionTask(tempDir, "1h");
      // await compactionTask.RunAsync();

      // TODO: Run Query tool and capture output

      // TODO: Assert output matches expected results

      // Cleanup
      Directory.Delete(tempDir, true);
    }
  }
}
