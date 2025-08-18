namespace LogTapestry.Integration.Tests
{
  [TestClass]
  public class TestCompactor
  {
    [TestMethod]
    public async Task Compactor_Should_Merge_Landing_Files_And_Create_Marker()
    {
      // Arrange: Setup test partition directory and landing files
      string tempDir = Path.Combine(Path.GetTempPath(), "LogTapestryTestPartition");
      Directory.CreateDirectory(tempDir);
      string landingDir = Path.Combine(tempDir, "landing");
      Directory.CreateDirectory(landingDir);

      // TODO: Create several small Parquet files in landingDir

      // Act: Run CompactionTask
      var compactionTask = new LogTapestry.Compactor.CompactionTask(tempDir, "1h");
      await compactionTask.RunAsync();

      // Assert: landing/ should not exist, marker file should exist
      Assert.IsFalse(Directory.Exists(landingDir), "landing/ directory should be deleted after compaction.");
      Assert.IsTrue(File.Exists(Path.Combine(tempDir, "_COMPACTION_COMPLETE")), "Marker file should be created.");

      // TODO: Assert that large Parquet files exist and row counts match

      // Cleanup
      Directory.Delete(tempDir, true);
    }
  }
}
