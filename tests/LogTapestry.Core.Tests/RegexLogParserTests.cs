namespace LogTapestry.Core.Tests;

[TestClass]
public sealed class RegexLogParserTests
{
  [TestMethod]
  public void RegexLogParser_ParsesSimpleLogLine()
  {
    var pluginSettings = new PluginSettings {
      Type = "regex",
      Name = "test",
      IncludePatterns = ["*.log"],
      Config = new RegexPluginConfig {
        StartOfEntryRegex = @"^\d{4}-\d{2}-\d{2}",
        TimestampRegex = @"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)",
        LevelRegex = @" (INFO|WARN|ERROR) ",
        FieldsRegexes = [
          new FieldRegex { Regex = @"user_id=(\d+)", FieldName = "user_id", Type = "long" }
        ]
      }
    };

    var parser = new RegexLogParser(pluginSettings, "test.log");
    var lines = new[] { "2025-08-18T16:00:01Z INFO User login succeeded user_id=101 username=\"alice\"" };
    var results = parser.Parse(lines).ToList();
    var flushResult = parser.Flush();
    if (flushResult != null) results.Add(flushResult);

    Assert.AreEqual(1, results.Count);
    var result = results[0];
    Assert.IsTrue(result.IsSuccess);
    Assert.IsNotNull(result.Entry);
    Assert.AreEqual("INFO", result.Entry.Level);
    Assert.AreEqual(101L, result.Entry.Fields["user_id"]);
    Assert.AreEqual("test.log", result.Entry.Source);
  }
}
