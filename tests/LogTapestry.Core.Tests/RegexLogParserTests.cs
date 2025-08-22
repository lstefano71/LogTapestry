﻿using System.Text;

namespace LogTapestry.Core.Tests;

[TestClass]
public sealed class LogParserTests
{
  [TestMethod]
  public async Task RegexLogParser_ParsesSimpleLogLine()
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

    await using var parser = new RegexLogParser(pluginSettings, "test.log");
    var logData = "2025-08-18T16:00:01Z INFO User login succeeded user_id=101 username=\"alice\"\n";
    var stream = new MemoryStream(Encoding.UTF8.GetBytes(logData));
    
    var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
    
    Assert.AreEqual(1, result.SuccessfulEntries.Count);
    Assert.AreEqual(0, result.Failures.Count);
    
    var entry = result.SuccessfulEntries[0];
    Assert.AreEqual("INFO", entry.Level);
    Assert.AreEqual(101L, entry.Fields["user_id"]);
    Assert.AreEqual("test.log", entry.Source);
  }

  [TestMethod]
  public async Task CsvLogParser_ParsesSimpleCsvRecord()
  {
    var pluginSettings = new PluginSettings {
      Type = "csv",
      Name = "test_csv",
      IncludePatterns = ["*.csv"],
      CsvConfig = new CsvPluginConfig {
        HasHeader = true,
        Delimiter = ",",
        TimestampColumn = "timestamp",
        LevelColumn = "level",
        MessageColumn = "message",
        TimestampFormat = "yyyy-MM-dd HH:mm:ss",
        FieldMappings = [
          new CsvFieldMapping { ColumnName = "user_id", FieldName = "user_id", Type = "long" },
          new CsvFieldMapping { ColumnName = "session_id", FieldName = "session_id", Type = "string" }
        ]
      }
    };

    await using var parser = new CsvLogParser(pluginSettings, "test.csv");
    var csvData = "timestamp,level,message,user_id,session_id\n2025-08-18 16:00:01,INFO,User login succeeded,101,abc123\n";
    var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));
    
    var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
    
    Assert.AreEqual(1, result.SuccessfulEntries.Count);
    Assert.AreEqual(0, result.Failures.Count);
    
    var entry = result.SuccessfulEntries[0];
    Assert.AreEqual("INFO", entry.Level);
    Assert.AreEqual("User login succeeded", entry.Message);
    Assert.AreEqual(101L, entry.Fields["user_id"]);
    Assert.AreEqual("abc123", entry.Fields["session_id"]);
    Assert.AreEqual("test.csv", entry.Source);
  }

  [TestMethod]
  public async Task CsvLogParser_HandlesQuotedFields()
  {
    var pluginSettings = new PluginSettings {
      Type = "csv",
      Name = "test_csv",
      IncludePatterns = ["*.csv"],
      CsvConfig = new CsvPluginConfig {
        HasHeader = true,
        Delimiter = ",",
        MessageColumn = "message",
        FieldMappings = [
          new CsvFieldMapping { ColumnName = "description", FieldName = "description", Type = "string" }
        ]
      }
    };

    await using var parser = new CsvLogParser(pluginSettings, "test.csv");
    var csvData = "message,description\n\"Error occurred\",\"This is a \"\"quoted\"\" description\"\n";
    var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));
    
    var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
    
    Assert.AreEqual(1, result.SuccessfulEntries.Count);
    var entry = result.SuccessfulEntries[0];
    Assert.AreEqual("Error occurred", entry.Message);
    Assert.AreEqual("This is a \"quoted\" description", entry.Fields["description"]);
  }

  [TestMethod]
  public async Task CsvLogParser_HandlesMultipleRecords()
  {
    var pluginSettings = new PluginSettings {
      Type = "csv",
      Name = "test_csv",
      IncludePatterns = ["*.csv"],
      CsvConfig = new CsvPluginConfig {
        HasHeader = true,
        LevelColumn = "level",
        MessageColumn = "message",
        MaxRecordsPerChunk = 10
      }
    };

    await using var parser = new CsvLogParser(pluginSettings, "test.csv");
    var csvData = "level,message\nINFO,First log\nWARN,Second log\nERROR,Third log\n";
    var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));
    
    var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
    
    Assert.AreEqual(3, result.SuccessfulEntries.Count);
    Assert.AreEqual("INFO", result.SuccessfulEntries[0].Level);
    Assert.AreEqual("WARN", result.SuccessfulEntries[1].Level);
    Assert.AreEqual("ERROR", result.SuccessfulEntries[2].Level);
  }

  [TestMethod]
  public async Task CsvLogParser_HandlesMultiLineFields()
  {
    var pluginSettings = new PluginSettings {
      Type = "csv",
      Name = "test_csv",
      IncludePatterns = ["*.csv"],
      CsvConfig = new CsvPluginConfig {
        HasHeader = true,
        Delimiter = ",",
        MessageColumn = "message",
        FieldMappings = [
          new CsvFieldMapping { ColumnName = "description", FieldName = "description", Type = "string" }
        ]
      }
    };

    await using var parser = new CsvLogParser(pluginSettings, "test.csv");
    // CSV with multi-line field containing actual newlines
    var csvData = "message,description\n\"Single line\",\"This is a\nmulti-line\ndescription\"\n\"Another record\",\"Simple description\"\n";
    var stream = new MemoryStream(Encoding.UTF8.GetBytes(csvData));
    
    var result = await parser.ParseNextChunkAsync(stream, CancellationToken.None);
    
    Assert.AreEqual(2, result.SuccessfulEntries.Count);
    Assert.AreEqual(0, result.Failures.Count);
    
    var firstEntry = result.SuccessfulEntries[0];
    Assert.AreEqual("Single line", firstEntry.Message);
    Assert.AreEqual("This is a\nmulti-line\ndescription", firstEntry.Fields["description"]);
    
    var secondEntry = result.SuccessfulEntries[1];
    Assert.AreEqual("Another record", secondEntry.Message);
    Assert.AreEqual("Simple description", secondEntry.Fields["description"]);
  }
}
