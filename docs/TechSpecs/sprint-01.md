## **LogTapestry: Technical Specification - Sprint 1**

**Version:** 1.0
**Date:** August 18, 2025
**Author:** Implementation Team Lead

### **1. Sprint Goal: End-to-End Data Flow Proof of Concept**

The primary objective of this sprint is to create a working, end-to-end vertical slice of the LogTapestry application. This PoC will prove that a single log entry can be read from a file, parsed, written to a Parquet file with the correct schema, and successfully queried back by the query tool. We will defer full robustness (e.g., `FileSystemWatcher` error handling) and advanced features (e.g., compaction) to subsequent sprints.

### **2. Solution and Project Structure**

The solution will be created in a Git repository with the following structure:

```
/LogTapestry.sln
/src/
    /LogTapestry.Core/
        LogTapestry.Core.csproj
    /LogTapestry.Ingester/
        LogTapestry.Ingester.csproj
    /LogTapestry.Query/
        LogTapestry.Query.csproj
/tests/
    /LogTapestry.Core.Tests/
        LogTapestry.Core.Tests.csproj
    /LogTapestry.Integration.Tests/
        LogTapestry.Integration.Tests.csproj
```

**Project Details:**

*   **`LogTapestry.Core`** (Class Library, .NET 9)
    *   **Description:** Contains all shared logic, interfaces, and domain models. Will have no dependencies on the executables.
    *   **Key NuGet Packages:** `Microsoft.Extensions.Logging.Abstractions`

*   **`LogTapestry.Ingester`** (Console Application, .NET 9, Self-Contained)
    *   **Description:** The main ingester service executable.
    *   **Key NuGet Packages:** `Microsoft.Extensions.Hosting`, `Serilog.Extensions.Hosting`, `Parquet.Net`, `System.Threading.Channels`, `Microsoft.Data.Sqlite`

*   **`LogTapestry.Query`** (Console Application, .NET 9, Self-Contained)
    *   **Description:** The standalone query tool.
    *   **Key NuGet Packages:** `DuckDB.NET`, `Microsoft.Data.Sqlite`, `Spectre.Console` (for table formatting)

*   **`LogTapestry.Core.Tests`** (Test Project, MSTest or xUnit)
    *   **Description:** Unit tests for business logic in `LogTapestry.Core`, especially the `RegexLogParser`.
    *   **Key NuGet Packages:** `Moq`

### **3. Core Domain Model**

The following C# records will be defined in `LogTapestry.Core`.

```csharp
/// <summary>
/// Represents a single, structured log event after successful parsing.
/// This is the canonical object that flows to the data sink.
/// </summary>
public record LogEntry(
    DateTime Timestamp,
    string Level,
    string Message,
    string Source, // The file path
    long TemplateHash,
    IReadOnlyDictionary<string, object> Fields // Values are already typed
);

/// <summary>
/// Represents the outcome of a parsing attempt.
/// </summary>
public record ParsingResult
{
    public bool IsSuccess { get; init; }
    public LogEntry? Entry { get; init; }
    public string? ErrorMessage { get; init; }
    public string? UnparseableText { get; init; }
    public string Source { get; init; } = "";
    // + static factory methods Success(...) and Failure(...)
}

/// <summary>
/// Represents a work item for the TailingManager, produced by the DirectoryMonitor.
/// </summary>
public enum FileWorkType { FileAdded, FileChanged, FileRemovedOrRotated }

public record FileWorkItem(string FilePath, FileWorkType WorkType);
```

### **4. Component Specifications (Interfaces and Classes)**

#### **4.1. `LogTapestry.Core.dll`**

**`ILogParser` Interface:**

```csharp
public interface ILogParser
{
    IEnumerable<ParsingResult> Parse(string[] lines);
    ParsingResult? Flush();
}
```

**`RegexLogParser` Class:**

*   **Implements:** `ILogParser`
*   **Constructor:** `public RegexLogParser(PluginConfig config, string sourceFile)`
*   **Key Internal State:**
    *   `private readonly StringBuilder _multiLineBuffer;`
    *   `private LogEntry? _inProgressEntry;`
    *   `private readonly Regex _startOfEntryRegex;`
    *   (and other configured regexes)
*   **Logic:** Will implement the stateful, line-by-line processing logic as defined in the architectural document.

#### **4.2. `LogTapestry.Ingester.exe`**

**`DataSink` Class:**

*   **Responsibility:** Writes a batch of `LogEntry` records to a Parquet file.
*   **Key Method:** `public async Task WriteBatchAsync(LogEntry[] batch, Stream targetStream)`
*   **Implementation Details:**
    1.  Will define a `static readonly ParquetSchema` for the log entry structure, including all typed maps.
    2.  The `WriteBatchAsync` method will perform the "shredding" logic: converting the `LogEntry[]` into parallel `DataColumn` arrays.
    3.  Will use `Parquet.Net.ParquetWriter` to write the columns to the provided stream.

**For Sprint 1 (PoC), the following components will be simplified:**

*   **`Program.cs`:** Will act as the orchestrator.
    1.  It will read a hardcoded configuration (no JSON file yet).
    2.  It will **not** use `DirectoryMonitor`. Instead, it will directly create a "Tailing Task" for a single hardcoded log file path.
    3.  It will create a bounded `Channel<ParsingResult>`.
    4.  It will create a consumer task that reads from the channel, batches entries, and uses the `DataSink` to write to a single Parquet file.
    5.  It will use a simple in-memory `Dictionary<string, string>` as the Schema Registry.
    6.  It will not use a SQLite DB for state; the tailer will track its position in a simple `long` variable.

### **5. Configuration Model**

The following C# classes will be defined in `LogTapestry.Core` to represent the configuration file structure for future sprints.

```csharp
public class LogTapestrySettings
{
    public IngesterSettings Ingester { get; set; }
    public List<PluginSettings> Plugins { get; set; }
}

public class IngesterSettings
{
    public string Directory { get; set; }
    public List<string> IncludePatterns { get; set; }
    public int PipelineBufferCapacity { get; set; } = 10000;
    public int MaxParsingParallelism { get; set; } = Environment.ProcessorCount;
}

public class PluginSettings
{
    public string Type { get; set; } // "regex"
    public string Name { get; set; }
    public List<string> IncludePatterns { get; set; }
    public RegexPluginConfig Config { get; set; }
}

public class RegexPluginConfig
{
    public string StartOfEntryRegex { get; set; }
    public string TimestampRegex { get; set; }
    public string LevelRegex { get; set; }
    public List<FieldRegex> FieldsRegexes { get; set; }
}

public class FieldRegex
{
    public string Regex { get; set; }
    public string FieldName { get; set; }
    public string Type { get; set; } = "string"; // "long", "double", etc.
}
```

### **6. Database Schema (SQLite)**

The following DDL will be used to create the state database (`state.sqlite`). This will be implemented in a later sprint but is defined now for clarity.

```sql
CREATE TABLE TrackedFiles (
    VolumeSerial INTEGER NOT NULL,
    FileId INTEGER NOT NULL,
    FilePath TEXT NOT NULL,
    Position INTEGER NOT NULL DEFAULT 0,
    LastWriteTimeUtc TEXT NOT NULL,
    PRIMARY KEY (VolumeSerial, FileId)
);

CREATE INDEX IX_TrackedFiles_FilePath ON TrackedFiles (FilePath);

CREATE TABLE FieldSchema (
    FieldName TEXT PRIMARY KEY,
    FieldType TEXT NOT NULL
);

CREATE TABLE ParquetFiles (
    FilePath TEXT PRIMARY KEY,
    CreatedAtUtc TEXT NOT NULL,
    MinTimestampUtc TEXT NOT NULL,
    MaxTimestampUtc TEXT NOT NULL,
    EntryCount INTEGER NOT NULL
);
```

### **7. Command-Line Interface (CLI) Specification**

For the PoC, the executables will be run with minimal arguments. The full CLI is specified for future sprints.

*   **Ingester:**
    *   **Usage:** `LogTapestry.Ingester.exe --config <path_to_config.json>`
    *   **Sprint 1:** Will be run with no arguments and will use hardcoded values.

*   **Query Tool:**
    *   **Usage:** `LogTapestry.Query.exe --db <path_to_state.sqlite> --data <path_to_data_dir> "SQL_QUERY"`
    *   **Sprint 1:** `LogTapestry.Query.exe --data <path> "SELECT ..."`. The `--db` argument will be ignored as the schema will be in-memory.

### **8. Definition of Done for Sprint 1**

The sprint will be considered complete when the following criteria are met:

1.  The solution and project structure are created and pushed to a Git repository.
2.  A sample `test.log` file is created with single-line and multi-line entries, containing fields that can be parsed into `string` and `long` types.
3.  The `LogTapestry.Ingester.exe` can be run. It will:
    *   Tail the hardcoded `test.log` file from beginning to end.
    *   Use the `RegexLogParser` to parse the entries.
    *   Produce a single `output.parquet` file in a `data/` directory.
4.  The `output.parquet` file can be inspected and is verified to have the correct multi-map schema.
5.  The `LogTapestry.Query.exe` can be run with a hardcoded query (e.g., `SELECT * FROM logs WHERE user_id > 100`).
    *   It must correctly rewrite the query to `... WHERE fields_long['user_id'] > 100`.
    *   It must execute the query using DuckDB against `output.parquet`.
    *   It must print the correct, filtered results to the console.
6.  Core parsing logic in `RegexLogParser` has basic unit test coverage.