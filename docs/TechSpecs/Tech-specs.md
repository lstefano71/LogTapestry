You are absolutely correct. My apologies for jumping ahead. The request was for the technical specification that would be the *output* of a planning phase, which would then *enable* the implementation of the PoC and the rest of the project.

You are right to insist on this. A thorough "how" document is the critical bridge between architecture and code.

Here is the comprehensive Technical Specification document. This is the detailed blueprint for the implementation team.

---

## **LogTapestry: Full Technical Specification**

**Version:** 1.0
**Date:** August 18, 2025
**Status:** Approved for Implementation

### **1. Introduction**

This document provides the detailed technical specification for the LogTapestry project. It expands upon the approved Architectural Design Document, providing implementation-level details for each component. This specification will serve as the primary guide for the development team through all implementation sprints.

### **2. System Overview**

The system consists of three primary executables and a core logic library, operating on a SQLite state database and a Parquet-based data store.

 *(Conceptual Diagram)*

### **3. Project and Solution Structure**

The solution will be organized into a multi-project structure to ensure separation of concerns and code reusability.

*   **Solution File:** `LogTapestry.sln`
*   **Projects:**
    *   **`LogTapestry.Core`** (Class Library, .NET 9): Contains all shared domain models, interfaces, and cross-cutting concerns (e.g., configuration models, core data structures).
    *   **`LogTapestry.Ingester`** (Console Application, .NET 9): The main ingester service.
    *   **`LogTapestry.Compactor`** (Console Application, .NET 9): The standalone compaction utility.
    *   **`LogTapestry.Query`** (Console Application, .NET 9): The standalone query tool.
    *   **`LogTapestry.Core.Tests`** (Test Project, xUnit): Unit tests for logic in the Core library.
    *   **`LogTapestry.Integration.Tests`** (Test Project, xUnit): Integration and end-to-end tests.

*   **Key NuGet Dependencies:**
    *   `LogTapestry.Core`: `Microsoft.Extensions.Logging.Abstractions`
    *   `LogTapestry.Ingester`: `Microsoft.Extensions.Hosting`, `Serilog.Extensions.Hosting`, `Parquet.Net`, `System.Threading.Channels`, `Microsoft.Data.Sqlite`, `System.CommandLine`
    *   `LogTapestry.Compactor`: `Parquet.Net`, `DuckDB.NET` (for reading/rewriting), `System.CommandLine`
    *   `LogTapestry.Query`: `DuckDB.NET`, `Microsoft.Data.Sqlite`, `System.CommandLine`, `Spectre.Console` (for output formatting)

### **4. Core Data Models and Interfaces (`LogTapestry.Core`)**

The following structures define the data contracts that flow through the system.

```csharp
// --- Ingested Data Structures ---

public record LogEntry(DateTime Timestamp, string Level, string Message, string Source, long TemplateHash, IReadOnlyDictionary<string, object> Fields);

public record ParsingResult { /* As defined previously, with IsSuccess, Entry, ErrorMessage, etc. */ }

// --- File Monitoring ---

public enum FileWorkType { FileAdded, FileChanged, FileRemovedOrRotated }
public record FileWorkItem(string FilePath, FileWorkType WorkType, ulong? FileId = null); // FileId is Windows-specific

// --- Interfaces ---

public interface ILogParser { /* As defined previously */ }
public interface IStateProvider
{
    Task<TrackedFileInfo?> GetTrackedFileAsync(ulong fileId);
    Task UpdateTrackedFilePositionAsync(ulong fileId, string path, long newPosition, DateTime lastWriteTime);
    Task<string?> GetFieldTypeAsync(string fieldName);
    Task RegisterFieldTypeAsync(string fieldName, string fieldType);
}
```

### **5. Component Implementation Details**

#### **5.1. Directory Monitor (`LogTapestry.Ingester`)**

*   **Class:** `DirectoryMonitor`
*   **Responsibilities:** Discovers files, watches for changes, and produces `FileWorkItem`s.
*   **Output:** `ChannelReader<FileWorkItem>`
*   **Core Logic:**
    1.  **`RunAsync(CancellationToken token)`:** The main execution loop.
    2.  **Initial Scan:**
        *   On startup, queries the `TrackedFiles` table from the `IStateProvider`.
        *   Recursively enumerates all files matching the include/exclude patterns.
        *   For each file, it will use P/Invoke to get the **NTFS File ID**.
        *   **Reconciliation:** It compares the on-disk files against the tracked files in the state DB to generate initial `FileAdded` or `FileChanged` work items.
    3.  **`FileSystemWatcher` Integration:**
        *   After the scan, it initializes a `FileSystemWatcher` with a large internal buffer.
        *   **Event Handlers:**
            *   `OnCreated`: Gets the File ID, creates a `FileAdded` work item.
            *   `OnChanged`: Creates a `FileChanged` work item. The Tailing Task will handle rotation detection based on the File ID.
            *   `OnRenamed`: Creates a `FileRemovedOrRotated` work item for the old path and a `FileAdded` work item for the new path.
            *   `OnError`: Logs a critical error, disposes the current watcher, and triggers a full resynchronization scan to recover state.
        *   **Debouncing:** A `ConcurrentDictionary<string, Timer>` will be used to debounce `Changed` events, consolidating multiple events for the same file within a 250ms window into a single `FileChanged` work item.

#### **5.2. Tailing Manager and Tailing Tasks (`LogTapestry.Ingester`)**

*   **Class:** `TailingManager`
*   **Responsibilities:** Manages the lifecycle of concurrent tailing tasks.
*   **Internal State:** `private readonly ConcurrentDictionary<ulong, CancellationTokenSource> _activeTailers;` (Keyed by File ID).
*   **Core Logic:**
    1.  **`RunAsync(ChannelReader<FileWorkItem> workChannel, ChannelWriter<ParsingResult> outputChannel, CancellationToken token)`:**
    2.  Reads `FileWorkItem`s from the monitor's channel.
    3.  **`FileAdded`/`FileChanged`:** If a tailing task for the `FileId` is not already running, it spawns a new `TailingTask` and adds its `CancellationTokenSource` to the dictionary.
    4.  **`FileRemovedOrRotated`:** It retrieves the `CancellationTokenSource` for the `FileId`, cancels the token, and removes it from the dictionary.
    5.  **Graceful Shutdown:** It tracks the number of active tailers. On shutdown, it cancels all tasks and waits for them to complete before marking the output channel as complete.

*   **Class:** `TailingTask` (Internal to TailingManager)
*   **Logic:**
    1.  Opens the file stream with `FileShare.ReadWrite`.
    2.  Seeks to the last known position from the `IStateProvider`.
    3.  Enters a loop, reading new bytes from the file.
    4.  Manages a buffer to handle partial lines between reads.
    5.  Feeds complete lines to its dedicated `ILogParser` instance.
    6.  Writes all `ParsingResult` objects to the central output channel.
    7.  Periodically updates its progress via the `IStateProvider`.
    8.  On cancellation, calls `parser.Flush()` and writes the final result before exiting.

#### **5.3. Data Sink (`LogTapestry.Ingester`)**

*   **Class:** `DataSink`
*   **Responsibilities:** Writes batches of `LogEntry` records to partitioned Parquet files.
*   **Key Method:** `public async Task WriteBatchAsync(LogEntry[] batch)`
*   **Logic:**
    1.  Determines the correct partition path (e.g., `data/year=2025/month=08/day=18/landing/`) based on the timestamp of the first entry in the batch.
    2.  Generates a unique filename (e.g., `part-GUID.parquet`).
    3.  Writes to a `.tmp` file first.
    4.  **Shredding:** Implements the logic to convert `LogEntry[]` into `DataColumn` arrays for each field in the Parquet schema, distributing dynamic fields into the correct `fields_*` maps.
    5.  Uses `Parquet.Net` to write the data.
    6.  On successful write, performs an atomic **rename** from `.tmp` to `.parquet`.
    7.  Implements a retry policy with exponential backoff for all write operations.

#### **5.4. Query Tool (`LogTapestry.Query`)**

*   **CLI:** Implemented using `System.CommandLine`.
    *   **Command:** `logtapestry-query`
    *   **Arguments:** `sql-query` (string, required)
    *   **Options:** `--database-path` (required), `--data-path` (required), `--output-format <table|json|csv>` (default: table).
*   **Core Logic:**
    1.  **Parse CLI arguments.**
    2.  **Instantiate `SqliteStateProvider`** to connect to the state DB.
    3.  **Instantiate `QueryRewriter`**. This class will parse the user's SQL string to identify field names in the `WHERE` and `SELECT` clauses.
    4.  For each field, it calls `IStateProvider.GetFieldTypeAsync` and rewrites the logical field name (e.g., `user_id`) to the physical column name (e.g., `fields_long['user_id']`). It also handles the `UNION ALL` logic for querying both the landing and optimized zones.
    5.  **Instantiate `DuckDB.NET` connection.**
    6.  Execute the rewritten query. The `FROM` clause will use `read_parquet('{data_path}/**/*.parquet', hive_partitioning = true)`.
    7.  **Instantiate `OutputFormatter`**. Based on the `--output-format` flag, it will format the DuckDB result set and print it to the console using `Spectre.Console` for the table format.

### **6. Database Schema (SQLite DDL)**

This is the definitive schema to be used.

```sql
-- Tracks ingestion progress for each unique file on the filesystem
CREATE TABLE TrackedFiles (
    VolumeSerial INTEGER NOT NULL,
    FileId INTEGER NOT NULL,
    FilePath TEXT NOT NULL,
    Position INTEGER NOT NULL DEFAULT 0,
    LastWriteTimeUtc TEXT NOT NULL,
    PRIMARY KEY (VolumeSerial, FileId)
);

CREATE INDEX IX_TrackedFiles_FilePath ON TrackedFiles (FilePath);

-- The canonical schema for all dynamically discovered fields
CREATE TABLE FieldSchema (
    FieldName TEXT PRIMARY KEY,
    FieldType TEXT NOT NULL -- 'string', 'long', 'double', 'boolean'
);

-- An index of all written data files for fast query planning
CREATE TABLE ParquetFiles (
    FilePath TEXT PRIMARY KEY,
    CreatedAtUtc TEXT NOT NULL,
    MinTimestampUtc TEXT NOT NULL,
    MaxTimestampUtc TEXT NOT NULL,
    EntryCount INTEGER NOT NULL
);
```

### **7. Sprints Breakdown**

The project will be implemented in the following sequence of sprints:

*   **Sprint 1: The Proof of Concept.**
    *   **Goal:** Build the end-to-end data flow as a "happy path" spike.
    *   **Tasks:** Implement the simplified, hardcoded versions of the ingester and query tool as specified in the previous "Sprint 1" document. This validates the core technology choices (`Parquet.Net`, `DuckDB.NET`) and the shredding/rewriting logic.

*   **Sprint 2: Robust Ingestion and State Management.**
    *   **Goal:** Build the full `DirectoryMonitor` and `TailingManager`.
    *   **Tasks:** Implement the full NTFS File ID-based tracking, the `FileSystemWatcher` with error recovery, and the SQLite `IStateProvider` for persistent state.

*   **Sprint 3: Configuration and Operability.**
    *   **Goal:** Make the ingester configurable and operable as a service.
    *   **Tasks:** Implement JSON configuration loading, add the health and metrics endpoints, and create deployment scripts for running as a Windows Service.

*   **Sprint 4: Compaction and Query Tool Finalization.**
    *   **Goal:** Implement the storage optimization and finalize the user-facing tool.
    *   **Tasks:** Build the standalone `logtapestry-compactor.exe`. Finalize the `logtapestry-query.exe` with all CLI options and output formats.

*   **Sprint 5: Testing, Hardening, and Documentation.**
    *   **Goal:** Ensure the system is production-ready.
    *   **Tasks:** Expand integration and E2E test coverage. Perform failure testing. Write user and operator documentation.

