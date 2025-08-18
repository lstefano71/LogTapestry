# LogTapestry: Full Implementation Reference

This document provides a complete and thorough reference for the implementation of **LogTapestry**, a system designed for the ingestion, storage, and querying of text-based logs from legacy applications. It consolidates all brainstorming and design decisions into a single, coherent guide.

## 1. Introduction and Architecture

LogTapestry is designed to handle the challenges of legacy logging environments, where logs are often unstructured, have varying formats, and are stored in a complex directory of files. The system provides a real-time ingestion pipeline, a query-optimized storage backend, and a user-friendly query interface, all while ensuring robust operation and minimal impact on the source application's host system.

### 1.1. High-Level Architecture

The system is composed of several key components that work in concert:

1. **Ingester:** A long-running agent that actively monitors a directory, tails log files, parses their content using a flexible plugin system, and writes the structured data into a storage pipeline.
2. **State and Metadata Store (SQLite):** A central SQLite database that acts as the system's "brain." It tracks the ingestion progress for each file, stores metadata about the physical data files, and maintains a schema registry for all discovered log fields.
3. **Data Store (Parquet):** The primary repository for log data. Log entries are stored in highly compressed, columnar Parquet files, organized in a Hive-partitioned directory structure for efficient time-series querying.
4. **Compactor:** A standalone utility that periodically runs to optimize storage by merging many small, recently ingested Parquet files into larger files, dramatically improving query performance.
5. **Query Engine:** A standalone command-line tool that provides a user-friendly SQL interface for querying the log data. It leverages the **DuckDB** in-process database for high-speed querying of Parquet files and uses the SQLite metadata to provide a simplified, logical view of the data to the user.

*(Conceptual diagram showing Directory Monitor feeding a Tailing Manager, which spawns multiple producers (Tailing Tasks). These feed a central channel, which is consumed by a pipeline that writes to Parquet files. The Query Engine uses both the SQLite DB and Parquet files to serve user queries.)*

### 1.2. Technology Stack and Key Libraries

To realize this architecture, the following key technologies and libraries have been selected based on their performance, maturity, and suitability for the project's goals:

| Component | Technology / Library | Rationale |
| :--- | :--- | :--- |
| **Concurrency** | **`System.Threading.Channels`** | A modern, high-performance primitive for producer-consumer scenarios. Chosen over TPL Dataflow for its lower-level control and more fluent, readable syntax for linear pipelines. |
| **Parquet I/O** | **`Parquet.Net`** | A mature, fully-managed .NET library. Chosen over `Apache.Arrow` for its simplicity, lack of native dependencies (ensuring cross-platform portability), and sufficient performance for this I/O-bound application. |
| **Querying** | **`DuckDB.NET`** | Bindings for DuckDB, an in-process analytical database that is exceptionally fast at querying Parquet files, understands Hive partitioning natively, and requires no external server process. |
| **State/Metadata** | **`Microsoft.Data.Sqlite`** | The standard, lightweight library for accessing the SQLite database. |
| **Logging** | **`Serilog`** | A powerful structured logging framework that integrates with standard `Microsoft.Extensions.Logging` abstractions, essential for creating actionable, machine-parseable operational logs. |

---

## 2. The Ingestion Pipeline

The ingestion pipeline is the real-time component responsible for discovering, reading, and processing log data. It's a concurrent system designed for high throughput and resilience.

### 2.1. Core Concurrency Model

The ingester is built around a multi-producer, single-consumer model using `System.Threading.Channels`.

* **Producers (Tailing Tasks):** A dedicated asynchronous task is spawned for each log file being tailed. These tasks read lines, parse them, and push the results into a single, central channel.
* **Central Channel:** This channel acts as a shared, bounded buffer. It provides the system's primary **backpressure** mechanism. If the downstream consumer (which performs disk I/O) is slow, the channel fills up, causing the file-reading tasks to asynchronously wait without blocking threads, thus preventing unbounded memory consumption. The channel's capacity is a key performance tuning parameter.
* **Consumer Pipeline:** A single chain of consumers reads from the central channel to filter failed parses, batch the successful entries, and write them to the data store.
* **Shutdown:** The system implements both graceful shutdown (ensuring all buffered data is written) and abrupt cancellation (using `CancellationToken` propagation) to ensure clean and predictable termination.

### 2.2. Directory Monitoring and File Tracking

The `DirectoryMonitor` is the system's "eyes and ears," responsible for reliably detecting file changes.

* **Platform-Specific Reliability (Windows/NTFS):** To robustly handle log rotation, the monitor will leverage the **NTFS File ID**. This ID uniquely identifies a file's content on a volume, regardless of its name. This is 100% reliable for detecting renames and truncate-and-rewrite scenarios, making it superior to heuristic methods. **Windows (on NTFS) is the primary supported platform for v1.**
* **Stateful Design:** It combines a full directory scan on startup with `FileSystemWatcher` for ongoing monitoring. Its source of truth is the **`TrackedFiles` table in the SQLite database**.
* **Robustness:** The monitor is designed to handle the known fragility of `FileSystemWatcher` by triggering a full resynchronization scan if the watcher's internal buffer overflows and by debouncing event storms.

### 2.3. The Parsing System

Once a file is being tailed, its content is fed into a stateful parser.

* **Parser Instances:** A dedicated parser instance is created for each log file being tailed. This is essential for correctly handling multi-line log entries that may be split across different file read operations.
* **Contract (`ILogParser`):** A parser instance must manage its own internal state, particularly a buffer for multi-line entries.
  * `Parse(string[] lines)`: Consumes new lines and yields completed log entries.
  * `Flush()`: Finalizes and returns any partial entry held in its buffer.
* **`RegexLogParser` Implementation:** The default parser uses a `_startOfEntryRegex` to identify the beginning of a new log entry. Any subsequent lines that do not match are appended to the message of the in-progress entry.
* **Error Handling:** If a line cannot be parsed, the parser produces a `ParsingResult.Failure` object. The consumer pipeline logs this as a **Warning** and discards the entry, preventing a single bad line from halting ingestion.

### 2.4. Plugin Configuration

The ingester's behavior is controlled by a central JSON configuration file.

* **Format:** JSON is chosen for its simplicity and widespread support, avoiding the complexity of YAML.
* **Structure:** The configuration defines global settings (directory, include/exclude patterns) and a list of plugin instances, each targeting a subset of files.
* **Typed Fields:** The `regex` plugin allows each field extraction rule to specify a `type` (`string`, `long`, `double`, etc.), instructing the parser to convert the value.

**Example Configuration Snippet:**

```json
{
  "ingester": {
    "directory": "/var/log/sofia",
    "include_patterns": ["*.log"]
  },
  "plugins": [
    {
      "type": "regex",
      "name": "sofia-main-logs",
      "include_patterns": ["*.log"],
      "config": {
        "timestamp_regex": "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",
        "fields_regexes": [
          { "regex": "user_id=(\\d+)", "field_name": "user_id", "type": "long" }
        ]
      }
    }
  ]
}
```

---

## 3. Data Storage

LogTapestry employs a dual-storage strategy optimized for fast queries and crash recovery.

### 3.1. Durability and the Write-Ahead Log (WAL)

A separate WAL is **not required**. The system's design provides an **at-least-once delivery guarantee** through a *de facto* WAL:

1. **The "Log":** The original source log files themselves.
2. **The "Commit Pointer":** The `Position` field in the SQLite `TrackedFiles` table.

On restart after a crash, the ingester resumes reading from the last committed position, effectively replaying any data that was in memory at the time of the crash.

### **3.2. Physical Storage (Parquet)

The multi-map schema is **superseded**. We will now use a single `fields` column that is a `List` of a complex `Struct`, which is a more robust and flexible approach.

* **Format:** Apache Parquet.
* **Partitioning:** Hive Partitioning (`year=YYYY/month=MM/day=DD/`) remains in effect.
* **Revised Schema:**
  * `timestamp` (Timestamp), `level` (String), `source` (String), `message` (String)
  * `template_hash` (Long)
  * **`fields` (List<Struct>):** A single column to hold all dynamic fields. The structure is defined as:
    * `element` (Struct)
      * `key` (String)
      * `value` (Struct)
        * `string_value` (Nullable String)
        * `long_value` (Nullable Long)
        * `double_value` (Nullable Double)
        * `boolean_value` (Nullable Boolean)

* **Implementation (`Parquet.Net` Schema):**

    ```csharp
    private static readonly ParquetSchema LogEntrySchema = new(
        new DataField<DateTime>("timestamp"),
        new DataField<string>("level"),
        new DataField<string>("source"),
        new DataField<long>("template_hash"),
        new DataField<string>("message"),
        new ListField("fields", new StructField("element",
            new DataField<string>("key"),
            new StructField("value",
                new DataField<string?>("string_value"),
                new DataField<long?>("long_value"),
                new DataField<double?>("double_value"),
                new DataField<bool?>("boolean_value")
            )
        ))
    );
    ```

### 3.3. Metadata and State (SQLite)

* **`TrackedFiles` Table:** Stores the ingestion state (File ID, path, byte position).
* **`ParquetFiles` Table:** An index of all data files, storing their path and the min/max timestamp of their contents.
* **`FieldSchema` Table (Schema Registry):** The canonical source of truth for the data type of every dynamically extracted field.

### 3.4. Schema Management and Conflict Resolution

* **Conflict Handling:** When a value is encountered for a field (e.g., `http_status`) that does not match its registered canonical type (e.g., `long`), the value is simply written into the `string_value` sub-field of the struct for that key. This preserves the data without corrupting the typed columns and keeps it within the same logical structure.

---

## 4. Compaction

The system addresses the "small file problem" with a background compaction process.

### 4.1. Architecture: Landing and Optimized Zones

* **Landing Zone:** The ingester writes frequent, small batches to a `landing/` subdirectory for low latency.
* **Optimized Zone:** The parent directory contains large Parquet files optimized for fast scanning.
* **Querying:** The query engine seamlessly combines results from both zones using a `UNION ALL`.

### 4.2. The Standalone Compactor Tool

* **Decoupled Process:** Compaction is performed by a separate `logtapestry-compactor.exe`. This isolates the resource-intensive task from the ingestion pipeline.
* **Idempotent Logic:** The compactor rewrites small files into large ones and leaves a `_COMPACTION_COMPLETE` marker file to prevent reprocessing.
* **Flexibility:** This design allows compaction to be run on a schedule on the live system or on-demand in a lab environment.

---

## 5. Querying

The query tool provides a simple, powerful interface to the log data.

### 5.1. Query Engine: DuckDB

LogTapestry uses **DuckDB** for its exceptional speed in querying Parquet files, its native understanding of Hive partitioning, and its in-process, serverless nature.

### **5.2. The Smart Query Layer - **CRITICAL REVISION**

* **Query Rewriting:** The `QueryRewriter` will now translate a user's logical query on a dynamic field into a physical query that uses the `UNNEST` function to flatten the `fields` list for filtering.
* **Example:**
  * **User's Logical Query:**
        `SELECT message WHERE user_id > 100 AND level = 'ERROR'`
  * **Rewritten Physical Query (DuckDB SQL):**

        ```sql
        SELECT
            t.message
        FROM
            read_parquet('path/to/data/**/*.parquet', hive_partitioning = true) AS t
        WHERE
            t.level = 'ERROR'
            AND EXISTS (
                SELECT 1
                FROM UNNEST(t.fields) AS f
                WHERE f.key = 'user_id' AND f.value.long_value > 100
            );
        ```

    *(Note: Using `EXISTS` with a subquery is often more performant for filtering than a full cross-join `UNNEST` in the `FROM` clause.)* The query rewriter will be responsible for generating this optimized physical SQL.

---

## 6. Operational Considerations and Performance

LogTapestry is designed to be a reliable, well-behaved production system.

### 6.1. "Good Citizen" Mode and Resource Throttling

To minimize impact on the host system, its resource usage is tunable:

| Resource | Control Knob (Configuration Setting) | Impact It Mitigates |
| :--- | :--- | :--- |
| **File Access** | `FileShare.ReadWrite` (Hardcoded) | Prevents `IOException` when reading logs being actively written by another application. |
| **CPU Usage** | `ingester.max_parsing_parallelism` | Limits the number of CPU cores used for log parsing. |
| **Disk I/O** | `compactor.schedule`, `compactor.io_delay_ms` | Runs heavy disk activity during off-peak hours and throttles its speed. |
| **Memory** | `ingester.pipeline_buffer_capacity` | Configures the central buffer size to control memory usage and provide backpressure. |

### 6.2. Deployment, Monitoring, and Health Checks

* **Packaging:** Deployed as a self-contained executable, designed to run as a background service (Windows Service, Linux systemd).
* **Structured Logging:** All operational logs are written as structured JSON. A separate file captures only `Warning` level and above for easy identification of issues.
* **Health and Metrics Endpoints:** An embedded web server exposes a `/health` endpoint for liveness checks and a `/metrics` endpoint in the Prometheus format for detailed observability.

### 6.3. User Experience and Safety

* **Validation Mode:** A `--validate` flag allows an operator to perform a "dry run" that checks the configuration for errors without starting the full ingestion process.

---

## 7. First Implementation Slice (Proof of Concept)

To de-risk the project, the first implementation step should be a vertical slice that proves the end-to-end data flow:

1. Hardcode a single file tailer.
2. Implement the stateful `RegexLogParser`.
3. Use a Channel to pass data to a simple Data Sink.
4. Implement the data "shredding" and write a Parquet file using the multi-map schema with `Parquet.Net`.
5. Build a rudimentary query tool that uses a hardcoded schema to rewrite a single query and execute it with `DuckDB.NET`.

Successfully building this PoC will validate all core architectural assumptions and technology choices, providing a solid foundation for the rest of the development.

---

## 8. Appendix: Core Data Structures (C#)

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
    // The values in this dictionary are already strongly typed (long, double, etc.)
    IReadOnlyDictionary<string, object> Fields
);

/// <summary>
/// Represents the outcome of a parsing attempt on a block of text.
/// It can be either a success with a LogEntry or a failure with details.
/// </summary>
public record ParsingResult
{
    public bool IsSuccess { get; }
    public LogEntry? Entry { get; }
    public string? ErrorMessage { get; }
    public string? UnparseableText { get; }
    public string Source { get; }

    // Factory methods for success and failure would be included here
}

/// <summary>
/// Defines the contract for a stateful parser that processes a stream of log lines.
/// A single instance is used for a single log file stream.
/// </summary>
public interface ILogParser
{
    /// <summary>
    /// Processes a new batch of complete lines from the log file.
    /// It may buffer a partial entry internally if it suspects a multi-line entry is in progress.
    /// </summary>
    IEnumerable<ParsingResult> Parse(string[] lines);

    /// <summary>
    /// Signals that the file stream has ended (e.g., file was rotated).
    /// This method should process and return any remaining buffered log entry.
    /// </summary>
    ParsingResult? Flush();
}

// --- New Helper Records for Parquet Writing ---
public record FieldElement(string Key, FieldValue Value);
public record FieldValue(string? string_value, long? long_value, double? double_value, bool? boolean_value);
```
