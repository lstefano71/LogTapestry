# Design Refinement and Brainstorming

Here is a breakdown of your sections with added details, potential challenges, and C#/.NET-specific implementation ideas.

## 1. Overall Architecture

Your proposed architecture is sound. We can visualize it as a pipeline of distinct components:

1. **Directory Monitor:** A dedicated service that uses `FileSystemWatcher` but adds a layer of robustness to handle event storms and debounce notifications.
2. **File Manager:** Manages the lifecycle of tracked files. It decides whether a file is new, has been appended to, or has been rotated. It consults the State Store.
3. **Tailing Engine:** Responsible for reading new lines from a specific file, starting from a known offset (byte position).
4. **Parsing Pipeline:** Takes raw string lines, identifies the correct plugin, and transforms them into structured data. This is where plugins live.
5. **Data Sink:** Receives structured data and writes it efficiently to Parquet files.
6. **State Store:** The SQLite database that persists file offsets and other metadata, ensuring crash recovery.

This separation of concerns will make the system easier to develop, test, and maintain.

## 2. Tailing and State Management (The SQLite "State Store")

Your idea to use SQLite is excellent. It's transactional, file-based, and vastly superior to a custom state file. Let's define the schema more concretely.

**Proposed SQLite Schema:**

```sql
CREATE TABLE TrackedFiles (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    FilePath TEXT NOT NULL UNIQUE,
    -- The byte offset we have successfully processed up to.
    Position INTEGER NOT NULL DEFAULT 0,
    -- Last write time of the file when we last processed it.
    LastWriteTimeUtc TEXT NOT NULL,
    -- A hash of the first few KB of the file to reliably detect rotations
    -- where the file name is reused (e.g., app.log is truncated and rewritten).
    FileSignature TEXT
);

CREATE TABLE PluginStates (
    -- Allows plugins to store their own state, e.g., for multi-line correlation.
    PluginName TEXT NOT NULL,
    StateKey TEXT NOT NULL,
    StateValue TEXT,
    PRIMARY KEY (PluginName, StateKey)
);
```

**Handling Log Rotation:**
This is a critical and tricky part. `FileSystemWatcher` will give you `Renamed`, `Created`, and `Changed` events. A robust strategy is:

1. When a file you are tracking (`app.log`) is renamed to `app.log.1`, the `Renamed` event is your trigger. You should finish reading the old file path, update its state in the database as "archived" or remove it, and create a new entry for the *new* file name (`app.log`) if it appears.
2. If `app.log` suddenly shrinks in size (truncation), your check on `Position` vs. current file size will fail. This is where `FileSignature` is vital. If the signature of the new, smaller `app.log` is different, you treat it as a *new file* and start from byte 0. If the signature is the same, it's a genuine truncation, which is a rare but possible error condition to log.

## 3. The Plugin System

This is the core of your system's flexibility.

**Dynamic Loading in .NET:**
To add/remove plugins without restarting, you will need to use `AssemblyLoadContext`. This is the modern .NET way to load assemblies into a collectible context, allowing you to truly unload them and their dependencies.

* Each plugin would be loaded into its own `AssemblyLoadContext`.
* The main application would watch a `plugins` directory. When a new DLL appears, it loads it. When a DLL is removed, it unloads the corresponding context.

**Plugin Interface Definition (C# Example):**

A simple, clear interface is key.

```csharp
// Represents a single, parsed log entry.
public record LogEntry(
    DateTime Timestamp,
    string Level,
    string Message,
    string Source,
    IReadOnlyDictionary<string, object> Fields,
    string Template
);

// Interface for a plugin factory that creates parser instances.
public interface ILogParserPlugin
{
    // The unique name for this plugin, e.g., "regex_default".
    string Name { get; }

    // Creates a configured instance of the parser.
    // The 'config' object could be a JsonElement or similar.
    ILogParser CreateParser(object config);
}

// Interface for the actual parser instance.
public interface ILogParser
{
    // Takes a stream of lines and yields parsed entries.
    // The buffer allows handling of multi-line entries.
    IEnumerable<LogEntry> Parse(IBufferedLogLines lines);
}
```

This design separates the plugin's identity (`ILogParserPlugin`) from its configured instance (`ILogParser`), which maps well to your configuration file.

## 4. Data Storage and Querying (Parquet + SQLite)

This is a powerful combination often called "metadata indexing."

* **Data (The "Hot" Store):** Parquet files are your primary data store. They are columnar, compressed, and excellent for analytic queries. You should write data in batches to new Parquet files, for example:
  * Create a new file every 100,000 log entries.
  * Create a new file every 15 minutes.
  * Create a new file once the current one reaches 128MB.

* **Metadata (The "Index"):** Your SQLite database should also contain metadata about the Parquet files themselves to enable fast queries.

**Proposed Parquet Metadata Schema (in the same SQLite DB):**

```sql
CREATE TABLE ParquetFiles (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    FilePath TEXT NOT NULL UNIQUE,
    CreatedAtUtc TEXT NOT NULL,
    MinTimestampUtc TEXT NOT NULL, -- The timestamp of the first entry.
    MaxTimestampUtc TEXT NOT NULL, -- The timestamp of the last entry.
    EntryCount INTEGER NOT NULL
);
```

**How Querying Works:**
When a user wants to query logs between `T1` and `T2`:

1. **Query the SQLite DB first:** `SELECT FilePath FROM ParquetFiles WHERE MaxTimestampUtc >= T1 AND MinTimestampUtc <= T2;`
2. This gives you a small, precise list of Parquet files that *might* contain relevant data.
3. **Query the data:** Use a library like DuckDB (which has excellent C# bindings and is incredibly fast with Parquet) or another Parquet reader to scan only these files for the full log data.

This avoids a full scan of all your data and will be extremely fast.

## 5. Configuration

Your choice of JSON over YAML is a matter of preference; both are supportable. The structure you've proposed is excellent.

**Refinement:**
Consider using a "type" or "kind" field for plugins to make it explicit, rather than relying on the "name".

```json
{
  "ingester": { ... },
  "plugins": [
    {
      "type": "regex", // Explicitly states the type of plugin
      "name": "sofia-main-logs", // A user-defined instance name
      "include_patterns": ["*.log"],
      "config": { ... }
    },
    {
      "type": "json", // A hypothetical future plugin for JSON logs
      "name": "sofia-audit-logs",
      "include_patterns": ["*.audit.log"],
      "config": { ... }
    }
  ]
}
```

In C#, you can use `System.Text.Json` to deserialize this into a strongly-typed configuration object hierarchy. With .NET 9, you can leverage source generation for high-performance, reflection-free deserialization.

## 6. Performance Considerations

* **I/O:** Do not read files line-by-line using `File.ReadLines`. For high-performance I/O, especially with tailing, `System.IO.Pipelines` is the state-of-the-art approach in .NET. It minimizes memory allocations and context switching.
* **Parallelism:** Be careful here. Parsing can be parallelized, but writing to a single Parquet file or updating the SQLite state database must be synchronized. A good model is a producer-consumer pipeline using `TPL Dataflow`:
    1. **`BufferBlock<string>`:** A single-threaded tailer reads lines and posts them here.
    2. **`TransformBlock<string, LogEntry>`:** This block executes your parsing logic. You can configure its `MaxDegreeOfParallelism` to use multiple cores for CPU-bound regex parsing.
    3. **`BatchBlock<LogEntry>`:** Gathers parsed entries into batches.
    4. **`ActionBlock<LogEntry[]>`:** A single-threaded block that takes a batch and writes it to the current Parquet file and updates the metadata store.

This architecture provides high throughput, backpressure (so the reader doesn't outpace the writer), and clear control over parallelism.

## 7. Missing Piece: The Query Interface

The design is focused on ingestion. How will a user query this data? You should plan for this from the start.

* **A Standalone Query Tool (CLI):** A simple command-line tool `logtapestry-query.exe` that takes a time range and filter conditions, uses the SQLite/Parquet strategy described above, and prints results.
* **A Library (NuGet Package):** Expose a query library so other .NET applications can directly query the log data.
* **A Minimal API Endpoint:** A lightweight web server that exposes a REST or gRPC endpoint for querying.

Starting with a simple CLI is likely the most pragmatic approach.

This expanded design addresses potential pitfalls like log rotation and dynamic plugin loading, and it provides a clear path for a high-performance implementation in modern C#/.NET. The foundation you've laid out is very strong.
