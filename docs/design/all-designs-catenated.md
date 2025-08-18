

---
# brainstorming.md
---

# Ingester

## Context

Sofia is an old school application where everything is implemented from scratch. The Database is a collection of files in various different formats, all in a directory and subdirectories. The same directory is used for the configuration files, the logs, and the data files.
New logs are born almost every day and they have very different structures, names, conventions for the names and even extensions. We are interested in the logs in text format.
In the future we hope to transition to a central log collector for structured logs but for now we need to implement a solution that can handle the current situation.
The idea is to create an ingester which in real time tails all the logs, including the new ones as they are added to the directory, wherever they may be in the directory tree.
The ingested entries should be committed to a database, so that they can be queried later.
The database should be able to deal with entries which are not structured, so that we can handle the current situation where the logs are not structured and have different formats. But a structure albeit primitive is provided by the ingestion, those fields should be used to efficiently query the logs later.
It should be possible to query the database even as the ingester is running.
I am leaning towards using parquet files accompanied by a sqlite database to store the state of the ingester, and the metadata that describes the parquet files. 

## Requirements

- The ingester should be able to handle new log files as they are created in the directory
- The ingester should be able to handle different log formats and structures
- The ingester should be able to handle large log files
- The ingester should be able to handle logs in text format

## Plugins

- The ingester should support plugins to handle different log formats and structures
- Plugins should be able to be added or removed without restarting the ingester
- Plugins should be able to specify how to parse the log files and extract the relevant information
- The plugins should themselves be able to be configured to specify how to parse the log files. A set of prebuilt plugins should be versatile enough to cover most of the log files.

In particular, plugins should be at the very minimum able to parse the timestamp of the log entry and turn it into a UTC timestamp.
Each log entry will be parsed into a structured format with the following fields:

- `timestamp`: The UTC timestamp of the log entry
- `level`: The log level (e.g., INFO, ERROR, DEBUG)
- `message`: The log message
- `source`: The source of the log entry (e.g., file name, line number)

The built-in plugins should be able to be configured with a list of regexes to extract fields. The fields will be extracted from the log entry and added to the structured format. Extracting fields should be optional, and the plugin should be able to handle log entries that do not match the regexes. Extracting fields will build a template that will be included in the structured format.

### Example

Assuming a property configured plugin, from the entry:

```
2023-10-01 12:00:00 INFO [myapp] User logged in: user_id=12345, session_id=abcde
```

in file `logs/dbver.log`, the plugin could extract:

The plugin could extract:

- `timestamp`: 2023-10-01T12:00:00Z
- `level`: INFO
- `message`: User logged in: user_id=12345, session_id=abcde
- `source`: logs/dbver.log
- `fields`: { "user_id": "12345", "session_id": "abcde" }
- `template`: "User logged in: user_id={user_id}, session_id={session_id}"

## Configuration

The ingester should be configurable to specify:

- the directory to watch for new log files
- a list of glob patterns to match the log files
- a list of glob patterns to exclude certain log files

## How to tail log files

The ingester should use a file system watcher to monitor the specified directory for new log files. When a new log file is created, the ingester should start tailing the file in real time. The ingester should also be able to handle existing log files that are already present in the directory when it starts.
In general the ingester should read the log files line by line and parse each log entry using the configured. The built-in plugin should have a multiline mode capable of entries which span multiple lines.

During the execution the ingester should keep track of the latest processed log entry for each file, so that it can resume processing from the last entry in case of a restart or crash.
This can be done by maintaining a state file that stores the last processed entry for each file. The state file should probably be a sqlite database to mimic what the tail plugin of fluent bit does.
The ingester should also be able to handle log rotation, where a log file is renamed and a new log file is created. The ingester should be able to detect the rotation and continue processing the new log file from the last processed entry.

### Postponing processing of log entries

The ingester should be able to postpone the processing of log entries that are not yet complete, such as multiline entries that span multiple lines but not limited to those. As we know, I/O does not guarantee that even single lines will be written in full at the end of each write operation, so the ingester should be able to handle partial lines and wait for the next write operation to complete the line.

## Configuration of the default plugin
The configuration file should be simple for humans. Abominations like YAML should be avoided. A simple JSON or INI format should be used instead.
The global glob patterns specified a the level of the ingester will control the list of interesting files.
Below those, a pair include/exclude lists of glob patterns will identify a subset of files to be processed by a certain instance of a plugin. So, for example:
```json
{
  "ingester": {
    "directory": "/var/log/sofia",
    "include_patterns": ["*.log", "*.txt"],
    "exclude_patterns": ["*.gz", "*.zip"]
  },
  "plugins": [
    {
      "name": "default",
      "include_patterns": ["*.log"],
      "exclude_patterns": ["*.debug.log"],
      "config": {
        "timestamp_regex": "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",
        "utc_timestamp": true,
        "level_regex": "(INFO|ERROR|DEBUG)",
        "message_regex": "(.*)",
        "fields_regexes": [
          {"regex": "user_id=(\\d+)", "field_name": "user_id"},
          {"regex": "session_id=([a-z0-9]+)", "field_name": "session_id"}
        ]
      }
    },
    {
      "name": "default",
      "include_patterns": ["*.txt"],
      "exclude_patterns": ["*.debug.txt"],
      "config": {
        "timestamp_regex": "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",
        "level_regex": "(INFO|ERROR|DEBUG)",
        "utc_timestamp": false,
        "message_regex": "(.*)",
        "fields_regexes": [
          {"regex": "user_id=(\\d+)", "field_name": "user_id"},
          {"regex": "session_id=([a-z0-9]+)", "field_name": "session_id"}
        ]
      }
  ]
}
```

## Error Handling
The ingester should handle errors gracefully. If a log file cannot be read or parsed, the ingester should log the error and continue processing other log files. The ingester should also be able to handle malformed log entries and skip them without crashing.
The ingester should log errors in a separate error log file, which can be configured in the ingester configuration. The error log file should include the timestamp, log level, and a message describing the error.

## Performance Considerations
The ingester should be able to handle large log files efficiently. It should use buffered I/O to read log files in chunks and process them in parallel if possible. The ingester should also be able to handle a large number of log files without consuming too much memory or CPU resources.
The ingester should be able to handle high log volumes without dropping log entries. It should be able to process log entries in real time and keep up with the rate of incoming log entries.

---
# design-01.md
---

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

---
# design-02.md
---

# Channels vs. TPL Dataflow for LogTapestry

That's an excellent question, and you're pointing to a very relevant debate in modern .NET concurrency. You are correct: `System.Threading.Channels` is a fantastic, high-performance primitive, and libraries like `Open.ChannelExtensions` make them incredibly powerful and expressive.

Let's break down the comparison between TPL Dataflow and Channels for your **LogTapestry** project. This isn't a case of one being universally better, but rather a trade-off in abstraction, features, and style.

### The Core Difference: Abstraction Level

* **TPL Dataflow:** Is a higher-level framework. It provides "blocks" (like `TransformBlock`, `BatchBlock`) which are like pre-built components. You declare what each component does and then link them together into a pipeline or mesh. It's more of a declarative, "agent-based" model.
* **System.Threading.Channels:** Is a lower-level, more fundamental primitive. It's essentially a highly optimized, thread-safe, in-memory queue designed for producer-consumer scenarios. You are responsible for creating the tasks that read from and write to the channel. It's more of an imperative, "plumbing" model.

The library you mentioned, `Open.ChannelExtensions`, brilliantly bridges this gap by adding declarative, LINQ-style extension methods on top of the imperative Channel primitive.

### Head-to-Head Comparison for Your Project

| Feature | TPL Dataflow | `System.Threading.Channels` (+ `Open.ChannelExtensions`) | Verdict for LogTapestry |
| :--- | :--- | :--- | :--- |
| **Simplicity** | Can feel complex initially due to many block types and options. However, for a known pattern (transform -> batch -> act), it's very concise. | The base Channel API is dead simple. The extension library adds a fluent, easy-to-read syntax that many developers find more intuitive than Dataflow's linking API. | **Channels have a slight edge.** The fluent syntax of the extension library is often more discoverable and readable for linear pipelines. |
| **Performance** | Extremely fast, but each block has a small amount of overhead (for scheduling, buffering, state management). | Generally lower overhead per-element as it's a more fundamental type. It can be slightly faster in microbenchmarks for simple, linear pipelines. | **Effectively a tie.** The performance difference is unlikely to be the bottleneck in your application. I/O (disk reads/writes) and Regex parsing will dominate. Both are "fast enough" by a huge margin. |
| **Built-in Features** | **This is Dataflow's key strength.** `MaxDegreeOfParallelism`, `BoundedCapacity`, `Greedy` vs. `Non-Greedy` linking, `BroadcastBlock`, `JoinBlock`, built-in batching (`BatchBlock`) are all first-class concepts. | Base Channels have capacity bounding and that's it. You must implement parallelism, batching, etc., yourself by managing multiple consumer tasks. `Open.ChannelExtensions` adds excellent helpers for these, but the underlying task management is abstracted away by the library. | **Dataflow has a strong advantage.** The parallelism control (`MaxDegreeOfParallelism`) on the parsing block is a single line of configuration. Implementing this safely and efficiently with raw Channels is non-trivial. The extension library simplifies this, but Dataflow's implementation is battle-tested and part of the core framework. |
| **Versatility** | Excellent for complex graphs (not just linear pipelines). If you ever need to fork the stream (e.g., send errors to a separate pipeline), Dataflow's predicate-based linking is superb. | Extremely versatile. As a fundamental building block, you can construct any workflow with them. The extension library makes many complex workflows much easier to build. | **A tie.** Both can achieve the same results. Dataflow provides pre-built solutions for complex topologies, while Channels give you the raw material to build them yourself, with libraries like `Open.ChannelExtensions` providing the blueprints. |
| **Backpressure** | Handled automatically and transparently. If a downstream block is busy, the upstream block will stop sending it messages (or will buffer them up to its `BoundedCapacity`). | Handled automatically and transparently. Awaiting `writer.WriteAsync(...)` will pause the producer if the channel is full. This is the core strength of Channels. | **Both are excellent.** This is a primary design goal for both technologies. |

### How the Code Would Look (Conceptual)

Let's imagine the core pipeline: `Read Lines -> Parse Logs (Parallel) -> Batch Results -> Write to Parquet (Serial)`

**TPL Dataflow Implementation:**

```csharp
var options = new ExecutionDataflowBlockOptions {
    MaxDegreeOfParallelism = Environment.ProcessorCount,
    BoundedCapacity = 1000
};

// Block 1: Parses a string line into a LogEntry. Runs in parallel.
var parserBlock = new TransformBlock<string, LogEntry>(line => ParseLog(line), options);

// Block 2: Gathers LogEntry objects into batches of 1000.
var batchBlock = new BatchBlock<LogEntry>(1000);

// Block 3: Writes a batch to Parquet. Runs as a single task.
var writerBlock = new ActionBlock<LogEntry[]>(batch => WriteToParquet(batch));

// Link the pipeline together.
parserBlock.LinkTo(batchBlock, new DataflowLinkOptions { PropagateCompletion = true });
batchBlock.LinkTo(writerBlock, new DataflowLinkOptions { PropagateCompletion = true });

// To use it:
// while (reading file) {
//     await parserBlock.SendAsync(line);
// }
// parserBlock.Complete();
// await writerBlock.Completion;
```**`Open.ChannelExtensions` Implementation:**
```csharp
var channel = Channel.CreateBounded<string>(1000);

// Set up the entire pipeline declaratively.
Task pipelineTask = channel.Reader
    // Block 1: Parses in parallel.
    .Transform(
        line => ParseLog(line),
        maxParallelism: Environment.ProcessorCount
    )
    // Block 2: Batches the results.
    .Batch(1000)
    // Block 3: Writes batches serially.
    .ForAllAsync(batch => WriteToParquet(batch));

// To use it:
// while (reading file) {
//     await channel.Writer.WriteAsync(line);
// }
// channel.Writer.Complete();
// await pipelineTask;
```

### Conclusion and Recommendation

**You are right to be drawn to the Channel-based approach.** The code with the extension library is arguably more readable and fluent. For the strictly linear pipeline you've described, it is an outstanding choice.

Here is my final recommendation:

1. **For *this specific* pipeline:** The `Open.ChannelExtensions` approach is excellent. It's clean, performant, and leverages modern .NET patterns beautifully. It perfectly accomplishes the goal.
2. **Why I initially suggested Dataflow:** TPL Dataflow remains the "out-of-the-box" standard for complex data processing graphs. Its strengths become more apparent if your pipeline needs to become more complex. For example, if `ParseLog` could fail and you wanted to route failed lines to a separate "dead-letter" `ActionBlock` for logging, Dataflow handles this branching logic very gracefully.

**Final Verdict:** Stick with your instinct. **Using `System.Threading.Channels` with the `Open.ChannelExtensions` library is a sound, modern, and highly effective design choice for this project.** It gives you high performance with a very readable and maintainable syntax. My suggestion for Dataflow comes from a place of "industrial-strength, batteries-included" thinking, but for your defined needs, the channel-based solution is more elegant.

---
# design-03.md
---

# Storage

### 1. Do We Need a Write-Ahead Log (WAL)?

That's a very insightful question. It gets to the heart of data durability and crash recovery.

The short answer is: **No, you likely do not need a *separate* WAL, because your current design cleverly uses the source log files and the SQLite state database *as a de facto WAL*.**

Let's break down why. A WAL's primary purpose is to guarantee that data that has been acknowledged as "received" is not lost in a crash, even if it hasn't been written to the main data store yet.

Consider your proposed crash/recovery scenario:

1. **Steady State:** The ingester is reading a file, `sofia.log`. It has processed up to byte `1,048,576`. This position is saved in the SQLite `TrackedFiles` table.
2. **Processing a New Batch:** The ingester reads the next 100kb of data from the file. It parses these lines into `LogEntry` objects, which now exist in memory inside your Channel/Dataflow pipeline.
3. **CRASH!** The power goes out. The application terminates instantly. The in-memory `LogEntry` objects are gone. The new Parquet file was never written. Crucially, the `Position` in the SQLite database for `sofia.log` was **not updated** to the new position of `1,148,576`.
4. **Restart:** The ingester starts back up. It consults the SQLite database. It sees that for `sofia.log`, the last successfully processed position was `1,048,576`.
5. **Recovery:** It opens `sofia.log` and seeks to that position. It then re-reads the same 100kb of data that it was processing when it crashed. The data is "replayed" from the source log file.

In this model:

* **The Source Log File** is your "log."
* **The SQLite `Position` field** is your "commit pointer."

This provides an **at-least-once delivery guarantee**, which is typically perfect for logging systems. You don't lose data, but in the rare event of a crash, you might process a few log entries twice. This is a standard and acceptable trade-off that avoids the complexity of building and managing a true, separate WAL.

A dedicated WAL would only be necessary if you couldn't afford to re-read from the source files (e.g., if the source was a transient network stream), but since you are reading from the local file system, your design is already robust.

---

### 2. How to Structure the Content of the Parquet Files?

This is the most important question for long-term query performance. Parquet is a columnar format, and designing your schema to take advantage of that is key.

Here is a recommended schema for your Parquet files, aiming for a balance of query performance and storage efficiency.

#### Logical Schema (The Columns)

1. **`timestamp` (Timestamp, nanosecond precision, UTC)**
    * This is your most critical column. It will be used in almost every query's `WHERE` clause. Storing it as a native `TIMESTAMP` type is essential for fast range scans.

2. **`level` (String, Dictionary Encoded)**
    * Since there are few unique values (INFO, ERROR, DEBUG), Parquet's automatic dictionary encoding will make this column extremely small and fast to filter on.

3. **`source` (String, Dictionary Encoded)**
    * Similar to `level`, the number of unique file paths will be much smaller than the number of log entries. Dictionary encoding will be very effective here.

4. **`template_hash` (64-bit Long/Integer)**
    * **Optimization:** Instead of storing the full template string (`"User logged in: user_id={user_id}, ..."`) in every single row, which is highly redundant, store a 64-bit hash (e.g., a non-cryptographic xxHash) of it. This column will be tiny and very fast for grouping log messages of the same type (`GROUP BY template_hash`).
    * You would maintain a separate lookup file or a simple table in your SQLite DB (`TemplateHashes(Hash, TemplateString)`) to resolve the hash back to the human-readable string when needed.

5. **`fields` (Map of String -> String)**
    * This is the "correct" way to store your dynamic, extracted fields (`{ "user_id": "12345", ... }`). Parquet has a native `Map` type.
    * Modern query engines like DuckDB, Spark, and Dremio can directly query data within these nested maps. For example: `SELECT * FROM logs.parquet WHERE fields['user_id'] = '12345'`.
    * **Avoid the temptation to store this as a JSON string.** Doing so would make it an opaque blob to the query engine, forcing it to read the entire string for every row and then parse it, completely defeating the purpose of a columnar store.

6. **`message` (String)**
    * The full, raw, unparsed log message. You still want this for full-text search and for cases where parsing might have missed something. This column might be large, but because Parquet is columnar, queries that don't select `message` won't pay the I/O cost to read it.

#### Physical Layout: Partitioning

Equally important as the schema is how you organize the files on disk. You should use **Hive Partitioning**. This means organizing Parquet files into a directory structure based on the values of certain columns.

For time-series data like logs, the obvious and most effective strategy is to partition by date. Your data directory should look like this:

```
/path/to/data/
├── year=2025/
│   ├── month=08/
│   │   ├── day=17/
│   │   │   ├── part-00001-uuid.parquet
│   │   │   └── part-00002-uuid.parquet
│   │   └── day=18/
│   │       └── part-00001-uuid.parquet
│   └── month=09/
│       └── ...
└── year=2026/
    └── ...
```

**Why is this a massive performance win?**

When you run a query like `SELECT * FROM logs WHERE timestamp >= '2025-08-17T10:00:00Z' AND timestamp < '2025-08-17T11:00:00Z'`, a partition-aware query engine will:

1. Look at the file paths.
2. Immediately know that it only needs to open the files inside the `/year=2025/month=08/day=17/` directory.
3. It completely **skips listing and reading** all the other directories (all other days, months, and years). This is called "predicate pushdown" or "partition pruning" and it can reduce query times from minutes to milliseconds.

Your ingester's "Data Sink" component would be responsible for writing to the correct partitioned directory based on the `timestamp` of the log entries in the current batch.

---
# design-04.md
---

# Typed fields

### 1. Updating the Configuration of the Default Plugin

We need to add a `type` property to the `fields_regexes` objects in our configuration. We can also add an optional `format` for types like `datetime`.

The configuration for a plugin would be updated to look like this. I've added a new regex for `response_time_ms` to illustrate a numeric type.

```json
{
  "name": "default",
  "include_patterns": ["*.log"],
  "config": {
    "timestamp_regex": "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",
    "utc_timestamp": true,
    "level_regex": "(INFO|ERROR|DEBUG)",
    "message_regex": "(.*)",
    "fields_regexes": [
      {
        "regex": "user_id=(\\d+)", 
        "field_name": "user_id",
        "type": "long"  // New: Specify the type
      },
      {
        "regex": "session_id=([a-z0-9]+)", 
        "field_name": "session_id" 
        // Type is omitted, so it defaults to "string"
      },
      {
        "regex": "response_time_ms=(\\d+\\.?\\d*)",
        "field_name": "response_time_ms",
        "type": "double" // New: A floating-point type
      }
    ]
  }
}
```

**Proposed Types:**
You should support a core set of primitive types that map well to Parquet and common log data:

* `string` (the default if `type` is omitted)
* `long` (for 64-bit integers like IDs, counters)
* `double` (for floating-point numbers like durations)
* `boolean` (for flags)
* `datetime` (with an optional `format` string for parsing, e.g., `"yyyy-MM-dd'T'HH:mm:ss.fffZ"`)

#### Plugin Parsing Logic Changes

The plugin's internal logic now has an extra step:

1. Extract the value as a string using the regex.
2. Look up the configured `type` for that field.
3. **Attempt to parse the string into the target type.** For example, use `long.TryParse()`, `double.TryParse()`, etc.
4. **Error Handling:** If parsing fails (e.g., the value for a "long" field is "N/A"), the plugin must handle it gracefully. The best strategy is to log the parsing error and either:
    * Skip adding the field for this log entry.
    * (Better) Add the unparseable value to a special "fallback" map of string-to-string fields.

### 2. Modifying the Structure of the Parquet File

This is the most critical part. You **cannot** have a single Parquet `Map` column with values of different types (e.g., `Map<string, object>`). This would be an anti-pattern that defeats the purpose of columnar storage.

The most performant and query-friendly approach is to use **multiple, type-specific map columns.**

Instead of one generic `fields` column, your Parquet schema will now have a dedicated map column for each data type you support.

**New Parquet Schema:**

* `timestamp` (Timestamp)
* `level` (String)
* `source` (String)
* `template_hash` (Long)
* `message` (String)
* **`fields_string` (Map<String, String>)**
* **`fields_long` (Map<String, Long>)**
* **`fields_double` (Map<String, Double>)**
* **`fields_boolean` (Map<String, Boolean>)**
* **`fields_datetime` (Map<String, Timestamp>)**

#### How It Works in Practice

Let's take a log entry and see how the ingester would populate this structure.

**Log Entry:**
`2025-08-18 10:30:00 INFO [api] Request finished: user_id=12345 session_id=xyz789 response_time_ms=55.4`

**1. In-Memory Representation (C#):**
The parsed data would be held in a `Dictionary<string, object>`:

```csharp
{
    "user_id": 12345L,           // Parsed as a long
    "session_id": "xyz789",      // Parsed as a string
    "response_time_ms": 55.4     // Parsed as a double
}
```

**2. Writing to Parquet:**
When writing this entry to a Parquet file, the Data Sink would distribute the values into the appropriate typed map columns. For this single row, the columns would look like this:

| ... | `fields_string` | `fields_long` | `fields_double` | `fields_boolean` |
| :-- | :--- | :--- | :--- | :--- |
| **Row 1** | `{"session_id": "xyz789"}` | `{"user_id": 12345}` | `{"response_time_ms": 55.4}` | *(empty map or null)* |
| **Row 2** | `{"user_id": "FAILED_TO_PARSE"}` | *(empty map or null)* | `{"response_time_ms": 102.1}` | *(empty map or null)* |

In the second conceptual row, `user_id` failed to parse as a long, so it was placed in the string map as a fallback.

### Benefits of This Approach

1. **Massive Query Performance Gain:** A query like `SELECT * FROM logs WHERE fields_long['response_time_ms'] > 100` can use native numeric operations directly on the compressed columnar data. It doesn't need to read and cast strings from every row. This is the single biggest advantage.
2. **Storage Efficiency:** Storing numbers as their native binary types (`long`, `double`) is far more space-efficient than storing them as strings. Parquet's encoding schemes (like Delta encoding for integers) can further compress this numeric data.
3. **Data Integrity:** It enforces a schema on your unstructured data, making it more reliable and predictable for analytics.

This change adds a little complexity to the ingestion logic but pays huge dividends in query performance and storage efficiency. It's the standard, best-practice approach for handling semi-structured data in columnar formats.

---
# design-05.md
---

You have hit upon the two most critical challenges of this entire design. Your concerns are 100% valid.

1. **Query User Experience:** Forcing a user to know that `user_id` is in `fields_long` but `session_id` is in `fields_string` is a deal-breaker. It makes the system unusable.
2. **Schema Conflicts:** A single field name having different incompatible types across different log sources is a fundamental problem in data integration that must be handled gracefully.

The multi-map solution is the correct *physical storage strategy*, but it must not be the *logical query model*. We need an abstraction layer to solve both problems.

The solution is to introduce a **Schema Registry** that lives in your SQLite database. This registry will be the "source of truth" for the type of every field.

### 1. The Schema Registry

In your SQLite state database, create a new table:

```sql
CREATE TABLE FieldSchema (
    FieldName TEXT PRIMARY KEY,
    -- 'string', 'long', 'double', 'boolean', etc.
    FieldType TEXT NOT NULL
);
```

This table will store the "official" or "canonical" type for every dynamic field discovered across all log files.

### 2. Ingestion Logic with Conflict Resolution

The ingestion pipeline needs to be updated to use this schema registry. Here is the new logic when a plugin parses a field (e.g., it extracts `("http_status", "200")` and is configured to treat it as a `long`).

1. **Check the Registry:** The ingester looks up `http_status` in the `FieldSchema` table.

2. **Case A: The field is new.**
    * `http_status` is not in the table.
    * The ingester registers it: `INSERT INTO FieldSchema (FieldName, FieldType) VALUES ('http_status', 'long');`
    * It then attempts to parse `"200"` as a `long`. It succeeds.
    * The value `200` is written to the `fields_long` map column in Parquet.

3. **Case B: The field exists and the type matches.**
    * The ingester finds that `http_status` is of type `long`.
    * The current plugin also wants to treat it as a `long`. This is the happy path.
    * The value is parsed and written to the `fields_long` map.

4. **Case C: A Type Conflict Occurs.**
    * The ingester finds that `http_status` is already registered as type `long`.
    * However, a different plugin (or a different log line) now produces the value `"OK"` for the field `http_status`.
    * The ingester attempts to parse `"OK"` as a `long`. **This fails.**
    * **Conflict Resolution:** Instead of crashing or dropping the data, the ingester writes this incompatible value to a dedicated fallback column.

Let's refine the Parquet schema to support this robustly.

#### Revised Parquet Schema with a Fallback Column

Instead of just typed maps, we add one more for these "exceptions".

* `timestamp` (Timestamp)
* ... (level, source, etc.)
* `fields_string` (Map<String, String>)
* `fields_long` (Map<String, Long>)
* `fields_double` (Map<String, Double>)
* **`fields_fallback` (Map<String, String>)** <- **The new addition**

Now, in Case C, the ingester would log a clear warning:
`"Type conflict for field 'http_status'. Canonical type is 'long', but received unparseable value 'OK' from source 'other.log'. Storing in fallback map."`

And it would write the entry to Parquet like this:

* `fields_long`: (does not contain `http_status` for this row)
* `fields_fallback`: `{"http_status": "OK"}`

This **Strict First-Come, First-Served** strategy with a fallback is the best of both worlds:

* It protects the data type of the primary columns, keeping them clean and fast for the 99% of data that conforms.
* It prevents data loss for the 1% of data that is messy or conflicting.
* It provides clear diagnostics about data quality problems in the source logs.

### 3. The Smart Query Layer (Solving the UX Problem)

Now, how does this fix the user experience? Your query tool (CLI, API, etc.) becomes the smart abstraction layer. It will use the `FieldSchema` table to rewrite user queries into physical queries.

**User's Query:**
`SELECT message WHERE level = 'ERROR' AND http_status > 500`

**Query Tool's Internal Logic:**

1. The tool parses the query and sees the predicate `http_status > 500`.
2. It queries the local SQLite database: `SELECT FieldType FROM FieldSchema WHERE FieldName = 'http_status';`
3. The database returns `'long'`.
4. The tool now knows that the canonical `http_status` field lives in the `fields_long` map.
5. It **rewrites the query** to target the correct physical Parquet column:
    `SELECT message FROM 'path/to/data/**/*.parquet' WHERE level = 'ERROR' AND fields_long['http_status'] > 500`

**What about querying the fallback data?**

If the user writes a query that is incompatible with the canonical type, the query tool can intelligently target the fallback column.

**User's Query:**
`SELECT message WHERE http_status = 'OK'`

**Query Tool's Logic:**

1. The tool sees `http_status` and the value `'OK'`.
2. It looks up the canonical type for `http_status` and finds `'long'`.
3. It recognizes that the value `'OK'` is a `string`, which is incompatible with `long`.
4. Therefore, it intelligently rewrites the query to target the **fallback column**:
    `SELECT message FROM 'path/to/data/**/*.parquet' WHERE fields_fallback['http_status'] = 'OK'`

This design completely hides the physical storage complexity from the end-user. They interact with a simple, logical schema (`timestamp`, `level`, `http_status`, `user_id`), and the query tool handles the mapping to the highly optimized, physically distinct Parquet columns.

---
# design-06.md
---

# Query Engine

### The Core Technology: DuckDB

For the query engine itself, I strongly recommend using **DuckDB**. It is the perfect technology for this specific use case.

* **In-Process:** DuckDB is an in-process analytical database. It runs as a library inside your query tool's process. There is no server to install or manage.
* **Zero-Dependency:** A C# application using it can be published as a self-contained executable, which is ideal for your "offline mode" in a lab.
* **SQL Interface:** It provides a rich, standard SQL interface that is familiar to everyone.
* **Optimized for Parquet:** DuckDB is arguably the fastest engine in the world for querying Parquet files. It can read them directly, understands Hive partitioning natively, and uses predicate pushdown and columnar scanning to achieve incredible speeds.
* **Excellent C# Bindings:** The official `DuckDB.NET` bindings are mature and easy to use.

DuckDB will serve as the engine that reads the Parquet files. Your query tool will be the "smart" layer on top that uses your SQLite metadata to present a simple, logical view to the user.

---

### 1. Offline Mode: The Foundation

This is the simpler mode and serves as the foundation for the whole design.

**The "Snapshot" Package:**
To move a system to a lab for inspection, a user would copy two things:

1. The entire data directory (e.g., `/var/logtapestry/data/`) containing the Hive-partitioned Parquet files.
2. The SQLite state database file (e.g., `/var/logtapestry/state.sqlite`).

**The Query Tool (`logtapestry-query.exe`):**
This would be a standalone command-line application.

**Workflow:**

1. A user runs the tool, providing the paths to the snapshot:

    ```bash
    logtapestry-query.exe --db C:\lab\snapshot\state.sqlite --data C:\lab\snapshot\data\ `
    "SELECT timestamp, message, user_id FROM logs WHERE level = 'ERROR' AND user_id = 12345 ORDER BY timestamp DESC LIMIT 100"`
    ```

2. **Initialization:** The tool starts a DuckDB instance in-memory.

3. **Schema Loading:** It connects to the provided `state.sqlite` file and reads the entire `FieldSchema` table into its own memory. This gives it the mapping of `FieldName -> FieldType`.

4. **Query Rewriting (The "Magic"):** The tool parses the user's SQL query. It identifies the logical field names like `user_id`. Using its cached schema, it rewrites the query into the physical query that DuckDB can understand.
    * **User Query:** `... WHERE user_id = 12345`
    * **Internal Lookup:** Schema says `user_id` is a `long`.
    * **Rewritten Query:** `... WHERE fields_long['user_id'] = 12345`

5. **Execution:** The tool executes the rewritten SQL against DuckDB. It tells DuckDB to treat the data directory as a Hive-partitioned dataset. The SQL for DuckDB would look something like this:

    ```sql
    SELECT 
        timestamp, 
        message, 
        fields_long['user_id'] AS user_id -- Aliasing for clean output
    FROM read_parquet('C:/lab/snapshot/data/*/*/*/*.parquet', hive_partitioning = true)
    WHERE level = 'ERROR' AND fields_long['user_id'] = 12345
    ORDER BY timestamp DESC
    LIMIT 100;
    ```

    DuckDB handles all the complexity of partition pruning and reading the correct Parquet files.

6. **Output:** The results are formatted and printed to the console.

---

### 2. Online Mode: Reading from a Live System

The online mode uses the **exact same query tool and logic**. The only difference is that it points to the live directories where the ingester is actively writing.

**Coordination Between Ingester and Query Engine:**

To prevent the query engine from reading a file that the ingester is in the middle of writing, the ingester must write files atomically.

* **Ingester's Writing Process:**
    1. Write a full batch of data to a temporary file: `part-00001-uuid.parquet.tmp`.
    2. Once the file is fully written and closed, perform an atomic **rename** operation: `part-00001-uuid.parquet.tmp` -> `part-00001-uuid.parquet`.
    3. A file rename is an atomic metadata operation on virtually all modern filesystems. The query engine will never see a partially written file.

* **Query Engine's Reading Process:**
    1. The query engine simply targets the glob pattern `.../*.parquet`. It will never see the `.tmp` files.
    2. DuckDB is designed to be resilient to the underlying set of files changing between queries. When a query starts, it gets a snapshot of the files that exist at that moment.

### The Challenge: Querying "Uncommitted" In-Flight Data

This is a very common requirement. The answer depends on the definition of "real-time."

**The Pragmatic "Near Real-Time" Approach (Recommended):**

The best strategy is to **not** query in-memory data directly. Instead, make the ingestion pipeline's batching frequent enough that the data is "near real-time."

* The ingester's `BatchBlock` or `channel.Batch()` can be configured to flush based on two triggers:
    1. **Count:** After 10,000 log entries.
    2. **Timeout:** Every 5 seconds, regardless of the count.

This means that any log entry will be persisted to a queryable Parquet file within a maximum of 5 seconds of its arrival. For log analysis, a 5-second delay between event occurrence and queryability is almost always acceptable and is **vastly simpler** than true real-time querying.

**Why avoid true real-time?**

To query the in-flight data, the query engine would need to:

1. Query DuckDB for all the historical data on disk.
2. Somehow ask the live ingester process for all the data currently in its memory buffers. This would require complex Inter-Process Communication (IPC) and locking, which would hurt ingestion performance.
3. Merge the two datasets (from disk and from memory).

This adds enormous complexity for a marginal benefit. The "near real-time" approach provides the best balance of performance, simplicity, and freshness. When a user runs a query in online mode, they are getting results that are, at most, a few seconds old.

---
# design-07.md
---

# Compaction

You are absolutely right. That is the critical trade-off, and you've pinpointed the single biggest weakness of a simple, aggressive timeout-based flushing strategy. It prioritizes low data latency at the cost of query performance.

This is a classic problem in data stream processing known as the **"small file problem."**

**The Problems Caused by Many Small Files:**
*   **Query Performance Degradation:** Query engines like DuckDB are heavily optimized for scanning large, contiguous chunks of data. The fixed cost of opening a file, reading its metadata footer, and setting up a scan is significant. Reading one 100MB file is orders of magnitude faster than reading one thousand 100KB files, even though the total data volume is the same.
*   **Filesystem Metadata Overhead:** Managing hundreds of thousands or millions of files puts a strain on the filesystem's metadata layer (inodes). Listing directories becomes slow.
*   **Inefficient Compression:** Parquet's compression algorithms are far more effective on larger data chunks where they can build better dictionaries and find more patterns.

The solution is not to abandon the timeout but to introduce a background **Compaction Process**. This creates a two-tier storage architecture: a "hot" landing zone for fresh data and a "cold" optimized zone for historical data.

### The Two-Tier Architecture: Landing Zone and Optimized Zone

1.  **Tier 1: The Landing Zone (for "Hot" Data)**
    *   **Purpose:** To receive data from the ingester with the lowest possible latency.
    *   **Characteristics:** Contains many small Parquet files. This is *expected and accepted* here.
    *   **Ingester's Role:** The ingester **only ever writes to the Landing Zone.** It continues to use the hybrid flush trigger (e.g., 10,000 records or 5 seconds).

    Your directory structure would now reflect this:
    ```
    /path/to/data/year=2025/month=08/day=18/
    ├── landing/
    │   ├── 10:00-part-uuid1.parquet  (150 KB)
    │   ├── 10:01-part-uuid2.parquet  (210 KB)
    │   └── 10:02-part-uuid3.parquet  (95 KB)
    └── (Optimized files will appear here later)
    ```

2.  **Tier 2: The Optimized Zone (for "Cold" Data)**
    *   **Purpose:** To store data for the long term in a format optimized for fast queries.
    *   **Characteristics:** Contains a small number of large, well-structured Parquet files (e.g., target size of 128MB - 512MB).
    *   **Compactor's Role:** A background process is responsible for creating these files.

### The Compaction Process

This is a separate, asynchronous process or a dedicated thread within the ingester application. It runs on a schedule (e.g., every 15 minutes or every hour).

**Workflow of the Compactor:**
1.  **Trigger:** Let's say the compactor runs at 11:00 AM. Its job is to compact the data from the previous hour (10:00 AM to 10:59 AM).
2.  **Identify Candidates:** It scans the `landing` directories for the target time window (e.g., `.../day=18/landing/`). It finds all the small Parquet files written during that hour.
3.  **Read and Rewrite:** It reads all of these small files into memory and writes their combined data into one (or more) new, large Parquet files. These new files are written directly to the parent, "optimized" directory (`.../day=18/`).
4.  **Atomic Swap:**
    *   The new, large file is written with a temporary name (e.g., `10:00-to-10:59-compacted-uuid.parquet.tmp`).
    *   Once the write is complete and successful, the compactor **deletes** all the small source files from the `landing` directory.
    *   Finally, it renames the temporary file to its final name (`10:00-to-10:59-compacted-uuid.parquet`).

This "delete-then-rename" sequence ensures that a query engine will see either the old set of small files or the new large file, but never both or a partial state.

### How the Query Engine Handles This

This is the clever part. The query engine is made aware of the two-tier structure and queries both locations seamlessly. The complexity is hidden from the user.

When the user runs a query for the last 3 hours:
`"SELECT * FROM logs WHERE timestamp > now() - interval '3 hours'"`

The query tool internally generates a more complex query for DuckDB that combines both zones using a `UNION ALL`:

```sql
-- The physical query executed by DuckDB
(
    -- Query the hot, uncompacted data in the landing zone
    SELECT *
    FROM read_parquet('/path/to/data/*/*/*/landing/*.parquet', hive_partitioning=true)
    WHERE timestamp > now() - interval '3 hours'
)
UNION ALL
(
    -- Query the cold, optimized historical data
    SELECT *
    FROM read_parquet('/path/to/data/*/*/*/*.parquet', hive_partitioning=true)
    WHERE timestamp > now() - interval '3 hours'
);

```
*DuckDB is smart enough to use the `hive_partitioning` and `timestamp` predicates to avoid scanning files outside the time range in *both* locations.*

**Benefits of this Design:**

*   **Low Ingestion Latency:** You keep your 5-second flush, so data is queryable almost instantly in the `landing` zone.
*   **High Query Performance:** Any query touching data older than the compaction window (e.g., older than an hour) will hit the large, optimized Parquet files, ensuring maximum speed.
*   **Resilience:** The system is self-healing. Even if the compactor fails for a few hours, the system continues to operate correctly; queries will just be a bit slower as they scan more small files in the `landing` zone until the compactor catches up.

This two-tier landing/compaction model is the industry-standard solution for this exact problem and gives you the best of both worlds.

---
# design-08.md
---

Absolutely. That's an excellent design principle that moves the architecture from just being functional to being operationally robust and flexible.

Making the compactor a process that can run offline is not just possible, it's the *preferred* approach. It means decoupling the "maintenance" task of compaction from the "real-time" task of ingestion.

Here’s how we design for this:

### The Standalone Compactor Tool

The key is to build the compactor as a **separate, standalone executable**, for example `logtapestry-compactor.exe`. This tool is completely independent of the live ingester process.

Its sole responsibility is to scan a given data directory, find directories that are eligible for compaction, and perform the compaction.

#### State Management: The Compaction "Receipt"

How does the compactor know what it has already compacted, especially if it's running in a lab on copied data? It shouldn't rely on the central SQLite database. The state of compaction should live *with the data itself*.

The best practice for this is a simple **marker file** or "receipt."

**The Compaction Logic:**

1. The compactor scans the data directory (e.g., `/path/to/data/`).
2. It looks for a time-partitioned directory (like `/year=2025/month=08/day=18/`).
3. It checks for two conditions:
    * Does this directory contain a `landing/` subdirectory with data in it?
    * Does this directory **NOT** contain a marker file named `_COMPACTION_COMPLETE`?
4. If both conditions are true, the directory is a candidate for compaction.
5. The compactor then performs the read/rewrite/swap process for that directory as we discussed.
6. **Crucially:** After it has successfully deleted the small files from the `landing` directory, the very last thing it does is create the empty marker file: `/year=2025/month=08/day=18/_COMPACTION_COMPLETE`.
7. The next time the compactor runs, it will see this marker file and know to skip this directory entirely, making the process highly efficient and idempotent.

**Handling the "Current" Time Window:**
To prevent race conditions with the live ingester, the compactor should be configured to ignore the most recent, "hot" time window. For example, if the compaction runs hourly, it should only compact hours that are fully in the past. If it runs at 11:15, it will only process the 10:00-11:00 window, leaving the current 11:00-12:00 window alone.

---

### How This Enables Both Modes of Operation

With this design, running online vs. offline is just a matter of where and when you run the exact same tool.

#### 1. Online Mode (Live System)

* **Execution:** The `logtapestry-compactor.exe` tool is run on the production server via a scheduler (like a cron job on Linux or a Scheduled Task on Windows).
* **Frequency:** It could be scheduled to run once an hour.
* **Command:** `logtapestry-compactor.exe --data /var/logtapestry/data/ --compact-older-than 1h`
* **Behavior:** When it runs, it will find the single, most recent hour of data that needs compaction, process it quickly, and then exit. The resource usage is predictable and happens in short, periodic bursts.

#### 2. Offline Mode (Lab Environment)

* **Execution:** An operator needs to investigate a production issue.
* **Step 1: Copy.** They copy the entire `/var/logtapestry/data/` directory from the production server to their lab machine. This directory may contain several days' worth of small, uncompacted files in various `landing` subdirectories if the online compactor had been disabled.
* **Step 2: Run.** On their lab machine, they run the exact same command:
    `logtapestry-compactor.exe --data C:\lab\snapshot\data\`
* **Behavior:** The tool starts up. It finds *all* the directories that are missing the `_COMPACTION_COMPLETE` marker. It will then churn through them one by one, compacting hours or days of backlog. This can run for as long as needed, using 100% of the lab machine's resources, without any impact on the live production system.
* **Step 3: Analyze.** Once the process is complete, the data in `C:\lab\snapshot\data\` is clean, optimized, and ready for high-speed analysis with the query tool.

### Benefits of this Decoupled Design

* **Resource Isolation:** Heavy I/O from compaction doesn't interfere with the latency-sensitive ingestion process on the production machine.
* **Robustness:** If the ingester is restarted, it doesn't affect the compactor. If the compactor fails, it doesn't affect the ingester. The system is more resilient.
* **Simplicity:** The ingester's logic is simpler: it only ever writes small files to one place. The compactor's logic is also simple: find work, do work, leave a receipt.
* **Flexibility:** It gives you complete operational control. You can choose to run compaction on a more powerful, dedicated machine or at off-peak hours, and it fully enables the critical lab analysis/recovery use case.

---
# design-09.md
---

You've laid out a very comprehensive and well-reasoned design. The architectural choices (Parquet, SQLite for state, DuckDB for querying, Channels/TPL Dataflow, two-tier storage with compaction) are all sound and leverage modern, effective technologies.

However, moving from a high-level design to implementation always reveals detail-level questions. Here are some of the most critical "loose ends" that I anticipate would require concrete decisions and design before you can start coding effectively, particularly concerning core interfaces and how components interact:

### 1. **The `ILogParser` Interface and Multi-Line Parsing (The "Parser Core")**

This is probably the single most complex part of the parsing pipeline.

* **Precise Input:** Your current `ILogParser.Parse(IBufferedLogLines lines)` is a good start. But what exactly is `IBufferedLogLines`?
  * Does it provide lines one by one?
  * Does it offer `PeekLine()` or `TryPeekLine()` capabilities for multi-line detection? (e.g., "If the next line *doesn't* match the timestamp regex, it's part of the current entry.")
  * How does it handle partial lines (the "last fragment") at the end of a read operation? Does it manage the buffer itself, or does the ingester feed it already-buffered complete lines?
* **Output Semantics:** `IEnumerable<LogEntry>` implies streaming. How does the parser yield entries efficiently, especially for large multi-line blocks?
* **Multi-Line Detection Logic:** This is often regex-based (e.g., "a new log entry starts with a timestamp pattern, otherwise it's part of the previous entry"). The `ILogParser` (or its concrete regex implementation) needs to embody this complex stateful logic. This might require the parser to hold an internal buffer.

**Action Item:** Define the exact contract for `IBufferedLogLines` and precisely how the multi-line detection state is managed *within* the parser, or how the ingester facilitates it. This will heavily influence the parser's implementation and efficiency.

### 2. **Dynamic Plugin Loading Details (AssemblyLoadContext)**

You've identified `AssemblyLoadContext` as the mechanism, but the practicalities need nailing down:

* **Plugin Manifest/Discovery:** How does the host application discover available plugins in the plugin directory? Do plugins need to ship with a manifest file (e.g., `plugin.json`) that describes their main assembly and type name, or does the host scan all DLLs and search for types implementing `ILogParserPlugin`?
* **Configuration to Plugin Mapping:** How does the `object config` parameter in `ILogParserPlugin.CreateParser(object config)` get its actual type? Is it a `JsonElement` that the plugin then deserializes internally? Or do you need a more robust system for passing plugin-specific, strongly-typed configuration? The latter is cleaner but requires more setup.
* **Dependency Isolation:** If plugins have external NuGet dependencies, how are these managed within their `AssemblyLoadContext` to avoid conflicts with the host or other plugins? This is a notorious challenge with dynamic loading.
* **Error Handling:** What if a plugin DLL is corrupt, or its `CreateParser` method throws an exception? How is this isolated and logged without crashing the ingester?

**Action Item:** Define the plugin packaging and discovery mechanism. Decide on the exact type of the `config` parameter for `CreateParser`.

### 3. **The Parquet Writing Library for .NET**

This is a critical third-party dependency.

* **Choice:** There isn't a single, universally dominant Parquet library for .NET like there might be for Python (pyarrow) or Java (Apache Parquet).
  * `Apache.Arrow.Flight.Client` (part of the Apache Arrow project) includes Parquet reader/writer capabilities, often leveraging underlying native libraries.
  * There are some community-driven `.NET` Parquet libraries (e.g., `Parquet.Net` on GitHub).
* **Schema Mapping:** How easily does the chosen library map your C# `LogEntry` records and their nested `IDictionary<string, object>` (which contains the mixed-typed fields) to the Parquet schema with `Map<String, String>`, `Map<String, Long>`, etc.? This mapping can be complex.
* **Performance and Stability:** You'll need to evaluate the chosen library's performance, stability, and ongoing maintenance.

**Action Item:** Research and select the specific .NET Parquet writing library. Prototype its usage to confirm it can handle your proposed typed map schema and performance requirements.

### 4. **Initial Scan on Startup & File System Watcher Robustness**

* **Initial Scan:** When the ingester starts, it needs to process all *existing* log files in the watched directory and its subdirectories from their last known positions. This is distinct from the `FileSystemWatcher` events for *new* files or *changes*. How does this initial scan integrate with the pipeline?
* **FileSystemWatcher Limitations:** `FileSystemWatcher` is known for being somewhat fragile (missing events under heavy load, duplicated events, not always reporting the *original* file path in `Renamed` events).
  * How will you debounce events?
  * How will you handle cases where the watcher stops working (e.g., due to buffer overflow) and needs to be re-initialized?
  * How do you detect log rotations where a file is truncated and rewritten with the *same name*? This needs more than just `Position` tracking; you'd likely need to combine it with file size changes and maybe a content hash (as discussed, the `FileSignature` in SQLite).

**Action Item:** Design a robust `DirectoryMonitor` component that handles both initial scan and `FileSystemWatcher` events, incorporating debouncing, error recovery, and comprehensive log rotation detection logic.

### 5. **Error Handling and Logging Granularity**

You've mentioned logging errors, but the specifics:

* **Logging Framework:** Which one? `Microsoft.Extensions.Logging` is the standard for modern .NET, with providers like Serilog for rich logging.
* **Error Log File Format:** Define the specific format for the separate error log file.
* **Skipping Malformed Entries:** When a parsing error occurs for a single line, how exactly is it reported (which file, which line number, what was the problematic content) without stopping the whole pipeline?
* **Metrics:** Beyond errors, how will you collect operational metrics (e.g., "lines processed per second," "files currently tailed," "bytes ingested") for monitoring? This helps confirm the system is working as expected.

**Action Item:** Choose a logging framework. Define the logging strategy (e.g., which log levels for which events, structured logging properties). Consider integrating basic metrics.

### 6. **Concurrency Model Details (Channels/Dataflow)**

While you've chosen Channels, the full implementation needs clarity:

* **Backpressure Points:** Where exactly will the bounded channels be placed in the pipeline (e.g., between reader and parser, or parser and writer)? How will their capacities be tuned?
* **Completion Propagation:** How do you signal the end of the stream (e.g., `writer.Complete()`) and await the full pipeline completion (`await pipelineTask`) across all stages, especially when dealing with multiple log files being tailed concurrently?
* **Cancellation:** How does a graceful shutdown (e.g., Ctrl+C) propagate a cancellation token through the entire pipeline (readers, parsers, writers)?

**Action Item:** Sketch out the exact channel pipeline stages and their bounded capacities. Plan the completion and cancellation token propagation from start to finish.

Addressing these points will move your design from a high-level conceptual framework to a concrete, implementable plan.

## First decision

Realtime loading of plugins is an overkill and we postpone it for v3 :-)

---
# design-10.md
---

Of course. Let's concentrate on Point 1 and design the core parsing components. This is the heart of the ingestion pipeline, and getting the contract right is essential.

We will design a stateful, instance-based parsing system where a dedicated parser object is responsible for a single log file stream. This is necessary to correctly handle multi-line entries that are split across different read buffers.

### Core Data Structures and Interfaces

First, let's define the key C# data structures. We'll use records for immutable data transfer objects.

**1. The `LogEntry` Record:** This is the successful output of a parsing operation. It contains the strongly-typed data ready for the Parquet writer.

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
    IReadOnlyDictionary<string, object> Fields // Values are already typed (long, double, string, etc.)
);
```

**2. The `ParsingResult` Union Type:** A parser can either succeed or fail. A simple and robust way to represent this is with a result type.

```csharp
/// <summary>
/// Represents the outcome of a parsing attempt on a block of text.
/// It can be either a success with a LogEntry or a failure with details.
/// </summary>
public record ParsingResult
{
    // Private constructor to force using the factory methods
    private ParsingResult() { } 

    public bool IsSuccess { get; private init; }
    public LogEntry? Entry { get; private init; }
    public string? ErrorMessage { get; private init; }
    public string? UnparseableText { get; private init; }
    public string Source { get; private init; } = "";

    // Factory for a successful result
    public static ParsingResult Success(LogEntry entry) => new()
    {
        IsSuccess = true,
        Entry = entry,
        Source = entry.Source
    };

    // Factory for a failed result
    public static ParsingResult Failure(string errorMessage, string text, string source) => new()
    {
        IsSuccess = false,
        ErrorMessage = errorMessage,
        UnparseableText = text,
        Source = source
    };
}
```

**3. The `ILogParser` Interface:** This is the central contract. Crucially, it must include a method to finalize any buffered data.

```csharp
/// <summary>
/// Defines the contract for a stateful parser that processes a stream of log lines.
/// A single instance of an ILogParser should be used for a single log file stream.
/// </summary>
public interface ILogParser
{
    /// <summary>
    /// Processes a new batch of complete lines from the log file.
    /// It may buffer a partial entry internally if it suspects a multi-line entry is in progress.
    /// </summary>
    /// <param name="lines">An array of complete log lines read from the file.</param>
    /// <returns>An enumeration of parsing results ready to be processed.</returns>
    IEnumerable<ParsingResult> Parse(string[] lines);

    /// <summary>
    /// Signals that the file stream has ended (e.g., file was rotated).
    /// This method should process and return any remaining buffered log entry.
    /// </summary>
    /// <returns>The final parsing result from the buffer, or null if the buffer was empty.</returns>
    ParsingResult? Flush();
}
```

### The `RegexLogParser` Implementation Logic

This will be your default plugin. An instance of this class will be created for *each file* being tailed.

**Key State Fields:**

```csharp
private readonly StringBuilder _multiLineBuffer = new();
private LogEntry? _inProgressEntry = null; // Holds the "header" of a potential multi-line entry
private readonly Regex _startOfEntryRegex; // A regex that identifies the start of a new log entry
// ... other regexes and configuration fields
```

**`Parse(string[] lines)` Method Logic:**

The method iterates through the provided lines, making decisions based on its current state (`_inProgressEntry`).

```csharp
public IEnumerable<ParsingResult> Parse(string[] lines)
{
    foreach (var line in lines)
    {
        // 1. Check if the current line marks the beginning of a NEW log entry
        if (_startOfEntryRegex.IsMatch(line))
        {
            // 2. If a previous entry was in progress, it's now complete. Finalize and yield it.
            if (_inProgressEntry != null)
            {
                // The _multiLineBuffer contains all the content for the previous entry.
                var finalMessage = _multiLineBuffer.ToString();
                var (templateHash, fields) = ExtractFieldsAndTemplate(finalMessage); // Your logic here
                
                yield return ParsingResult.Success(
                    _inProgressEntry with { Message = finalMessage, TemplateHash = templateHash, Fields = fields }
                );

                // Reset the state for the new entry
                _multiLineBuffer.Clear();
            }

            // 3. Start processing the new entry.
            // Parse the "header" (timestamp, level) from the current line.
            // If header parsing fails, yield a failure and continue.
            var (timestamp, level, initialMessage) = ParseHeader(line);
            if (timestamp == null) 
            {
                yield return ParsingResult.Failure("Timestamp not found", line, this.sourceFile);
                _inProgressEntry = null; // Invalidate state
                continue;
            }
            
            // 4. Store the new entry's header and buffer the first line of its message.
            _inProgressEntry = new LogEntry(timestamp.Value, level, "", this.sourceFile, 0, new Dictionary<string, object>());
            _multiLineBuffer.AppendLine(initialMessage);
        }
        else
        {
            // 5. This line is a CONTINUATION of the previous entry.
            if (_inProgressEntry != null)
            {
                // Just append it to the buffer.
                _multiLineBuffer.AppendLine(line);
            }
            else
            {
                // 6. This is an orphan line (e.g., junk at the start of a file).
                // It doesn't start a new entry and there's no entry in progress.
                yield return ParsingResult.Failure("Orphan log line", line, this.sourceFile);
            }
        }
    }
    // IMPORTANT: Do NOT yield the _inProgressEntry here. It might be incomplete.
    // The next call to Parse() or Flush() will handle it.
}
```

**`Flush()` Method Logic:**

This is simpler. It just finalizes whatever is left in the buffer.

```csharp
public ParsingResult? Flush()
{
    if (_inProgressEntry == null)
    {
        return null; // Nothing to flush
    }

    var finalMessage = _multiLineBuffer.ToString();
    var (templateHash, fields) = ExtractFieldsAndTemplate(finalMessage);
    
    var result = ParsingResult.Success(
        _inProgressEntry with { Message = finalMessage, TemplateHash = templateHash, Fields = fields }
    );

    // Clean up state
    _inProgressEntry = null;
    _multiLineBuffer.Clear();

    return result;
}
```

### Responsibilities of the Tailing Engine

This design creates a clear separation of concerns. The Tailing Engine's job is to feed the parser:

1. Read a raw block of bytes from a file.
2. Prepend any leftover partial line from the previous read.
3. Scan the block for newline characters, splitting it into an array of complete `string` lines.
4. Store the new leftover partial line (the text after the last newline).
5. Retrieve the correct `ILogParser` instance for this specific file.
6. Call `parser.Parse(lines)` and send the resulting `ParsingResult` objects down the processing pipeline (the Channel).
7. When it detects a file has been rotated or deleted, it must call `parser.Flush()` to get the last entry before discarding the parser instance.

---
# design-11.md
---

Excellent. Let's move on to the critical choice of the Parquet writing library. This decision directly impacts the complexity of your "Data Sink" component and the performance of the entire system.

The core challenge is to take a batch of in-memory C# `LogEntry` records and efficiently "shred" them into the columnar Parquet format, paying close attention to our specialized schema with multiple typed map columns.

### Choosing the Right Library

For a modern .NET project, you have two primary, high-quality choices.

1. **Parquet.Net:** A mature, high-performance, fully-managed library written entirely in C#. It has zero native dependencies, which is a significant advantage for simple deployment and cross-platform compatibility (e.g., running on both Windows x64 and Linux ARM64 without issue).
2. **Apache Arrow (`Apache.Arrow` package):** This library provides C# bindings for the official Apache Arrow C++ implementation. It is part of a massive, industry-standard data processing ecosystem. It can offer unparalleled performance but comes with the added complexity of managing native dependencies.

**Recommendation:**

For this project, **start with `Parquet.Net`**.

**Reasoning:**

* **Simplicity and Portability:** The lack of native dependencies makes development, debugging, and deployment vastly simpler. You can just add the NuGet package and it works everywhere.
* **Sufficient Performance:** It is highly optimized and is more than capable of handling the write throughput required for a logging system. The bottleneck in your system will almost certainly be I/O or Regex parsing, not the Parquet serialization itself.
* **Idiomatic .NET API:** Its API is designed from the ground up for .NET developers and feels natural to use with standard C# objects and collections.

You should only consider switching to Apache Arrow if rigorous benchmarking on your production hardware reveals that Parquet serialization is, against all expectations, your primary performance bottleneck.

### Implementation with Parquet.Net

Let's design the `DataSink` component that writes a batch of `LogEntry` records to a new Parquet file.

**Step 1: Define the Parquet Schema**

`Parquet.Net` allows you to define the schema programmatically. This object can be created once and reused for all writes.

```csharp
using Parquet.Schema;

// This can be a static field in your DataSink class
private static readonly ParquetSchema LogEntrySchema = new(
    // Primitives
    new DataField<DateTime>("timestamp"),
    new DataField<string>("level"),
    new DataField<string>("source"),
    new DataField<long>("template_hash"),
    new DataField<string>("message"),

    // The typed maps for our dynamic fields
    new MapField("fields_string", new DataField<string>("key"), new DataField<string>("value")),
    new MapField("fields_long", new DataField<string>("key"), new DataField<long>("value")),
    new MapField("fields_double", new DataField<string>("key"), new DataField<double>("value")),
    new MapField("fields_boolean", new DataField<string>("key"), new DataField<bool>("value")),
    
    // The fallback map for type conflicts
    new MapField("fields_fallback", new DataField<string>("key"), new DataField<string>("value"))
);
```

**Step 2: The Data "Shredding" Logic**

This is the most important part. We need to take a `LogEntry[]` and transform it into a set of parallel arrays, one for each column in our schema.

```csharp
using Parquet;
using Parquet.Data;

public async Task WriteBatchAsync(LogEntry[] batch, Stream targetStream)
{
    if (batch.Length == 0) return;

    // 1. Initialize arrays for each top-level column
    var timestamps = new DateTime[batch.Length];
    var levels = new string[batch.Length];
    var sources = new string[batch.Length];
    var templateHashes = new long[batch.Length];
    var messages = new string[batch.Length];

    // 2. Initialize lists for the complex map columns
    var fieldsString = new Dictionary<string, string>[batch.Length];
    var fieldsLong = new Dictionary<string, long>[batch.Length];
    var fieldsDouble = new Dictionary<string, double>[batch.Length];
    var fieldsBoolean = new Dictionary<string, bool>[batch.Length];
    var fieldsFallback = new Dictionary<string, string>[batch.Length];

    // 3. Loop through the batch and "shred" the data into the column arrays
    for (int i = 0; i < batch.Length; i++)
    {
        var entry = batch[i];
        timestamps[i] = entry.Timestamp;
        levels[i] = entry.Level;
        sources[i] = entry.Source;
        templateHashes[i] = entry.TemplateHash;
        messages[i] = entry.Message;

        // --- This is the key logic for distributing the typed fields ---
        var stringMap = new Dictionary<string, string>();
        var longMap = new Dictionary<string, long>();
        var doubleMap = new Dictionary<string, double>();
        var boolMap = new Dictionary<string, bool>();
        // Fallback is handled implicitly by string casting below
        
        foreach (var kvp in entry.Fields)
        {
            switch (kvp.Value)
            {
                case long val: longMap[kvp.Key] = val; break;
                case double val: doubleMap[kvp.Key] = val; break;
                case bool val: boolMap[kvp.Key] = val; break;
                // All other types, including those in the fallback map, are treated as strings
                default: stringMap[kvp.Key] = kvp.Value.ToString(); break;
            }
        }

        fieldsString[i] = stringMap;
        fieldsLong[i] = longMap;
        fieldsDouble[i] = doubleMap;
        fieldsBoolean[i] = boolMap;
        // fields_fallback is populated by the default case above and written to the string map
    }

    // 4. Create Parquet DataColumns from the arrays
    var timestampColumn = new DataColumn(LogEntrySchema.FindField("timestamp") as DataField, timestamps);
    var levelColumn = new DataColumn(LogEntrySchema.FindField("level") as DataField, levels);
    // ... create columns for source, template_hash, message ...

    var fieldsStringColumn = new DataColumn(LogEntrySchema.FindField("fields_string") as DataField, fieldsString);
    var fieldsLongColumn = new DataColumn(LogEntrySchema.FindField("fields_long") as DataField, fieldsLong);
    var fieldsDoubleColumn = new DataColumn(LogEntrySchema.FindField("fields_double") as DataField, fieldsDouble);
    var fieldsBooleanColumn = new DataColumn(LogEntrySchema.FindField("fields_boolean") as DataField, fieldsBoolean);

    // 5. Write to the stream
    using (var parquetWriter = await ParquetWriter.CreateAsync(LogEntrySchema, targetStream))
    {
        using (var rowGroupWriter = parquetWriter.CreateRowGroup())
        {
            await rowGroupWriter.WriteColumnAsync(timestampColumn);
            await rowGroupWriter.WriteColumnAsync(levelColumn);
            // ... write other columns ...
            await rowGroupWriter.WriteColumnAsync(fieldsStringColumn);
            await rowGroupWriter.WriteColumnAsync(fieldsLongColumn);
            await rowGroupWriter.WriteColumnAsync(fieldsDoubleColumn);
            await rowGroupWriter.WriteColumnAsync(fieldsBooleanColumn);
        }
    }
}
```

*Note: The above logic assumes the `Fields` dictionary in `LogEntry` already contains properly typed objects. The logic for handling `fields_fallback` would need a slight refinement: your parser should add a flag or place conflicting fields in a separate dictionary within the `LogEntry` to distinguish them, so they can be written to the correct map.*

### Action Items

1. **Add the `Parquet.Net` NuGet package** to your project.
2. **Create a Proof-of-Concept:** Before integrating this into the main ingester, create a small, separate console application. In this app, define the schema, create a sample batch of `LogEntry` records with complex `Fields` dictionaries, and use the logic above to write them to a file.
3. **Verify the Output:** Use a Parquet file viewer tool (like Dremio, DuckDB CLI, or a Python script with pyarrow) to inspect the output file. Confirm that the schema is correct and that the data, especially in the nested maps, is exactly as you expect. This validation step is crucial to ensure your data is being stored correctly before you build the rest of the system on top of it.

---
# design-12.md
---

Of course. Point 4 is about the crucial "first mile" of ingestion: reliably discovering and monitoring log files. This component, which we can call the `DirectoryMonitor`, is the eyes and ears of the entire system. Its failure or fragility would undermine everything else.

Let's design a robust `DirectoryMonitor` that handles both the initial scan and the ongoing watching, addressing the known limitations of `FileSystemWatcher`.

### The Core Problem: State and Reliability

The `DirectoryMonitor`'s primary job is to produce a reliable stream of "work items" (e.g., "file X has new data") for the Tailing Engine to consume. It must be resilient to event floods, missed events, and complex file rotation scenarios.

The solution is a stateful component that combines a full directory scan with `FileSystemWatcher`, using the SQLite `TrackedFiles` table as its persistent source of truth.

### 1. The Initial Scan on Startup

This is a one-time, comprehensive process that runs when the ingester starts. It synchronizes the state in the SQLite database with the reality on the file system.

**Workflow:**

1. **Query SQLite:** The monitor starts by loading all records from the `TrackedFiles` table into an in-memory `ConcurrentDictionary<string, TrackedFileInfo>`. This dictionary will be the live, in-memory state.
2. **Enumerate Files:** It performs a full, recursive scan of the target directory, getting a list of all files that match the configured include/exclude glob patterns.
3. **Reconcile:** For each file found on the disk, it performs the following logic:
    * **Is the file in our dictionary?**
        * **No (New File):** This file is completely new. The monitor creates a "work item" of type `FileAdded` and sends it to the Tailing Engine. A new record is inserted into the SQLite `TrackedFiles` table with `Position = 0`.
        * **Yes (Existing File):** The file is already being tracked. The monitor compares the file's current metadata (e.g., `LastWriteTimeUtc`, size) with the information stored in the dictionary.
            * If the metadata on disk is newer, it means the file was modified while the ingester was offline. The monitor creates a `FileChanged` work item for the Tailing Engine to process from its last known `Position`.
            * If the metadata is the same, the file is unchanged. Do nothing.
4. **Check for Deleted Files:** After the scan, the monitor iterates through its in-memory dictionary. Any file in the dictionary that was *not* found on the disk during the scan has been deleted while the ingester was offline. These records can be marked as "archived" or removed from the `TrackedFiles` table.

### 2. Ongoing Watching and `FileSystemWatcher` Robustness

After the initial scan is complete, the `FileSystemWatcher` is enabled to handle real-time changes. Here's how we make it robust.

**A. Debouncing Event Storms**

* **Problem:** A single large write operation or a "save-all" in an editor can trigger a flood of `Changed` events for the same file in rapid succession. Acting on every single one is inefficient and can cause race conditions.
* **Solution:** Implement a debouncer. Instead of acting on an event immediately, the monitor adds the file path to a thread-safe queue and starts a short timer (e.g., 250 milliseconds). If another event for the same file arrives, the timer is reset. When the timer finally elapses, a single, consolidated `FileChanged` work item is generated for that file.

**B. Handling `FileSystemWatcher` Errors**

* **Problem:** Under heavy I/O load, the `FileSystemWatcher`'s internal buffer can overflow. When this happens, it stops sending events and raises its `Error` event, telling you that its state is no longer reliable.
* **Solution: Resynchronization.** The handler for the `Error` event must assume the worst: that we have missed an unknown number of changes. The *only* safe way to recover is to:
    1. Temporarily pause the processing of new events.
    2. Trigger a full resynchronization scan, identical to the "Initial Scan on Startup."
    3. Once the scan is complete, re-enable the watcher and resume normal operation. This "brute force" recovery is simple and guarantees correctness.

**C. Robustly Handling Log Rotation (The Hardest Part)**

This is where the `FileSignature` (a hash of the first few KB of a file) stored in the SQLite DB becomes essential.

* **Scenario 1: Simple Rename (`app.log` -> `app.log.1`)**
  * The `Renamed` event is triggered.
  * The monitor sees that `app.log` (which it was tracking) is now `app.log.1`.
  * It tells the Tailing Engine to `Flush` and finalize all processing for `app.log`.
  * It updates the SQLite table, either renaming the entry or marking it as archived.
  * A `Created` event will likely fire for a new `app.log`. This is handled like any other new file.

* **Scenario 2: Truncate and Rewrite (`app.log` is overwritten)**
  * A `Changed` event is triggered. The Tailing Engine checks the file size and sees it's now *smaller* than its last known `Position`. This is an immediate red flag.
  * The monitor then calculates the `FileSignature` of the new, smaller file.
  * It compares this signature to the one stored in the SQLite DB for `app.log`.
    * **If the signatures are different (99% of cases):** This is a completely new file, despite having the same name. The monitor treats this as a **rotation**. It tells the Tailing Engine to `Flush` the old context and then generates a `FileAdded` work item to process the "new" `app.log` from the beginning (Position 0).
    * **If the signatures are the same:** This is a genuine, but rare, file truncation. This should be logged as a severe warning, and the `Position` for the file should be reset to the new, smaller file size.

### The `DirectoryMonitor`'s Output Contract

The `DirectoryMonitor`'s job is to abstract away all this complexity. It should expose a simple, reliable output stream for the Tailing Engine. A `System.Threading.Channels.Channel` is a perfect mechanism for this.

```csharp
// The types of work the monitor can produce
public enum FileWorkType { FileAdded, FileChanged, FileRemovedOrRotated }

// The DTO sent through the channel
public record FileWorkItem(string FilePath, FileWorkType WorkType);

// The monitor class
public class DirectoryMonitor
{
    private readonly Channel<FileWorkItem> _workChannel;

    public ChannelReader<FileWorkItem> WorkChannelReader => _workChannel.Reader;

    // ... internal logic for scanning and watching ...
}
```

The Tailing Engine simply becomes a consumer of this channel. It doesn't need to know *why* a file was added to the channel (initial scan vs. watcher); it just knows it has work to do.

### Action Items

1. **Define the `TrackedFileInfo` schema** in the SQLite database, ensuring it includes `FilePath`, `Position`, `LastWriteTimeUtc`, and `FileSignature`.
2. **Implement the Initial Scan logic.** This is a straightforward, non-event-driven process and is a good place to start.
3. **Implement the `FileSystemWatcher` handlers,** paying special attention to the `Error` event (triggering a rescan) and the `Changed`/`Renamed` events (for rotation detection).
4. **Choose a simple, fast non-cryptographic hashing algorithm** (like xxHash) for the `FileSignature`. You only need to hash the first 4-8KB of a file to get a reliable signature.
5. **Build the debouncing mechanism.** This can be done with a `ConcurrentDictionary` and `Timer` or by using more advanced techniques like Reactive Extensions (Rx.NET).

---
Yes, absolutely. Using the NTFS File ID is a brilliant and powerful technique to make log rotation handling dramatically more reliable on Windows. It's a significant improvement over relying on file signatures and heuristics alone.

This is a perfect example of leveraging a low-level platform feature to solve a high-level application problem robustly.

### What is an NTFS File ID?

On an NTFS filesystem, every file and directory has a unique 64-bit ID (formally a "File Reference Number"). This ID is an entry in the Master File Table (MFT), which is the master index of the entire volume.

The crucial property of this ID is that it **remains constant even if the file is renamed or moved anywhere within the same volume.** The ID only changes if the file is deleted and recreated, or if it's moved to a different volume (which is a copy-and-delete operation).

This gives us a definitive way to track a specific file's content, regardless of its name.

### How to Get the File ID in .NET

This functionality is not exposed in the standard `System.IO` libraries. You must use P/Invoke to call the native Windows API function `GetFileInformationByHandle`.

Here is a helper class that encapsulates this logic:

```csharp
using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

public static class NtfsUtils
{
    // Represents the file's unique ID and the volume it's on.
    public record FileIdentifier(long VolumeSerial, ulong FileId);

    public static FileIdentifier? GetFileIdentifier(string filePath)
    {
        // P/Invoke structures and functions
        [StructLayout(LayoutKind.Sequential)]
        internal struct BY_HANDLE_FILE_INFORMATION
        {
            public uint FileAttributes;
            public System.Runtime.InteropServices.ComTypes.FILETIME CreationTime;
            public System.Runtime.InteropServices.ComTypes.FILETIME LastAccessTime;
            public System.Runtime.InteropServices.ComTypes.FILETIME LastWriteTime;
            public uint VolumeSerialNumber;
            public uint FileSizeHigh;
            public uint FileSizeLow;
            public uint NumberOfLinks;
            public uint FileIndexHigh;
            public uint FileIndexLow;
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool GetFileInformationByHandle(SafeFileHandle hFile, out BY_HANDLE_FILE_INFORMATION lpFileInformation);

        try
        {
            // We must open the file to get a handle to it.
            using var fileHandle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            
            if (GetFileInformationByHandle(fileHandle, out var fileInfo))
            {
                ulong fileId = ((ulong)fileInfo.FileIndexHigh << 32) | fileInfo.FileIndexLow;
                return new FileIdentifier(fileInfo.VolumeSerialNumber, fileId);
            }
        }
        catch (IOException)
        {
            // File might be locked or gone, handle gracefully.
            return null;
        }

        throw new Win32Exception(Marshal.GetLastWin32Error());
    }
}
```

### Integrating File IDs into the `DirectoryMonitor`

This changes our core tracking mechanism from being path-based to ID-based.

**1. Update the SQLite `TrackedFiles` Schema**

The `FilePath` is no longer the primary key. The File ID and Volume Serial are.

```sql
CREATE TABLE TrackedFiles (
    -- The composite primary key that uniquely identifies a file's content
    VolumeSerial INTEGER NOT NULL,
    FileId INTEGER NOT NULL,

    -- The last known path of the file (this can now change)
    FilePath TEXT NOT NULL,

    Position INTEGER NOT NULL DEFAULT 0,
    LastWriteTimeUtc TEXT NOT NULL,
    
    PRIMARY KEY (VolumeSerial, FileId)
);

-- An index on the path is still crucial for fast lookups
CREATE INDEX IX_TrackedFiles_FilePath ON TrackedFiles (FilePath);
```

**2. Revise the `DirectoryMonitor` Logic**

The new logic is simpler and far less ambiguous.

* **On Initial Scan / New File:**
    1. When a new file `app.log` is discovered, get its `FileIdentifier` using our helper.
    2. Insert a new record into `TrackedFiles` with its `VolumeSerial`, `FileId`, `FilePath`, and `Position = 0`.

* **Handling the "Truncate and Rewrite" Scenario (The Killer Feature):**
    1. A `Changed` event arrives for `app.log`.
    2. We get its current `FileIdentifier`.
    3. We query our database for the `FileIdentifier` we have stored for the *path* `app.log`.
    4. **If the IDs are different:** We know with 100% certainty that the original file was deleted and a new one was created with the same name. This is a rotation. We `Flush` the old context, delete the old DB record, and treat this as a completely new file. The `FileSignature` check is no longer needed.
    5. **If the IDs are the same:** It's the same file. We just continue processing from the last known `Position`.

* **Handling the `Renamed` Event (`app.log` -> `app.log.1`)**
    1. A `Renamed` event arrives, giving us `OldFullPath` and `FullPath`.
    2. We look up `OldFullPath` (`app.log`) in our database via the `FilePath` index. We retrieve its `VolumeSerial` and `FileId`.
    3. We simply execute an SQL `UPDATE` statement: `UPDATE TrackedFiles SET FilePath = 'app.log.1' WHERE VolumeSerial = ? AND FileId = ?`.
    4. Done. We are now tracking the exact same file content under its new name. The process is seamless and unambiguous. There is no need to guess or correlate; the filesystem tells us exactly what happened.

### The Downsides and Limitations (Important!)

1. **Platform Specificity:** This is an **NTFS-only** feature. The code will fail on Linux, macOS, or any non-NTFS Windows volume (like FAT32, ReFS). This is the single biggest drawback.
2. **Cross-Volume Moves:** If a log file is moved from `C:\logs` to `D:\logs`, its File ID will change. This is a rare edge case for logs but is a limitation of the mechanism.

### The Recommended Hybrid Approach

To build a truly robust and cross-platform solution, you should implement a hybrid strategy:

* **On startup, detect the operating system.**
* **If on Windows:** Use the NTFS File ID as the primary tracking mechanism. It's superior in every way.
* **If on Linux/macOS/Other:** Fall back to the previously discussed `FileSignature` heuristic. Linux/macOS have a similar concept called an "inode number," and you could use a similar P/Invoke-style mechanism to read it, but a file signature is a perfectly viable cross-platform fallback.

This approach gives you the "best of breed" reliability on the platform that provides it, while still ensuring the application is portable and functional everywhere else. It's more work but results in a far superior product.

## Supported Platforms

LogTapestry will support only Windows and NTFS file system for now. The NTFS File ID is a powerful feature that we will leverage to ensure robust log rotation handling.

---
# design-13.md
---

Excellent. Let's focus on defining a robust and practical strategy for error handling and logging. This is a critical component that turns a prototype into a production-ready, operable piece of software.

The guiding philosophy should be: **Resilience and Clarity.** The ingester must not crash because of a single malformed log line or a transient file access issue. When things do go wrong, the logs it produces should be clear, structured, and actionable for a system operator.

### 1. The Logging Framework: The Foundation

First, we need a standard logging framework. For a modern .NET application, this is a solved problem.

* **Abstraction:** Use the standard `Microsoft.Extensions.Logging` abstractions (`ILogger<T>`, `ILoggerFactory`). This decouples your code from any specific logging implementation.
* **Implementation:** Use **Serilog** as the concrete implementation. It is the de facto standard for serious .NET applications due to its powerful structured logging capabilities and its rich ecosystem of "sinks" (destinations for log events).

**Why Structured Logging is Non-Negotiable:**
Instead of writing text messages like:
`_logger.LogError("Failed to parse line in file " + filePath);`

You write structured events:
`_logger.LogError("Failed to parse line in file {FilePath} on line {LineNumber}", filePath, lineNumber);`

Serilog will capture not just the message but also the key-value pairs (`FilePath`, `LineNumber`). When written as JSON, this allows you to easily filter and query your logs: "Show me all parsing errors for the file `sofia_api.log`."

### 2. Logging Granularity: What to Log and When

We need to define clear log levels for different events to control verbosity and highlight important issues.

| Level | When to Use | Example Events |
| :--- | :--- | :--- |
| **Debug** | Extremely verbose, for deep diagnostics. Disabled in production. | `FileSystemWatcher` event received for file {FilePath}.<br>Batch of {Count} entries flushed to channel.<br>Compactor starting scan of directory {DirectoryPath}. |
| **Information** | Normal, significant application events. This is the default level for production. | Ingester starting up (Version {Version}).<br>Configuration loaded from {ConfigPath}.<br>Now tailing new file {FilePath}.<br>Compaction for {DirectoryPath} completed, compacted {FileCount} files into {NewFileSize}MB. |
| **Warning** | An unexpected event occurred, but the application can continue without issue. An operator should be aware of these. | A single log line could not be parsed. File: {FilePath}, Line: {LineNumber}, Content: {LineContent}.<br>Type conflict for field {FieldName}. Canonical type is {CanonicalType}, received value {Value}. Storing in fallback map.<br>A tracked file was not found on disk during rescan, assuming deleted: {FilePath}. |
| **Error** | A specific operation failed, and data might have been dropped or a component is non-functional. The application as a whole continues. | Failed to read from file {FilePath} due to {Exception}. Will retry.<br>Failed to write Parquet file {ParquetPath} after {RetryCount} retries. Batch of {LogEntryCount} logs will be dropped.<br>SQLite database is locked or corrupt: {Exception}. Tailing state is unreliable. |
| **Critical** | The application cannot continue and must shut down. | Log directory {DirectoryPath} does not exist or is not accessible.<br>Configuration file {ConfigPath} is missing or invalid.<br>Failed to initialize the SQLite state database. |

### 3. Handling Specific Failure Scenarios

Here's how we apply this logging strategy to the failure points we've identified.

**A. File I/O Errors (Reading)**

* **Problem:** A log file is suddenly deleted, or its permissions change, leading to an `IOException`.
* **Handling:**
    1. The `Tailing Engine` must wrap all `File.Open` and `stream.Read` calls in a `try...catch` block.
    2. On catching an `IOException`, it should log an **Error** level event: `_logger.LogError(ex, "Failed to read from file {FilePath}", filePath);`.
    3. It should then signal to the `DirectoryMonitor` that this file is now in an error state.
    4. The `DirectoryMonitor` can then decide to either pause monitoring of this file for a period (e.g., 60 seconds) before retrying, or wait for a rescan to pick it up again.
    5. This isolates the failure to a single file, allowing the rest of the system to proceed.

**B. Parsing Errors (Malformed Log Lines)**

* **Problem:** A log line doesn't match the regexes, a timestamp is in the wrong format, etc.
* **Handling:**
    1. This is where our `ParsingResult` union type shines. The `ILogParser` will produce a `ParsingResult.Failure(...)` object.
    2. The pipeline consumer that receives `ParsingResult` objects will have a simple `switch` or `if/else`.
    3. If `IsSuccess` is true, the `LogEntry` is sent to the data sink.
    4. If `IsSuccess` is false, it logs a **Warning** level event with all the details from the failure object: `_logger.LogWarning("Log parsing failed: {ErrorMessage}. Source: {Source}, Text: {UnparseableText}", result.ErrorMessage, result.Source, result.UnparseableText);`.
    5. The malformed data is then **discarded**. The system moves on to the next line. This prevents a single bad line from stopping ingestion.

**C. Data Sink Errors (Writing)**

* **Problem:** The disk is full, or a bug in the Parquet library causes a write to fail. This is more serious as it can lead to data loss.
* **Handling:**
    1. Wrap the `ParquetWriter.CreateAsync` and `WriteColumnAsync` calls in a `try...catch` block.
    2. On failure, implement a **retry policy with exponential backoff**. Attempt the write again after 1s, then 2s, then 4s, up to a limit (e.g., 3 retries).
    3. If all retries fail, log an **Error** level event: `_logger.LogError(ex, "Failed to write Parquet batch to {FilePath} after {RetryCount} retries. {LogEntryCount} entries will be lost.", filePath, retryCount, batch.Length);`.
    4. At this point, you must decide what to do with the in-memory batch. The simplest strategy is to drop it. A more advanced "dead-letter queue" strategy would write the batch to a simple local text file for later manual recovery, but this adds complexity.

### 4. The Error Log File

* **Configuration:** With Serilog, configuring a separate error log is trivial. You define two "sinks" in your configuration.
  * **Main Sink:** A file sink (e.g., `ingester.log`) that writes `Information` level and above.
  * **Error Sink:** A separate file sink (e.g., `ingester_errors.log`) that is configured to *only* write `Warning` level and above.
* **Format:** The error log should be written in a structured format like **JSON**. This makes it machine-parseable, so you can easily ingest your error logs into another analysis tool or write scripts to count specific error types.

### Action Items

1. **Integrate `Microsoft.Extensions.Logging` and `Serilog`** into your application's entry point (`Program.cs`).
2. **Configure Serilog** to use at least two sinks: a rolling file sink for general operational logs (`Information`+) and a separate rolling file sink for actionable issues (`Warning`+), preferably in JSON format.
3. **Instrument the Code:** Go through the components (`DirectoryMonitor`, `Tailing Engine`, `RegexLogParser`, `DataSink`) and add structured logging statements at the key decision points outlined in the granularity table.
4. **Implement `try...catch` blocks** for all I/O operations (file reading, file writing, SQLite access) and log exceptions at the `Error` level.
5. **Implement the consumer logic** for the `ParsingResult` type, directing failures to the warning log.
6. **Decide on and implement a retry policy** for the `DataSink`.

---
# design-14.md
---

This is an absolutely critical point for any agent-like software that must coexist on a live production system. Your concern is spot on: LogTapestry must be a "good citizen" and its presence should be as close to invisible as possible to the primary application ("Sofia").

Let's break down the potential areas of conflict and design specific, configurable mechanisms to minimize the impact. The goal is to make LogTapestry's resource usage tunable, allowing an operator to dial it down from "high-throughput" to "ultra-discreet" mode.

### 1. File Locking and Concurrency (The Most Critical Point)

This directly addresses your fear of conflicting with Sofia. Old applications often assume they have exclusive access to their files and may even hold long-lived write locks.

**The Problem:** When LogTapestry tries to open a log file for reading, it might fail if Sofia has a restrictive lock on it.

**The Solution: Lenient File Sharing**

When the `Tailing Engine` opens a file to read from it, it must not use a simple `File.OpenRead()`. It must explicitly specify the most permissive `FileShare` mode.

```csharp
// In the Tailing Engine, when opening a file for tailing:
try
{
    // The key is FileShare.ReadWrite.
    // This tells the OS: "I want to read, but I will allow other processes
    // to continue reading AND writing to this file simultaneously."
    // This is the fundamental requirement for any log tailing agent.
    var fileStream = new FileStream(
        filePath,
        FileMode.Open,      // Open the existing file
        FileAccess.Read,    // We only need to read
        FileShare.ReadWrite // <<< THE MOST IMPORTANT SETTING
    );
    // ... proceed with reading from the stream ...
}
catch (IOException ex)
{
    // What if Sofia holds an EXCLUSIVE lock (FileShare.None)?
    // The open will fail. We MUST handle this gracefully.
    _logger.LogWarning(ex, "Could not open file {FilePath}, it may be exclusively locked. Will retry.", filePath);
    // The DirectoryMonitor logic should then schedule a retry after a backoff period.
}
```

This `FileShare.ReadWrite` flag is the single most important piece of code for ensuring peaceful coexistence.

### 2. I/O Throttling (Disk Usage)

The two most I/O-intensive operations will be the initial scan and, more significantly, the **compaction process**. Compaction reads potentially gigabytes of small files and writes them out to a new large file, which can saturate the disk I/O.

**The Problem:** Compaction could starve Sofia of the disk access it needs, causing application-level slowdowns.

**The Solution: Configurable Compaction Scheduling and Throttling**

The standalone `logtapestry-compactor.exe` tool must have configuration options to control its aggressiveness.

1. **Off-Peak Scheduling (Primary Solution):** The simplest and best solution is to only run compaction during known quiet periods. The compactor should be configurable to run via a cron-like schedule.
    * **Configuration:** `compactor.schedule = "0 2 * * *"` (Run at 2:00 AM every day).

2. **I/O Priority (OS-Level Throttling):** On both Windows and Linux, you can run a process with a lower I/O priority. This tells the OS scheduler to only service this process's disk requests after all normal-priority requests (like Sofia's) have been met.
    * **Implementation:** This is often done when launching the process (`ionice` on Linux) or can be set programmatically.

3. **Artificial Delays (Application-Level Throttling):** A cruder, but effective, cross-platform method is to inject small delays into the compaction loop.
    * **Logic:** After reading a small file or writing a chunk to the large file, inject a delay: `await Task.Delay(50);`. This yields the I/O bus to other processes.
    * **Configuration:** `compactor.io_delay_ms = 50`. Setting this to `0` would disable the delay for maximum speed.

### 3. CPU Throttling (Processor Usage)

The most CPU-intensive part of the ingestion pipeline will be the parallel parsing of log lines, especially if the regexes are complex.

**The Problem:** On a busy system, LogTapestry's parsing could consume multiple CPU cores, stealing cycles from Sofia.

**The Solution: Configurable Parallelism**

The TPL Dataflow `TransformBlock` or the Channel-based pipeline gives you a perfect control knob.

* **Logic:** The `MaxDegreeOfParallelism` option controls how many threads are used for the parsing stage.
* **Configuration:** Add a setting in the main configuration file.

    ```json
    "ingester": {
      // ... other settings ...
      "max_parsing_parallelism": 2 // Use a low number for discreet mode
    }
    ```

    In your code:
    `var options = new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = config.MaxParsingParallelism };`
    The default could be `Environment.ProcessorCount`, but a production system operator could turn it down to `1` or `2` to guarantee minimal CPU impact.

### A "Good Citizen" Mode

To make this easy for an operator, you can bundle these settings into a single "mode" configuration.

```json
"ingester": {
  // "HighThroughput" or "GoodCitizen"
  "performance_profile": "GoodCitizen" 
}
```

Your application would then translate this profile into the low-level settings on startup:

* **GoodCitizen:** `MaxDegreeOfParallelism = 1`, set process priority to `BelowNormal`, enable I/O delays in the compactor.
* **HighThroughput:** `MaxDegreeOfParallelism = Environment.ProcessorCount`, use normal process priority, disable I/O delays.

### Summary of Control Knobs for Discreet Operation

| Resource | Control Knob (Configuration Setting) | Impact It Mitigates |
| :--- | :--- | :--- |
| **File Access** | `FileShare.ReadWrite` (Hardcoded) | Prevents `IOException` when reading logs being actively written by another application. |
| **Disk I/O** | `compactor.schedule` | Runs heavy disk activity during off-peak hours. |
| **Disk I/O** | `compactor.io_delay_ms` | Artificially throttles the compactor's read/write speed to yield the disk to other processes. |
| **CPU Usage** | `ingester.max_parsing_parallelism` | Limits the number of CPU cores used for log parsing. |
| **Overall** | `ingester.performance_profile` | A single setting that applies a suite of "discreet" configurations. |
| **Memory** | Bounded Channel/Block Capacity | (Already in design) Prevents unbounded memory growth by creating backpressure. |

By implementing these configurable throttles, you give system administrators the confidence that they can deploy LogTapestry safely, knowing they have full control over its resource footprint.

---
# design-15.md
---

Of course. Let's dive into the specifics of the concurrency model. This is where we define the application's "nervous system"—how data flows between components, how we manage backpressure, and how the application starts and stops cleanly.

We've chosen `System.Threading.Channels` as our core primitive, which is an excellent choice for its performance and clarity. We'll design a multi-producer, single-consumer pipeline.

### The High-Level Pipeline Flow

Our concurrent system will consist of several distinct parts working together:

1. **The `DirectoryMonitor` (1 Task):** Watches the filesystem and produces `FileWorkItem`s (File Added/Changed/Removed). It's the "scout."
2. **The `TailingManager` (1 Task):** Consumes `FileWorkItem`s. Its job is to start new "Tailing Tasks" for new files and signal existing tasks to stop when files are removed. It's the "foreman."
3. **Tailing Tasks (Many Producers):** Each active log file gets its own dedicated async task. Each task reads lines from its file, feeds them to its dedicated `ILogParser` instance, and writes the resulting `ParsingResult` objects into a *single, central channel*.
4. **The Processing Pipeline (The "Factory"):** A chain of consumers that reads from the central channel to parse, batch, and write the data.

```
                  +-------------------+
                  | DirectoryMonitor  |
                  +-------------------+
                          | (FileWorkItem)
                          v
                  +-------------------+
                  |  TailingManager   |
                  +-------------------+
                          | (Spawns/Stops Tasks)
+--------------------------------------------------------------------------+
|                        MANY PRODUCER TASKS                               |
|                                                                          |
|  +--------------+    +--------------+          +--------------+          |
|  | TailerTask 1 |    | TailerTask 2 |   ...    | TailerTask N |          |
|  | (file_a.log) |    | (file_b.log) |          | (file_n.log) |          |
|  +--------------+    +--------------+          +--------------+          |
|         |                 |                           |                  |
|         +-----------------+---------------------------+                  |
|                           | (ParsingResult)                              |
|                           v                                              |
+--------------------------------------------------------------------------+
                  +--------------------------------+
                  | Central Channel<ParsingResult> |  <-- BACKPRESSURE POINT
                  +--------------------------------+
                          | (ParsingResult)
                          v
+--------------------------------------------------------------------------+
|                     SINGLE CONSUMER PIPELINE                             |
|                                                                          |
|    Filter Success -> Transform to LogEntry -> Batch -> Write to Parquet  |
|                                                                          |
+--------------------------------------------------------------------------+
```

### 1. Backpressure Points and Channel Configuration

The most important backpressure point is the central channel that sits between the many producer tasks and the single consumer pipeline.

* **What it does:** This channel acts as a shared, thread-safe buffer. If the consumer pipeline (which involves slow disk I/O) cannot keep up with the producers (which are doing fast in-memory reading), this channel will fill up.
* **The Backpressure Mechanism:** When a producer task calls `await channel.Writer.WriteAsync(result)`, that call will asynchronously wait (without blocking a thread) if the channel is full. This automatically and gracefully throttles all the fast-moving file readers, preventing unbounded memory consumption.
* **Configuration:** The channel's capacity is a critical tuning parameter that should be in your configuration file.

    ```json
    "ingester": {
      "pipeline_buffer_capacity": 10000 
    }
    ```

  * **A small capacity (e.g., 1,000)** minimizes memory usage but means the readers will be paused more often. This is ideal for a "good citizen" mode.
  * **A large capacity (e.g., 100,000)** uses more memory but can absorb large bursts of log activity, providing smoother throughput.

### 2. Completion Propagation (Graceful Shutdown)

This is the "happy path" shutdown, ensuring no data is lost when the application is told to stop (e.g., `SIGTERM` from a service manager).

**The Challenge:** The consumer pipeline will only terminate after the channel is marked as "complete." We can only mark the channel as complete after *all* producer tasks have finished.

**The Logic:**

1. A shutdown is requested (e.g., `CancellationToken` is cancelled, see next section).
2. The `TailingManager` is signaled to stop. It will not start any *new* tailing tasks.
3. The `TailingManager` signals all of its active, running `TailingTask`s to stop.
4. Each `TailingTask` finishes its current loop, calls `parser.Flush()` to get any buffered data, sends that final result to the channel, and then exits.
5. We need a mechanism to track when all producers have exited. A `CountdownEvent` or a simple `Interlocked` counter is perfect for this. Let's use a counter. The `TailingManager` maintains a count of active tailers. Each time a tailer exits, it decrements the counter.
6. When the counter reaches zero, the `TailingManager` (or the main application thread) calls `channel.Writer.Complete()`.
7. The consumer pipeline, which is `await`ing the channel, will then process the remaining items in the buffer and exit cleanly.
8. The main application thread `await`s the completion of the consumer pipeline task before finally exiting.

### 3. Cancellation (Abrupt Shutdown)

This handles the "interrupted path" shutdown, like a user pressing Ctrl+C. The priority is to exit quickly, accepting that some in-memory data might be lost.

**The Mechanism:** The standard .NET `CancellationTokenSource` and `CancellationToken`.

1. **Creation:** A single `CancellationTokenSource` is created in your `Program.cs`. It is wired up to listen for console exit signals (`Console.CancelKeyPress`).
2. **Propagation:** The `CancellationToken` from this source is passed down to **every single asynchronous method** in the application:
    * The `DirectoryMonitor`'s main loop.
    * The `TailingManager`'s main loop.
    * All `channel.Reader.WaitToReadAsync(cancellationToken)` and `channel.Writer.WriteAsync(cancellationToken)` calls.
    * The `Open.ChannelExtensions` methods (`.Transform`, `.Batch`, etc.) all have overloads that accept a `CancellationToken`.
3. **The Effect:** When Ctrl+C is pressed, the token is cancelled. This causes an `OperationCanceledException` to be thrown inside any async method currently `await`ing something with that token. This exception rapidly unwinds the call stack of all concurrent tasks, causing the application to terminate very quickly.

### Conceptual Code Skeleton

```csharp
public class Program
{
    public static async Task Main(string[] args)
    {
        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (s, e) =>
        {
            Console.WriteLine("Shutdown requested...");
            cts.Cancel();
            e.Cancel = true; // Prevent the process from terminating immediately
        };

        // 1. Create the central, bounded channel
        var options = new BoundedChannelOptions(10000) { FullMode = BoundedChannelFullMode.Wait };
        var centralChannel = Channel.CreateBounded<ParsingResult>(options);

        // 2. Set up the consumer pipeline (using Open.ChannelExtensions)
        Task consumerPipelineTask = centralChannel.Reader
            .Where(r => r.IsSuccess, cts.Token) // Filter out failed parses
            .Transform(r => r.Entry!, cts.Token) // Extract the successful LogEntry
            .Batch(1000, cts.Token) // Batch into groups of 1000
            .ForAllAsync(batch => WriteBatchToParquetAsync(batch, cts.Token), cts.Token);

        // 3. Start the producers
        var tailingManager = new TailingManager(centralChannel.Writer);
        Task producerManagerTask = tailingManager.RunAsync(cts.Token);

        // 4. Wait for shutdown signal and orchestrate graceful completion
        try
        {
            // Wait for either the producers or consumers to finish (or be cancelled)
            await Task.WhenAny(producerManagerTask, consumerPipelineTask);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Cancellation token triggered. Shutting down abruptly.");
        }
        finally
        {
            // This block ensures graceful shutdown is attempted even on cancellation
            Console.WriteLine("Waiting for producer manager to complete...");
            // The TailingManager's RunAsync should handle the countdown and call Complete()
            await producerManagerTask; 
            
            Console.WriteLine("Producers finished. Waiting for consumer pipeline to drain...");
            await consumerPipelineTask;
        }

        Console.WriteLine("LogTapestry has shut down cleanly.");
    }

    static Task WriteBatchToParquetAsync(LogEntry[] batch, CancellationToken token)
    {
        // Your DataSink logic goes here...
        return Task.CompletedTask;
    }
}
```

---
# design-16.md
---

Yes. We have designed a very solid and robust "engine," but a complete project also needs a "chassis" and a "dashboard." Now is the perfect time to consider the operational and usability aspects that wrap around the core engine.

Here are the most important remaining points that will bridge the gap between a library of code and a finished, deployable product.

### 1. Operability and Production Readiness

How will a system administrator run, monitor, and trust this application?

*   **Deployment & Packaging:** How will the application be deployed? The best approach would be to publish it as a **single, self-contained executable** for each target platform (Windows x64, Linux x64). This requires no .NET runtime installation on the target machine. You should plan for it to run as a background service:
    *   On Windows: It should be compatible with running as a **Windows Service**.
    *   On Linux: It should be packaged with a **`systemd` unit file**.
    *   **Docker:** A `Dockerfile` should be created to containerize the entire application, which is the standard for modern deployments.

*   **Health Checks:** How do you know the ingester is *actually working* and not just silently stuck? A simple HTTP endpoint exposed by the ingester process is the standard solution.
    *   Implement a minimal web API (using `WebApplication.CreateSlimBuilder()` in modern .NET) that exposes a `/health` endpoint.
    *   A `GET` request to `/health` should perform checks like:
        1.  Can it connect to the SQLite database?
        2.  Is the log directory accessible?
        3.  What is the timestamp of the last successfully committed log entry? (A crucial liveness check).
    *   This allows external monitoring tools (like Nagios, Zabbix, Prometheus) to automatically verify the application's health.

*   **Metrics and Observability:** Logs tell you what went wrong, but metrics tell you what's going on. Using the modern `System.Diagnostics.Metrics` API in .NET, you should expose key performance indicators:
    *   **Counters:** `log_entries_ingested_total`, `bytes_processed_total`, `parsing_errors_total`.
    *   **Gauges:** `files_currently_tailed_count`, `pipeline_buffer_size`.
    *   **Histograms:** `batch_processing_duration_seconds`, `compaction_duration_seconds`.
    *   These can be exposed via a `/metrics` endpoint in the Prometheus exposition format, which has become the industry standard.

### 2. Testing Strategy

A complex, concurrent system like this *must* have a robust testing strategy beyond simple unit tests.

*   **Unit Tests:** These are for pure, isolated logic. The `RegexLogParser` is a perfect candidate. You can feed it arrays of strings (single-line, multi-line, malformed) and assert that its output is correct without touching the file system.
*   **Integration Tests:** These test how components work *together*. The most critical integration test would be:
    1.  Programmatically start the `DirectoryMonitor`.
    2.  Create a temporary directory on disk.
    3.  Write, append to, and rename fake log files in that directory.
    4.  Assert that the `DirectoryMonitor` produces the correct `FileWorkItem`s on its output channel in the correct sequence.
*   **End-to-End (E2E) Tests:** This is the ultimate confidence builder. An E2E test script would:
    1.  Start the *entire* LogTapestry application as a separate process.
    2.  Write a set of known log files to its monitored directory.
    3.  Wait a few seconds.
    4.  Shut down the application.
    5.  Use the **`logtapestry-query.exe` tool** to query the generated Parquet files and SQLite database.
    6.  Assert that the query results exactly match the data that was written to the fake log files.

### 3. User Experience (UX) and Safety

How do you make the system easy and safe for a human to configure and use?

*   **Configuration Validation ("Dry Run" Mode):** A typo in a regex or a file path can cause silent failures. You should add a validation mode to the main executable.
    *   `logtapestry.exe --config myconfig.json --validate`
    *   This mode would load and parse the configuration, compile all the regexes, check that the log directory exists, and report any errors to the user **without starting any of the long-running processes.** This provides instant feedback and prevents misconfigurations in production.

*   **Query Tool Ergonomics:** The query tool is the primary interface to the data. Consider features beyond just executing a SQL string:
    *   **Output Formats:** Allow the user to specify the output format: `--output=table` (default), `--output=json`, `--output=csv`.
    *   **Interactive Mode (REPL):** Running `logtapestry-query.exe` with no query could launch an interactive shell where the user can run multiple queries in a session without the startup cost each time.

### 4. Long-Term Maintenance: Schema Evolution

This is a forward-looking "v2" or "v3" problem, but it's worth acknowledging now.

*   **The Problem:** What happens in a year when an operator wants to change the type of a field that is already registered in the `FieldSchema` table? For example, `http_status` was initially a `string` (because of values like "OK"), but now they want to ensure it's a `long`.
*   **The Challenge:** You cannot change the type in historical Parquet files. This means your schema is effectively immutable for data that has already been written.
*   **The Future Solution:** A real-world schema evolution solution would involve versioning the schema. The query engine would need to be aware that `http_status` was a string in files from 2025 but a long in files from 2026, and it would need to handle this (e.g., by trying to cast the old string values to long at query time).

This is a very complex problem. For v1, the simplest approach is to document that **field types, once established, cannot be changed.** If a change is required, the user must choose a new field name (e.g., `http_status_code`). Acknowledging this limitation from the start is crucial.

By thinking through these four areas, you will be well-prepared for the practical challenges of building, deploying, and maintaining LogTapestry as a complete and reliable system.

---
# design-17.md
---

Thank you for asking. I believe the design we have outlined is **more than sufficient to begin implementation.** We have covered the vast majority of technical and operational challenges, moving from high-level architecture to specific implementation details for the most critical components. Further high-level design would likely yield diminishing returns until you have a working prototype to validate the current assumptions.

However, if I were to expand on one final point, it would be to consolidate several of our discussions into a single, tangible **"First Vertical Slice" or Proof of Concept (PoC).**

This isn't about adding new features, but about defining the most valuable and high-risk piece of the system to build *first* to prove the core architecture is sound.

### The Most Valuable Proof of Concept: The End-to-End Data Flow

The goal of this PoC is to validate the entire lifecycle of a single log entry, from disk to query result. It would touch almost every critical component we've designed, but in a simplified, "happy path" form.

**What this PoC would implement:**

1.  **A Hardcoded "Tailing Task":**
    *   Forget the `DirectoryMonitor` for now. The PoC would simply be pointed at a single, known text file (`test.log`).
    *   It would read the file line-by-line, keeping track of its position in a simple variable (ignoring the SQLite DB for now).

2.  **The `RegexLogParser`:**
    *   Implement the stateful `ILogParser` we designed, but configured with a simple, hardcoded regex for a specific log format. It should handle a basic multi-line entry.

3.  **The Channel:**
    *   The tailing task would push `ParsingResult` objects into a `System.Threading.Channels.Channel`.

4.  **A Simplified "Data Sink" Consumer:**
    *   A single consumer task would read from the channel.
    *   It would use `Parquet.Net` to write batches of entries to a *single* Parquet file.
    *   Crucially, it **must implement the multi-map schema** (`fields_string`, `fields_long`, etc.) as this is a core architectural assumption.

5.  **A Hardcoded "Schema Registry":**
    *   Instead of a SQLite table, the PoC could use a simple `Dictionary<string, string>` in memory to store the type for a couple of known fields (e.g., `{"user_id", "long"}`).

6.  **A Rudimentary "Query Tool":**
    *   This would be a separate console project.
    *   It would use the in-memory schema registry to rewrite a single, hardcoded user query (e.g., `SELECT * WHERE user_id > 100`) into the physical query (`SELECT * WHERE fields_long['user_id'] > 100`).
    *   It would use `DuckDB.NET` to execute this rewritten query against the Parquet file generated by the data sink and print the results.

**Why This PoC is So Valuable:**

*   **De-risks Technology Choices:** It forces you to get your hands dirty with the key third-party libraries (`Parquet.Net`, `DuckDB.NET`) and immediately confirms they can do what you need them to do, especially regarding the complex typed-map schema.
*   **Validates Core Architecture:** It proves that the "shredding" of data into typed columns and the "rewriting" of queries at the query layer actually works in practice. This is the most complex part of the entire design.
*   **Builds a Skeleton:** The successful PoC provides a working, end-to-end skeleton of the application. The remaining work then becomes "fleshing out" this skeleton: replacing the hardcoded file path with the `DirectoryMonitor`, replacing the variable position with the SQLite state, building the full configuration system, etc. This is a much more manageable and less risky way to build out the full application.

In summary, the design is complete enough. The most valuable next step is not to design more, but to **validate the existing design** by building this focused proof of concept. It will give you immense confidence in the architecture and a solid foundation for the rest of the implementation.
