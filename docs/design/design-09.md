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
