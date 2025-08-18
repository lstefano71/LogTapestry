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
