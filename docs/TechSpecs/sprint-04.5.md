# **LogTapestry: Technical Specification - Sprint 4.5**

**Version:** 1.0
**Date:** August 18, 2025
**Author:** Implementation Team Lead
**Status:** Approved for Implementation

## Ths issues

### **Executive Summary: Critical Scaling Issues**

The current design, while robust for a smaller number of files, will face three critical, likely show-stopping issues when scaled to monitor 15,000+ files. These must be addressed.

1. **File Handle Exhaustion:** The "one task per file" model in `TailingManager.cs` will attempt to open a `FileStream` for every active log file. A standard Windows process has a default limit of 10,000 file handles, which this design will exceed, causing `IOException`s and failures to monitor new files.
2. **Thread Pool Starvation & Scheduler Contention:** Spawning 15,000+ long-running `Task.Run` operations will overwhelm the .NET ThreadPool scheduler. This will lead to significant context-switching overhead, unpredictable delays in processing, and could negatively impact the performance of the host system.
3. **SQLite Write Contention:** Each of the 15,000 tailing tasks independently updates its position in the `state.sqlite` database. Since SQLite allows only one writer at a time, this will create a massive contention point, with thousands of tasks constantly waiting for a lock on the database, effectively serializing their progress.

The following analysis breaks down these and other issues in detail.

---

### **Category 1: Ingestion and File Monitoring (The "15,000 Files" Problem)**

This is the area most sensitive to the number of files.

#### **Problem: `FileSystemWatcher` Event Handling is a Performance Bottleneck**

* **Analysis:** The implementation in `DirectoryMonitor.cs` has `OnDeleted` and `OnRenamed` events triggering a full `InitialScanAsync`. The architectural documents (`design-12.md`) describe a much more sophisticated approach using NTFS File IDs to handle renames without a full scan. The current implementation deviates from this and is a significant performance risk. A full scan of 150,000 files in the root directory could take minutes, during which the ingester is effectively blind to real-time changes. Similarly, the `OnError` handler's only recourse is a full rescan, which is brutal if the event buffer overflows frequently.
* **Suspicion:** The system will appear to freeze or lag for extended periods under moderate file churn (e.g., during application deployments or log rotations).
* **Recommendation:**
    1. **Refactor `OnRenamed`:** The handler should use the NTFS File ID to perform a simple `UPDATE` on the `TrackedFiles` table with the new path, as designed in the architecture. It should *not* trigger a full rescan.
    2. **Make Buffer Size Configurable:** The `FileSystemWatcher.InternalBufferSize` is hardcoded. This should be exposed in `appsettings.json` so operators can tune it for the specific I/O patterns of the host, making buffer overflows less likely.

#### **Problem: Initial Scan is Single-Threaded and Slow**

* **Analysis:** The `InitialScanAsync` method performs a recursive file enumeration using a single thread. For 150,000 files, the combination of I/O and P/Invoke calls to `GetFileIdentifier` will make the startup process very slow.
* **Suspicion:** The ingester service could take 5-10 minutes or more to become "live" and start processing real-time events after a restart.
* **Recommendation:** Parallelize the initial scan. The monitor should enumerate the top-level directories and create a work queue. A pool of tasks can then process these subdirectories in parallel to significantly speed up the reconciliation process.

### **Category 2: Concurrency and Resource Management**

This category addresses the critical issues outlined in the summary.

#### **Problem: File Handle and Thread Exhaustion (CRITICAL)**

* **Analysis:** The `TailingManager` class implements a "one task per file" model. If 15,000 log files are active, it will spawn 15,000 tasks, each of which opens and holds a `FileStream`. This will breach the default per-process handle limit in Windows and severely degrade system performance.
* **Suspicion:** The service will start throwing `IOException`s ("The operating system handle limit for this process has been exceeded.") and will fail to monitor new files once the limit is reached. Overall system responsiveness will suffer.
* **Recommendation:** This requires a fundamental architectural change from the current implementation.
  * **Implement a Worker Pool Model:** Instead of one task per file, create a fixed-size pool of worker tasks (e.g., `Environment.ProcessorCount * 2`).
  * The `DirectoryMonitor`'s output should be work items like `ReadData(FileIdentifier, last_position)`.
  * A worker task would pick up a work item, **open the file**, read new data, push it to the central channel, update the state, and **close the file**.
  * This ensures that only a small, fixed number of file handles are open at any given time, completely solving the handle exhaustion and thread pool saturation problems.

#### **Problem: Massive SQLite Write Contention (CRITICAL)**

* **Analysis:** The `TailingTask` in `TailingManager.cs` calls `_stateProvider.UpdateTrackedFileAsync` after every read that produces data. With 15,000 tasks attempting to do this concurrently, they will spend most of their time waiting for the single writer lock on the SQLite database.
* **Suspicion:** The actual log processing throughput will be a fraction of what it could be, as tasks are blocked on database I/O, not file I/O. CPU usage will be low, but progress will be slow.
* **Recommendation:**
    1. **Batch State Updates:** Do not update the database after every read.
    2. Create a dedicated, concurrent queue or channel for position updates (`ConcurrentQueue<TrackedFileInfo>`).
    3. Each worker task adds its progress to this queue.
    4. A single, dedicated "State Writer" task runs on a timer (e.g., every 5 seconds), dequeues all pending updates, and writes them to SQLite in a **single transaction**. This is orders of magnitude more efficient than thousands of individual transactions.

### **Category 3: Data Storage and Compaction**

#### **Problem: Compactor Memory Usage and Inefficiency**

* **Analysis:** The `CompactionTask.cs` reads all Parquet files from a landing partition into an in-memory DuckDB table. If the ingester was offline and a large volume of logs were written, the landing zone could contain gigabytes of data. Loading this all into memory risks an `OutOfMemoryException`. Furthermore, it then reads the data *out* of DuckDB and uses `Parquet.Net` to write it back.
* **Suspicion:** The compactor will crash on large partitions or will be unnecessarily slow and resource-intensive.
* **Recommendation:**
    1. **Stream Data:** The compactor should not read the entire partition at once. It should iterate through the landing files one by one, streaming their contents to the new, large Parquet file.
    2. **Use DuckDB for Writing:** Instead of using `Parquet.Net` to write the compacted file, leverage DuckDB's highly optimized Parquet writer. The entire compaction operation for a partition could be a single SQL command:

        ```sql
        COPY (SELECT * FROM read_parquet('path/to/landing/*.parquet'))
        TO 'path/to/optimized/compacted-file.parquet' (FORMAT 'PARQUET', CODEC 'ZSTD');
        ```

        This will be significantly faster and more memory-efficient.

### **Category 4: Querying and Usability**

#### **Problem: Query Rewriter is Brittle**

* **Analysis:** The `QueryRewriter.cs` uses a series of regular expressions to find and replace identifiers in the user's SQL query. This approach is fragile and will easily break with more complex queries (e.g., nested expressions, aliases, comments, CTEs). For example, a query like `WHERE user_id = (SELECT max(user_id) FROM logs)` would likely fail to be rewritten correctly.
* **Suspicion:** Users will report that seemingly valid queries fail for obscure reasons.
* **Recommendation:** For v1.0, this may be acceptable if the scope of queries is limited and documented. For a more robust future version, the rewriter should use a proper SQL parser library (like ANTLR with a SQL grammar) to build an Abstract Syntax Tree (AST). Rewriting would then involve manipulating the tree, which is a far more reliable method.

### **Final Conclusion**

The project has a very strong and well-documented architectural design. The sprint-based implementation has successfully laid down the core components. However, **a critical gap exists between the current implementation and the requirements of the target scale (15,000+ files).**

The immediate priority must be to **refactor the ingestion concurrency model** to address the file handle, thread, and SQLite contention issues. Adopting a worker pool for file reading and batching state updates will ensure the system remains stable and performant under the expected production load.

## The Sprint

### **1. Sprint Goal: Architect for Scale and Robustness**

The primary objective of this sprint is to **refactor the core ingestion pipeline from a "one-task-per-file" model to a scalable, declarative, worker-based architecture.** This sprint directly addresses the identified bottlenecks related to file handles, thread count, and database contention, ensuring the system can reliably monitor 15,000+ files. We will also implement a priority-aware file discovery mechanism to eliminate data latency during application startup.

### **2. Scope and Objectives**

* **Implement a Declarative Pipeline:** Replace the manual concurrency management in the `IngesterService` with a top-to-bottom declarative pipeline using `System.Threading.Channels` and `Open.ChannelExtensions`.
* **Decouple File Reading:** Replace the `TailingManager` and its persistent tasks with a fixed-size pool of `FileReader` workers managed by the declarative pipeline.
* **Eliminate Database Contention:** Implement a dedicated `StateWriterService` that performs batched, transactional updates to the SQLite database.
* **Implement Priority-Aware File Discovery:** Re-architect the `DirectoryMonitor` to use a priority multiplexer, ensuring real-time file events are processed immediately, even during a slow initial scan.
* **Refactor for Performance:** Parallelize the initial directory scan to significantly reduce application startup time.

### **3. Architectural Changes**

This sprint represents a fundamental refactoring of the ingestion engine.

| Old Component / Model (Pre-4.5) | New Component / Model (Sprint 4.5) | Rationale |
| :--- | :--- | :--- |
| `TailingManager` ("one-task-per-file") | **Declarative Worker Pool** (via `.Transform`) | Solves file handle and thread exhaustion. |
| Individual SQLite updates from each task | **`StateWriterService`** (batched, transactional) | Solves SQLite write contention. |
| Monolithic `DirectoryMonitor` | **Priority Monitor Pipeline** (fast/slow channels) | Soloves startup latency and race conditions. |
| Manual, timer-based consumer loop | **Fully Declarative Consumer Pipeline** | Increases readability, robustness, and performance. |

### **4. Detailed Component Specifications**

#### **4.1. The New `DirectoryMonitor` Pipeline**

The `DirectoryMonitor` will be refactored into a self-contained pipeline that produces a clean, deduplicated stream of work.

* **Producers:**
    1. **`InitialScanProducer`:** A single-run task that performs a *parallelized* scan of the log directory. For each file found, it writes a `FileEvent` to a "slow" channel.
    2. **`WatcherProducer`:** A long-running task that manages the `FileSystemWatcher`. On a file system event, it immediately writes a `FileEvent` to a "fast" channel.
* **The Merger (`PriorityMonitor` class):**
  * **Inputs:** Reads from both the "slow" and "fast" channels.
  * **Output:** Writes `FileCheckRequest` objects to the main `workChannel`.
  * **Logic:**
        1. The merger will always prioritize reading from the "fast" channel.
        2. It will maintain a `ConcurrentDictionary<ulong, DateTime>` to track recently forwarded `FileId`s.
        3. When an event arrives (from either channel), it checks the dictionary. If the `FileId` was forwarded recently (e.g., within the last minute), the event is discarded as a duplicate.
        4. Otherwise, the `FileId` is added to the dictionary, and a `FileCheckRequest` is created and sent to the main `workChannel`. This elegantly solves the race condition between the initial scan and the real-time watcher.

#### **4.2. The Main `IngesterService` Pipeline**

The `IngesterService` will be gutted and replaced with the orchestration of three top-level pipelines.

1. **Main Data Pipeline:** This is the core data flow.

    ```csharp
    // C# Implementation Sketch
    Task mainPipelineTask = workChannel.Reader
        // STAGE 1: READ & PARSE (The Worker Pool)
        .Transform(
            request => ReadAndParseFileAsync(request, stateUpdateChannel.Writer),
            maxParallelism: _settings.FileReaderThreadPoolSize // Configurable, e.g., 8
        )
        // STAGE 2: FLATTEN RESULTS
        .SelectMany(results => results)
        // STAGES 3-5: FILTER, BATCH, WRITE
        .Where(result => result.IsSuccess)
        .Select(result => result.Entry!)
        .Batch(_settings.BatchSize, TimeSpan.FromSeconds(_settings.BatchTimeout))
        .ForAllAsync(batch => _dataSink.WriteBatchAsync(batch.ToArray()));
    ```

2. **State Update Pipeline:** A simple, powerful side-effect pipeline.

    ```csharp
    // C# Implementation Sketch
    Task stateWriterTask = stateUpdateChannel.Reader
        .Batch(1000, TimeSpan.FromSeconds(2)) // Batch up to 1000 updates or every 2s
        .ForAllAsync(updates => _stateProvider.UpdateTrackedFilesBatchAsync(updates.ToArray()));
    ```

3. **Directory Monitor Pipeline:** The service will be responsible for creating and starting the monitor's producers and merger.

#### **4.3. The `FileReader` Logic (New Method)**

This is no longer a class but a transient function that encapsulates a single unit of work.

* **Signature:** `private async Task<IEnumerable<ParsingResult>> ReadAndParseFileAsync(FileCheckRequest request, ChannelWriter<PositionUpdate> stateWriter)`
* **Responsibilities:**
    1. Get the last known position from the `IStateProvider`.
    2. **Open** the file stream inside a `using` block.
    3. Read all new lines from the file.
    4. **Close** the file stream, immediately releasing the handle.
    5. Pass the lines to a new `RegexLogParser` instance.
    6. Enqueue a `PositionUpdate` record into the `stateWriter` channel. This is a non-blocking, "fire-and-forget" operation.
    7. Return the collection of `ParsingResult` objects.

#### **4.4. The `StateWriterService` (New `IHostedService`)**

* **Class:** `StateWriterService : IHostedService`
* **Constructor:** Injects the `stateUpdateChannel`'s reader and the `IStateProvider`.
* **`StartAsync`:** Kicks off the state update pipeline defined above.
* **`IStateProvider` Refactoring:**
  * The interface will be updated with a new method: `Task UpdateTrackedFilesBatchAsync(PositionUpdate[] updates)`.
  * The `SqliteStateProvider` implementation will process this batch within a single transaction for maximum efficiency.

### **5. Testing Strategy for Sprint 4.5**

* **Integration Test:** `PriorityMonitor_Should_Prioritize_Fast_Channel_And_Deduplicate`
  * **Setup:** Create a mock `PriorityMonitor` with real channels.
  * **Act:**
        1. Write a `FileEvent` for `fileId=123` to the *slow* channel.
        2. Immediately write a `FileEvent` for `fileId=123` to the *fast* channel.
        3. Let the merger run.
  * **Assert:**
        1. Assert that the main `workChannel` receives exactly **one** `FileCheckRequest` for `fileId=123`.
        2. Assert that the event was consumed from both the slow and fast channels.

* **Performance Test:** `Ingester_Should_Maintain_Low_File_Handle_Count_Under_Load`
  * **Setup:** Create a test directory with 20,000 dummy log files.
  * **Act:** Start the full `IngesterService`.
  * **Assert:** Use `System.Diagnostics.Process` to monitor the current process's handle count. Assert that the handle count remains low and stable (e.g., under 200), and does not grow linearly with the number of files.

### **6. Definition of Done**

The sprint is complete when:

1. The `TailingManager` class and its "one-task-per-file" logic have been completely removed from the codebase.
2. The `IngesterService` has been refactored to orchestrate the new declarative pipelines.
3. The `FileReader` logic (open, read, close) is implemented and managed by the `.Transform()` stage of the pipeline.
4. The `StateWriterService` is implemented and correctly performs batched, transactional updates to the SQLite database.
5. The `DirectoryMonitor` is refactored into a priority-aware pipeline that demonstrably processes real-time events with low latency during the initial scan.
6. All new and existing integration tests pass, especially the new tests for the priority merger and file handle count.
7. The application successfully ingests data from a large number of files (15,000+) in a test environment without crashing or exhibiting resource exhaustion.
