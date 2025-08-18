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
