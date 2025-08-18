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