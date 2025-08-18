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
