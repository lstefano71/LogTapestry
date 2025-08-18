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
