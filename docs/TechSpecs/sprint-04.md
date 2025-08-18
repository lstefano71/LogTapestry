Of course. Here is the full technical specification for Sprint 4. This sprint focuses on the final pieces of the puzzle: optimizing the long-term data store and polishing the primary user-facing tool into a finished product.

---

## **LogTapestry: Technical Specification - Sprint 4**

**Version:** 1.0
**Date:** August 18, 2025
**Author:** Implementation Team Lead

### **1. Sprint Goal: Compaction and Query Tool Finalization**

The primary objective of this sprint is to build the standalone compaction utility to ensure long-term query performance and to finalize the query tool, transforming it from a PoC into an ergonomic and fully-featured command-line interface for the end-user.

### **2. Scope and Objectives**

* **Build the Standalone Compactor:** Create the `LogTapestry.Compactor.exe` executable with the logic to find, merge, and replace small Parquet files.
* **Finalize Query Tool CLI:** Implement a full-featured CLI for `LogTapestry.Query.exe` using `System.CommandLine`, including all options and output formats.
* **Implement Query Rewriting:** Build the `QueryRewriter` class that uses the SQLite schema to translate a user's logical query into a physical query for DuckDB.
* **Implement Output Formatting:** Create formatters for table, JSON, and CSV output.
* **End-to-End Testing:** Create an end-to-end test that simulates the entire data lifecycle: ingestion, compaction, and querying.

### **3. Detailed Component Specifications**

#### **3.1. Compactor (`LogTapestry.Compactor.exe`)**

* **Project:** `LogTapestry.Compactor` (New Console Application)
* **CLI Specification:**
  * **Usage:** `LogTapestry.Compactor.exe --data <path_to_data_dir> [--compact-older-than <timespan>]`
  * **`--data` (required):** The root directory of the Parquet data store.
  * **`--compact-older-than` (optional):** A `TimeSpan` string (e.g., "1h", "2d"). The compactor will not touch any partitions newer than this duration to avoid race conditions with a live ingester. Defaults to "1h".
* **Core Logic Class:** `CompactionTask`
    1. **`RunAsync()`:** The main orchestration method.
    2. **`FindCompactionCandidates()`:**
        * Recursively scans the `--data` directory for partition directories (e.g., `/day=18/`).
        * A directory is a candidate if it:
            1. Is older than the `--compact-older-than` threshold.
            2. Contains a `landing/` subdirectory.
            3. Does **not** contain a `_COMPACTION_COMPLETE` marker file.
    3. **`ExecuteCompaction(string partitionPath)`:**
        * **Read:** Uses **DuckDB** to read all Parquet files from the `landing/` subdirectory into memory. The query will be simple: `SELECT * FROM read_parquet('{partitionPath}/landing/*.parquet')`. DuckDB is used here because of its high performance in reading multiple Parquet files.
        * **Rewrite:** Once the data is loaded into a DuckDB result set, it will be iterated and "shredded" back into `DataColumn` arrays compatible with `Parquet.Net`. This is necessary to control the output file size.
        * **Write:** Writes the data to one or more new, large Parquet files in the parent (optimized) directory. The target size for each new file will be a constant (e.g., 128MB).
        * **Atomic Swap:**
            1. The new files are written with a `.tmp` extension.
            2. After all new files are successfully written, the entire `landing/` directory is deleted.
            3. The `.tmp` files are renamed to their final `.parquet` names.
            4. An empty `_COMPACTION_COMPLETE` file is created in the partition directory.
        * **Error Handling:** All steps are wrapped in `try/catch` blocks. If any step fails, the temporary files are cleaned up, and the `landing/` directory is left untouched, ensuring the process is idempotent and can be safely retried.

#### **3.2. Query Tool (`LogTapestry.Query.exe`)**

This sprint involves a major refactoring of the Sprint 1 PoC into a polished tool.

* **CLI Refactoring:** The `System.CommandLine` library will be fully implemented.
  * **Root Command:** `logtapestry-query`
  * **Required Options:** `--database-path`, `--data-path`.
  * **Optional Options:** `--output <table|json|csv>` (default: `table`).
  * **Required Argument:** `query` (The SQL string).
* **Class: `QueryRewriter`**
  * **Constructor:** `public QueryRewriter(IStateProvider stateProvider)`
  * **Key Method:** `public async Task<string> RewriteQueryAsync(string userQuery)`
  * **Logic:**
        1. **Parse SQL:** Uses a simple regex or a lightweight parser to find all identifiers (potential field names) in the `SELECT`, `WHERE`, and `ORDER BY` clauses of the `userQuery`.
        2. **Schema Lookup:** For each identifier, it calls `_stateProvider.GetFieldTypeAsync(identifier)`.
        3. **Rewrite:** It reconstructs the query. For any clause involving a dynamic field, it will generate the appropriate `EXISTS (SELECT 1 FROM UNNEST(t.fields) ...)` subquery, targeting the correct sub-field (`.value.long_value`, `.value.string_value`, etc.) based on the schema lookup.
        4. **FROM Clause Injection:** It replaces the logical table name (e.g., `FROM logs`) with the physical DuckDB path: `FROM read_parquet('{data_path}/**/*.parquet', hive_partitioning = true)`.
        5. **Return:** Returns the fully rewritten, physical SQL string.
* **Class: `OutputFormatter` (and subclasses)**
  * **Interface:** `IOutputFormatter`
    * `void WriteHeader(IEnumerable<string> columnNames);`
    * `void WriteRow(object[] values);`
    * `void WriteFooter();`
  * **`TableOutputFormatter`:** Uses `Spectre.Console` to render a rich table.
  * **`JsonOutputFormatter`:** Writes each row as a JSON object within a JSON array.
  * **`CsvOutputFormatter`:** Writes a header and comma-separated values for each row.
* **`Program.cs` Orchestration:**
    1. Parse command-line arguments using `System.CommandLine`.
    2. Instantiate `SqliteStateProvider`.
    3. Instantiate `QueryRewriter`.
    4. Call `RewriteQueryAsync` to get the physical query.
    5. Open a `DuckDBConnection`.
    6. Execute the physical query using `connection.ExecuteReader()`.
    7. Instantiate the correct `IOutputFormatter` based on the `--output` flag.
    8. Pass the column names from the reader to `formatter.WriteHeader()`.
    9. Iterate through the result set, passing each row's values to `formatter.WriteRow()`.
    10. Call `formatter.WriteFooter()`.

### **4. Testing Strategy for Sprint 4**

* **Integration Tests (`LogTapestry.Integration.Tests`):**
  * **Test Case:** `Compactor_Should_Merge_Landing_Files_And_Create_Marker`
    * **Setup:** Programmatically create a partitioned directory structure with several small Parquet files in a `landing/` subdirectory.
    * **Act:** Run the `CompactionTask.RunAsync()` method.
    * **Assert:**
            1. Assert that the `landing/` directory no longer exists.
            2. Assert that one or more large Parquet files now exist in the parent directory.
            3. Assert that the `_COMPACTION_COMPLETE` marker file has been created.
            4. Use DuckDB to query both the "before" and "after" states and assert that the total row count is identical.
  * **Test Case:** `QueryRewriter_Should_Correctly_Map_Typed_Fields`
    * **Setup:** Use a mock `IStateProvider` that returns predefined types for "user_id" (`long`) and "session_id" (`string`).
    * **Act:** Pass a query like `SELECT user_id FROM logs WHERE session_id = 'abc'` to the `QueryRewriter`.
    * **Assert:** Assert that the output string is `SELECT fields_long['user_id'] FROM ... WHERE fields_string['session_id'] = 'abc'`.

* **End-to-End Test (New Test Project or Script):**
    1. **Start Ingester:** Run `LogTapestry.Ingester.exe` as a background process, configured to watch a temporary directory.
    2. **Generate Logs:** Write a set of test log files into the temporary directory.
    3. **Wait and Stop:** Wait for a period (e.g., 10 seconds) to allow ingestion to complete, then gracefully stop the ingester process.
    4. **Run Compactor:** Run `LogTapestry.Compactor.exe` on the output data directory.
    5. **Run Query:** Run `LogTapestry.Query.exe` with a specific query that filters the test data.
    6. **Assert:** Capture the query tool's output and assert that it exactly matches the expected results from the generated test logs.

### **5. Definition of Done**

The sprint is complete when:

1. The `LogTapestry.Compactor.exe` can be run and successfully compacts a directory of test data, removing the `landing` zone and creating the `_COMPACTION_COMPLETE` marker.
2. The `LogTapestry.Query.exe` CLI is fully implemented with all specified arguments and options.
3. The `QueryRewriter` correctly translates logical queries with multiple typed fields into physical queries.
4. The query tool can successfully output results in `table`, `json`, and `csv` formats.
5. All new integration tests for the compactor and query rewriter pass successfully.
6. The full end-to-end test, covering ingestion, compaction, and querying, passes successfully.
7. The project's README file is updated with usage instructions for both the compactor and the query tool.
