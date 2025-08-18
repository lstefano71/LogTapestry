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
