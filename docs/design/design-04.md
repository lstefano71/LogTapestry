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
