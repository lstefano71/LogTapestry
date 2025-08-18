Excellent. Let's move on to the critical choice of the Parquet writing library. This decision directly impacts the complexity of your "Data Sink" component and the performance of the entire system.

The core challenge is to take a batch of in-memory C# `LogEntry` records and efficiently "shred" them into the columnar Parquet format, paying close attention to our specialized schema with multiple typed map columns.

### Choosing the Right Library

For a modern .NET project, you have two primary, high-quality choices.

1. **Parquet.Net:** A mature, high-performance, fully-managed library written entirely in C#. It has zero native dependencies, which is a significant advantage for simple deployment and cross-platform compatibility (e.g., running on both Windows x64 and Linux ARM64 without issue).
2. **Apache Arrow (`Apache.Arrow` package):** This library provides C# bindings for the official Apache Arrow C++ implementation. It is part of a massive, industry-standard data processing ecosystem. It can offer unparalleled performance but comes with the added complexity of managing native dependencies.

**Recommendation:**

For this project, **start with `Parquet.Net`**.

**Reasoning:**

* **Simplicity and Portability:** The lack of native dependencies makes development, debugging, and deployment vastly simpler. You can just add the NuGet package and it works everywhere.
* **Sufficient Performance:** It is highly optimized and is more than capable of handling the write throughput required for a logging system. The bottleneck in your system will almost certainly be I/O or Regex parsing, not the Parquet serialization itself.
* **Idiomatic .NET API:** Its API is designed from the ground up for .NET developers and feels natural to use with standard C# objects and collections.

You should only consider switching to Apache Arrow if rigorous benchmarking on your production hardware reveals that Parquet serialization is, against all expectations, your primary performance bottleneck.

### Implementation with Parquet.Net

Let's design the `DataSink` component that writes a batch of `LogEntry` records to a new Parquet file.

**Step 1: Define the Parquet Schema**

`Parquet.Net` allows you to define the schema programmatically. This object can be created once and reused for all writes.

```csharp
using Parquet.Schema;

// This can be a static field in your DataSink class
private static readonly ParquetSchema LogEntrySchema = new(
    // Primitives
    new DataField<DateTime>("timestamp"),
    new DataField<string>("level"),
    new DataField<string>("source"),
    new DataField<long>("template_hash"),
    new DataField<string>("message"),

    // The typed maps for our dynamic fields
    new MapField("fields_string", new DataField<string>("key"), new DataField<string>("value")),
    new MapField("fields_long", new DataField<string>("key"), new DataField<long>("value")),
    new MapField("fields_double", new DataField<string>("key"), new DataField<double>("value")),
    new MapField("fields_boolean", new DataField<string>("key"), new DataField<bool>("value")),
    
    // The fallback map for type conflicts
    new MapField("fields_fallback", new DataField<string>("key"), new DataField<string>("value"))
);
```

**Step 2: The Data "Shredding" Logic**

This is the most important part. We need to take a `LogEntry[]` and transform it into a set of parallel arrays, one for each column in our schema.

```csharp
using Parquet;
using Parquet.Data;

public async Task WriteBatchAsync(LogEntry[] batch, Stream targetStream)
{
    if (batch.Length == 0) return;

    // 1. Initialize arrays for each top-level column
    var timestamps = new DateTime[batch.Length];
    var levels = new string[batch.Length];
    var sources = new string[batch.Length];
    var templateHashes = new long[batch.Length];
    var messages = new string[batch.Length];

    // 2. Initialize lists for the complex map columns
    var fieldsString = new Dictionary<string, string>[batch.Length];
    var fieldsLong = new Dictionary<string, long>[batch.Length];
    var fieldsDouble = new Dictionary<string, double>[batch.Length];
    var fieldsBoolean = new Dictionary<string, bool>[batch.Length];
    var fieldsFallback = new Dictionary<string, string>[batch.Length];

    // 3. Loop through the batch and "shred" the data into the column arrays
    for (int i = 0; i < batch.Length; i++)
    {
        var entry = batch[i];
        timestamps[i] = entry.Timestamp;
        levels[i] = entry.Level;
        sources[i] = entry.Source;
        templateHashes[i] = entry.TemplateHash;
        messages[i] = entry.Message;

        // --- This is the key logic for distributing the typed fields ---
        var stringMap = new Dictionary<string, string>();
        var longMap = new Dictionary<string, long>();
        var doubleMap = new Dictionary<string, double>();
        var boolMap = new Dictionary<string, bool>();
        // Fallback is handled implicitly by string casting below
        
        foreach (var kvp in entry.Fields)
        {
            switch (kvp.Value)
            {
                case long val: longMap[kvp.Key] = val; break;
                case double val: doubleMap[kvp.Key] = val; break;
                case bool val: boolMap[kvp.Key] = val; break;
                // All other types, including those in the fallback map, are treated as strings
                default: stringMap[kvp.Key] = kvp.Value.ToString(); break;
            }
        }

        fieldsString[i] = stringMap;
        fieldsLong[i] = longMap;
        fieldsDouble[i] = doubleMap;
        fieldsBoolean[i] = boolMap;
        // fields_fallback is populated by the default case above and written to the string map
    }

    // 4. Create Parquet DataColumns from the arrays
    var timestampColumn = new DataColumn(LogEntrySchema.FindField("timestamp") as DataField, timestamps);
    var levelColumn = new DataColumn(LogEntrySchema.FindField("level") as DataField, levels);
    // ... create columns for source, template_hash, message ...

    var fieldsStringColumn = new DataColumn(LogEntrySchema.FindField("fields_string") as DataField, fieldsString);
    var fieldsLongColumn = new DataColumn(LogEntrySchema.FindField("fields_long") as DataField, fieldsLong);
    var fieldsDoubleColumn = new DataColumn(LogEntrySchema.FindField("fields_double") as DataField, fieldsDouble);
    var fieldsBooleanColumn = new DataColumn(LogEntrySchema.FindField("fields_boolean") as DataField, fieldsBoolean);

    // 5. Write to the stream
    using (var parquetWriter = await ParquetWriter.CreateAsync(LogEntrySchema, targetStream))
    {
        using (var rowGroupWriter = parquetWriter.CreateRowGroup())
        {
            await rowGroupWriter.WriteColumnAsync(timestampColumn);
            await rowGroupWriter.WriteColumnAsync(levelColumn);
            // ... write other columns ...
            await rowGroupWriter.WriteColumnAsync(fieldsStringColumn);
            await rowGroupWriter.WriteColumnAsync(fieldsLongColumn);
            await rowGroupWriter.WriteColumnAsync(fieldsDoubleColumn);
            await rowGroupWriter.WriteColumnAsync(fieldsBooleanColumn);
        }
    }
}
```

*Note: The above logic assumes the `Fields` dictionary in `LogEntry` already contains properly typed objects. The logic for handling `fields_fallback` would need a slight refinement: your parser should add a flag or place conflicting fields in a separate dictionary within the `LogEntry` to distinguish them, so they can be written to the correct map.*

### Action Items

1. **Add the `Parquet.Net` NuGet package** to your project.
2. **Create a Proof-of-Concept:** Before integrating this into the main ingester, create a small, separate console application. In this app, define the schema, create a sample batch of `LogEntry` records with complex `Fields` dictionaries, and use the logic above to write them to a file.
3. **Verify the Output:** Use a Parquet file viewer tool (like Dremio, DuckDB CLI, or a Python script with pyarrow) to inspect the output file. Confirm that the schema is correct and that the data, especially in the nested maps, is exactly as you expect. This validation step is crucial to ensure your data is being stored correctly before you build the rest of the system on top of it.
