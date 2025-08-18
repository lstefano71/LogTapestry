
using LogTapestry.Core;

using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace LogTapestry.Ingester
{
  public class DataSink
  {
    // Helper record for a field value (union type)
    public record FieldValue(string? StringValue = null, long? LongValue = null, double? DoubleValue = null, bool? BoolValue = null);

    // Helper record for a field element
    public record FieldElement(string Key, FieldValue Value);

    // Parquet schema: List of Structs for dynamic fields
    private static readonly ParquetSchema Schema = new ParquetSchema(
        new DataField<DateTime>("Timestamp"),
        new DataField<string>("Level"),
        new DataField<string>("Message"),
        new DataField<string>("Source"),
        new DataField<long>("TemplateHash"),
        new ListField("Fields",
            new StructField("FieldElement",
                new DataField<string>("Key"),
                new DataField<string>("StringValue", true),
                new DataField<long?>("LongValue", true),
                new DataField<double?>("DoubleValue", true),
                new DataField<bool?>("BoolValue", true)
            )
        )
    );

    public async Task WriteBatchAsync(LogEntry[] batch, Stream targetStream)
    {
      using var parquetWriter = await ParquetWriter.CreateAsync(Schema, targetStream);
      using var groupWriter = parquetWriter.CreateRowGroup();

      await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[0], batch.Select(e => e.Timestamp).ToArray()));
      await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[1], batch.Select(e => e.Level).ToArray()));
      await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[2], batch.Select(e => e.Message).ToArray()));
      await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[3], batch.Select(e => e.Source).ToArray()));
      await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[4], batch.Select(e => e.TemplateHash).ToArray()));

      // Shred dynamic fields into List<FieldElement>
      var fieldsList = batch.Select(entry =>
          entry.Fields.Select(kv => {
            var key = kv.Key;
            var value = kv.Value;
            if (value is long l)
              return new FieldElement(key, new FieldValue(LongValue: l));
            if (value is double d)
              return new FieldElement(key, new FieldValue(DoubleValue: d));
            if (value is bool b)
              return new FieldElement(key, new FieldValue(BoolValue: b));
            // Default to string
            return new FieldElement(key, new FieldValue(StringValue: value?.ToString()));
          }).ToList()
      ).ToList();

      // Flatten for columnar writing
      var allElements = fieldsList.SelectMany(x => x);
      var keys = allElements.Select(e => e.Key);
      var stringVals = allElements.Select(e => e.Value.StringValue ?? string.Empty);
      // Create arrays of non-nullable values
      var longVals = allElements.Where(e => e.Value.LongValue.HasValue).Select(e => e.Value.LongValue.Value);
      var doubleVals = allElements.Where(e => e.Value.DoubleValue.HasValue).Select(e => e.Value.DoubleValue.Value);
      var boolVals = allElements.Where(e => e.Value.BoolValue.HasValue).Select(e => e.Value.BoolValue.Value);

      // Build definition/repetition levels for Parquet list-of-struct
      var defLevels = new List<int>();
      var repLevels = new List<int>();

      // These definition levels are for the nullability of the values themselves
      var longDefLevels = new List<int>();
      var doubleDefLevels = new List<int>();
      var boolDefLevels = new List<int>();

      for (int i = 0; i < fieldsList.Count; i++) {
        var list = fieldsList[i];
        if (list.Count == 0) {
          // If a log entry has no fields, we should represent this with a lower definition level.
          // This part can be enhanced depending on how empty lists should be represented.
          // For simplicity, this example assumes lists are non-empty if present.
        }
        for (int j = 0; j < list.Count; j++) {
          var element = list[j];
          // Definition level '1' here means the struct 'FieldElement' itself is present.
          defLevels.Add(1);

          // Repetition level '0' for the start of a new list, '1' for subsequent items.
          repLevels.Add(j == 0 ? 0 : 1);

          // Now, build the definition levels for each nullable field within the struct.
          // Level 2 means the value is present and not null.
          // Level 1 would mean the value is null.
          longDefLevels.Add(element.Value.LongValue.HasValue ? 2 : 1);
          doubleDefLevels.Add(element.Value.DoubleValue.HasValue ? 2 : 1);
          boolDefLevels.Add(element.Value.BoolValue.HasValue ? 2 : 1);
        }
      }

      // Get DataFields for struct members
      var listField = (ListField)Schema.Fields[5];
      var structField = (StructField)listField.Item;
      var keyField = (DataField)structField.Fields[0];
      var stringField = (DataField)structField.Fields[1];
      var longField = (DataField)structField.Fields[2];
      var doubleField = (DataField)structField.Fields[3];
      var boolField = (DataField)structField.Fields[4];

      // Write nested columns
      await groupWriter.WriteColumnAsync(new DataColumn(keyField, keys.ToArray(), [.. defLevels], [.. repLevels]));
      await groupWriter.WriteColumnAsync(new DataColumn(stringField, stringVals.ToArray(), [.. defLevels], [.. repLevels]));
      await groupWriter.WriteColumnAsync(new DataColumn(longField, longVals.ToArray(), [.. defLevels], [.. repLevels]));
      await groupWriter.WriteColumnAsync(new DataColumn(doubleField, doubleVals.ToArray(), [.. defLevels], [.. repLevels]));
      await groupWriter.WriteColumnAsync(new DataColumn(boolField, boolVals.ToArray(), [.. defLevels], [.. repLevels]));
    }
  }
}
