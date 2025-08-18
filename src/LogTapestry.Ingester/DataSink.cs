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

      // Write top-level columns
      await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[0], batch.Select(e => e.Timestamp).ToArray()));
      await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[1], batch.Select(e => e.Level).ToArray()));
      await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[2], batch.Select(e => e.Message).ToArray()));
      await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[3], batch.Select(e => e.Source).ToArray()));
      await groupWriter.WriteColumnAsync(new DataColumn(Schema.DataFields[4], batch.Select(e => e.TemplateHash).ToArray()));

      // --- Start of Corrected Section ---

      // 1. Shred dynamic fields
      var fieldsList = batch.Select(entry =>
          entry.Fields.Select(kv => {
            var key = kv.Key;
            var value = kv.Value;
            return value switch {
              long l => new FieldElement(key, new FieldValue(LongValue: l)),
              double d => new FieldElement(key, new FieldValue(DoubleValue: d)),
              bool b => new FieldElement(key, new FieldValue(BoolValue: b)),
              _ => new FieldElement(key, new FieldValue(StringValue: value?.ToString()))
            };
          }).ToList()
      ).ToList();

      // 2. Initialize lists for levels and non-null values
      var repLevels = new List<int>();
      var keyDefLevels = new List<int>();
      var stringDefLevels = new List<int>();
      var longDefLevels = new List<int>();
      var doubleDefLevels = new List<int>();
      var boolDefLevels = new List<int>();

      var keys = new List<string>();
      var stringVals = new List<string>();
      var longVals = new List<long>();
      var doubleVals = new List<double>();
      var boolVals = new List<bool>();

      // 3. Iterate and build all lists according to the 3-LEVEL LIST specification
      foreach (var list in fieldsList) {
        if (list.Count == 0) {
          repLevels.Add(0);
          keyDefLevels.Add(1);
          stringDefLevels.Add(1);
          longDefLevels.Add(1);
          doubleDefLevels.Add(1);
          boolDefLevels.Add(1);
        } else {
          for (int j = 0; j < list.Count; j++) {
            var element = list[j];
            repLevels.Add(j == 0 ? 0 : 1);

            // --- THE CRITICAL CHANGE IS HERE ---
            // For the 'Key' field, the max DL must be 4, just like the optional fields.
            keyDefLevels.Add(4);
            keys.Add(element.Key);

            // For optional fields, the max DL is 4.
            if (element.Value.StringValue != null) {
              stringDefLevels.Add(4);
              stringVals.Add(element.Value.StringValue);
            } else { stringDefLevels.Add(3); }

            if (element.Value.LongValue.HasValue) {
              longDefLevels.Add(4);
              longVals.Add(element.Value.LongValue.Value);
            } else { longDefLevels.Add(3); }

            if (element.Value.DoubleValue.HasValue) {
              doubleDefLevels.Add(4);
              doubleVals.Add(element.Value.DoubleValue.Value);
            } else { doubleDefLevels.Add(3); }

            if (element.Value.BoolValue.HasValue) {
              boolDefLevels.Add(4);
              boolVals.Add(element.Value.BoolValue.Value);
            } else { boolDefLevels.Add(3); }
          }
        }
      }

      // 4. Get DataFields
      var listField = (ListField)Schema.Fields[5];
      var structField = (StructField)listField.Item;
      var keyField = (DataField)structField.Fields[0];
      var stringField = (DataField)structField.Fields[1];
      var longField = (DataField)structField.Fields[2];
      var doubleField = (DataField)structField.Fields[3];
      var boolField = (DataField)structField.Fields[4];

      // 5. Write columns
      await groupWriter.WriteColumnAsync(new DataColumn(keyField, keys.ToArray(), keyDefLevels.ToArray(), repLevels.ToArray()));
      await groupWriter.WriteColumnAsync(new DataColumn(stringField, stringVals.ToArray(), stringDefLevels.ToArray(), repLevels.ToArray()));
      await groupWriter.WriteColumnAsync(new DataColumn(longField, longVals.ToArray(), longDefLevels.ToArray(), repLevels.ToArray()));
      await groupWriter.WriteColumnAsync(new DataColumn(doubleField, doubleVals.ToArray(), doubleDefLevels.ToArray(), repLevels.ToArray()));
      await groupWriter.WriteColumnAsync(new DataColumn(boolField, boolVals.ToArray(), boolDefLevels.ToArray(), repLevels.ToArray()));
    }
  }
}
