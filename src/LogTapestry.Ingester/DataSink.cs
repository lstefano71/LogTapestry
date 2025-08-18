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

    // Helper to compute repetition levels for a list of lists
    private static int[] ComputeRepLevels(List<List<FieldElement>> fieldsList)
    {
      var repLevels = new List<int>();
      foreach (var list in fieldsList) {
        for (int j = 0; j < list.Count; j++) {
          repLevels.Add(j == 0 ? 0 : 1);
        }
      }
      return repLevels.ToArray();
    }

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
            return value switch {
              long l => new FieldElement(key, new FieldValue(LongValue: l)),
              double d => new FieldElement(key, new FieldValue(DoubleValue: d)),
              bool b => new FieldElement(key, new FieldValue(BoolValue: b)),
              _ => new FieldElement(key, new FieldValue(StringValue: value?.ToString()))
            };
          }).ToList()
      ).ToList();

      // Flatten for columnar writing
      var allElements = fieldsList.SelectMany(x => x);
      var keys = allElements.Select(e => e.Key);
      var stringVals = allElements.Where(e => e.Value.StringValue != null).Select(e => e.Value.StringValue);
      var longVals = allElements.Where(e => e.Value.LongValue.HasValue).Select(e => e.Value.LongValue.Value);
      var doubleVals = allElements.Where(e => e.Value.DoubleValue.HasValue).Select(e => e.Value.DoubleValue.Value);
      var boolVals = allElements.Where(e => e.Value.BoolValue.HasValue).Select(e => e.Value.BoolValue.Value);

      // Build definition/repetition levels for Parquet list-of-struct
      var structDefLevels = new List<int>();
      var stringDefLevels = new List<int>();
      var longDefLevels = new List<int>();
      var doubleDefLevels = new List<int>();
      var boolDefLevels = new List<int>();

      foreach (var list in fieldsList) {
        for (int j = 0; j < list.Count; j++) {
          var element = list[j];
          structDefLevels.Add(1);
          stringDefLevels.Add(element.Value.StringValue != null ? 2 : 1);
          longDefLevels.Add(element.Value.LongValue.HasValue ? 2 : 1);
          doubleDefLevels.Add(element.Value.DoubleValue.HasValue ? 2 : 1);
          boolDefLevels.Add(element.Value.BoolValue.HasValue ? 2 : 1);
        }
      }

      var structRepLevels = ComputeRepLevels(fieldsList);

      // Get DataFields for struct members
      var listField = (ListField)Schema.Fields[5];
      var structField = (StructField)listField.Item;
      var keyField = (DataField)structField.Fields[0];
      var stringField = (DataField)structField.Fields[1];
      var longField = (DataField)structField.Fields[2];
      var doubleField = (DataField)structField.Fields[3];
      var boolField = (DataField)structField.Fields[4];

      // Write nested columns with correct defLevels
      await groupWriter.WriteColumnAsync(new DataColumn(keyField, keys.ToArray(), [.. structDefLevels], structRepLevels));
      await groupWriter.WriteColumnAsync(new DataColumn(stringField, stringVals.ToArray(), [.. stringDefLevels], structRepLevels));
      await groupWriter.WriteColumnAsync(new DataColumn(longField, longVals.ToArray(), [.. longDefLevels], structRepLevels));
      await groupWriter.WriteColumnAsync(new DataColumn(doubleField, doubleVals.ToArray(), [.. doubleDefLevels], structRepLevels));
      await groupWriter.WriteColumnAsync(new DataColumn(boolField, boolVals.ToArray(), [.. boolDefLevels], structRepLevels));
    }
  }
}
