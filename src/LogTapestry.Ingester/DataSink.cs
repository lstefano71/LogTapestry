using LogTapestry.Core;
using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace LogTapestry.Ingester
{
    public class DataSink
    {
        public async Task WriteBatchAsync(LogEntry[] batch, Stream targetStream)
        {
            var fields = batch.Select(e => e.Fields).ToArray();

            var schema = new Parquet.Schema(
                new DataField("Timestamp", typeof(DateTime)),
                new DataField("Level", typeof(string)),
                new DataField("Message", typeof(string)),
                new DataField("Source", typeof(string)),
                new DataField("TemplateHash", typeof(long)),
                new MapField("Fields", new DataField("Key", typeof(string)), new DataField("Value", typeof(string)))
            );

            using (var parquetWriter = await ParquetWriter.CreateAsync(schema, targetStream))
            {
                using (var groupWriter = parquetWriter.CreateRowGroup())
                {
                    await groupWriter.WriteColumnAsync(new DataColumn((DataField)schema.Fields[0], batch.Select(e => e.Timestamp).ToArray()));
                    await groupWriter.WriteColumnAsync(new DataColumn((DataField)schema.Fields[1], batch.Select(e => e.Level).ToArray()));
                    await groupWriter.WriteColumnAsync(new DataColumn((DataField)schema.Fields[2], batch.Select(e => e.Message).ToArray()));
                    await groupWriter.WriteColumnAsync(new DataColumn((DataField)schema.Fields[3], batch.Select(e => e.Source).ToArray()));
                    await groupWriter.WriteColumnAsync(new DataColumn((DataField)schema.Fields[4], batch.Select(e => e.TemplateHash).ToArray()));

                    var mapField = (MapField)schema.Fields[5];
                    var keyList = new List<string>();
                    var valueList = new List<string?>();

                    foreach (var dict in fields)
                    {
                        if(dict == null) continue;
                        foreach (var kvp in dict)
                        {
                            keyList.Add(kvp.Key);
                            valueList.Add(kvp.Value?.ToString());
                        }
                    }

                    await groupWriter.WriteColumnAsync(new DataColumn(mapField.Key, keyList.ToArray()));
                    await groupWriter.WriteColumnAsync(new DataColumn(mapField.Value, valueList.ToArray()));
                }
            }
        }
    }
}