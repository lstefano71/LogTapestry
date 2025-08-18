using DuckDB.NET.Data;
using Spectre.Console;
using System;
using System.Collections.Generic;

namespace LogTapestry.Query
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 2 || args[0] != "--data")
            {
                Console.WriteLine("Usage: LogTapestry.Query.exe --data <path_to_data_dir> \"SQL_QUERY\"");
                return;
            }

            var dataPath = args[1];
            var query = args[2];

            // Simplified query rewriting for the PoC
            var rewrittenQuery = query.Replace("FROM logs", $"FROM read_parquet('{dataPath.Replace("\\", "/")}/output.parquet')");

            using (var duckDBConnection = new DuckDBConnection("Data Source=:memory:"))
            {
                duckDBConnection.Open();

                using (var command = duckDBConnection.CreateCommand())
                {
                    command.CommandText = rewrittenQuery;
                    using (var reader = command.ExecuteReader())
                    {
                        var table = new Table();

                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            table.AddColumn(reader.GetName(i));
                        }

                        while (reader.Read())
                        {
                            var row = new List<string>();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                row.Add(reader.GetValue(i)?.ToString() ?? "NULL");
                            }
                            table.AddRow(row.ToArray());
                        }

                        AnsiConsole.Write(table);
                    }
                }
            }
        }
    }
}