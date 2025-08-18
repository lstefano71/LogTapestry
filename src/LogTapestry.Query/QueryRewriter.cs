// LogTapestry.Query/QueryRewriter.cs

using System.Text.RegularExpressions;

namespace LogTapestry.Query
{
  /// <summary>
  /// Rewrites a user's logical SQL query into a physical SQL query that can be executed by DuckDB
  /// against our nested Parquet schema. This version is built to match the exact schema
  /// produced by the DataSink's shredding logic.
  /// </summary>
  public class QueryRewriter
  {
    // For the Sprint 1 PoC, we hardcode the schema.
    // In the full product, this will come from the SqliteStateProvider.
    private readonly Dictionary<string, string> _dynamicFieldSchema = new()
    {
            // Field Name -> Canonical Type (in lowercase)
            { "user_id", "long" },
            { "session_id", "string" },
            { "response_time_ms", "double" }
        };

    public string Rewrite(string userQuery, string parquetSource)
    {
      // Step 1: Replace the logical table name 'logs' with the physical Parquet source
      // and give it a mandatory alias 't'.
      string rewrittenQuery = Regex.Replace(userQuery, @"\bFROM\s+logs\b",
          $"FROM {parquetSource} AS t", RegexOptions.IgnoreCase);

      // Step 2: Build a regex pattern that finds any of our known dynamic fields.
      var fieldNamesPattern = string.Join("|", _dynamicFieldSchema.Keys);
      var regex = new Regex($@"\b({fieldNamesPattern})\b");

      // Step 3: Use a MatchEvaluator to replace each found field with a subquery.
      rewrittenQuery = regex.Replace(rewrittenQuery, match => {
        string fieldName = match.Value;
        string fieldType = _dynamicFieldSchema[fieldName];

        // ============================================================================== //
        // ==                            THE FIX IS HERE                               == //
        // ============================================================================== //

        // 1. Convert the canonical type (e.g., "long") to the physical PascalCase
        //    column name from the schema (e.g., "LongValue").
        string physicalColumnName = char.ToUpper(fieldType[0]) + fieldType.Substring(1) + "Value";

        // 2. Generate the subquery using the correct UNNEST syntax and direct access
        //    to the flattened, PascalCase fields within the 'element' struct.
        return $"(SELECT element.{physicalColumnName} FROM UNNEST(t.Fields) AS tbl(element) WHERE element.Key = '{fieldName}' LIMIT 1)";
      });

      return rewrittenQuery;
    }
  }
}
