using LogTapestry.Core;

namespace LogTapestry.Query
{
  public class QueryRewriter
  {
    private readonly IStateProvider _stateProvider;

    public QueryRewriter(IStateProvider stateProvider)
    {
      _stateProvider = stateProvider;
    }

    public async Task<string> RewriteQueryAsync(string userQuery)
    {
      // Parse identifiers in SELECT, WHERE, ORDER BY
      var identifierRegex = new System.Text.RegularExpressions.Regex(@"\b([a-zA-Z_][a-zA-Z0-9_]*)\b");
      var identifiers = identifierRegex.Matches(userQuery)
        .Select(m => m.Groups[1].Value)
        .Distinct()
        .Where(id => id != "SELECT" && id != "FROM" && id != "WHERE" && id != "ORDER" && id != "BY" && id != "AND" && id != "OR" && id != "NOT" && id != "IN" && id != "AS" && id != "ON" && id != "GROUP" && id != "BY" && id != "LIMIT" && id != "OFFSET")
        .ToList();

      var typeMap = new Dictionary<string, string>();
      foreach (var id in identifiers) {
        var type = await _stateProvider.GetFieldTypeAsync(id);
        if (type != null)
          typeMap[id] = type;
      }

      // Rewrite identifiers to DuckDB field access
      string rewritten = userQuery;
      foreach (var kvp in typeMap) {
        if (kvp.Value == "long")
          rewritten = rewritten.Replace(kvp.Key, $"fields_long['{kvp.Key}']");
        else if (kvp.Value == "string")
          rewritten = rewritten.Replace(kvp.Key, $"fields_string['{kvp.Key}']");
        // Add more types as needed
      }

      // Replace FROM clause with DuckDB physical path
      rewritten = System.Text.RegularExpressions.Regex.Replace(
        rewritten,
        @"FROM\s+logs",
        "FROM read_parquet('{data_path}/**/*.parquet', hive_partitioning = true)",
        System.Text.RegularExpressions.RegexOptions.IgnoreCase
      );

      return rewritten;
    }

    // Temporary for legacy usage
    public string Rewrite(string userQuery, string fromClause)
    {
      // Placeholder: Replace FROM clause
      return userQuery.Replace("FROM logs", $"FROM {fromClause}");
    }
  }
}
