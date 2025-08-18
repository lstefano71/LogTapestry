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
      // Step 1: Identify fields and their types
      var identifierRegex = new System.Text.RegularExpressions.Regex(@"\b([a-zA-Z_][a-zA-Z0-9_]*)\b");
      var identifiers = identifierRegex.Matches(userQuery)
        .Select(m => m.Groups[1].Value)
        .Distinct()
        .Where(id => id != "SELECT" && id != "FROM" && id != "WHERE" && id != "ORDER" && id != "BY" && id != "AND" && id != "OR" && id != "NOT" && id != "IN" && id != "AS" && id != "ON" && id != "GROUP" && id != "LIMIT" && id != "OFFSET")
        .ToList();

      var typeMap = new Dictionary<string, string>();
      foreach (var id in identifiers) {
        var type = await _stateProvider.GetFieldTypeAsync(id);
        if (type != null)
          typeMap[id] = type;
      }

      // Step 2: Rewrite WHERE clause for dynamic fields using EXISTS + UNNEST (DataSink schema)
      var whereRegex = new System.Text.RegularExpressions.Regex(@"WHERE(.*?)(ORDER|GROUP|LIMIT|OFFSET|$)", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
      var whereMatch = whereRegex.Match(userQuery);
      string whereClause = whereMatch.Success ? whereMatch.Groups[1].Value : "";
      var staticConds = new List<string>();
      var dynamicConds = new List<string>();
      foreach (var cond in whereClause.Split(new[] { "AND" }, StringSplitOptions.RemoveEmptyEntries)) {
        var trimmed = cond.Trim();
        var found = false;
        foreach (var kvp in typeMap) {
          if (trimmed.Contains(kvp.Key)) {
            // Dynamic field: rewrite as EXISTS UNNEST using DataSink schema
            string valueExpr = kvp.Value switch {
              "long" => $"f.LongValue",
              "double" => $"f.DoubleValue",
              "bool" => $"f.BoolValue",
              _ => $"f.StringValue"
            };
            // Extract operator and value
            var opMatch = System.Text.RegularExpressions.Regex.Match(trimmed, $@"{kvp.Key}\s*([<>=!]+)\s*(.+)");
            var op = opMatch.Success ? opMatch.Groups[1].Value : "=";
            var val = opMatch.Success ? opMatch.Groups[2].Value.Trim('"', '\'') : "";
            dynamicConds.Add($"EXISTS (SELECT 1 FROM UNNEST(t.Fields) AS f WHERE f.Key = '{kvp.Key}' AND {valueExpr} {op} {val})");
            found = true;
            break;
          }
        }
        if (!found && !string.IsNullOrWhiteSpace(trimmed))
          staticConds.Add(trimmed);
      }

      // Step 3: Build rewritten query
      string rewritten = userQuery;
      // Replace FROM clause
      rewritten = System.Text.RegularExpressions.Regex.Replace(
        rewritten,
        @"FROM\s+logs",
        "FROM read_parquet('{data_path}/**/*.parquet', hive_partitioning = true) AS t",
        System.Text.RegularExpressions.RegexOptions.IgnoreCase
      );
      // Replace WHERE clause
      var allConds = new List<string>();
      allConds.AddRange(staticConds);
      allConds.AddRange(dynamicConds);
      if (allConds.Count > 0) {
        var newWhere = "WHERE " + string.Join(" AND ", allConds);
        rewritten = whereRegex.Replace(rewritten, newWhere + " $2");
      }

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
