using System.Data;
using DuckDB.NET.Data;

namespace MongoParquetApi.Services;

public sealed class ParquetSqlService
{
    /// <summary>
    /// Run a SQL query on a parquet file using DuckDB.
    /// Replace "FROM parquet" in the query with the actual parquet reader call.
    /// Example:
    ///   SELECT * FROM parquet WHERE Price > 10
    /// becomes:
    ///   SELECT * FROM read_parquet('path') WHERE Price > 10
    /// Or allow {{file}} token.
    /// </summary>
    public async Task<List<Dictionary<string, object?>>> QueryFileAsync(string parquetPath, string sql, CancellationToken ct = default)
    {
        // Normalize query
        string finalSql = sql.Contains("{{file}}", StringComparison.OrdinalIgnoreCase)
            ? sql.Replace("{{file}}", $"read_parquet('{Escape(parquetPath)}')", StringComparison.OrdinalIgnoreCase)
            : ReplaceFromParquet(sql, parquetPath);

        using var conn = new DuckDBConnection("Data Source=:memory:");
        await conn.OpenAsync(ct);

        using var cmd = conn.CreateCommand();
        cmd.CommandText = finalSql;

        using var reader = await cmd.ExecuteReaderAsync(ct);
        var result = new List<Dictionary<string, object?>>();
        while (await reader.ReadAsync(ct))
        {
            var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < reader.FieldCount; i++)
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            result.Add(row);
        }
        return result;

        static string Escape(string p) => p.Replace("'", "''");
        static string ReplaceFromParquet(string s, string path)
        {
            // naive but effective: replace "from parquet" (any casing) with read_parquet('...')
            return System.Text.RegularExpressions.Regex.Replace(
                s,
                @"from\s+parquet\b",
                $"FROM read_parquet('{path.Replace("'", "''")}')",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        }
    }
}
