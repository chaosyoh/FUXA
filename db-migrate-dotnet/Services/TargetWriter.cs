using Fuxa.DbMigrate.Models;
using SqlSugar;

namespace Fuxa.DbMigrate.Services;

/// <summary>
/// Writes data to MySQL or SQL Server using SqlSugar ORM
/// </summary>
public class TargetWriter : IDisposable
{
    private readonly TargetConfig _config;
    private SqlSugarClient? _db;
    private bool _disposed;

    public TargetWriter(TargetConfig config)
    {
        _config = config;
    }

    private bool IsMySql => _config.DbType.Equals("MySql", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Initialize the SqlSugar connection
    /// </summary>
    public void Connect()
    {
        var connStr = BuildConnectionString();
        var dbType = IsMySql ? DbType.MySql : DbType.SqlServer;

        _db = new SqlSugarClient(new ConnectionConfig
        {
            ConnectionString = connStr,
            DbType = dbType,
            IsAutoCloseConnection = true,
        });
    }

    /// <summary>
    /// Test the database connection
    /// </summary>
    public async Task<bool> TestConnectionAsync()
    {
        EnsureConnected();
        var result = await _db!.Ado.GetIntAsync("SELECT 1");
        return result == 1;
    }

    /// <summary>
    /// Create a table in the target database, optionally dropping it first
    /// </summary>
    public async Task CreateTableAsync(string tableName, TableDef tableDef, string? dbPrefix = null, bool dropIfExists = false)
    {
        EnsureConnected();
        var fullName = dbPrefix != null ? $"{dbPrefix}_{tableName}" : tableName;

        if (dropIfExists)
        {
            await DropTableAsync(fullName);
        }

        var sql = IsMySql ? BuildCreateTableMySql(fullName, tableDef) : BuildCreateTableMssql(fullName, tableDef);
        await _db!.Ado.ExecuteCommandAsync(sql);
    }

    /// <summary>
    /// Drop a table if it exists
    /// </summary>
    private async Task DropTableAsync(string tableName)
    {
        EnsureConnected();
        if (IsMySql)
        {
            await _db!.Ado.ExecuteCommandAsync($"DROP TABLE IF EXISTS `{tableName}`");
        }
        else
        {
            await _db!.Ado.ExecuteCommandAsync(
                $"IF OBJECT_ID('dbo.[{tableName}]', 'U') IS NOT NULL DROP TABLE dbo.[{tableName}]");
        }
    }

    /// <summary>
    /// Insert rows into a table using batch insert (UPSERT when primary key exists)
    /// </summary>
    public async Task<long> InsertRowsAsync(string tableName, List<Dictionary<string, object?>> rows,
        TableDef tableDef, string? dbPrefix = null, int batchSize = 1000)
    {
        if (rows.Count == 0) return 0;

        EnsureConnected();
        var fullName = dbPrefix != null ? $"{dbPrefix}_{tableName}" : tableName;
        long totalInserted = 0;

        // Process in batches
        for (int i = 0; i < rows.Count; i += batchSize)
        {
            var batch = rows.Skip(i).Take(batchSize).ToList();
            await InsertBatchAsync(fullName, batch, tableDef);
            totalInserted += batch.Count;
        }

        return totalInserted;
    }

    /// <summary>
    /// Insert a single batch of rows
    /// </summary>
    private async Task InsertBatchAsync(string tableName, List<Dictionary<string, object?>> rows, TableDef tableDef)
    {
        if (rows.Count == 0) return;

        var columns = tableDef.Columns;
        var colNames = string.Join(", ", columns.Select(c => IsMySql ? $"`{c.Name}`" : $"[{c.Name}]"));

        if (tableDef.PrimaryKey != null)
        {
            // UPSERT: use REPLACE INTO for MySQL, MERGE for SQL Server
            if (IsMySql)
            {
                await UpsertBatchMySqlAsync(tableName, rows, tableDef);
            }
            else
            {
                await UpsertBatchMssqlAsync(tableName, rows, tableDef);
            }
        }
        else
        {
            // Plain INSERT
            await PlainInsertBatchAsync(tableName, rows, tableDef);
        }
    }

    /// <summary>
    /// MySQL: REPLACE INTO for upsert
    /// </summary>
    private async Task UpsertBatchMySqlAsync(string tableName, List<Dictionary<string, object?>> rows, TableDef tableDef)
    {
        var colNames = string.Join(", ", tableDef.Columns.Select(c => $"`{c.Name}`"));
        var paramList = new List<string>();
        var parameters = new List<SugarParameter>();
        int paramIdx = 0;

        foreach (var row in rows)
        {
            var placeholders = new List<string>();
            foreach (var col in tableDef.Columns)
            {
                var paramName = $"@p{paramIdx++}";
                placeholders.Add(paramName);
                parameters.Add(new SugarParameter(paramName, FormatValue(row.GetValueOrDefault(col.Name), col)));
            }
            paramList.Add($"({string.Join(", ", placeholders)})");
        }

        var sql = $"REPLACE INTO `{tableName}` ({colNames}) VALUES {string.Join(", ", paramList)}";
        await _db!.Ado.ExecuteCommandAsync(sql, parameters.ToArray());
    }

    /// <summary>
    /// SQL Server: MERGE for upsert
    /// </summary>
    private async Task UpsertBatchMssqlAsync(string tableName, List<Dictionary<string, object?>> rows, TableDef tableDef)
    {
        var pk = tableDef.PrimaryKey!;
        var colNames = tableDef.Columns.Select(c => $"[{c.Name}]").ToList();
        var colNamesStr = string.Join(", ", colNames);
        var parameters = new List<SugarParameter>();
        int paramIdx = 0;

        // Build VALUES clause
        var valueRows = new List<string>();
        foreach (var row in rows)
        {
            var placeholders = new List<string>();
            foreach (var col in tableDef.Columns)
            {
                var paramName = $"@p{paramIdx++}";
                placeholders.Add(paramName);
                parameters.Add(new SugarParameter(paramName, FormatValue(row.GetValueOrDefault(col.Name), col)));
            }
            valueRows.Add($"({string.Join(", ", placeholders)})");
        }

        var valuesStr = string.Join(", ", valueRows);
        var sourceCols = string.Join(", ", tableDef.Columns.Select(c => $"src.[{c.Name}]"));
        var updateSet = string.Join(", ", tableDef.Columns
            .Where(c => c.Name != pk)
            .Select(c => $"tgt.[{c.Name}] = src.[{c.Name}]"));

        var sql = $"""
            MERGE INTO [{tableName}] AS tgt
            USING (VALUES {valuesStr}) AS src ({colNamesStr})
            ON tgt.[{pk}] = src.[{pk}]
            WHEN MATCHED THEN UPDATE SET {updateSet}
            WHEN NOT MATCHED THEN INSERT ({colNamesStr}) VALUES ({sourceCols});
            """;

        await _db!.Ado.ExecuteCommandAsync(sql, parameters.ToArray());
    }

    /// <summary>
    /// Plain INSERT (no primary key, no upsert)
    /// </summary>
    private async Task PlainInsertBatchAsync(string tableName, List<Dictionary<string, object?>> rows, TableDef tableDef)
    {
        var colNames = string.Join(", ", tableDef.Columns.Select(c => IsMySql ? $"`{c.Name}`" : $"[{c.Name}]"));
        var paramList = new List<string>();
        var parameters = new List<SugarParameter>();
        int paramIdx = 0;

        foreach (var row in rows)
        {
            var placeholders = new List<string>();
            foreach (var col in tableDef.Columns)
            {
                var paramName = $"@p{paramIdx++}";
                placeholders.Add(paramName);
                parameters.Add(new SugarParameter(paramName, FormatValue(row.GetValueOrDefault(col.Name), col)));
            }
            paramList.Add($"({string.Join(", ", placeholders)})");
        }

        var prefix = IsMySql ? $"`{tableName}`" : $"[{tableName}]";
        var sql = $"INSERT INTO {prefix} ({colNames}) VALUES {string.Join(", ", paramList)}";
        await _db!.Ado.ExecuteCommandAsync(sql, parameters.ToArray());
    }

    /// <summary>
    /// Format a value for insertion based on column type
    /// </summary>
    private static object? FormatValue(object? value, ColumnDef col)
    {
        if (value == null || value is DBNull)
            return DBNull.Value;

        // Integer types
        if (col.MysqlType is "INT" or "BIGINT" or "INT AUTO_INCREMENT")
        {
            if (long.TryParse(value.ToString(), out var num))
                return num;
            return DBNull.Value;
        }

        // DateTime types
        if (col.MysqlType is "DATETIME")
        {
            if (value is DateTime dt)
                return dt;
            if (DateTime.TryParse(value.ToString(), out var parsed))
                return parsed;
            return DBNull.Value;
        }

        return value.ToString();
    }

    #region DDL Builders

    private static string BuildCreateTableMySql(string tableName, TableDef tableDef)
    {
        var columns = tableDef.Columns.Select(col => $"  `{col.Name}` {col.MysqlType}");
        var pk = tableDef.PrimaryKey != null
            ? $", PRIMARY KEY (`{tableDef.PrimaryKey}`)"
            : "";

        return $"CREATE TABLE IF NOT EXISTS `{tableName}` (\n{string.Join(",\n", columns)}{pk}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";
    }

    private static string BuildCreateTableMssql(string tableName, TableDef tableDef)
    {
        var columns = tableDef.Columns.Select(col => $"  [{col.Name}] {col.MssqlType}");
        var pk = tableDef.PrimaryKey != null
            ? $", CONSTRAINT [PK_{tableName}] PRIMARY KEY ([{tableDef.PrimaryKey}])"
            : "";

        return $"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{tableName}' AND xtype='U')
            CREATE TABLE [{tableName}] (
            {string.Join(",\n", columns)}{pk}
            )
            """;
    }

    #endregion

    private string BuildConnectionString()
    {
        if (IsMySql)
        {
            return $"Server={_config.Host};Port={_config.Port};Database={_config.Database};Uid={_config.User};Pwd={_config.Password};CharSet=utf8mb4;AllowUserVariables=True;";
        }
        else
        {
            var encrypt = _config.Encrypt ? "True" : "False";
            var trustCert = _config.TrustServerCertificate ? "True" : "False";
            return $"Server={_config.Host},{_config.Port};Database={_config.Database};User Id={_config.User};Password={_config.Password};Encrypt={encrypt};TrustServerCertificate={trustCert};";
        }
    }

    private void EnsureConnected()
    {
        if (_db == null)
            throw new InvalidOperationException("Not connected. Call Connect() first.");
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _db?.Dispose();
        GC.SuppressFinalize(this);
    }
}
