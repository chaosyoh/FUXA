using Core.Settings;
using Microsoft.Extensions.Logging;
using SqlSugar;

namespace Runtime.Storage;

/// <summary>
/// SqlSugar connection provider.
/// Creates a single shared SqlSugarScope for all modules.
/// </summary>
public class SqlSugarProvider : IDisposable
{
    private readonly SqlSugarScope _scope;

    public SqlSugarProvider(AppSettings settings, ILoggerFactory loggerFactory)
    {
        var dbType = DbType.MySql;
        if (!string.IsNullOrEmpty(settings.Database.Type))
        {
            dbType = settings.Database.Type.ToLowerInvariant() switch
            {
                "mysql" => DbType.MySql,
                "mssql" or "sqlserver" => DbType.SqlServer,
                "postgresql" => DbType.PostgreSQL,
                _ => DbType.MySql,
            };
        }

        _scope = new SqlSugarScope(new ConnectionConfig
        {
            ConnectionString = settings.Database.ConnectionString,
            DbType = dbType,
            IsAutoCloseConnection = true,
        });
    }

    public ISqlSugarClient GetClient() => _scope;

    public void Dispose()
    {
        _scope.Close();
    }
}
