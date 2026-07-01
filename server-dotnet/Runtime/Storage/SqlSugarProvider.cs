using System.Collections.Concurrent;
using Core.Settings;
using Microsoft.Extensions.Logging;
using SqlSugar;

namespace Runtime.Storage;

/// <summary>
/// SqlSugar connection provider.
/// SQLite mode: one SqlSugarScope per module (each backed by a separate .db file).
/// MySQL mode: a single shared SqlSugarScope for all modules.
/// </summary>
public class SqlSugarProvider : ISqlSugarProvider, IDisposable
{
    private readonly AppSettings _settings;
    private readonly ILoggerFactory _loggerFactory;
    private readonly bool _isSqlite;

    // SQLite: per-module scopes (lazy created)
    private readonly ConcurrentDictionary<string, SqlSugarScope> _scopes = new();

    // MySQL: single shared scope
    private readonly SqlSugarScope? _sharedScope;

    public SqlSugarProvider(AppSettings settings, ILoggerFactory loggerFactory)
    {
        _settings = settings;
        _loggerFactory = loggerFactory;
        _isSqlite = string.IsNullOrEmpty(settings.Database.Type) ||
                     settings.Database.Type.Equals("sqlite", StringComparison.OrdinalIgnoreCase);

        if (!_isSqlite)
        {
            // MySQL (or other databases): single shared scope
            _sharedScope = new SqlSugarScope(new ConnectionConfig
            {
                ConnectionString = settings.Database.ConnectionString,
                DbType = DbType.MySql,
                IsAutoCloseConnection = true,
            });
        }
    }

    public bool IsSqlite => _isSqlite;

    public ISqlSugarClient GetClient(string moduleName)
    {
        if (!_isSqlite)
        {
            return _sharedScope!;
        }

        return _scopes.GetOrAdd(moduleName, name =>
        {
            var dbPath = GetSqliteDbPath(name);
            return new SqlSugarScope(new ConnectionConfig
            {
                ConnectionString = $"Data Source={dbPath};",
                DbType = DbType.Sqlite,
                IsAutoCloseConnection = true,
            });
        });
    }

    private string GetSqliteDbPath(string moduleName)
    {
        // Currentstorage uses DbDir, all others use WorkDir
        if (moduleName == "Currentstorage")
        {
            return Path.Combine(_settings.DbDir, "currentTagReadings.db");
        }

        var fileName = moduleName switch
        {
            "ProjectStorage" => "project.fuxap.db",
            "UserService" => "users.fuxap.db",
            "AlarmStorage" => "alarms.fuxap.db",
            "NotifyStorage" => "notifications.fuxap.db",
            "ApiKeyStorage" => "apikeys.fuxap.db",
            "SchedulerStorage" => "scheduler.fuxap.db",
            _ => $"{moduleName.ToLowerInvariant()}.fuxap.db",
        };

        return Path.Combine(_settings.WorkDir, fileName);
    }

    public void Dispose()
    {
        if (_sharedScope != null)
        {
            _sharedScope.Close();
        }

        foreach (var scope in _scopes.Values)
        {
            scope.Close();
        }

        _scopes.Clear();
    }
}
