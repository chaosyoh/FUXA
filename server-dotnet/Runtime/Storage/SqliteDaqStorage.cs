using Core.Models;
using Core.Settings;
using Microsoft.Extensions.Logging;
using SqlSugar;

namespace Runtime.Storage;

/// <summary>
/// SQLite-based DAQ storage implementation (default)
/// </summary>
public class SqliteDaqStorage : IStorage
{
    private readonly ILogger<SqliteDaqStorage> _logger;
    private readonly ISqlSugarClient _db;

    public SqliteDaqStorage(ILogger<SqliteDaqStorage> logger)
    {
        _logger = logger;
        var settings = AppSettings.GetSettings();
        var dbFile = Path.Combine(settings.DbDir, "daq.fuxap.db");
        _db = new SqlSugarScope(new ConnectionConfig()
        {
            ConnectionString = $"Data Source={dbFile};",
            DbType = DbType.Sqlite,
            IsAutoCloseConnection = true,
        });
        var sql = "CREATE TABLE if not exists meters (dt DATETIME, tag_id TEXT, tag_value TEXT);";
        sql += "CREATE INDEX if not exists idx_meters_tag_dt ON meters(tag_id, dt);";
        try
        {
            _db.Ado.ExecuteCommand(sql);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SQLite DAQ storage initialization failed!");
        }
    }

    public async Task AddDaqValues(Dictionary<string, Tag> tagValues, string deviceId)
    {
        var now = DateTime.Now;
        var sql = string.Empty;
        foreach (var tag in tagValues.Values)
        {
            if (!tag.Daq.Enabled) continue;
            var value = tag.Value?.ToString()?.Replace("'", "''") ?? string.Empty;
            sql += $"INSERT INTO meters (dt, tag_id, tag_value) VALUES('{now:yyyy-MM-dd HH:mm:ss}','{tag.Id}','{value}');";
        }
        if (!string.IsNullOrEmpty(sql))
        {
            await _db.Ado.ExecuteCommandAsync(sql);
        }
    }

    public Task<List<DaqValue>> GetDaqValue(string tagId, DateTime start, DateTime end)
    {
        var sql = $"SELECT dt, tag_value FROM meters WHERE tag_id='{tagId.Replace("'", "''")}' AND dt >= '{start:yyyy-MM-dd HH:mm:ss}' AND dt <= '{end:yyyy-MM-dd HH:mm:ss}' ORDER BY dt";
        return _db.Ado.SqlQueryAsync<DaqValue>(sql);
    }

    public Dictionary<string, bool> GetDaqMap(string tagId)
    {
        return new Dictionary<string, bool> { { tagId, true } };
    }

    public void Close()
    {
        _db.Close();
    }

    public async Task<int> DeleteBefore(DateTime cutoff)
    {
        var sql = $"DELETE FROM meters WHERE dt < '{cutoff:yyyy-MM-dd HH:mm:ss}'";
        return await _db.Ado.ExecuteCommandAsync(sql);
    }
}
