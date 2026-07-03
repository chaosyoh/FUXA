using Core.Models;
using Core.Settings;
using Microsoft.Extensions.Logging;
using SqlSugar;

namespace Runtime.Storage;

public class QuestDb
{
    private readonly ILogger<QuestDb> _logger;
    private readonly ISqlSugarClient _db;

    public QuestDb(ILogger<QuestDb> logger, AppSettings settings)
    {
        _logger = logger;
        _db = new SqlSugarScope(new ConnectionConfig()
        {
            ConnectionString = settings.DaqStore.Url,
            DbType = DbType.QuestDB,
            IsAutoCloseConnection = true,
        });
    }

    public void InitTables()
    {
        try
        {
            _db.Ado.ExecuteCommand("CREATE TABLE if not exists meters (dt DATETIME, tag_id TEXT, tag_value TEXT);");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "QuestDb meters table initialization failed!");
        }
    }

    public Task AddDaqValues(Dictionary<string, Tag> tagValues, string deviceId)
    {
        var now = DateTime.Now;
        List<Meters> values = new List<Meters>();
        foreach (var tag in tagValues.Values)
        {
            if (!tag.Daq.Enabled) continue;
            values.Add(new Meters
            {
                Dt = now,
                Tag_Id = tag.Id,
                Tag_Value = tag.Value?.ToString(),
            });
        }
        return _db.Insertable(values).ExecuteCommandAsync();
    }

    public Task<List<DaqValue>> GetDaqValue(string tagId, DateTime start, DateTime end)
    {
        return _db.Queryable<Meters>().Where(x => x.Tag_Id == tagId && x.Dt >= start && x.Dt <= end).Select(x => new DaqValue
        {
            Dt = x.Dt,
            Value = x.Tag_Value,
        }).ToListAsync();
    }

    public void Close()
    {
        _db.Close();
        _db.Dispose();
    }

    public async Task<int> DeleteBefore(DateTime cutoff)
    {
        return await _db.Deleteable<Meters>().Where(x => x.Dt < cutoff).ExecuteCommandAsync();
    }
}
