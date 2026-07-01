using Core.Models;
using Core.Settings;
using Microsoft.Extensions.Logging;
using SqlSugar;

namespace Runtime.Storage;

public class QuestDb : IStorage
{
    private readonly ILogger<QuestDb> _logger;
    private readonly ICurrentstorage _currentstorage;
    private readonly ISqlSugarClient _db;

    public QuestDb(ILogger<QuestDb> logger, ICurrentstorage currentstorage)
    {
        _logger = logger;
        _currentstorage = currentstorage;
        var settings = AppSettings.GetSettings();
        _db = new SqlSugarScope(new ConnectionConfig()
        {
            ConnectionString = settings.DaqStore.Url,
            DbType = DbType.QuestDB,
            IsAutoCloseConnection = true,
        });


    }

    public Task AddDaqValues(Dictionary<string, Tag> tagValues, string deviceId)
    {
        var now = DateTime.Now;
        List<Meters> values = new List<Meters>();
        List<TagValue> dataToRestore = new List<TagValue>();
        foreach (var tag in tagValues.Values)
        {
            var value = tag.Value?.ToString();
            if (string.IsNullOrEmpty(value))
            {
                dataToRestore.Add(new TagValue
                {
                    Id = tag.Id,
                    DeviceId = deviceId,
                    Value = value,
                });
            }

            if (!tag.Daq.Enabled) continue;
            values.Add(new Meters
            {
                Dt = now,
                Tag_Id = tag.Id,
                Tag_Value = value,
            });
        }
        ;

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

    public Dictionary<string, bool> GetDaqMap(string tagId)
    {
        var dummy = new Dictionary<string, bool>();
        dummy.Add(tagId, true);
        return dummy;
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
