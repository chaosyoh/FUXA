using Core.Models;
using Microsoft.Extensions.Logging;
using SqlSugar;

namespace Runtime.Storage;

public class Currentstorage
{
    public Dictionary<string, Tag> DataQueue = new Dictionary<string, Tag>();

    private readonly ILogger<Currentstorage> _logger;
    private readonly ISqlSugarClient _db;

    public Currentstorage(ILogger<Currentstorage> logger, ISqlSugarClient db)
    {
        _logger = logger;
        _db = db;
    }

    public void InitTables()
    {
        try
        {
            _db.CodeFirst.InitTables<TagStorage>();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "currentstorage table initialization failed!");
        }
    }

    public async Task WriteValues()
    {
        var list = DataQueue.Select(x => new TagStorage
        {
            TagId = x.Key,
            Value = x.Value.Value?.ToString(),
            DeviceId = x.Value.DeviceId
        }).ToList();
        await _db.Storageable(list).ExecuteCommandAsync();
    }

    public void SetValues(List<Tag> tags)
    {
        foreach (var tag in tags)
        {
            DataQueue[tag.Id] = tag;
        }
    }

    public Task<List<TagValue>> GetValuesByDeviceId(string deviceId)
    {
        return _db.Queryable<TagStorage>().Where(x => x.DeviceId == deviceId)
            .Select(x => new TagValue
            {
                Id = x.TagId,
                Value = x.Value
            }).ToListAsync();
    }

    public void Close()
    {
        _db.Close();
    }

    public async Task ClearAll()
    {
        await _db.Deleteable<TagStorage>().ExecuteCommandAsync();
        DataQueue.Clear();
    }
}
