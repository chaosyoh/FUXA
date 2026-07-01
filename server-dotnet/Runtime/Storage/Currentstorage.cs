using Core.Models;
using Core.Settings;
using Microsoft.Extensions.Logging;
using Runtime.Storage;
using SqlSugar;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Runtime.Storage;

public class Currentstorage : ICurrentstorage
{
    public Dictionary<string, Tag> DataQueue = new Dictionary<string, Tag>();

    private readonly ILogger<Currentstorage> _logger;

    private readonly ISqlSugarClient db_current;

    public Currentstorage(ILogger<Currentstorage> logger, ISqlSugarProvider provider)
    {
        _logger = logger;
        db_current = provider.GetClient("Currentstorage");
        try
        {
            db_current.CodeFirst.InitTables<TagStorage>();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "currentstorage.bind failed!");
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
        await db_current.Storageable(list).ExecuteCommandAsync();
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
        return db_current.Queryable<TagStorage>().Where(x => x.DeviceId == deviceId)
            .Select(x => new TagValue
            {
                Id = x.TagId,
                Value = x.Value
            }).ToListAsync();
    }

    public void Close()
    {
        db_current.Close();
    }

    public async Task ClearAll()
    {
        await db_current.Deleteable<TagStorage>().ExecuteCommandAsync();
        DataQueue.Clear();
    }
}
