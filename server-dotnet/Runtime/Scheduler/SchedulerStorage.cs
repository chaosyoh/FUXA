using Core.Models;
using Core.Utils;
using Microsoft.Extensions.Logging;
using SqlSugar;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Runtime.Scheduler;

public class SchedulerStorage
{
    private readonly ILogger<SchedulerStorage> _logger;
    private readonly ISqlSugarClient _db;
    private static JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public SchedulerStorage(ILogger<SchedulerStorage> logger, ISqlSugarClient db)
    {
        _logger = logger;
        _db = db;
    }

    public async Task<SchedulerData?> GetSchedulerData(string schedulerId)
    {
        var entity = await _db.Queryable<SchedulerEntity>()
            .Where(x => x.Id == schedulerId).FirstAsync();
        if (entity == null || string.IsNullOrEmpty(entity.Data)) return null;
        return JsonSerializer.Deserialize<SchedulerData>(entity.Data, JsonHelper.Default);
    }

    public async Task SetSchedulerData(string schedulerId, SchedulerData data)
    {
        var json = JsonSerializer.Serialize(data, _jsonOptions);
        var now = DateTime.UtcNow;
        var existing = await _db.Queryable<SchedulerEntity>()
            .Where(x => x.Id == schedulerId).FirstAsync();
        if (existing != null)
        {
            existing.Data = json;
            existing.UpdatedAt = now;
            await _db.Updateable(existing).ExecuteCommandAsync();
        }
        else
        {
            await _db.Insertable(new SchedulerEntity
            {
                Id = schedulerId,
                Data = json,
                CreatedAt = now,
                UpdatedAt = now,
            }).ExecuteCommandAsync();
        }
    }

    public async Task<Dictionary<string, SchedulerData>> GetAllSchedulers()
    {
        var result = new Dictionary<string, SchedulerData>();
        var entities = await _db.Queryable<SchedulerEntity>().ToListAsync();
        foreach (var entity in entities)
        {
            if (string.IsNullOrEmpty(entity.Data)) continue;
            var data = JsonSerializer.Deserialize<SchedulerData>(entity.Data, JsonHelper.Default);
            if (data != null) result[entity.Id] = data;
        }
        return result;
    }

    public async Task DeleteSchedulerData(string schedulerId)
    {
        await _db.Deleteable<SchedulerEntity>()
            .Where(x => x.Id == schedulerId).ExecuteCommandAsync();
    }

    public void Close()
    {
        _db.Close();
    }
}
