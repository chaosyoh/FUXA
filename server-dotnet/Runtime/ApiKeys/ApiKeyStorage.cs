using Core.Entity;
using Microsoft.Extensions.Logging;
using SqlSugar;

namespace Runtime.ApiKeys;

public class ApiKeyStorage
{
    private readonly ILogger<ApiKeyStorage> _logger;
    private readonly ISqlSugarClient _db;

    public ApiKeyStorage(ILogger<ApiKeyStorage> logger, ISqlSugarClient db)
    {
        _logger = logger;
        _db = db;
    }

    public void InitTables()
    {
        try
        {
            _db.CodeFirst.InitTables<ApiKey>();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ApiKey storage table initialization failed");
        }
    }

    public async Task<List<ApiKey>> GetApiKeys()
    {
        try
        {
            return await _db.Queryable<ApiKey>().ToListAsync();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to query apikeys, returning empty list");
            return new List<ApiKey>();
        }
    }

    public async Task SetApiKeys(List<ApiKey> apiKeys)
    {
        foreach (var apiKey in apiKeys)
        {
            var existing = await _db.Queryable<ApiKey>().Where(x => x.Id == apiKey.Id).FirstAsync();
            if (existing != null)
            {
                await _db.Updateable(apiKey).ExecuteCommandAsync();
            }
            else
            {
                await _db.Insertable(apiKey).ExecuteCommandAsync();
            }
        }
    }

    public async Task RemoveApiKeys(List<ApiKey> apiKeys)
    {
        foreach (var apiKey in apiKeys)
        {
            await _db.Deleteable<ApiKey>().Where(x => x.Id == apiKey.Id).ExecuteCommandAsync();
        }
    }
}
