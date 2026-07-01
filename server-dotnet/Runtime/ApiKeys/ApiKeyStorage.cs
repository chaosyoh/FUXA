using Core.Entity;
using Core.Settings;
using Microsoft.Extensions.Logging;
using Runtime.Storage;
using SqlSugar;

namespace Runtime.ApiKeys;

public class ApiKeyStorage : IApiKeyStorage
{
    private readonly ILogger<ApiKeyStorage> _logger;
    private readonly ISqlSugarClient _db;

    public ApiKeyStorage(ILogger<ApiKeyStorage> logger, ISqlSugarProvider provider)
    {
        _logger = logger;
        _db = provider.GetClient("ApiKeyStorage");
        try
        {
            _db.CodeFirst.InitTables<ApiKey>();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ApiKey storage initialization failed, attempting to recreate table");
            try
            {
                _db.DbMaintenance.DropTable("apikeys");
                _db.CodeFirst.InitTables<ApiKey>();
            }
            catch (Exception ex2)
            {
                _logger.LogError(ex2, "ApiKey table recreation failed");
            }
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
