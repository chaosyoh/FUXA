using Core.Entity;

namespace Runtime.ApiKeys;

public interface IApiKeyStorage
{
    Task<List<ApiKey>> GetApiKeys();
    Task SetApiKeys(List<ApiKey> apiKeys);
    Task RemoveApiKeys(List<ApiKey> apiKeys);
}
