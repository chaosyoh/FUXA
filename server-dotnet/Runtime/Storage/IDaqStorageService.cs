
namespace Runtime.Storage;

public interface IDaqStorageService
{
    Task CheckRetention();
    Task<Dictionary<string, List<DaqValue>>> GetNodesValues(List<string> tagids, long fromts, long tots);
    Task<List<DaqValue>> GetNodeValues(string tagid, long fromts, long tots);
}