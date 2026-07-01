
using Core.Models;

namespace Runtime.Storage;

public interface IStorage
{
    Task AddDaqValues(Dictionary<string, Tag> tagValues, string deviceId);
    void Close();
    Dictionary<string, bool> GetDaqMap(string tagId);
    Task<List<DaqValue>> GetDaqValue(string tagId, DateTime start, DateTime end);
    Task<int> DeleteBefore(DateTime cutoff);
}