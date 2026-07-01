
using Core.Models;

namespace Runtime.Storage;

public interface ICurrentstorage
{
    Task ClearAll();
    void Close();
    Task<List<TagValue>> GetValuesByDeviceId(string deviceId);
    void SetValues(List<Tag> tags);
    Task WriteValues();
}