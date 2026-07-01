using Core.Entity;

namespace Core.Models.Requests;

public class ApiKeySaveRequest
{
    public List<ApiKey>? Params { get; set; }
}
