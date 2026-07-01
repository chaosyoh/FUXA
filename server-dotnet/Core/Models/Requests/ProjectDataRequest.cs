using System.Text.Json;

namespace Core.Models.Requests;

public class ProjectDataRequest
{
    public string Cmd { get; set; } = string.Empty;
    public JsonElement Data { get; set; }
}
