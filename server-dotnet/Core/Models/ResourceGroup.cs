using System.Text.Json.Serialization;

namespace Core.Models;

public class ResourceGroup
{
    public string Name { get; set; } = string.Empty;
    public List<ResourceItem> Items { get; set; } = new();
}

public class ResourceItem
{
    public string Path { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Label { get; set; }
}

public class ResourceListResult
{
    public List<ResourceGroup> Groups { get; set; } = new();
}
