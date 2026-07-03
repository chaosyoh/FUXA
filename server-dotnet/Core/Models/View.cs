using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Core.Models;

public class View
{
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Captures all frontend-defined properties (Name, Profile, Items, Variables,
    /// Svgcontent, Type, ViewProperty, etc.) that the backend does not need to access directly.
    /// </summary>
    [JsonExtensionData]
    public Dictionary<string, JsonElement>? ExtensionData { get; set; }
}
