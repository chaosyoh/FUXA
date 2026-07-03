using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Core.Models;

public class Alarm
{
    public string Name { get; set; } = string.Empty;
    public AlarmProperty? Property { get; set; }
    public AlarmSubProperty? Highhigh { get; set; }
    public AlarmSubProperty? High { get; set; }
    public AlarmSubProperty? Low { get; set; }
    public AlarmSubProperty? Info { get; set; }
    public AlarmSubActions? Actions { get; set; }
    public string? Value { get; set; }

    [JsonExtensionData]
    public Dictionary<string, JsonElement>? ExtensionData { get; set; }
}
