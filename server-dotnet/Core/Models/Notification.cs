using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Core.Models;

public class Notification
{
    public string Id { get; set; } = string.Empty;

    public string Name { get; set; } = string.Empty;

    public string Receiver { get; set; } = string.Empty;

    public bool Enabled { get; set; } = true;

    public string Type { get; set; } = string.Empty;

    public Dictionary<string, bool> Subscriptions { get; set; } = [];

    [JsonExtensionData]
    public Dictionary<string, JsonElement>? ExtensionData { get; set; }
}

public enum NotificationMode
{
    All = 0,
    Single = 1,
}