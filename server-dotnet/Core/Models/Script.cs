using System.Text.Json;

namespace Core.Models;

public class Script
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Code { get; set; } = string.Empty;
    public bool Sync { get; set; } = false;
    public List<ScriptParam> Parameters { get; set; } = [];
    public ScriptScheduling? Scheduling { get; set; }
    public int? Permission { get; set; }
    /// <summary>
    /// SERVER/CLIENT
    /// </summary>
    public string Mode { get; set; } = string.Empty;
    public PermissionRoles? PermissionRoles { get; set; }
}

public class ScriptScheduling
{
    public string Mode { get; set; } = string.Empty;
    public int Interval { get; set; }
}




public class ScriptParam
{
    public string Name { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
    public JsonElement Value { get; set; }
}
