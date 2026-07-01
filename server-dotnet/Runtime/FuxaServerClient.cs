using Core.Const;
using Core.Models;
using Microsoft.AspNetCore.SignalR;
using Runtime.Project;
using System.Data;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Runtime;

/// <summary>
/// FuxaServer 内部设备驱动
/// 支持普通内部变量和计算变量（基于算术表达式引用其他设备标签值）
/// </summary>
public class FuxaServerClient : DeviceBase
{
    private readonly IProjectService _project;
    private IDeviceRegistry? _registry;

    // Calculated tags with compiled expressions
    private readonly Dictionary<string, CalculatedTagInfo> _calculatedTags = new();

    public FuxaServerClient(IHubContext<DataHub> hubCtx, IProjectService project) : base(hubCtx)
    {
        _project = project;
    }

    /// <summary>
    /// Inject device registry for cross-device tag value access (called by DeviceManager)
    /// </summary>
    public void BindRegistry(IDeviceRegistry registry)
    {
        _registry = registry;
    }

    public override void Load(Device data)
    {
        base.Load(data);
        _calculatedTags.Clear();

        var projectData = _project.GetProject();

        foreach (var tag in Data.Tags.Values)
        {
            if (tag.Type == "calculated")
            {
                tag.Access = "ro";
                tag.Value = null;

                var compiled = CompileExpression(tag, projectData);
                if (compiled != null)
                {
                    _calculatedTags[tag.Id] = compiled;
                }
            }
            else
            {
                // Initialize normal tags
                if (!string.IsNullOrEmpty(tag.Init))
                {
                    tag.Value = ParseInitValue(tag.Init, tag.Type);
                }
                else
                {
                    tag.Value = tag.Type switch
                    {
                        "boolean" => false,
                        "number" => 0.0,
                        "string" => "",
                        _ => null
                    };
                }
            }
        }
    }

    public override Task<bool> Connect()
    {
        connected = true;
        Status = DeviceStatus.Ok;
        _ = NotifyStatus(DeviceStatus.Ok);
        return Task.FromResult(true);
    }

    public override Task<bool> Disconnect()
    {
        connected = false;
        monitored = false;
        ClearVarsValue();
        _ = NotifyStatus(DeviceStatus.Off);
        return Task.FromResult(true);
    }

    public override async Task<bool> Polling()
    {
        if (!connected) return false;
        if (!CheckWorking(true)) return false;

        try
        {
            var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Pass 1: update timestamps for non-calculated tags
            foreach (var tag in Data.Tags.Values)
            {
                if (tag.Type == "calculated") continue;
                tag.Timestamp = ts;
            }

            // Pass 2: evaluate calculated tags
            foreach (var (tagId, calcInfo) in _calculatedTags)
            {
                var tag = calcInfo.Tag;
                var badQualityMode = GetBadQualityMode(tag);
                var values = new Dictionary<string, double>();
                var allGood = true;

                foreach (var depId in calcInfo.DependencyTagIds)
                {
                    var depValue = GetDependencyValue(depId);
                    var isGood = !IsBadQuality(depValue);

                    if (!isGood)
                    {
                        allGood = false;
                        depValue = badQualityMode switch
                        {
                            1 => 0.0,
                            2 => calcInfo.LastGoodValues.TryGetValue(depId, out var last) ? last : 0.0,
                            _ => null
                        };
                    }
                    else
                    {
                        calcInfo.LastGoodValues[depId] = Convert.ToDouble(depValue ?? 0.0);
                    }

                    values[depId] = depValue != null ? Convert.ToDouble(depValue) : 0.0;
                }

                if (!allGood && badQualityMode == 0)
                {
                    tag.Value = null;
                }
                else
                {
                    try
                    {
                        tag.Value = EvaluateExpression(calcInfo.CompiledExpression, values);
                    }
                    catch
                    {
                        tag.Value = null;
                    }
                }
                tag.Timestamp = ts;
            }

            lastReadTimestamp = DateTime.Now;
            AddDaq();
            CheckWorking(false);
            await Task.CompletedTask;
            return true;
        }
        catch
        {
            CheckWorking(false);
            return false;
        }
    }

    public override Task<bool> SetValue(string id, object value)
    {
        if (!connected) return Task.FromResult(false);

        if (!Data.Tags.TryGetValue(id, out var tag)) return Task.FromResult(false);

        // Reject writes to calculated tags
        if (tag.Type == "calculated")
        {
            return Task.FromResult(false);
        }

        // Normal tags: set value directly
        tag.Value = UnwrapJsonValue(value);
        return Task.FromResult(true);
    }

    #region Expression Compilation

    /// <summary>
    /// Compile a calculated tag's expression at load time.
    /// Replaces {DeviceName.TagName} with tag IDs, validates safety.
    /// </summary>
    private CalculatedTagInfo? CompileExpression(Tag tag, ProjectData projectData)
    {
        var expr = tag.Expression;
        if (string.IsNullOrWhiteSpace(expr)) return null;
    
        // Safety: only allow safe characters
        if (!Regex.IsMatch(expr, @"^[\d\s+\-*/().\_%a-zA-Z{}]+$"))
        {
            return null;
        }
    
        var dependencyTagIds = new List<string>();
        var compiledExpr = expr;
        var seen = new Dictionary<string, string>();
    
        // Match {DeviceName.TagName}
        var matches = Regex.Matches(expr, @"\{([^}]+)\.([^}]+)\}");
        foreach (Match match in matches)
        {
            var deviceName = match.Groups[1].Value;
            var tagName = match.Groups[2].Value;
            var refKey = $"{deviceName}.{tagName}";

            if (!seen.ContainsKey(refKey))
            {
                var tagId = ResolveTagId(projectData, deviceName, tagName);
                if (tagId == null) return null;

                // Use the tagId as the variable name in the compiled expression
                // Replace dots and special chars for DataTable.Compute compatibility
                var varName = $"[{tagId}]";
                seen[refKey] = varName;
                dependencyTagIds.Add(tagId);
            }

            compiledExpr = compiledExpr.Replace(match.Value, seen[refKey]);
        }

        if (dependencyTagIds.Count == 0) return null;

        return new CalculatedTagInfo
        {
            Tag = tag,
            CompiledExpression = compiledExpr,
            DependencyTagIds = dependencyTagIds,
            LastGoodValues = new Dictionary<string, double>()
        };
    }

    /// <summary>
    /// Resolve a tag ID by device name and tag name from project data
    /// </summary>
    private static string? ResolveTagId(ProjectData projectData, string deviceName, string tagName)
    {
        foreach (var device in projectData.Devices.Values)
        {
            if (device.Name == deviceName)
            {
                foreach (var tag in device.Tags.Values)
                {
                    if (tag.Name == tagName)
                    {
                        return tag.Id;
                    }
                }
                break;
            }
        }
        return null;
    }

    #endregion

    #region Expression Evaluation

    /// <summary>
    /// Evaluate compiled expression using DataTable.Compute
    /// </summary>
    private static object? EvaluateExpression(string expression, Dictionary<string, double> values)
    {
        var expr = expression;
        foreach (var (tagId, val) in values)
        {
            expr = expr.Replace($"[{tagId}]", val.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        using var dt = new DataTable();
        var result = dt.Compute(expr, string.Empty);
        if (result is double d)
        {
            if (!double.IsFinite(d)) return null;
            return d;
        }
        if (result is IConvertible conv)
        {
            var dResult = conv.ToDouble(System.Globalization.CultureInfo.InvariantCulture);
            if (!double.IsFinite(dResult)) return null;
            return dResult;
        }
        return null;
    }

    /// <summary>
    /// Get the value of a dependency tag, possibly from another device
    /// </summary>
    private object? GetDependencyValue(string tagId)
    {
        // First check if it's a local tag
        if (Data.Tags.TryGetValue(tagId, out var localTag))
        {
            return localTag.Value;
        }

        // Look up from other devices via registry
        if (_registry == null) return null;

        foreach (var deviceId in GetAllDeviceIds())
        {
            var client = _registry.GetDeviceClient(deviceId);
            if (client == null) continue;
            var val = client.GetValue(tagId);
            if (val != null) return val;
            // Check if tag exists but value is null (vs tag doesn't exist)
            var tagProp = client.GetTagProperty(tagId);
            if (tagProp != null) return null; // tag exists, value is null = bad quality
        }
        return null;
    }

    /// <summary>
    /// Get all device IDs from project data
    /// </summary>
    private IEnumerable<string> GetAllDeviceIds()
    {
        var projectData = _project.GetProject();
        return projectData.Devices.Keys;
    }

    #endregion

    #region Helpers

    private static bool IsBadQuality(object? value)
    {
        if (value == null) return true;
        if (value is double d && !double.IsFinite(d)) return true;
        if (value is float f && !float.IsFinite(f)) return true;
        return false;
    }

    private static int GetBadQualityMode(Tag tag)
    {
        if (string.IsNullOrEmpty(tag.Options)) return 0;
        try
        {
            var opts = JsonSerializer.Deserialize<JsonElement>(tag.Options);
            if (opts.TryGetProperty("badQualityMode", out var mode))
            {
                return mode.ValueKind == JsonValueKind.Number ? mode.GetInt32() : 0;
            }
        }
        catch { }
        return 0;
    }

    private static object? ParseInitValue(string init, string type)
    {
        return type switch
        {
            "number" => double.TryParse(init, System.Globalization.NumberStyles.Any,
                System.Globalization.CultureInfo.InvariantCulture, out var d) ? d : 0.0,
            "boolean" => init.Equals("true", StringComparison.OrdinalIgnoreCase) || init == "1",
            "string" => init,
            _ => null
        };
    }

    #endregion

    private class CalculatedTagInfo
    {
        public Tag Tag { get; set; } = null!;
        public string CompiledExpression { get; set; } = string.Empty;
        public List<string> DependencyTagIds { get; set; } = new();
        public Dictionary<string, double> LastGoodValues { get; set; } = new();
    }
}
