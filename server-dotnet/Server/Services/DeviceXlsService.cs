using Core.Models;
using MiniExcelLibs;
using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Server.Services;

/// <summary>
/// Service for generating and parsing device xlsx files
/// </summary>
public class DeviceXlsService
{
    private static readonly string[] BaseDeviceKeys = ["id", "name", "enabled", "type", "polling"];
    private static readonly string[] BaseTagKeys = ["id", "name", "label", "type", "memaddress", "address",
        "divisor", "init", "format", "description", "scaleReadFunction", "scaleReadParams",
        "scaleWriteFunction", "scaleWriteParams", "sysType", "direction", "edge"];
    private static readonly string[] DaqKeys = ["enabled", "changed", "interval", "restored"];

    /// <summary>
    /// Generate xlsx byte array from devices
    /// </summary>
    public byte[] GenerateXls(ConcurrentDictionary<string, Device> devices, List<JsonNode?> scripts, ConcurrentDictionary<string, DeviceFolder>? deviceFolders = null)
    {
        var deviceList = devices.Values.ToList();
        var scriptNameById = BuildScriptNameByIdMap(scripts);

        // Collect property keys
        var propertyKeys = CollectPropertyKeys(deviceList);

        // Build Devices sheet data
        var devicesSheetData = new List<Dictionary<string, object?>>();
        foreach (var device in deviceList)
        {
            var row = new Dictionary<string, object?>
            {
                ["id"] = device.Id,
                ["name"] = device.Name,
                ["enabled"] = device.Enabled,
                ["type"] = device.Type,
                ["polling"] = device.Polling,
                ["folderId"] = device.FolderId ?? ""
            };

            foreach (var pk in propertyKeys)
            {
                row[$"property.{pk}"] = GetPropertyValue(device.Property, pk);
            }

            devicesSheetData.Add(row);
        }

        // Build Tags sheet data
        var tagsSheetData = new List<Dictionary<string, object?>>();
        foreach (var device in deviceList)
        {
            foreach (var tagKv in device.Tags)
            {
                var tag = tagKv.Value;
                var row = new Dictionary<string, object?>
                {
                    ["deviceId"] = device.Id,
                    ["id"] = tag.Id,
                    ["name"] = tag.Name,
                    ["label"] = tag.Label,
                    ["type"] = tag.Type,
                    ["memaddress"] = tag.Memaddress,
                    ["address"] = tag.Address,
                    ["divisor"] = tag.Divisor,
                    ["init"] = tag.Init,
                    ["format"] = tag.Format,
                    ["options"] = tag.Options,
                    ["description"] = tag.Description,
                    ["scaleReadFunction"] = ResolveScriptName(tag.ScaleReadFunction, scriptNameById),
                    ["scaleReadParams"] = tag.ScaleReadParams,
                    ["scaleWriteFunction"] = ResolveScriptName(tag.ScaleWriteFunction, scriptNameById),
                    ["scaleWriteParams"] = tag.ScaleWriteParams,
                    ["scale"] = tag.Scale != null ? JsonSerializer.Serialize(tag.Scale) : "",
                    ["deadband"] = tag.Deadband != null ? JsonSerializer.Serialize(tag.Deadband) : "",
                    ["sysType"] = tag.SysType,
                    ["direction"] = tag.Direction,
                    ["edge"] = tag.Edge,
                    ["daq.enabled"] = tag.Daq?.Enabled ?? false,
                    ["daq.changed"] = tag.Daq?.Changed ?? false,
                    ["daq.interval"] = tag.Daq?.Interval ?? 60,
                };

                tagsSheetData.Add(row);
            }
        }

        // Build DeviceFolders sheet data
        var foldersSheetData = new List<Dictionary<string, object?>>();
        if (deviceFolders != null)
        {
            foreach (var folder in deviceFolders.Values)
            {
                foldersSheetData.Add(new Dictionary<string, object?>
                {
                    ["id"] = folder.Id,
                    ["name"] = folder.Name,
                    ["parentId"] = folder.ParentId
                });
            }
        }

        // Write to xlsx with multiple sheets
        using var stream = new MemoryStream();
        var sheets = new Dictionary<string, object>
        {
            ["Devices"] = devicesSheetData,
            ["Tags"] = tagsSheetData,
            ["DeviceFolders"] = foldersSheetData
        };
        MiniExcel.SaveAs(stream, sheets);
        return stream.ToArray();
    }

    /// <summary>
    /// Parse xlsx stream to devices and deviceFolders
    /// </summary>
    public (List<Device> Devices, Dictionary<string, DeviceFolder> DeviceFolders) ParseXls(Stream fileStream, bool isTemplate, List<JsonNode?> scripts)
    {
        var scriptIdByName = BuildScriptIdByNameMap(scripts);
        var devices = new Dictionary<string, Device>();

        // Parse Devices sheet
        var devRows = MiniExcel.Query(fileStream, sheetName: "Devices", useHeaderRow: true).ToList();
        foreach (var row in devRows)
        {
            var rowDict = (IDictionary<string, object?>)row;
            var device = new Device
            {
                Id = GetStringValue(rowDict, "id"),
                Name = GetStringValue(rowDict, "name"),
                Enabled = GetBoolValue(rowDict, "enabled"),
                Type = GetStringValue(rowDict, "type"),
                Polling = GetIntValue(rowDict, "polling", 1000),
                FolderId = GetStringValue(rowDict, "folderId"),
                Property = new DeviceProperty(),
                Tags = new Dictionary<string, Tag>()
            };

            // Parse property.* columns
            foreach (var key in rowDict.Keys)
            {
                if (key.StartsWith("property."))
                {
                    var propKey = key["property.".Length..];
                    SetPropertyValue(device.Property, propKey, rowDict[key]);
                }
            }

            if (!string.IsNullOrEmpty(device.Id))
            {
                devices[device.Id] = device;
            }
        }

        // Parse Tags sheet
        fileStream.Position = 0;
        try
        {
            var tagRows = MiniExcel.Query(fileStream, sheetName: "Tags", useHeaderRow: true).ToList();
            foreach (var row in tagRows)
            {
                var rowDict = (IDictionary<string, object?>)row;
                var deviceId = GetStringValue(rowDict, "deviceId");
                if (string.IsNullOrEmpty(deviceId) || !devices.ContainsKey(deviceId))
                    continue;

                var tag = new Tag
                {
                    Id = GetStringValue(rowDict, "id"),
                    DeviceId = deviceId,
                    Name = GetStringValue(rowDict, "name"),
                    Label = GetStringValue(rowDict, "label"),
                    Type = GetStringValue(rowDict, "type"),
                    Memaddress = GetStringValue(rowDict, "memaddress"),
                    Address = GetStringValue(rowDict, "address"),
                    Divisor = GetNullableIntValue(rowDict, "divisor"),
                    Init = GetStringValue(rowDict, "init"),
                    Format = GetStringValue(rowDict, "format"),
                    Options = GetStringValue(rowDict, "options"),
                    Description = GetStringValue(rowDict, "description"),
                    ScaleReadFunction = ResolveScriptId(GetStringValue(rowDict, "scaleReadFunction"), scriptIdByName),
                    ScaleReadParams = GetStringValue(rowDict, "scaleReadParams"),
                    ScaleWriteFunction = ResolveScriptId(GetStringValue(rowDict, "scaleWriteFunction"), scriptIdByName),
                    ScaleWriteParams = GetStringValue(rowDict, "scaleWriteParams"),
                    SysType = GetNullableIntValue(rowDict, "sysType"),
                    Direction = GetStringValue(rowDict, "direction"),
                    Edge = GetStringValue(rowDict, "edge"),
                    Daq = new Daq
                    {
                        Enabled = GetBoolValue(rowDict, "daq.enabled"),
                        Changed = GetBoolValue(rowDict, "daq.changed"),
                        Interval = GetIntValue(rowDict, "daq.interval", 60)
                    }
                };

                // Parse scale JSON
                var scaleStr = GetStringValue(rowDict, "scale");
                if (!string.IsNullOrEmpty(scaleStr))
                {
                    try { tag.Scale = JsonSerializer.Deserialize<Scale>(scaleStr); }
                    catch { /* ignore parse errors */ }
                }

                // Parse deadband JSON
                var deadbandStr = GetStringValue(rowDict, "deadband");
                if (!string.IsNullOrEmpty(deadbandStr))
                {
                    try { tag.Deadband = JsonSerializer.Deserialize<TagDeadband>(deadbandStr); }
                    catch { /* ignore parse errors */ }
                }

                if (string.IsNullOrEmpty(tag.Id))
                {
                    tag.Id = "t_" + Guid.NewGuid().ToString("N")[..16];
                }

                devices[deviceId].Tags[tag.Id] = tag;
            }
        }
        catch (Exception)
        {
            // Tags sheet may not exist
        }

        // Parse DeviceFolders sheet
        var parsedDeviceFolders = new Dictionary<string, DeviceFolder>();
        try
        {
            fileStream.Position = 0;
            var folderRows = MiniExcel.Query(fileStream, sheetName: "DeviceFolders", useHeaderRow: true).ToList();
            foreach (var row in folderRows)
            {
                var rowDict = (IDictionary<string, object?>)row;
                var id = GetStringValue(rowDict, "id");
                if (!string.IsNullOrEmpty(id))
                {
                    parsedDeviceFolders[id] = new DeviceFolder
                    {
                        Id = id,
                        Name = GetStringValue(rowDict, "name"),
                        ParentId = GetStringValue(rowDict, "parentId")
                    };
                }
            }
        }
        catch (Exception)
        {
            // DeviceFolders sheet may not exist
        }

        var result = devices.Values.ToList();

        // Template mode: regenerate IDs
        if (isTemplate)
        {
            result = result.Where(d => d.Type != "FuxaServer").ToList();
            var oldToNewFolderId = new Dictionary<string, string>();
            var newFolders = new Dictionary<string, DeviceFolder>();

            // Regenerate folder IDs
            foreach (var folder in parsedDeviceFolders.Values)
            {
                var newId = "df_" + Guid.NewGuid().ToString("N")[..16];
                oldToNewFolderId[folder.Id] = newId;
                newFolders[newId] = new DeviceFolder { Id = newId, Name = folder.Name, ParentId = "" };
            }
            // Update parentId references
            foreach (var folder in newFolders.Values)
            {
                var oldFolder = parsedDeviceFolders.Values.FirstOrDefault(f => f.Name == folder.Name);
                if (oldFolder != null && !string.IsNullOrEmpty(oldFolder.ParentId) && oldToNewFolderId.ContainsKey(oldFolder.ParentId))
                {
                    folder.ParentId = oldToNewFolderId[oldFolder.ParentId];
                }
            }

            foreach (var device in result)
            {
                device.Id = "d_" + Guid.NewGuid().ToString("N")[..16];
                device.Name = device.Name + "_" + Guid.NewGuid().ToString("N")[..6];
                // Update device folderId
                if (!string.IsNullOrEmpty(device.FolderId) && oldToNewFolderId.ContainsKey(device.FolderId))
                {
                    device.FolderId = oldToNewFolderId[device.FolderId];
                }
                else
                {
                    device.FolderId = null;
                }
                var newTags = new Dictionary<string, Tag>();
                foreach (var tag in device.Tags.Values)
                {
                    var newId = "t_" + Guid.NewGuid().ToString("N")[..16];
                    tag.Id = newId;
                    tag.DeviceId = device.Id;
                    newTags[newId] = tag;
                }
                device.Tags = newTags;
            }
            parsedDeviceFolders = newFolders;
        }

        return (result, parsedDeviceFolders);
    }

    #region Helper Methods

    private static List<string> CollectPropertyKeys(List<Device> devices)
    {
        var keys = new HashSet<string> { "address", "port", "slot", "rack", "slaveid",
            "baudrate", "databits", "stopbits", "parity", "options", "method", "format",
            "connectionOption", "delay" };
        // Could extend dynamically from actual device properties
        return keys.OrderBy(k => k).ToList();
    }

    private static object? GetPropertyValue(DeviceProperty? prop, string key)
    {
        if (prop == null) return "";
        return key switch
        {
            "address" => prop.Address,
            "port" => prop.Port,
            "slot" => prop.Slot,
            "rack" => prop.Rack,
            "slaveid" => prop.SlaveId,
            "baudrate" => prop.Baudrate,
            "databits" => prop.Databits,
            "stopbits" => prop.Stopbits,
            "clientId" => prop.ClientId,
            "username" => prop.Username,
            "password" => prop.Password,
            "getUrl" => prop.GetUrl,
            "postUrl" => prop.PostUrl,
            _ => ""
        };
    }

    private static void SetPropertyValue(DeviceProperty prop, string key, object? value)
    {
        var strVal = value?.ToString() ?? "";
        switch (key)
        {
            case "address": prop.Address = strVal; break;
            case "port": prop.Port = strVal; break;
            case "slot": prop.Slot = strVal; break;
            case "rack": prop.Rack = strVal; break;
            case "slaveid": prop.SlaveId = strVal; break;
            case "baudrate": prop.Baudrate = strVal; break;
            case "databits": prop.Databits = strVal; break;
            case "stopbits": prop.Stopbits = strVal; break;
            case "clientId": prop.ClientId = strVal; break;
            case "username": prop.Username = strVal; break;
            case "password": prop.Password = strVal; break;
            case "getUrl": prop.GetUrl = strVal; break;
            case "postUrl": prop.PostUrl = strVal; break;
        }
    }

    private static Dictionary<string, string> BuildScriptNameByIdMap(List<JsonNode?> scripts)
    {
        var map = new Dictionary<string, string>();
        if (scripts == null) return map;
        foreach (var s in scripts)
        {
            if (s == null) continue;
            var id = s["id"]?.GetValue<string>();
            var name = s["name"]?.GetValue<string>();
            if (!string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(name))
                map[id] = name;
        }
        return map;
    }

    private static Dictionary<string, string> BuildScriptIdByNameMap(List<JsonNode?> scripts)
    {
        var map = new Dictionary<string, string>();
        if (scripts == null) return map;
        foreach (var s in scripts)
        {
            if (s == null) continue;
            var id = s["id"]?.GetValue<string>();
            var name = s["name"]?.GetValue<string>();
            if (!string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(name))
                map[name] = id;
        }
        return map;
    }

    private static string ResolveScriptName(string? value, Dictionary<string, string> map)
    {
        if (string.IsNullOrEmpty(value)) return "";
        return map.TryGetValue(value, out var name) ? name : value;
    }

    private static string ResolveScriptId(string? value, Dictionary<string, string> map)
    {
        if (string.IsNullOrEmpty(value)) return "";
        return map.TryGetValue(value, out var id) ? id : value;
    }

    private static string GetStringValue(IDictionary<string, object?> row, string key)
    {
        if (!row.TryGetValue(key, out var val) || val == null) return "";
        return val.ToString() ?? "";
    }

    private static bool GetBoolValue(IDictionary<string, object?> row, string key)
    {
        if (!row.TryGetValue(key, out var val) || val == null) return false;
        if (val is bool b) return b;
        var str = val.ToString()?.ToLower() ?? "";
        return str == "true" || str == "1";
    }

    private static int GetIntValue(IDictionary<string, object?> row, string key, int defaultVal = 0)
    {
        if (!row.TryGetValue(key, out var val) || val == null) return defaultVal;
        if (val is int i) return i;
        if (val is double d) return (int)d;
        return int.TryParse(val.ToString(), out var parsed) ? parsed : defaultVal;
    }

    private static int? GetNullableIntValue(IDictionary<string, object?> row, string key)
    {
        if (!row.TryGetValue(key, out var val) || val == null || string.IsNullOrEmpty(val.ToString()))
            return null;
        if (val is int i) return i;
        if (val is double d) return (int)d;
        return int.TryParse(val.ToString(), out var parsed) ? parsed : null;
    }

    #endregion
}
