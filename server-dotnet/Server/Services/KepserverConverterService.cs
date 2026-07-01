using Core.Models;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Server.Services;

/// <summary>
/// Converts KepServer exported JSON configuration into FUXA device objects.
/// Supported drivers: Siemens TCP/IP Ethernet → SiemensS7, Modbus TCP/IP Ethernet → ModbusTCP
/// </summary>
public class KepserverConverterService
{
    private const string DevicePrefix = "d_";
    private const string TagPrefix = "t_";
    private const string TagGroupPrefix = "tg_";

    // Driver type mapping
    private static readonly Dictionary<string, string> DriverMap = new()
    {
        ["Siemens TCP/IP Ethernet"] = "SiemensS7",
        ["Modbus TCP/IP Ethernet"] = "ModbusTCP"
    };

    // S7 Device Model → cpuType
    private static readonly Dictionary<int, string> S7ModelMap = new()
    {
        [0] = "S7200Smart",
        [1] = "S7300",
        [2] = "S7400",
        [3] = "S71200",
        [4] = "S71500"
    };

    // KepServer TAG_DATA_TYPE → S7 type string
    private static readonly Dictionary<int, string> S7TagTypeMap = new()
    {
        [1] = "Boolean",
        [2] = "Byte",
        [3] = "Byte",
        [4] = "Int",
        [5] = "Word",
        [6] = "DInt",
        [7] = "DWord",
        [8] = "Real",
        [9] = "Real"
    };

    // KepServer TAG_DATA_TYPE → Modbus type string
    private static readonly Dictionary<int, string> ModbusTagTypeMap = new()
    {
        [1] = "Boolean",
        [2] = "UInt16",
        [3] = "UInt16",
        [4] = "Int16",
        [5] = "UInt16",
        [6] = "Int32",
        [7] = "UInt32",
        [8] = "Float32",
        [9] = "Float64"
    };

    // Modbus area mapping
    private static readonly Dictionary<char, string> ModbusAreaMap = new()
    {
        ['0'] = "0",
        ['1'] = "100000",
        ['3'] = "300000",
        ['4'] = "400000"
    };

    // Modbus MLE (Mid-Little-Endian / word-swapped) type mapping
    // Applied when KepServer device has DEVICE_FIRST_WORD_LOW=true (32-bit) or DEVICE_FIRST_DWORD_LOW=true (64-bit)
    private static readonly Dictionary<string, string> ModbusMleWordMap = new()
    {
        ["Int32"] = "Int32MLE",
        ["UInt32"] = "UInt32MLE",
        ["Float32"] = "Float32MLE"
    };
    private static readonly Dictionary<string, string> ModbusMleDWordMap = new()
    {
        ["Float64"] = "Float64MLE"
    };

    private static string GenerateId(string prefix)
    {
        return prefix + Guid.NewGuid().ToString("N")[..16];
    }

    /// <summary>
    /// Convert KepServer JSON to FUXA devices
    /// </summary>
    public List<Device> ConvertKepserverToFuxa(JsonNode kepJson)
    {
        var devices = new List<Device>();
        var channels = kepJson?["project"]?["channels"]?.AsArray();
        if (channels == null) return devices;

        foreach (var channel in channels)
        {
            if (channel == null) continue;
            var driverName = channel["servermain.MULTIPLE_TYPES_DEVICE_DRIVER"]?.GetValue<string>() ?? "";
            if (!DriverMap.TryGetValue(driverName, out var fuxaType)) continue;

            var channelDevices = channel["devices"]?.AsArray();
            if (channelDevices == null) continue;

            foreach (var kepDevice in channelDevices)
            {
                if (kepDevice == null) continue;
                var device = ConvertDevice(kepDevice, fuxaType);
                if (device != null)
                {
                    devices.Add(device);
                }
            }
        }

        return devices;
    }

    private Device? ConvertDevice(JsonNode kepDevice, string fuxaType)
    {
        var deviceName = kepDevice["common.ALLTYPES_NAME"]?.GetValue<string>() ?? "Unnamed";
        var deviceModel = kepDevice["servermain.DEVICE_MODEL"]?.GetValue<int>() ?? -1;

        // Skip NetLink models (5, 6)
        if (deviceModel == 5 || deviceModel == 6) return null;

        var device = new Device
        {
            Id = GenerateId(DevicePrefix),
            Name = deviceName,
            Enabled = true,
            Type = fuxaType,
            Polling = kepDevice["servermain.DEVICE_SCAN_MODE_RATE_MS"]?.GetValue<int>() ?? 1000,
            Property = new DeviceProperty(),
            Tags = new Dictionary<string, Tag>(),
            TagGroups = new Dictionary<string, TagGroup>()
        };

        if (fuxaType == "SiemensS7")
        {
            FillS7Property(device, kepDevice);
        }
        else if (fuxaType == "ModbusTCP")
        {
            FillModbusProperty(device, kepDevice);
        }

        // Modbus byte order options (device-level)
        var firstWordLow = fuxaType == "ModbusTCP" && (kepDevice["modbus_ethernet.DEVICE_FIRST_WORD_LOW"]?.GetValue<bool>() == true);
        var firstDWordLow = fuxaType == "ModbusTCP" && (kepDevice["modbus_ethernet.DEVICE_FIRST_DWORD_LOW"]?.GetValue<bool>() == true);

        // Convert root-level tags
        var kepTags = kepDevice["tags"]?.AsArray();
        if (kepTags != null)
        {
            foreach (var kepTag in kepTags)
            {
                if (kepTag == null) continue;
                var tag = ConvertTag(kepTag, fuxaType, device.Id, firstWordLow, firstDWordLow);
                if (tag != null)
                {
                    device.Tags[tag.Id] = tag;
                }
            }
        }

        // Convert tag_groups (recursive)
        var kepTagGroups = kepDevice["tag_groups"]?.AsArray();
        if (kepTagGroups != null)
        {
            ProcessTagGroups(kepTagGroups, device, fuxaType, "", firstWordLow, firstDWordLow);
        }

        return device;
    }

    /// <summary>
    /// Recursively process KepServer tag_groups and populate device.TagGroups and Tags.
    /// </summary>
    private void ProcessTagGroups(JsonArray groups, Device device, string fuxaType, string parentGroupId, bool firstWordLow, bool firstDWordLow)
    {
        foreach (var groupNode in groups)
        {
            if (groupNode == null) continue;
            var groupName = groupNode["common.ALLTYPES_NAME"]?.GetValue<string>();
            if (string.IsNullOrEmpty(groupName)) continue;

            var groupId = GenerateId(TagGroupPrefix);
            device.TagGroups[groupId] = new TagGroup
            {
                Id = groupId,
                ParentId = parentGroupId,
                DeviceId = device.Id,
                Name = groupName
            };

            // Tags within this group
            var groupTags = groupNode["tags"]?.AsArray();
            if (groupTags != null)
            {
                foreach (var kepTag in groupTags)
                {
                    if (kepTag == null) continue;
                    var tag = ConvertTag(kepTag, fuxaType, device.Id, firstWordLow, firstDWordLow);
                    if (tag != null)
                    {
                        tag.GroupId = groupId;
                        device.Tags[tag.Id] = tag;
                    }
                }
            }

            // Nested tag_groups
            var nestedGroups = groupNode["tag_groups"]?.AsArray();
            if (nestedGroups != null)
            {
                ProcessTagGroups(nestedGroups, device, fuxaType, groupId, firstWordLow, firstDWordLow);
            }
        }
    }

    private void FillS7Property(Device device, JsonNode kepDevice)
    {
        var address = kepDevice["servermain.DEVICE_ID_STRING"]?.GetValue<string>() ?? "";
        var rack = kepDevice["siemens_tcpip_ethernet.DEVICE_S7_COMMUNICATIONS_CPU_RACK"]?.GetValue<int>();
        var slot = kepDevice["siemens_tcpip_ethernet.DEVICE_S7_COMMUNICATIONS_CPU_SLOT"]?.GetValue<int>();
        var model = kepDevice["servermain.DEVICE_MODEL"]?.GetValue<int>() ?? 3;
        var cpuType = S7ModelMap.GetValueOrDefault(model, "S71200");

        device.Property = new DeviceProperty
        {
            Address = address,
            Port = "102",
            Rack = rack?.ToString() ?? "0",
            Slot = slot?.ToString() ?? "0",
            CpuType = cpuType
        };
    }

    private void FillModbusProperty(Device device, JsonNode kepDevice)
    {
        var idString = kepDevice["servermain.DEVICE_ID_STRING"]?.GetValue<string>() ?? "";
        var ip = "";
        var slaveId = "1";

        // Parse format like "<192.168.5.61>.0"
        var match = Regex.Match(idString, @"^<([^>]+)>\.(\d+)$");
        if (match.Success)
        {
            ip = match.Groups[1].Value;
            slaveId = match.Groups[2].Value;
        }
        else
        {
            ip = idString;
        }

        var port = kepDevice["modbus_ethernet.DEVICE_ETHERNET_PORT_NUMBER"]?.GetValue<int>() ?? 502;

        device.Property = new DeviceProperty
        {
            Address = ip,
            Port = port.ToString(),
            SlaveId = slaveId,
            Baudrate = "9600",
            Databits = "8",
            Stopbits = "1"
        };
    }

    private Tag? ConvertTag(JsonNode kepTag, string fuxaType, string deviceId, bool firstWordLow, bool firstDWordLow)
    {
        var tagName = kepTag["common.ALLTYPES_NAME"]?.GetValue<string>();
        var tagDesc = kepTag["common.ALLTYPES_DESCRIPTION"]?.GetValue<string>() ?? "";
        var tagAddress = kepTag["servermain.TAG_ADDRESS"]?.GetValue<string>() ?? "";
        var tagDataType = kepTag["servermain.TAG_DATA_TYPE"]?.GetValue<int>() ?? 0;
        var scalingType = kepTag["servermain.TAG_SCALING_TYPE"]?.GetValue<int>() ?? 0;

        if (string.IsNullOrEmpty(tagName) || string.IsNullOrEmpty(tagAddress)) return null;

        string type;
        string address;
        string memaddress = "";

        if (fuxaType == "SiemensS7")
        {
            type = S7TagTypeMap.GetValueOrDefault(tagDataType, "Word");
            address = tagAddress;
        }
        else if (fuxaType == "ModbusTCP")
        {
            type = ModbusTagTypeMap.GetValueOrDefault(tagDataType, "UInt16");
            var parsed = ParseModbusAddress(tagAddress);
            if (parsed == null) return null;
            address = parsed.Value.address;
            memaddress = parsed.Value.memaddress;
            // For coil/discrete areas, force Boolean type
            if (memaddress == "0" || memaddress == "100000")
            {
                type = "Boolean";
            }
            else
            {
                // Apply MLE byte order based on device-level KepServer settings
                if (firstWordLow && ModbusMleWordMap.TryGetValue(type, out var mleType))
                {
                    type = mleType;
                }
                if (firstDWordLow && ModbusMleDWordMap.TryGetValue(type, out var mleDType))
                {
                    type = mleDType;
                }
            }
        }
        else
        {
            return null;
        }

        var tag = new Tag
        {
            Id = GenerateId(TagPrefix),
            DeviceId = deviceId,
            Name = tagName,
            Label = "",
            Type = type,
            Address = address,
            Memaddress = memaddress,
            Description = tagDesc,
            Divisor = 0,
            Init = "",
            Format = "0",
            Daq = new Daq
            {
                Enabled = false,
                Changed = false,
                Interval = 60
            }
        };

        // Scaling
        if (scalingType == 1)
        {
            tag.Scale = BuildLinearScale(kepTag);
        }

        return tag;
    }

    private (string memaddress, string address)? ParseModbusAddress(string addrStr)
    {
        if (string.IsNullOrEmpty(addrStr) || addrStr.Length < 2) return null;

        var areaChar = addrStr[0];
        if (!ModbusAreaMap.TryGetValue(areaChar, out var fuxaMemaddress)) return null;

        var registerStr = addrStr[1..];
        if (!int.TryParse(registerStr, out var registerNum) || registerNum < 1) return null;

        return (fuxaMemaddress, registerNum.ToString());
    }

    private Scale BuildLinearScale(JsonNode kepTag)
    {
        return new Scale
        {
            Mode = "linear",
            RawLow = kepTag["servermain.TAG_SCALING_RAW_LOW"]?.GetValue<decimal>() ?? 0,
            RawHigh = kepTag["servermain.TAG_SCALING_RAW_HIGH"]?.GetValue<decimal>() ?? 0,
            ScaledLow = kepTag["servermain.TAG_SCALING_SCALED_LOW"]?.GetValue<decimal>() ?? 0,
            ScaledHigh = kepTag["servermain.TAG_SCALING_SCALED_HIGH"]?.GetValue<decimal>() ?? 0,
            DateTimeFormat = "",
            ReadExpression = "",
            WriteExpression = ""
        };
    }

    /// <summary>
    /// Strip BOM and single-line comments from JSON text
    /// </summary>
    public static string StripJsonComments(string text)
    {
        // Strip BOM
        if (text.Length > 0 && text[0] == '\uFEFF')
        {
            text = text[1..];
        }

        // Remove single-line comments (// ...) but not inside strings
        var result = new StringBuilder(text.Length);
        bool inString = false;
        bool escaped = false;

        for (int i = 0; i < text.Length; i++)
        {
            char ch = text[i];
            if (escaped)
            {
                result.Append(ch);
                escaped = false;
                continue;
            }
            if (ch == '\\' && inString)
            {
                result.Append(ch);
                escaped = true;
                continue;
            }
            if (ch == '"')
            {
                inString = !inString;
                result.Append(ch);
                continue;
            }
            if (!inString && ch == '/' && i + 1 < text.Length && text[i + 1] == '/')
            {
                // Skip until end of line
                while (i < text.Length && text[i] != '\n')
                {
                    i++;
                }
                // Keep the newline
                if (i < text.Length)
                {
                    result.Append('\n');
                }
                continue;
            }
            result.Append(ch);
        }

        return result.ToString();
    }

    /// <summary>
    /// Merge converted devices with existing devices.
    /// Same name + same type → update property & merge tags by name.
    /// </summary>
    public List<Device> MergeDevices(List<Device> convertedDevices, IDictionary<string, Device> existingDevices)
    {
        // Build lookup: name|type → existing device
        var existingByNameType = new Dictionary<string, Device>();
        foreach (var dev in existingDevices.Values)
        {
            if (!string.IsNullOrEmpty(dev.Name) && !string.IsNullOrEmpty(dev.Type))
            {
                var key = $"{dev.Name}|{dev.Type}";
                existingByNameType[key] = dev;
            }
        }

        var result = new List<Device>();
        foreach (var newDev in convertedDevices)
        {
            var lookupKey = $"{newDev.Name}|{newDev.Type}";
            if (existingByNameType.TryGetValue(lookupKey, out var existing))
            {
                // Merge: keep existing id, update property
                var merged = new Device
                {
                    Id = existing.Id,
                    Name = existing.Name,
                    Enabled = newDev.Enabled,
                    Type = existing.Type,
                    Polling = newDev.Polling,
                    Property = newDev.Property,
                    Tags = new Dictionary<string, Tag>(existing.Tags),
                    TagGroups = new Dictionary<string, TagGroup>(existing.TagGroups)
                };

                // Build existing tags lookup by name
                var existingTagsByName = new Dictionary<string, string>();
                foreach (var kv in existing.Tags)
                {
                    if (!string.IsNullOrEmpty(kv.Value.Name))
                    {
                        existingTagsByName[kv.Value.Name] = kv.Key;
                    }
                }

                // Merge new tags
                foreach (var newTagKv in newDev.Tags)
                {
                    var newTag = newTagKv.Value;
                    if (existingTagsByName.TryGetValue(newTag.Name, out var existingTagId))
                    {
                        // Update existing tag: keep id, update fields
                        var existingTag = merged.Tags[existingTagId];
                        existingTag.Type = newTag.Type;
                        existingTag.Address = newTag.Address;
                        existingTag.Memaddress = newTag.Memaddress;
                        existingTag.Description = !string.IsNullOrEmpty(newTag.Description)
                            ? newTag.Description : existingTag.Description;
                        existingTag.Divisor = newTag.Divisor;
                        existingTag.Scale = newTag.Scale ?? existingTag.Scale;
                        existingTag.GroupId = newTag.GroupId ?? existingTag.GroupId;
                    }
                    else
                    {
                        // New tag
                        newTag.DeviceId = merged.Id;
                        merged.Tags[newTag.Id] = newTag;
                    }
                }

                // Merge tagGroups
                foreach (var kv in newDev.TagGroups)
                {
                    merged.TagGroups[kv.Key] = kv.Value;
                }

                result.Add(merged);
            }
            else
            {
                // New device
                result.Add(newDev);
            }
        }

        return result;
    }
}
