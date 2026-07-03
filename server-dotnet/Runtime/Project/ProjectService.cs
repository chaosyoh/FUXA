using Core.Const;
using Core.Models;
using Core.Utils;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;
using SqlSugar;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Runtime.Project;

public class ProjectService : IProjectService
{
    private readonly ILogger<ProjectService> _logger;
    private readonly ProjectStorage _prjStorage;

    private ProjectData data = new ProjectData();

    private ConcurrentDictionary<int, List<Tag>> ArchiveDic = new ConcurrentDictionary<int, List<Tag>>();

    private static readonly JsonSerializerOptions _options = JsonHelper.Default;

    public ProjectService(ILogger<ProjectService> logger, ProjectStorage prjStorage)
    {
        _logger = logger;
        _prjStorage = prjStorage;
    }

    public async Task Load()
    {
        data = new ProjectData();
        ArchiveDic.Clear();

        #region load general data
        var grows = await _prjStorage.GetSection(TableType.GENERAL);
        foreach (var row in grows)
        {
            if (row.Name == ProjectDataCmdType.HmiLayout)
            {
                data.Hmi.Layout = JsonSerializer.Deserialize<LayoutSettings>(row.Value, _options);
            }
            if (row.Name == ProjectDataCmdType.MobileLayout)
            {
                data.Hmi.MobileLayout = JsonSerializer.Deserialize<LayoutSettings>(row.Value, _options);
            }
            if (row.Name == "Version")
            {
                data.Version = JsonSerializer.Deserialize<string>(row.Value, _options) ?? string.Empty;
            }
            if (row.Name == ProjectDataCmdType.Charts)
            {
                data.Charts = JsonSerializer.Deserialize<List<Chart>>(row.Value, _options) ?? new List<Chart>();
            }
            if (row.Name == ProjectDataCmdType.Languages)
            {

                data.Languages = JsonSerializer.Deserialize<Languages>(row.Value, _options) ?? new Languages();
            }
            if (row.Name == ProjectDataCmdType.Graphs)
            {
                data.Graphs = JsonSerializer.Deserialize<List<Graph>>(row.Value, _options) ?? new List<Graph>();
            }
            if (row.Name == ProjectDataCmdType.ClientAccess)
            {
                data.ClientAccess = JsonSerializer.Deserialize<ClientAccess>(row.Value, _options) ?? new ClientAccess();
            }
            if (row.Name == "timestamp")
            {
                if (DateTime.TryParse(row.Value, out var dt))
                {
                    data.Timestamp = dt;
                }
                else if (long.TryParse(row.Value, out var ms))
                {
                    data.Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(ms).LocalDateTime;
                }
            }
        }
        #endregion

        #region load views
        var views = await _prjStorage.GetSection(TableType.VIEWS);
        foreach (var row in views)
        {
            var view = JsonSerializer.Deserialize<View>(row.Value, _options);
            if (view == null) continue;
            data.Hmi.Views.Add(view);
        }
        #endregion

        #region load devices
        var devices = await _prjStorage.GetSection(TableType.DEVICES);
        foreach (var row in devices)
        {
            var d = JsonSerializer.Deserialize<Device>(row.Value, _options) ?? new Device();
            if (string.Equals(row.Name, "Server", StringComparison.OrdinalIgnoreCase))
            {
                data.Server = d;
            }
            else
            {
                data.Devices.TryAdd(row.Name, d);
            }
            foreach (var kv in d.Tags)
            {
                var tag = kv.Value;
                data.Tags.TryAdd(kv.Key, kv.Value);
                if (tag.Daq.Enabled && !tag.Daq.Changed)
                {
                    if (ArchiveDic.ContainsKey(tag.Daq.Interval))
                    {
                        ArchiveDic[tag.Daq.Interval].Add(tag);
                    }
                    else
                    {
                        ArchiveDic.AddOrUpdate(tag.Daq.Interval, new List<Tag> { tag }, (k, v) =>
                        {
                            v.Add(tag);
                            return v;
                        });
                    }
                }
            }
        }
        #endregion

        #region load texts
        var texts = await _prjStorage.GetSection(TableType.TEXTS);
        foreach (var row in texts)
        {
            var node = JsonNode.Parse(row.Value);
            if (node != null) data.Texts.Add(node);
        }
        #endregion

        #region load alarms
        var alarms = await _prjStorage.GetSection(TableType.ALARMS);
        foreach (var row in alarms)
        {
            var value = JsonSerializer.Deserialize<Alarm>(row.Value, _options);
            if (value != null)
                data.Alarms.Add(value);
        }
        #endregion

        #region load notifications
        var notifications = await _prjStorage.GetSection(TableType.NOTIFICATIONS);
        foreach (var row in notifications)
        {
            var value = JsonSerializer.Deserialize<Notification>(row.Value, _options);
            if (value != null)
                data.Notifications.Add(value);
        }
        #endregion

        #region load scripts
        var scripts = await _prjStorage.GetSection(TableType.SCRIPTS);
        foreach (var row in scripts)
        {
            var node = JsonNode.Parse(row.Value);
            if (node != null) data.Scripts.Add(node);
        }
        #endregion

        #region load reports
        var reports = await _prjStorage.GetSection(TableType.REPORTS);
        foreach (var row in reports)
        {
            var node = JsonNode.Parse(row.Value);
            if (node != null) data.Reports.Add(node);
        }
        #endregion

        #region load MapsLocations
        var locations = await _prjStorage.GetSection(TableType.LOCATIONS);
        foreach (var row in locations)
        {
            var node = JsonNode.Parse(row.Value);
            if (node != null) data.MapsLocations.Add(node);
        }
        #endregion
    }

    public async Task SetProjectData(string cmd, JsonElement value)
    {
        var toremove = false;
        var section = new SqlSection { Table = "", Name = "", Value = value };
        if (cmd == ProjectDataCmdType.SetView)
        {
            var view = value.Deserialize<View>(_options);
            if (view == null) return;
            section.Table = TableType.VIEWS;
            section.Name = view.Id;
            var idx = data.Hmi.Views.FindIndex(x => x.Id == view.Id);
            if (idx >= 0) data.Hmi.Views[idx] = view;
            else data.Hmi.Views.Add(view);
        }
        else if (cmd == ProjectDataCmdType.DelView)
        {
            var view = value.Deserialize<View>(_options);
            if (view == null) return;
            var idx = data.Hmi.Views.FindIndex(x => x.Id == view.Id);
            if (idx == -1) return;
            toremove = true;
            section.Table = TableType.VIEWS;
            section.Name = view.Id;
            data.Hmi.Views.RemoveAt(idx);
        }
        else if (cmd == ProjectDataCmdType.HmiLayout)
        {
            var layout = value.Deserialize<LayoutSettings>(_options);
            if (layout == null) return;
            data.Hmi.Layout = layout;
            section.Table = TableType.GENERAL;
            section.Name = cmd;
        }
        else if (cmd == ProjectDataCmdType.MobileLayout)
        {
            var mobileLayout = value.Deserialize<LayoutSettings>(_options);
            if (mobileLayout == null) return;
            data.Hmi.MobileLayout = mobileLayout;
            section.Table = TableType.GENERAL;
            section.Name = cmd;
        }
        else if (cmd == ProjectDataCmdType.SetDevice)
        {
            var device = value.Deserialize<Device>(_options);
            if (device == null) return;
            section.Table = TableType.DEVICES;
            section.Name = device.Id;
            data.Devices[device.Id] = device;
        }
        else if (cmd == ProjectDataCmdType.DelDevice)
        {
            var device = value.Deserialize<Device>(_options);
            if (device == null) return;
            if (device.Id == "0")
                throw new InvalidOperationException("Internal device cannot be deleted");
            toremove = true;
            section.Table = TableType.DEVICES;
            section.Name = device.Id;
            data.Devices.TryRemove(device.Id, out _);
        }
        else if (cmd == ProjectDataCmdType.Charts)
        {
            var charts = value.Deserialize<List<Chart>>(_options);
            if (charts == null) return;
            section.Table = TableType.GENERAL;
            section.Name = cmd;
            data.Charts = charts;
        }
        else if (cmd == ProjectDataCmdType.Languages)
        {
            section.Table = TableType.GENERAL;
            section.Name = cmd;
            data.Languages = value.Deserialize<Languages>(_options) ?? new Languages();
        }
        else if (cmd == ProjectDataCmdType.Graphs)
        {
            section.Table = TableType.GENERAL;
            section.Name = cmd;
            data.Graphs = value.Deserialize<List<Graph>>(_options) ?? [];
        }
        else if (cmd == ProjectDataCmdType.ClientAccess)
        {
            section.Table = TableType.GENERAL;
            section.Name = cmd;
            data.ClientAccess = value.Deserialize<ClientAccess>(_options) ?? new ClientAccess();
        }
        else if (cmd == ProjectDataCmdType.SetText)
        {
            var node = ToJsonNode(value);
            var id = node?["id"]?.GetValue<string>();
            if (string.IsNullOrEmpty(id)) return;
            section.Table = TableType.TEXTS;
            section.Name = id;
            SetOrAddJsonNode(data.Texts, "id", node!);
        }
        else if (cmd == ProjectDataCmdType.DelText)
        {
            var node = ToJsonNode(value);
            var id = node?["id"]?.GetValue<string>();
            if (string.IsNullOrEmpty(id)) return;
            toremove = true;
            section.Table = TableType.TEXTS;
            section.Name = id;
            RemoveJsonNode(data.Texts, "id", id);
        }
        else if (cmd == ProjectDataCmdType.SetAlarm)
        {
            var alarm = value.Deserialize<Alarm>(_options);
            if (alarm == null) return;
            section.Table = TableType.ALARMS;
            section.Name = alarm.Name;
            var idx = data.Alarms.FindIndex(a => a.Name == alarm.Name);
            if (idx >= 0) data.Alarms[idx] = alarm;
            else data.Alarms.Add(alarm);
        }
        else if (cmd == ProjectDataCmdType.DelAlarm)
        {
            var alarm = value.Deserialize<Alarm>(_options);
            if (alarm == null) return;
            toremove = true;
            section.Table = TableType.ALARMS;
            section.Name = alarm.Name;
            var idx = data.Alarms.FindIndex(a => a.Name == alarm.Name);
            if (idx >= 0) data.Alarms.RemoveAt(idx);
        }
        else if (cmd == ProjectDataCmdType.SetNotification)
        {
            var notification = value.Deserialize<Notification>(_options);
            if (notification is null || string.IsNullOrEmpty(notification.Id)) return;
            section.Table = TableType.NOTIFICATIONS;
            section.Name = notification.Id;
            var idx = data.Notifications.FindIndex(n => n.Id == notification.Id);
            if (idx >= 0) data.Notifications[idx] = notification;
        }
        else if (cmd == ProjectDataCmdType.DelNotification)
        {
            var notification = value.Deserialize<Notification>(_options);
            if (notification is null || string.IsNullOrEmpty(notification.Id)) return;
            toremove = true;
            section.Table = TableType.NOTIFICATIONS;
            section.Name = notification.Id;
            var idx = data.Notifications.FindIndex(n => n.Id == notification.Id);
            if (idx >= 0) data.Notifications.RemoveAt(idx);

        }
        else if (cmd == ProjectDataCmdType.SetScript)
        {
            var node = ToJsonNode(value);
            var id = node?["id"]?.GetValue<string>();
            if (string.IsNullOrEmpty(id)) return;
            section.Table = TableType.SCRIPTS;
            section.Name = id;
            SetOrAddJsonNode(data.Scripts, "id", node!);
        }
        else if (cmd == ProjectDataCmdType.DelScript)
        {
            var node = ToJsonNode(value);
            var id = node?["id"]?.GetValue<string>();
            if (string.IsNullOrEmpty(id)) return;
            toremove = true;
            section.Table = TableType.SCRIPTS;
            section.Name = id;
            RemoveJsonNode(data.Scripts, "id", id);
        }
        else if (cmd == ProjectDataCmdType.SetReport)
        {
            var node = ToJsonNode(value);
            var id = node?["id"]?.GetValue<string>();
            if (string.IsNullOrEmpty(id)) return;
            section.Table = TableType.REPORTS;
            section.Name = id;
            SetOrAddJsonNode(data.Reports, "id", node!);
        }
        else if (cmd == ProjectDataCmdType.DelReport)
        {
            var node = ToJsonNode(value);
            var id = node?["id"]?.GetValue<string>();
            if (string.IsNullOrEmpty(id)) return;
            toremove = true;
            section.Table = TableType.REPORTS;
            section.Name = id;
            RemoveJsonNode(data.Reports, "id", id);
        }
        else if (cmd == ProjectDataCmdType.SetMapsLocation)
        {
            var node = ToJsonNode(value);
            var id = node?["id"]?.GetValue<string>();
            if (string.IsNullOrEmpty(id)) return;
            section.Table = TableType.LOCATIONS;
            section.Name = id;
            SetOrAddJsonNode(data.MapsLocations, "id", node!);
        }
        else if (cmd == ProjectDataCmdType.DelMapsLocation)
        {
            var node = ToJsonNode(value);
            var id = node?["id"]?.GetValue<string>();
            if (string.IsNullOrEmpty(id)) return;
            toremove = true;
            section.Table = TableType.LOCATIONS;
            section.Name = id;
            RemoveJsonNode(data.MapsLocations, "id", id);
        }
        else
        {
            _logger.LogWarning("Unknown project data cmd: {Cmd}", cmd);
            return;
        }

        if (toremove)
        {
            await _prjStorage.DeleteSection(section);
        }
        else
        {
            await _prjStorage.SetSection(section);
        }
    }

    public async Task SetProject(object projectJson)
    {
        await _prjStorage.ClearAll();

        var node = ToJsonNode(projectJson);
        if (node == null) return;

        var sections = new List<SqlSection>();

        // devices
        if (node["devices"] is JsonObject devicesObj)
        {
            foreach (var prop in devicesObj)
            {
                sections.Add(new SqlSection { Table = TableType.DEVICES, Name = prop.Key, Value = prop.Value });
            }
        }

        // server
        if (node["server"] != null)
        {
            sections.Add(new SqlSection { Table = TableType.DEVICES, Name = "Server", Value = node["server"]! });
        }

        // hmi.views
        if (node["hmi"]?["views"] is JsonArray viewsArr)
        {
            foreach (var v in viewsArr)
            {
                var id = v?["id"]?.GetValue<string>();
                if (!string.IsNullOrEmpty(id))
                    sections.Add(new SqlSection { Table = TableType.VIEWS, Name = id, Value = v });
            }
        }

        // hmi.layout
        if (node["hmi"]?["layout"] != null)
        {
            sections.Add(new SqlSection { Table = TableType.GENERAL, Name = ProjectDataCmdType.HmiLayout, Value = node["hmi"]!["layout"]! });
        }

        // hmi.mobileLayout
        if (node["hmi"]?["mobileLayout"] != null)
        {
            sections.Add(new SqlSection { Table = TableType.GENERAL, Name = ProjectDataCmdType.MobileLayout, Value = node["hmi"]!["mobileLayout"]! });
        }

        // charts
        if (node["charts"] != null)
        {
            sections.Add(new SqlSection { Table = TableType.GENERAL, Name = ProjectDataCmdType.Charts, Value = node["charts"]! });
        }

        // graphs
        if (node["graphs"] != null)
        {
            sections.Add(new SqlSection { Table = TableType.GENERAL, Name = ProjectDataCmdType.Graphs, Value = node["graphs"]! });
        }

        // languages
        if (node["languages"] != null)
        {
            sections.Add(new SqlSection { Table = TableType.GENERAL, Name = ProjectDataCmdType.Languages, Value = node["languages"]! });
        }

        // version
        if (node["version"] != null)
        {
            sections.Add(new SqlSection { Table = TableType.GENERAL, Name = "Version", Value = node["version"]! });
        }

        // texts
        if (node["texts"] is JsonArray textsArr)
        {
            foreach (var t in textsArr)
            {
                var id = t?["id"]?.GetValue<string>();
                if (!string.IsNullOrEmpty(id))
                    sections.Add(new SqlSection { Table = TableType.TEXTS, Name = id, Value = t });
            }
        }

        // alarms
        if (node["alarms"] is JsonArray alarmsArr)
        {
            foreach (var a in alarmsArr)
            {
                var name = a?["name"]?.GetValue<string>();
                if (!string.IsNullOrEmpty(name))
                    sections.Add(new SqlSection { Table = TableType.ALARMS, Name = name, Value = a });
            }
        }

        // notifications
        if (node["notifications"] is JsonArray notifsArr)
        {
            foreach (var n in notifsArr)
            {
                var id = n?["id"]?.GetValue<string>();
                if (!string.IsNullOrEmpty(id))
                    sections.Add(new SqlSection { Table = TableType.NOTIFICATIONS, Name = id, Value = n });
            }
        }

        // scripts
        if (node["scripts"] is JsonArray scriptsArr)
        {
            foreach (var s in scriptsArr)
            {
                var id = s?["id"]?.GetValue<string>();
                if (!string.IsNullOrEmpty(id))
                    sections.Add(new SqlSection { Table = TableType.SCRIPTS, Name = id, Value = s });
            }
        }

        // reports
        if (node["reports"] is JsonArray reportsArr)
        {
            foreach (var r in reportsArr)
            {
                var id = r?["id"]?.GetValue<string>();
                if (!string.IsNullOrEmpty(id))
                    sections.Add(new SqlSection { Table = TableType.REPORTS, Name = id, Value = r });
            }
        }

        // mapsLocations
        if (node["mapsLocations"] is JsonArray locationsArr)
        {
            foreach (var l in locationsArr)
            {
                var id = l?["id"]?.GetValue<string>();
                if (!string.IsNullOrEmpty(id))
                    sections.Add(new SqlSection { Table = TableType.LOCATIONS, Name = id, Value = l });
            }
        }

        // timestamp
        sections.Add(new SqlSection { Table = TableType.GENERAL, Name = "timestamp", Value = DateTime.Now.ToString("yyyy/M/d HH:mm:ss") });

        if (sections.Count > 0)
        {
            await _prjStorage.SetSections(sections);
        }

        await Load();
    }

    public ProjectData GetProject()
    {
        return data;
    }


    public async Task<ProjectData> _filterProjectPermission()
    {
        //TODO 需要实现权限过滤
        await Task.Delay(1);
        return data;
    }

    public ICollection<KeyValuePair<int, List<Tag>>> GetArchiveDic()
    {
        return ArchiveDic;
    }

    #region Helper methods

    private static JsonNode? ToJsonNode(object value)
    {
        if (value is JsonNode jn) return jn;
        // [FromBody] object? is bound as System.Text.Json.JsonElement by ASP.NET Core.
        string json = value is JsonElement je
            ? je.GetRawText()
            : JsonSerializer.Serialize(value);
        return JsonNode.Parse(json);
    }

    private static void SetOrAddJsonNode(List<JsonNode?> list, string key, JsonNode item)
    {
        var id = item[key]?.GetValue<string>();
        if (string.IsNullOrEmpty(id)) return;
        var idx = FindJsonNodeIndex(list, key, id);
        if (idx >= 0) list[idx] = item;
        else list.Add(item);
    }

    private static bool RemoveJsonNode(List<JsonNode?> list, string key, string value)
    {
        var idx = FindJsonNodeIndex(list, key, value);
        if (idx >= 0)
        {
            list.RemoveAt(idx);
            return true;
        }
        return false;
    }

    private static int FindJsonNodeIndex(List<JsonNode?> list, string key, string value)
    {
        for (int i = 0; i < list.Count; i++)
        {
            if (list[i]?[key]?.GetValue<string>() == value)
                return i;
        }
        return -1;
    }

    #endregion
}
