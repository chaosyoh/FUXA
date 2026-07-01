using Core.Const;
using Core.Models;
using Microsoft.AspNetCore.SignalR;
using MQTTnet;
using Runtime;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace DeviceMQTT;

public class MqttDeviceClient : DeviceBase, IDisposable
{
    private IMqttClient? _mqttClient;
    private readonly MqttClientFactory _mqttFactory = new();

    // topic -> list of tags subscribed to that topic
    private readonly Dictionary<string, List<Tag>> _topicsMap = new();

    public MqttDeviceClient(IHubContext<DataHub> hubCtx) : base(hubCtx)
    {
    }

    public override void Load(Device data)
    {
        base.Load(data);
        BuildTopicsMap();
    }

    private void BuildTopicsMap()
    {
        _topicsMap.Clear();
        foreach (var tag in Data.Tags.Values)
        {
            if (string.IsNullOrEmpty(tag.Address)) continue;
            if (!_topicsMap.TryGetValue(tag.Address, out var list))
            {
                list = [];
                _topicsMap[tag.Address] = list;
            }
            list.Add(tag);
        }
    }

    public override async Task<bool> Connect()
    {
        if (!CheckWorking(true)) return false;
        await NotifyStatus(DeviceStatus.Busy);

        try
        {
            _mqttClient?.Dispose();
            _mqttClient = _mqttFactory.CreateMqttClient();

            var address = Data.Property.Address;
            if (string.IsNullOrEmpty(address))
            {
                CheckWorking(false);
                await NotifyStatus(DeviceStatus.Error);
                return false;
            }

            // Parse broker URL: mqtt://host:port or tcp://host:port or just host:port
            var uri = address.Contains("://") ? new Uri(address) : new Uri("mqtt://" + address);
            var host = uri.Host;
            var port = uri.Port > 0 ? uri.Port : 1883;

            var optionsBuilder = new MqttClientOptionsBuilder()
                .WithTcpServer(host, port);

            if (!string.IsNullOrEmpty(Data.Property.ClientId))
                optionsBuilder.WithClientId(Data.Property.ClientId);
            else
                optionsBuilder.WithClientId($"fuxa_{Data.Id}_{Guid.NewGuid():N}"[..32]);

            if (!string.IsNullOrEmpty(Data.Property.Username))
                optionsBuilder.WithCredentials(Data.Property.Username, Data.Property.Password ?? "");

            var options = optionsBuilder.Build();

            _mqttClient.ApplicationMessageReceivedAsync += OnMessageReceived;

            await _mqttClient.ConnectAsync(options);

            // Subscribe to all topics
            var subscribeBuilder = _mqttFactory.CreateSubscribeOptionsBuilder();
            foreach (var topic in _topicsMap.Keys)
            {
                subscribeBuilder.WithTopicFilter(f => f.WithTopic(topic));
            }
            await _mqttClient.SubscribeAsync(subscribeBuilder.Build());

            connected = true;
            CheckWorking(false);
            await NotifyStatus(DeviceStatus.Ok);
            return true;
        }
        catch (Exception)
        {
            connected = false;
            CheckWorking(false);
            await NotifyStatus(DeviceStatus.Error);
            return false;
        }
    }

    private Task OnMessageReceived(MqttApplicationMessageReceivedEventArgs e)
    {
        var topic = e.ApplicationMessage.Topic;
        if (topic == null || !_topicsMap.TryGetValue(topic, out var tags)) return Task.CompletedTask;

        var payload = e.ApplicationMessage.ConvertPayloadToString();
        if (payload == null) return Task.CompletedTask;

        foreach (var tag in tags)
        {
            try
            {
                var tagType = tag.Type?.ToLowerInvariant();
                if (tagType == "json" && !string.IsNullOrEmpty(tag.Memaddress))
                {
                    // Parse JSON and extract field by path
                    var jsonNode = JsonNode.Parse(payload);
                    var token = GetByPath(jsonNode, tag.Memaddress);
                    tag.Value = GetNodeValue(token);
                }
                else
                {
                    // Raw value: try numeric conversion first
                    if (double.TryParse(payload, out var numVal))
                        tag.Value = numVal;
                    else
                        tag.Value = payload;
                }
            }
            catch
            {
                tag.Value = payload;
            }
        }

        return Task.CompletedTask;
    }

    public override async Task<bool> Disconnect()
    {
        try
        {
            if (_mqttClient?.IsConnected == true)
                await _mqttClient.DisconnectAsync();
            connected = false;
            monitored = false;
            ClearVarsValue();
            await NotifyStatus(DeviceStatus.Off);
            return true;
        }
        catch
        {
            connected = false;
            return false;
        }
    }

    public override async Task<bool> Polling()
    {
        if (!connected) return false;
        if (_mqttClient?.IsConnected != true)
        {
            connected = false;
            return false;
        }

        // MQTT is push-based: values arrive via subscription callbacks
        // Polling only triggers DAQ and updates timestamp
        lastReadTimestamp = DateTime.Now;
        AddDaq();
        return true;
    }

    public override async Task<bool> SetValue(string id, object value)
    {
        if (!connected || _mqttClient?.IsConnected != true) return false;
        if (!Data.Tags.TryGetValue(id, out var tag)) return false;
        if (string.IsNullOrEmpty(tag.Address)) return false;

        try
        {
            string payload;
            var tagType = tag.Type?.ToLowerInvariant();
            if (tagType == "json" && !string.IsNullOrEmpty(tag.Memaddress))
            {
                var jsonObj = new JsonObject { [tag.Memaddress] = JsonValue.Create(value) };
                payload = jsonObj.ToJsonString();
            }
            else
            {
                payload = value?.ToString() ?? "";
            }

            var msg = new MqttApplicationMessageBuilder()
                .WithTopic(tag.Address)
                .WithPayload(payload)
                .WithRetainFlag(true)
                .Build();

            await _mqttClient.PublishAsync(msg);
            tag.Value = value;
            return true;
        }
        catch
        {
            return false;
        }
    }

    public void Dispose()
    {
        try { _mqttClient?.Dispose(); } catch { }
    }

    /// <summary>
    /// Navigate JsonNode by dot-separated path (e.g. "a.b.c" or "arr[0].field")
    /// </summary>
    private static JsonNode? GetByPath(JsonNode? node, string path)
    {
        if (node == null || string.IsNullOrEmpty(path)) return node;
        var parts = path.Split('.');
        JsonNode? current = node;
        foreach (var part in parts)
        {
            if (current == null) return null;
            // Check for array index like "items[0]"
            var bracketIdx = part.IndexOf('[');
            if (bracketIdx >= 0)
            {
                var propName = part[..bracketIdx];
                if (!string.IsNullOrEmpty(propName))
                    current = current[propName];
                var idxStr = part[(bracketIdx + 1)..^1];
                if (int.TryParse(idxStr, out var arrIdx) && current is JsonArray arr)
                    current = arr[arrIdx];
                else
                    return null;
            }
            else
            {
                current = current[part];
            }
        }
        return current;
    }

    private static object? GetNodeValue(JsonNode? node)
    {
        if (node == null) return null;
        if (node is JsonValue val)
        {
            if (val.TryGetValue<long>(out var l)) return l;
            if (val.TryGetValue<double>(out var d)) return d;
            if (val.TryGetValue<bool>(out var b)) return b;
            if (val.TryGetValue<string>(out var s)) return s;
            return val.ToJsonString();
        }
        return node.ToJsonString();
    }
}
