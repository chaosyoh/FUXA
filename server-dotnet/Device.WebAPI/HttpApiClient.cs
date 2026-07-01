using Core.Const;
using Core.Models;
using Microsoft.AspNetCore.SignalR;
using Runtime;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace DeviceWebAPI;

public class HttpApiClient : DeviceBase, IWebApiTestable, ITagDiscoverable, IDisposable
{
    private readonly HttpClient _httpClient = new();
    private string? _getUrl;
    private string? _postUrl;
    private DateTime _lastTimestampRequest = DateTime.MinValue;

    // Maps tag.Address (JSON path) -> list of tags
    private readonly Dictionary<string, List<Tag>> _requestItemsMap = new();

    public HttpApiClient(IHubContext<DataHub> hubCtx) : base(hubCtx)
    {
        _httpClient.Timeout = TimeSpan.FromSeconds(10);
    }

    public override void Load(Device data)
    {
        base.Load(data);
        BuildRequestMap();
    }

    private void BuildRequestMap()
    {
        _requestItemsMap.Clear();
        foreach (var tag in Data.Tags.Values)
        {
            var key = !string.IsNullOrEmpty(tag.Address) ? tag.Address : tag.Id;
            if (!_requestItemsMap.TryGetValue(key, out var list))
            {
                list = [];
                _requestItemsMap[key] = list;
            }
            list.Add(tag);
        }
    }

    public override async Task<bool> Connect()
    {
        await NotifyStatus(DeviceStatus.Busy);

        // Determine URLs
        _getUrl = Data.Property.GetUrl;
        _postUrl = Data.Property.PostUrl;

        // Fall back to Address as GET URL
        if (string.IsNullOrEmpty(_getUrl) && !string.IsNullOrEmpty(Data.Property.Address))
            _getUrl = Data.Property.Address;

        if (string.IsNullOrEmpty(_getUrl))
        {
            await NotifyStatus(DeviceStatus.Error);
            return false;
        }

        connected = true;
        _lastTimestampRequest = DateTime.Now;
        await NotifyStatus(DeviceStatus.Ok);
        return true;
    }

    public override async Task<bool> Disconnect()
    {
        connected = false;
        ClearVarsValue();
        await NotifyStatus(DeviceStatus.Off);
        return true;
    }

    public override async Task<bool> Polling()
    {
        if (!connected || string.IsNullOrEmpty(_getUrl)) return false;
        if (!CheckWorking(true)) return false;

        try
        {
            // Timeout check: if no response for 3x polling interval
            var pollingInterval = Data.Polling > 0 ? Data.Polling : 3000;
            if (_lastTimestampRequest != DateTime.MinValue &&
                (DateTime.Now - _lastTimestampRequest).TotalMilliseconds > pollingInterval * 3)
            {
                await NotifyStatus(DeviceStatus.Error);
            }

            var response = await _httpClient.GetStringAsync(_getUrl);
            _lastTimestampRequest = DateTime.Now;

            if (string.IsNullOrEmpty(response))
            {
                CheckWorking(false);
                return true;
            }

            // Try "own" format first: [{id, value, type}, ...]
            if (TryParseOwnFormat(response))
            {
                // Values updated
            }
            else
            {
                // Custom JSON: flatten and map
                ParseCustomJson(response);
            }

            lastReadTimestamp = DateTime.Now;
            AddDaq();
            await NotifyStatus(DeviceStatus.Ok);
            CheckWorking(false);
            return true;
        }
        catch (Exception)
        {
            CheckWorking(false);
            await NotifyStatus(DeviceStatus.Error);
            return false;
        }
    }

    private bool TryParseOwnFormat(string json)
    {
        try
        {
            var arr = JsonNode.Parse(json)?.AsArray();
            if (arr == null) return false;
            var matched = false;
            foreach (var item in arr)
            {
                var id = item?["id"]?.GetValue<string>();
                if (id == null) continue;
                var value = item?["value"];
                if (Data.Tags.TryGetValue(id, out var tag))
                {
                    tag.Value = JsonNodeHelper.GetNodeValue(value);
                    matched = true;
                }
            }
            return matched;
        }
        catch
        {
            return false;
        }
    }

    private void ParseCustomJson(string json)
    {
        try
        {
            var node = JsonNode.Parse(json);
            if (node == null) return;
            var flat = JsonFlattener.Flatten(node);

            foreach (var (path, value) in flat)
            {
                if (_requestItemsMap.TryGetValue(path, out var tags))
                {
                    foreach (var tag in tags)
                        tag.Value = value;
                }
            }
        }
        catch
        {
            // JSON parse failed, ignore
        }
    }

    public override async Task<bool> SetValue(string id, object value)
    {
        if (!connected || string.IsNullOrEmpty(_postUrl)) return false;
        if (!Data.Tags.TryGetValue(id, out var tag)) return false;

        try
        {
            var payload = JsonSerializer.Serialize(new[] { new { id, value } });
            var content = new StringContent(payload, System.Text.Encoding.UTF8, "application/json");
            var response = await _httpClient.PostAsync(_postUrl, content);
            _lastTimestampRequest = DateTime.Now;

            if (response.IsSuccessStatusCode)
            {
                tag.Value = value;
                return true;
            }
            return false;
        }
        catch
        {
            return false;
        }
    }

    #region IWebApiTestable

    public async Task<object?> TestRequest(object property)
    {
        JsonNode? jObj;
        if (property is JsonElement je)
            jObj = JsonNode.Parse(je.GetRawText());
        else if (property is JsonNode jn)
            jObj = jn;
        else
            jObj = JsonNode.Parse(JsonSerializer.Serialize(property));

        var method = jObj?["method"]?.GetValue<string>()?.ToUpperInvariant() ?? "GET";
        var address = jObj?["address"]?.GetValue<string>() ?? "";
        var body = jObj?["body"]?.ToJsonString();

        HttpResponseMessage response;
        if (method == "POST")
        {
            var content = new StringContent(body ?? "", System.Text.Encoding.UTF8, "application/json");
            response = await _httpClient.PostAsync(address, content);
        }
        else
        {
            response = await _httpClient.GetAsync(address);
        }

        var responseBody = await response.Content.ReadAsStringAsync();
        return responseBody;
    }

    #endregion

    #region ITagDiscoverable

    public Task<object?> DiscoverTags()
    {
        var tags = new List<object>();
        foreach (var kv in _requestItemsMap)
        {
            foreach (var tag in kv.Value)
            {
                tags.Add(new
                {
                    id = tag.Id,
                    name = tag.Name,
                    address = tag.Address,
                    type = tag.Type,
                    value = tag.Value
                });
            }
        }
        return Task.FromResult<object?>(tags);
    }

    #endregion

    public void Dispose()
    {
        _httpClient.Dispose();
    }
}

/// <summary>
/// JSON flattener: recursively walks JSON and produces flat dictionary with ":" separator
/// Example: {"a":{"b":1},"c":[10,20]} -> {"a:b":1, "c:[0]":10, "c:[1]":20}
/// </summary>
public static class JsonFlattener
{
    public static Dictionary<string, object?> Flatten(JsonNode node, string prefix = "")
    {
        var result = new Dictionary<string, object?>();
        FlattenInternal(node, prefix, result);
        return result;
    }

    private static void FlattenInternal(JsonNode? node, string prefix, Dictionary<string, object?> result)
    {
        if (node is JsonObject obj)
        {
            foreach (var prop in obj)
            {
                var key = string.IsNullOrEmpty(prefix) ? prop.Key : $"{prefix}:{prop.Key}";
                FlattenInternal(prop.Value, key, result);
            }
        }
        else if (node is JsonArray arr)
        {
            var index = 0;
            foreach (var item in arr)
            {
                var key = $"{prefix}:[{index}]";
                FlattenInternal(item, key, result);
                index++;
            }
        }
        else if (node is JsonValue val)
        {
            result[prefix] = GetPrimitiveValue(val);
        }
        else
        {
            result[prefix] = null;
        }
    }

    private static object? GetPrimitiveValue(JsonValue val)
    {
        if (val.TryGetValue<long>(out var l)) return l;
        if (val.TryGetValue<double>(out var d)) return d;
        if (val.TryGetValue<bool>(out var b)) return b;
        if (val.TryGetValue<string>(out var s)) return s;
        return val.ToJsonString();
    }
}

file static class JsonNodeHelper
{
    public static object? GetNodeValue(JsonNode? node)
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
