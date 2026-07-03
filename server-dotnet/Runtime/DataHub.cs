using Core.Const;
using Core.Models;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Runtime.Alarms;
using Runtime.Project;
using Runtime.Storage;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace Runtime;

public class DataHub : Hub
{
    private readonly ILogger<DataHub> _logger;
    private readonly TagSubscribeService _service;
    private readonly IProjectService _project;
    private readonly IAlarmService _alarmService;
    private readonly DaqStorageService _daqService;
    private readonly IDeviceRegistry _registry;
    private readonly IHttpClientFactory _httpClientFactory;

    public DataHub(
        ILogger<DataHub> logger,
        TagSubscribeService service,
        IProjectService project,
        IAlarmService alarmService,
        DaqStorageService daqService,
        IDeviceRegistry registry,
        IHttpClientFactory httpClientFactory)
    {
        _logger = logger;
        _service = service;
        _project = project;
        _alarmService = alarmService;
        _daqService = daqService;
        _registry = registry;
        _httpClientFactory = httpClientFactory;
    }

    #region 订阅管理

    [HubMethodName(IoEventTypes.DEVICE_TAGS_SUBSCRIBE)]
    public async Task Subscribe(TagsSubscribeMsg msg)
    {
        _service.Subscribe(Context.ConnectionId, msg.TagsId);
    }

    [HubMethodName(IoEventTypes.DEVICE_TAGS_UNSUBSCRIBE)]
    public async Task Unsubscribe(List<string> tagsId)
    {
        _service.UnSubscribe(Context.ConnectionId, tagsId);
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        _service.UnSubscribe(Context.ConnectionId);
        await base.OnDisconnectedAsync(exception);
    }

    #endregion

    #region 设备值 (GET / SET)

    [HubMethodName(IoEventTypes.DEVICE_VALUES)]
    public async Task DeviceValues(DeviceValuesMsg msg)
    {
        if (msg.Cmd == "set" && msg.Var != null)
        {
            await _registry.SetDeviceTagValue(msg.Var.Source, msg.Var.Id, msg.Var.Value!);
            return;
        }

        // GET: 返回所有设备当前值
        var projectData = _project.GetProject();
        foreach (var device in projectData.Devices.Values)
        {
            var values = device.Tags.Select(t => new
            {
                id = t.Key,
                value = t.Value.Value,
                timestamp = t.Value.Timestamp
            }).ToList();

            if (values.Count > 0)
            {
                await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_VALUES, [new { values }]);
            }
        }
    }

    #endregion

    #region 设备状态

    [HubMethodName(IoEventTypes.DEVICE_STATUS)]
    public async Task GetDeviceStatus(string cmd)
    {
        var statuses = _registry.GetAllDeviceStatuses();
        var projectData = _project.GetProject();
        foreach (var d in projectData.Devices.Values)
        {
            var status = statuses.TryGetValue(d.Id, out var s) ? s : DeviceStatus.Off;
            await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_STATUS, [new
            {
                id = d.Id,
                status
            }]);
        }
    }

    #endregion

    #region 设备浏览 (OPC UA)

    [HubMethodName(IoEventTypes.DEVICE_BROWSE)]
    public async Task DeviceBrowse(DeviceBrowseMsg msg)
    {
        try
        {
            var client = _registry.GetDeviceClient(msg.Device);
            if (client is IBrowsableDevice browsable)
            {
                var nodeId = msg.Node?.Id;
                var result = await browsable.Browse(nodeId);
                await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_BROWSE, [new
                {
                    device = msg.Device,
                    node = msg.Node,
                    result
                }]);
            }
            else
            {
                await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_BROWSE, [new
                {
                    device = msg.Device,
                    node = msg.Node,
                    error = "device not found or browse not supported"
                }]);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "设备 {DeviceId} 浏览节点 {Node} 失败", msg.Device, msg.Node?.Id);
            await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_BROWSE, [new
            {
                device = msg.Device,
                node = msg.Node,
                error = ex.Message
            }]);
        }
    }

    #endregion

    #region 节点属性 (OPC UA)

    [HubMethodName(IoEventTypes.DEVICE_NODE_ATTRIBUTE)]
    public async Task DeviceNodeAttribute(DeviceNodeAttrMsg msg)
    {
        try
        {
            var client = _registry.GetDeviceClient(msg.Device);
            if (client is IBrowsableDevice browsable)
            {
                var nodeId = msg.Node?.Id;
                var attribute = await browsable.ReadNodeAttribute(nodeId);
                // 客户端期望: { device, node: { id, attribute: { 14: "UInt16", 13: "R/W" } } }
                await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_NODE_ATTRIBUTE, [new
                {
                    device = msg.Device,
                    node = new
                    {
                        id = msg.Node?.Id,
                        name = msg.Node?.Id,
                        attribute
                    }
                }]);
            }
            else
            {
                await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_NODE_ATTRIBUTE, [new
                {
                    device = msg.Device,
                    node = msg.Node,
                    error = "device not found or read attribute not supported"
                }]);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "设备 {DeviceId} 读取节点属性 {Node} 失败", msg.Device, msg.Node?.Id);
            await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_NODE_ATTRIBUTE, [new
            {
                device = msg.Device,
                node = msg.Node,
                error = ex.Message
            }]);
        }
    }

    #endregion

    #region 设备属性 (OPC UA Endpoints)

    [HubMethodName(IoEventTypes.DEVICE_PROPERTY)]
    public async Task DeviceProperty(DevicePropertyMsg msg)
    {
        try
        {
            if (string.IsNullOrEmpty(msg.Endpoint) || string.IsNullOrEmpty(msg.Type))
            {
                await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_PROPERTY, [new
                {
                    endpoint = msg.Endpoint,
                    type = msg.Type,
                    error = "wrong message"
                }]);
                return;
            }

            var result = await _registry.GetDeviceProperty(msg.Endpoint, msg.Type);
            await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_PROPERTY, [new
            {
                endpoint = msg.Endpoint,
                type = msg.Type,
                result
            }]);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "获取设备属性失败 endpoint={Endpoint} type={Type}", msg.Endpoint, msg.Type);
            await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_PROPERTY, [new
            {
                endpoint = msg.Endpoint,
                type = msg.Type,
                error = ex.Message
            }]);
        }
    }

    #endregion

    #region WebAPI 测试请求

    [HubMethodName(IoEventTypes.DEVICE_WEBAPI_REQUEST)]
    public async Task DeviceWebApiRequest(DeviceWebApiMsg msg)
    {
        try
        {
            if (msg.Property == null)
            {
                await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_WEBAPI_REQUEST, [new
                {
                    property = msg.Property,
                    error = "wrong message"
                }]);
                return;
            }

            var httpClient = _httpClientFactory.CreateClient();
            httpClient.Timeout = TimeSpan.FromSeconds(10);

            var method = msg.Property.Method?.ToUpperInvariant() ?? "GET";
            var address = msg.Property.Address ?? "";

            HttpResponseMessage response;
            if (method == "POST")
            {
                var content = new StringContent(msg.Property.Body ?? "", System.Text.Encoding.UTF8, "application/json");
                response = await httpClient.PostAsync(address, content);
            }
            else
            {
                response = await httpClient.GetAsync(address);
            }

            var body = await response.Content.ReadAsStringAsync();
            await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_WEBAPI_REQUEST, [new
            {
                property = msg.Property,
                result = body
            }]);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "WebAPI 测试请求失败");
            await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_WEBAPI_REQUEST, [new
            {
                property = msg.Property,
                error = ex.Message
            }]);
        }
    }

    #endregion

    #region 标签发现

    [HubMethodName(IoEventTypes.DEVICE_TAGS_REQUEST)]
    public async Task DeviceTagsRequest(DeviceTagsReqMsg msg)
    {
        try
        {
            var client = _registry.GetDeviceClient(msg.DeviceId);
            if (client is ITagDiscoverable discoverable)
            {
                var result = await discoverable.DiscoverTags();
                await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_TAGS_REQUEST, [new
                {
                    deviceId = msg.DeviceId,
                    result
                }]);
            }
            else
            {
                await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_TAGS_REQUEST, [new
                {
                    deviceId = msg.DeviceId,
                    error = "device not found or tag discovery not supported"
                }]);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "设备 {DeviceId} 标签发现失败", msg.DeviceId);
            await Clients.Caller.SendCoreAsync(IoEventTypes.DEVICE_TAGS_REQUEST, [new
            {
                deviceId = msg.DeviceId,
                error = ex.Message
            }]);
        }
    }

    #endregion

    #region 启用/禁用设备

    [HubMethodName(IoEventTypes.DEVICE_ENABLE)]
    public async Task DeviceEnable(DeviceEnableMsg msg)
    {
        try
        {
            _registry.SetDeviceEnabled(msg.DeviceName, msg.Enable);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "设备 {DeviceName} 启用/禁用失败", msg.DeviceName);
        }
    }

    #endregion

    #region 重启设备采集

    [HubMethodName(IoEventTypes.DEVICE_RESTART)]
    public async Task DeviceRestart(DeviceRestartMsg msg)
    {
        try
        {
            if (!string.IsNullOrEmpty(msg.Device))
            {
                await _registry.RestartDevice(msg.Device);
                _logger.LogInformation("设备 {DeviceId} 采集已通过 SignalR 重启", msg.Device);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "设备 {DeviceId} 重启采集失败", msg.Device);
        }
    }

    #endregion

    #region 报警状态

    [HubMethodName(IoEventTypes.ALARMS_STATUS)]
    public async Task GetAlarmStatus(string cmd)
    {
        var status = _alarmService.GetAlarmsStatus();
        await Clients.Caller.SendCoreAsync(IoEventTypes.ALARMS_STATUS, [status]);
    }

    #endregion

    #region DAQ 历史查询

    [HubMethodName(IoEventTypes.DAQ_QUERY)]
    public async Task DaqQuery(DaqQueryMsg msg)
    {
        try
        {
            if (msg.Sids == null || msg.Sids.Count == 0) return;

            var result = await _daqService.GetNodesValues(msg.Sids, msg.From, msg.To);
            await Clients.Caller.SendCoreAsync(IoEventTypes.DAQ_RESULT, [new
            {
                gid = msg.Gid,
                result
            }]);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DAQ query error");
        }
    }

    #endregion

    #region 主机网络接口

    [HubMethodName(IoEventTypes.HOST_INTERFACES)]
    public async Task GetHostInterfaces(string cmd)
    {
        try
        {
            var interfaces = NetworkInterface.GetAllNetworkInterfaces()
                .Where(ni => ni.OperationalStatus == OperationalStatus.Up)
                .Select(ni =>
                {
                    var ipProps = ni.GetIPProperties();
                    var ipv4 = ipProps.UnicastAddresses
                        .FirstOrDefault(a => a.Address.AddressFamily == AddressFamily.InterNetwork);
                    return new
                    {
                        name = ni.Name,
                        address = ipv4?.Address.ToString() ?? string.Empty,
                        mac = ni.GetPhysicalAddress().ToString(),
                    };
                })
                .Where(x => !string.IsNullOrEmpty(x.address))
                .ToList();

            await Clients.Caller.SendCoreAsync(IoEventTypes.HOST_INTERFACES, [interfaces]);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get host interfaces");
        }
    }

    #endregion

    #region 项目更新通知

    /// <summary>
    /// Editor 请求后端向所有客户端广播项目更新通知
    /// </summary>
    [HubMethodName(IoEventTypes.PROJECT_NOTIFY_UPDATE)]
    public async Task NotifyProjectUpdate()
    {
        try
        {
            var projectData = _project.GetProject();
            var timestamp = projectData.Timestamp?.ToString("yyyy/M/d HH:mm:ss")
                ?? DateTime.Now.ToString("yyyy/M/d HH:mm:ss");
            await Clients.All.SendCoreAsync(IoEventTypes.PROJECT_UPDATED, [new { timestamp }]);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to notify project update");
        }
    }

    #endregion
}

#region 消息模型

public class TagsSubscribeMsg
{
    public List<string> TagsId { get; set; } = new List<string>();
}

public class DeviceValuesMsg
{
    public string Cmd { get; set; } = string.Empty;
    public DeviceValueVar? Var { get; set; }
}

public class DeviceValueVar
{
    public string Source { get; set; } = string.Empty;
    public string Id { get; set; } = string.Empty;
    public object? Value { get; set; }
}

public class DaqQueryMsg
{
    public List<string>? Sids { get; set; }
    public long From { get; set; }
    public long To { get; set; }
    public string? Gid { get; set; }
}

public class DeviceBrowseMsg
{
    public string Device { get; set; } = string.Empty;
    public BrowseNode? Node { get; set; }
}

public class BrowseNode
{
    public string Id { get; set; } = string.Empty;
    public string? Parent { get; set; }
}

public class DeviceNodeAttrMsg
{
    public string Device { get; set; } = string.Empty;
    public BrowseNode? Node { get; set; }
}

public class DevicePropertyMsg
{
    public string Endpoint { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
}

public class DeviceWebApiMsg
{
    public WebApiProperty? Property { get; set; }
}

public class WebApiProperty
{
    public string? Method { get; set; }
    public string? Address { get; set; }
    public string? Body { get; set; }
}

public class DeviceTagsReqMsg
{
    public string DeviceId { get; set; } = string.Empty;
}

public class DeviceEnableMsg
{
    public string DeviceName { get; set; } = string.Empty;
    public bool Enable { get; set; }
}

public class DeviceRestartMsg
{
    public string Device { get; set; } = string.Empty;
}

#endregion
