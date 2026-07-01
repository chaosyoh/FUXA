using Core.Const;
using Core.Models;
using Microsoft.AspNetCore.SignalR;
using Runtime;
using Runtime.Project;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace Server;

public class DeviceManager : BackgroundService, IDeviceRegistry
{
    private readonly ILogger<DeviceManager> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly TimeSpan _reloadInterval = TimeSpan.FromSeconds(30);
    private readonly ConcurrentDictionary<string, DeviceBackgrouService> _activeCollectors = new();
    private readonly IProjectService _project;
    private CancellationToken _stoppingToken;

    public DeviceManager(ILogger<DeviceManager> logger, IServiceProvider serviceProvider, IProjectService project)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
        _project = project;
    }

    #region IDeviceRegistry

    public IDevice? GetDeviceClient(string deviceId)
    {
        return _activeCollectors.TryGetValue(deviceId, out var collector) ? collector.Client : null;
    }

    public Dictionary<string, string> GetAllDeviceStatuses()
    {
        var result = new Dictionary<string, string>();
        foreach (var kv in _activeCollectors)
        {
            result[kv.Key] = kv.Value.Client.GetStatus();
        }
        return result;
    }

    public void SetDeviceEnabled(string deviceId, bool enabled)
    {
        var projectData = _project.GetProject();
        if (projectData.Devices.TryGetValue(deviceId, out var device))
        {
            device.Enabled = enabled;
            _logger.LogInformation("设备 {DeviceId} 已{Action}", deviceId, enabled ? "启用" : "禁用");
            _ = Task.Run(async () =>
            {
                try { await ReloadDevicesAsync(_stoppingToken); }
                catch (Exception ex) { _logger.LogError(ex, "设备启用/禁用后重载异常"); }
            });
        }
    }

    public async Task<object?> GetDeviceProperty(string endpoint, string type)
    {
        if (type == DeviceType.OPCUA)
        {
            return await OpcUA.OpcUAClient.GetEndpointsStatic(endpoint);
        }
        return null;
    }

    public async Task<bool> SetTagValue(string tagId, object value)
    {
        foreach (var kv in _activeCollectors)
        {
            var client = kv.Value.Client;
            var tag = client.GetTagProperty(tagId);
            if (tag != null)
            {
                value = DeviceBase.PrepareWriteValue(value, tag);
                return await client.SetValue(tagId, value);
            }
        }
        return false;
    }

    public async Task<bool> SetDeviceTagValue(string deviceId, string tagId, object value)
    {
        if (!_activeCollectors.TryGetValue(deviceId, out var collector))
        {
            _logger.LogWarning("写入失败：设备 {DeviceId} 不存在或未连接", deviceId);
            return false;
        }

        var client = collector.Client;
        var tag = client.GetTagProperty(tagId);
        if (tag == null)
        {
            _logger.LogWarning("写入失败：设备 {DeviceId} 中未找到标签 {TagId}", deviceId, tagId);
            return false;
        }

        // Access control: reject write to read-only tags
        if (tag.Access == "ro")
        {
            _logger.LogWarning("设备 {DeviceId} 写入变量 {TagId} 被拒绝: 只读权限", deviceId, tagId);
            return false;
        }

        // Preprocess: unwrap JsonElement + apply reverse scaling
        value = DeviceBase.PrepareWriteValue(value, tag);

        try
        {
            return await client.SetValue(tagId, value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "设备 {DeviceId} 写入变量 {TagId} 失败", deviceId, tagId);
            return false;
        }
    }

    public async Task RestartDevice(string deviceId)
    {
        var projectData = _project.GetProject();
        if (!projectData.Devices.TryGetValue(deviceId, out var config))
        {
            _logger.LogWarning("重启设备 {DeviceId} 失败：设备不存在", deviceId);
            return;
        }

        if (_activeCollectors.TryRemove(deviceId, out var existingCollector))
        {
            await StopCollectorAsync(existingCollector, _stoppingToken);
            _logger.LogInformation("设备 {DeviceId} 已停止，准备重启", deviceId);
        }

        if (config.Enabled)
        {
            var newCollector = await CreateCollectorAsync(config, _stoppingToken);
            if (newCollector != null)
            {
                _activeCollectors.TryAdd(deviceId, newCollector);
                _logger.LogInformation("设备 {DeviceId} 采集已重启", deviceId);
            }
        }
    }

    #endregion

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stoppingToken = stoppingToken;

        await ReloadDevicesAsync(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_reloadInterval, stoppingToken);
                await ReloadDevicesAsync(stoppingToken);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "设备配置重载异常");
            }
        }
    }

    private async Task ReloadDevicesAsync(CancellationToken cancellationToken)
    {
        var projectData = _project.GetProject();

        foreach (var kv in _activeCollectors)
        {
            if (!projectData.Devices.TryGetValue(kv.Key, out var devCfg) || !devCfg.Enabled)
            {
                _logger.LogInformation("检测到设备 {DeviceId} 已移除或禁用，停止采集", kv.Key);
                await StopCollectorAsync(kv.Value, cancellationToken);
                _activeCollectors.TryRemove(kv.Key, out _);
            }
        }

        foreach (var newConfig in projectData.Devices.Values)
        {
            if (!newConfig.Enabled) continue;

            if (_activeCollectors.TryGetValue(newConfig.Id, out var existingCollector))
            {
                if (HasConfigChanged(existingCollector.Device, newConfig))
                {
                    _logger.LogInformation("设备 {DeviceId} 配置已变更，重启采集任务", newConfig.Id);
                    await StopCollectorAsync(existingCollector, cancellationToken);
                    _activeCollectors.TryRemove(newConfig.Id, out _);
                    var newCollector = await CreateCollectorAsync(newConfig, cancellationToken);
                    if (newCollector != null)
                        _activeCollectors.TryAdd(newConfig.Id, newCollector);
                }
            }
            else
            {
                var collector = await CreateCollectorAsync(newConfig, cancellationToken);
                if (collector != null)
                {
                    _logger.LogInformation("检测到新设备 {DeviceId} ({Type})，启动采集", newConfig.Id, newConfig.Type);
                    _activeCollectors.TryAdd(newConfig.Id, collector);
                }
            }
        }
    }

    private bool HasConfigChanged(Device oldConfig, Device newConfig)
    {
        if (oldConfig.Type != newConfig.Type) return true;
        if (oldConfig.Polling != newConfig.Polling) return true;
        if (oldConfig.Enabled != newConfig.Enabled) return true;
        if (oldConfig.Property.Address != newConfig.Property.Address) return true;
        if (oldConfig.Property.Port != newConfig.Property.Port) return true;
        if (oldConfig.Property.Rack != newConfig.Property.Rack) return true;
        if (oldConfig.Property.Slot != newConfig.Property.Slot) return true;
        if (oldConfig.Property.Baudrate != newConfig.Property.Baudrate) return true;
        if (oldConfig.Property.Databits != newConfig.Property.Databits) return true;
        if (oldConfig.Property.SlaveId != newConfig.Property.SlaveId) return true;
        if (oldConfig.Property.Stopbits != newConfig.Property.Stopbits) return true;
        if (oldConfig.Property.ClientId != newConfig.Property.ClientId) return true;
        if (oldConfig.Property.Username != newConfig.Property.Username) return true;
        if (oldConfig.Property.GetUrl != newConfig.Property.GetUrl) return true;
        if (oldConfig.Property.PostUrl != newConfig.Property.PostUrl) return true;
        return false;
    }

    private async Task<DeviceBackgrouService?> CreateCollectorAsync(Device config, CancellationToken cancellationToken)
    {
        var scope = _serviceProvider.CreateScope();
        var hubCtx = scope.ServiceProvider.GetRequiredService<IHubContext<DataHub>>();

        var client = DeviceFactory.Create(config, hubCtx, scope.ServiceProvider);
        if (client == null)
        {
            _logger.LogWarning("不支持的设备类型: {Type} (设备 {DeviceId})，跳过", config.Type, config.Id);
            scope.Dispose();
            return null;
        }

        // Inject registry for FuxaServer to enable cross-device tag access
        if (client is FuxaServerClient fsc)
        {
            fsc.BindRegistry(this);
        }

        var logger = scope.ServiceProvider.GetRequiredService<ILogger<DeviceBackgrouService>>();
        var writer = scope.ServiceProvider.GetRequiredService<ChannelWriter<Tag>>();
        var collector = new DeviceBackgrouService(config, client, logger, writer);
        await collector.StartAsync(cancellationToken);
        return collector;
    }

    private async Task StopCollectorAsync(DeviceBackgrouService collector, CancellationToken cancellationToken)
    {
        try
        {
            await collector.StopAsync(cancellationToken);
            collector.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "停止采集器时发生错误");
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var kv in _activeCollectors)
        {
            await StopCollectorAsync(kv.Value, cancellationToken);
        }
        _activeCollectors.Clear();
        await base.StopAsync(cancellationToken);
    }
}
