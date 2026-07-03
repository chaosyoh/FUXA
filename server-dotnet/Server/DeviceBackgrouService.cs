using Core.Const;
using Core.Models;
using Microsoft.AspNetCore.SignalR;
using Runtime;
using System.Threading.Channels;

namespace Server;

public class DeviceBackgrouService : BackgroundService
{
    private readonly Device _device;
    private readonly ILogger<DeviceBackgrouService> _logger;
    private readonly ChannelWriter<Tag> _writer;
    private readonly IDevice _client;

    private const int MaxConnectRetries = 3;
    private const int MaxPollFailures = 3;
    private const int DefaultPollingMs = 3000;
    private const int ReconnectDelayMs = 10000;

    // Degrade (backoff) reconnection parameters
    private readonly bool _degradeEnabled;
    private readonly int _degradeRetryCount;
    private readonly int _degradePeriodMs;

    // 连接状态数值回调（写入 FuxaServer 的 Connection Status 变量）
    private Action<int>? _onConnectionStatus;

    public Device Device => _device;
    public IDevice Client => _client;

    public DeviceBackgrouService(Device device, IDevice client,
        ILogger<DeviceBackgrouService> logger, ChannelWriter<Tag> writer)
    {
        _device = device;
        _client = client;
        _logger = logger;
        _writer = writer;
        _client.Load(device);

        // Initialize degrade parameters from device config
        _degradeEnabled = device.DegradeEnabled ?? true;
        _degradeRetryCount = device.DegradeRetryCount ?? 2;
        _degradePeriodMs = (device.DegradePeriod ?? 60) * 1000;
    }

    /// <summary>
    /// 绑定连接状态数值回调，每轮轮询后根据响应时效计算 0/1/3 并回调
    /// </summary>
    public void BindConnectionStatusCallback(Action<int> callback)
    {
        _onConnectionStatus = callback;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var pollingInterval = _device.Polling > 0 ? _device.Polling : DefaultPollingMs;

        // Degrade state tracking
        var degraded = false;
        var degradeAttempts = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            // 阶段1: 尝试连接
            var connectSuccess = false;
            for (var attempt = 0; attempt < MaxConnectRetries && !stoppingToken.IsCancellationRequested; attempt++)
            {
                try
                {
                    _logger.LogInformation("设备 {DeviceId} ({Type}) 第 {Attempt} 次连接尝试",
                        _device.Id, _device.Type, attempt + 1);

                    connectSuccess = await _client.Connect();
                    if (connectSuccess)
                    {
                        _logger.LogInformation("设备 {DeviceId} 连接成功", _device.Id);
                        break;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "设备 {DeviceId} 连接异常 (尝试 {Attempt}/{Max})",
                        _device.Id, attempt + 1, MaxConnectRetries);
                }

                if (attempt < MaxConnectRetries - 1)
                    await Task.Delay(5000, stoppingToken);
            }

            if (connectSuccess)
            {
                // 连接成功，重置降级状态
                degraded = false;
                degradeAttempts = 0;
            }
            else
            {
                // 连接失败
                try { await _client.Disconnect(); } catch { /* ignore */ }

                if (_degradeEnabled)
                {
                    degradeAttempts++;
                    if (degraded)
                    {
                        // 已在降级模式，按降级周期等待
                        _logger.LogWarning("设备 {DeviceId} 处于降级模式，{Period}s 后重试",
                            _device.Id, _degradePeriodMs / 1000);
                        await Task.Delay(_degradePeriodMs, stoppingToken);
                    }
                    else if (degradeAttempts >= _degradeRetryCount)
                    {
                        // 达到重试阈值，进入降级模式
                        degraded = true;
                        _logger.LogWarning("设备 {DeviceId} 连续 {Count} 次连接失败，进入降级模式，每 {Period}s 重试一次",
                            _device.Id, degradeAttempts, _degradePeriodMs / 1000);
                        await Task.Delay(_degradePeriodMs, stoppingToken);
                    }
                    else
                    {
                        // 还未达到降级阈值，正常等待后重试
                        _logger.LogWarning("设备 {DeviceId} 连接失败 ({Attempt}/{Max})，等待 {Delay}s 后重试",
                            _device.Id, degradeAttempts, _degradeRetryCount, ReconnectDelayMs / 1000);
                        await Task.Delay(ReconnectDelayMs, stoppingToken);
                    }
                }
                else
                {
                    // 未启用降级，使用原有逻辑
                    _logger.LogWarning("设备 {DeviceId} 连接失败 {Max} 次，等待 {Delay}s 后重试",
                        _device.Id, MaxConnectRetries, ReconnectDelayMs / 1000);
                    await Task.Delay(ReconnectDelayMs, stoppingToken);
                }
                continue;
            }

            // 阶段2: 轮询循环
            var pollFailures = 0;
            while (!stoppingToken.IsCancellationRequested && _client.IsConnected())
            {
                try
                {
                    var success = await _client.Polling();
                    if (success)
                    {
                        pollFailures = 0;
                    }
                    else
                    {
                        pollFailures++;
                        _logger.LogWarning("设备 {DeviceId} 轮询失败 ({Failures}/{Max})",
                            _device.Id, pollFailures, MaxPollFailures);
                    }
                }
                catch (Exception ex)
                {
                    pollFailures++;
                    _logger.LogWarning(ex, "设备 {DeviceId} 轮询异常 ({Failures}/{Max})",
                        _device.Id, pollFailures, MaxPollFailures);
                }

                if (pollFailures >= MaxPollFailures)
                {
                    _logger.LogWarning("设备 {DeviceId} 连续 {Max} 次轮询失败，断开重连",
                        _device.Id, MaxPollFailures);
                    try { await _client.Disconnect(); } catch { /* ignore */ }
                    break;
                }

                await Task.Delay(pollingInterval, stoppingToken);

                // 检查连接状态（与 Node.js Device.checkStatus 对齐）
                CheckConnectionStatus();
            }

            // 设备断开后重置降级状态，下次重新从正常重试开始
            degraded = false;
            degradeAttempts = 0;

            // 如果设备断开但不是因为取消，等一段时间再重连
            if (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(5000, stoppingToken);
            }
        }

        // 清理：确保断开连接
        try { await _client.Disconnect(); } catch { /* ignore */ }
    }

    /// <summary>
    /// 根据最后成功读取时间戳计算连接状态数值（0=离线，1=在线，3=警告）
    /// 对应 Node.js 端 Device.checkStatus 中的连接状态判断逻辑
    /// </summary>
    private void CheckConnectionStatus()
    {
        if (_onConnectionStatus == null) return;

        var pollingInterval = _device.Polling > 0 ? _device.Polling : DefaultPollingMs;
        var lastRead = _client.LastReadTimestamp();
        var now = DateTime.Now;

        int status;
        if (lastRead == DateTime.MinValue || (now - lastRead).TotalMilliseconds > pollingInterval * 5)
            status = ConnectionStatus.Off;   // 0
        else if ((now - lastRead).TotalMilliseconds > pollingInterval * 2)
            status = ConnectionStatus.Warning; // 3
        else
            status = ConnectionStatus.On;    // 1

        _onConnectionStatus(status);
    }
}
