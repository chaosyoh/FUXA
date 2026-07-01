namespace Runtime;

/// <summary>
/// 设备注册表：让 DataHub 能够访问运行中的设备实例
/// 由 DeviceManager 实现
/// </summary>
public interface IDeviceRegistry
{
    /// <summary>
    /// 获取指定设备的运行时客户端
    /// </summary>
    IDevice? GetDeviceClient(string deviceId);

    /// <summary>
    /// 获取所有活跃设备的状态
    /// </summary>
    Dictionary<string, string> GetAllDeviceStatuses();

    /// <summary>
    /// 启用/禁用设备
    /// </summary>
    void SetDeviceEnabled(string deviceId, bool enabled);

    /// <summary>
    /// 获取设备属性（如 OPC UA 端点安全策略），无状态操作
    /// </summary>
    Task<object?> GetDeviceProperty(string endpoint, string type);

    /// <summary>
    /// 通过 tag ID 查找所属设备并写值（报警联动 SetValue 动作）
    /// </summary>
    Task<bool> SetTagValue(string tagId, object value);

    /// <summary>
    /// 通过已知设备ID和tagID写值（含权限校验 + 值预处理 + 写入），
    /// 供 DataHub 使用
    /// </summary>
    Task<bool> SetDeviceTagValue(string deviceId, string tagId, object value);

    /// <summary>
    /// 重启指定设备的采集任务（停止后重新加载配置并启动）
    /// </summary>
    Task RestartDevice(string deviceId);
}
