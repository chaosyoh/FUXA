using Core.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace Runtime;

public interface IDevice
{
    /// <summary>
    /// 连接设备
    /// </summary>
    /// <returns></returns>
    public Task<bool> Connect();
    /// <summary>
    /// 断开连接
    /// </summary>
    /// <returns></returns>
    public Task<bool> Disconnect();
    /// <summary>
    /// 轮询
    /// </summary>
    /// <returns></returns>
    public Task<bool> Polling();
    /// <summary>
    /// 加载配置
    /// </summary>
    public void Load(Device data);
    /// <summary>
    /// 获取变量
    /// </summary>
    /// <returns></returns>
    public Dictionary<string,Tag> GetValues();
    /// <summary>
    /// 获取变量值
    /// </summary>
    /// <param name="id"></param>
    /// <returns></returns>
    public object? GetValue(string id);
    /// <summary>
    /// 获取设备状态
    /// </summary>
    /// <returns></returns>
    public string GetStatus();
    /// <summary>
    /// 获取变量属性
    /// </summary>
    /// <param name="id"></param>
    /// <returns></returns>
    public Tag? GetTagProperty(string id);

    public Task<bool> SetValue(string id, object value);

    public bool IsConnected();

    public void BindAddDaq(Action<Dictionary<string, Tag>, string> fnc);

    public DateTime LastReadTimestamp();

    public void BindGetProperty(Func<string, string, Task<object?>> fnc);

    public Daq? GetTagDaqSettings(string id);

    public void SetTagDaqSettings(string id, Daq daq);
}

/// <summary>
/// OPC UA 节点浏览与属性读取能力
/// </summary>
public interface IBrowsableDevice
{
    Task<object?> Browse(string? nodeId);
    Task<object?> ReadNodeAttribute(string? nodeId);
}

/// <summary>
/// WebAPI 端点测试能力
/// </summary>
public interface IWebApiTestable
{
    Task<object?> TestRequest(object property);
}

/// <summary>
/// 动态标签发现能力
/// </summary>
public interface ITagDiscoverable
{
    Task<object?> DiscoverTags();
}

