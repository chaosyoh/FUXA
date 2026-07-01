---
name: add-device-driver
description: 在FUXA项目中新增设备协议驱动的完整指南，覆盖Node.js和.NET双后端以及客户端适配。当需要接入新的PLC、传感器或通信协议时使用此Skill。
---

# 新增设备协议驱动

FUXA 采用双后端架构，新增驱动需在 Node.js 和/或 .NET 后端分别实现，并在客户端添加配置 UI。

## Node.js 后端新增驱动

### 步骤 1：创建驱动目录

在 `server/runtime/devices/` 下创建新协议目录：

```
server/runtime/devices/<new-protocol>/
└── index.js
```

### 步骤 2：实现标准驱动接口

参照 `server/runtime/devices/template/index.js` 模板：

```js
'use strict';

function DeviceNewProtocol(_data, _logger, _events) {
    var data = JSON.parse(JSON.stringify(_data));
    var logger = _logger;
    var events = _events;

    this.init = function (_type) { /* 初始化设备类型 */ }

    this.connect = function () {
        // 连接设备
        // events.emit('device-status:changed', { id: data.id, status: 'connect-ok' });
        // events.emit('device-status:changed', { id: data.id, status: 'connect-error' });
    }

    this.disconnect = function () {
        // 断开连接
        // events.emit('device-status:changed', { id: data.id, status: 'connect-off' });
    }

    this.polling = async function () {
        // 轮询读取标签值
        // events.emit('device-value:changed', { id: data.name, values: values });
    }

    this.load = function (_data) { /* 加载标签配置 */ }
    this.getValues = function () { /* 返回标签值数组 { id, value } */ }
    this.getValue = function (tagid) { /* 返回单个标签 { id, value, ts } */ }
    this.getStatus = function () { /* 返回 'connect-off'/'connect-ok'/'connect-error'/'connect-busy' */ }
    this.getTagProperty = function (tagid) { /* 返回前端显示的标签属性 */ }
    this.setValue = function (tagid, value) { /* 写入标签值 */ }
    this.isConnected = function () { /* 返回连接状态布尔值 */ }
    this.bindAddDaq = function (fnc) { this.addDaq = fnc; }
    this.lastReadTimestamp = () => { /* 返回最后读取时间戳 */ }
    this.getTagDaqSettings = (tagId) => { /* 返回标签 DAQ 设置 */ }
    this.setTagDaqSettings = (tagId, settings) => { /* 设置标签 DAQ */ }
}

module.exports = {
    init: function (settings) { },
    create: function (data, logger, events, manager) {
        return new DeviceNewProtocol(data, logger, events);
    }
}
```

### 步骤 3：注册驱动

编辑 `server/runtime/devices/device.js`：

1. **顶部添加 require**：
```js
var NewProtocolClient = require('./<new-protocol>');
```

2. **DeviceEnum 添加新枚举值**（约第 621 行）：
```js
var DeviceEnum = {
    // ... 现有枚举
    NewProtocol: 'NewProtocol',
}
```

3. **createDevice 分支中添加**（约第 59-130 行 if-else 链）：
```js
} else if (data.type === DeviceEnum.NewProtocol) {
    if (!NewProtocolClient) {
        return null;
    }
    comm = NewProtocolClient.create(data, logger, events, manager, runtime);
}
```

### 步骤 4：事件发射约定

驱动必须通过 `events` 通知运行时状态变更：

- 连接状态：`events.emit('device-status:changed', { id: data.id, status: '<status>' })`
  - status 值：`connect-ok`, `connect-error`, `connect-off`, `connect-busy`
- 值变更：`events.emit('device-value:changed', { id: data.name, values: <valuesArray> })`
  - valuesArray 元素格式：`{ id: tagName, value: tagValue }`

### 步骤 5：过载保护（_checkWorking）

device.js 中 `checkStatus` 使用 `_checkWorking` 计数连续失败，达到 3 次时自动断开重连。驱动需在 polling 中正确设置连接状态。

### 降级重连机制

device.js 内置降级（backoff）重连：
- `degradeEnabled`：是否启用（默认 true）
- `degradeRetryCount`：进入降级前重试次数（默认 2）
- `degradePeriod`：降级模式下的重连间隔秒数（默认 60）

## .NET 后端新增驱动

### 步骤 1：创建驱动项目

在 `server-dotnet/` 下创建新项目：

```
server-dotnet/Device.NewProtocol/
├── Device.NewProtocol.csproj
└── NewProtocolClient.cs
```

### 步骤 2：实现 IDevice 接口

参照 `server-dotnet/Runtime/IDevice.cs`：

```csharp
public class NewProtocolClient : IDevice
{
    private readonly IHubContext<DataHub> _hubCtx;

    public NewProtocolClient(IHubContext<DataHub> hubCtx) { _hubCtx = hubCtx; }

    public Task<bool> Connect() { /* 连接设备 */ }
    public Task<bool> Disconnect() { /* 断开连接 */ }
    public Task<bool> Polling() { /* 轮询读取标签值，通过 _hubCtx 推送 */ }
    public void Load(Device data) { /* 加载配置 */ }
    public Dictionary<string, Tag> GetValues() { /* 返回标签值 */ }
    public object? GetValue(string id) { /* 返回单个值 */ }
    public string GetStatus() { /* 返回状态 */ }
    public Tag? GetTagProperty(string id) { /* 返回标签属性 */ }
    public Task<bool> SetValue(string id, object value) { /* 写入值 */ }
    public bool IsConnected() { /* 返回连接状态 */ }
    public void BindAddDaq(Action<Dictionary<string, Tag>, string> fnc) { /* 绑定 DAQ */ }
    public DateTime LastReadTimestamp() { /* 返回最后读取时间 */ }
    public void BindGetProperty(Func<string, string, Task<object?>> fnc) { /* 绑定属性获取 */ }
    public Daq? GetTagDaqSettings(string id) { /* 获取 DAQ 设置 */ }
    public void SetTagDaqSettings(string id, Daq daq) { /* 设置 DAQ */ }
}
```

可选扩展接口：
- `IBrowsableDevice`：OPC UA 节点浏览能力
- `IWebApiTestable`：WebAPI 端点测试能力
- `ITagDiscoverable`：动态标签发现能力

### 步骤 3：添加设备类型常量

编辑 `server-dotnet/Core/Const/DeviceType.cs`：

```csharp
public const string NewProtocol = "NewProtocol";
```

### 步骤 4：注册到 DeviceFactory

编辑 `server-dotnet/Server/DeviceFactory.cs`：

```csharp
using DeviceNewProtocol;

public static IDevice? Create(Device config, IHubContext<DataHub> hubCtx)
{
    return config.Type switch
    {
        // ... 现有类型
        DeviceType.NewProtocol => new NewProtocolClient(hubCtx),
        _ => null
    };
}
```

### 步骤 5：添加项目引用

在 `Server/Server.csproj` 或 `SmartScada.slnx` 中添加对 `Device.NewProtocol` 项目的引用。

## 客户端适配

### 添加设备属性编辑组件

在 `client/src/app/device/` 中添加设备属性编辑对话框组件，用于配置协议特定的连接参数（如地址、端口、认证等）。

### 设备类型字符串必须与后端一致

客户端使用的设备类型字符串必须与 Node.js 的 `DeviceEnum` 和 .NET 的 `DeviceType` 常量**完全匹配**。

### 在 `_helpers/define.ts` 中注册

在设备类型相关的枚举/常量定义中添加新类型。

## 检查清单

- [ ] Node.js 驱动实现所有标准接口方法
- [ ] Node.js device.js 中已注册 DeviceEnum 和 createDevice 分支
- [ ] .NET 驱动实现 IDevice 接口
- [ ] .NET DeviceType.cs 已添加常量
- [ ] .NET DeviceFactory.cs 已注册
- [ ] .NET 项目引用已添加
- [ ] 客户端设备属性编辑组件已创建
- [ ] 设备类型字符串三端一致
- [ ] 事件发射格式正确（device-status:changed, device-value:changed）
- [ ] 驱动支持降级重连机制
