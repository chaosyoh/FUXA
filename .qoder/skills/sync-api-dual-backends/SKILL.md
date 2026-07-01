---
name: sync-api-dual-backends
description: 确保FUXA的Node.js和.NET双后端API接口、实时事件和数据模型保持同步的规范。当新增或修改REST API端点、SignalR/Socket.IO事件、或需要对齐双后端功能时使用此Skill。
---

# 双后端 API 同步

FUXA 采用双后端架构，Node.js（Socket.IO）和 .NET（SignalR）两个后端的 API 必须保持一致。

## 架构对比

| 维度 | Node.js (`server/`) | .NET (`server-dotnet/`) |
|------|---------------------|------------------------|
| Web 框架 | Express | ASP.NET Core |
| 实时通信 | Socket.IO | SignalR |
| API 入口 | `server/api/index.js` | `Server/Controllers/` |
| 实时入口 | Socket.IO events | `Runtime/DataHub.cs` |
| 事件常量 | 内联字符串 | `Core/Const/IoEventTypes.cs` |

## 通信协议切换机制

客户端通过 Angular `fileReplacements` 切换后端通信模式：

| 构建配置 | 替换规则 |
|----------|----------|
| `ng serve`（默认）| `hmi.service.ts`（Socket.IO 实现）|
| `ng serve -c signalr` | `hmi.service.ts` → `hmi.service.signalr.ts`（SignalR 实现）|
| `ng build -c signalr-production` | 同时替换 environment + hmi.service |

关键文件：
- `client/angular.json`（fileReplacements 配置，第 141-168 行）
- `client/src/app/_services/hmi.service.ts`（Socket.IO 实现）
- `client/src/app/_services/hmi.service.signalr.ts`（SignalR 实现）

## REST API 同步检查清单

以下 14 个 API 模块需在两端保持一致：

| 模块 | Node.js 路径 | .NET Controller |
|------|-------------|-----------------|
| 项目 | `server/api/projects/` | `ProjectController` |
| 设备 | `server/api/devices/` | `DeviceController` |
| 认证 | `server/api/auth/` | `AuthController` |
| 用户 | `server/api/users/` | `UserController` |
| API Key | `server/api/apikeys/` | `ApiKeysController` |
| 报警 | `server/api/alarms/` | `AlarmController` |
| 脚本 | `server/api/scripts/` | `ScriptController` |
| 调度 | `server/api/scheduler/` | `SchedulerController` |
| DAQ | `server/api/daq/` | `DaqController` |
| 资源 | `server/api/resources/` | `ResourceController` |
| 报表 | `server/api/reports/` (dist) | `ReportsController` |
| 插件 | `server/api/plugins/` | `PluginController` |
| 诊断 | `server/api/diagnose/` | `DiagnoseController` |
| 命令 | `server/api/command/` | `CommandController` |

### 同步要求

1. **端点路径一致**：如 `/api/projects`、`/api/devices` 等
2. **请求/响应 JSON 结构一致**：字段名（camelCase）、数据类型、嵌套结构必须相同
3. **HTTP 方法一致**：GET/POST/PUT/DELETE 语义一致
4. **错误码一致**：HTTP 状态码 + 错误消息格式

## 实时事件同步

### Socket.IO 事件（Node.js 端）

在 `server/runtime/` 中通过 `events.emit()` 发送：
- `device-status:changed`：设备连接状态变更
- `device-value:changed`：标签值变更
- `alarms-status`：报警状态

### SignalR Hub 方法（.NET 端）

在 `Runtime/DataHub.cs` 中定义，使用 `[HubMethodName]` 注解：

| IoEventTypes 常量 | 方法 | 说明 |
|-------------------|------|------|
| `device-status` | GetDeviceStatus | 获取/推送设备状态 |
| `device-values` | GetDeviceValues | 获取/推送标签值 |
| `device-browse` | DeviceBrowse | OPC UA 节点浏览 |
| `device-node-attribute` | DeviceNodeAttribute | OPC UA 节点属性 |
| `device-property` | DeviceProperty | 设备属性请求 |
| `device-webapi-request` | DeviceWebApiRequest | WebAPI 测试请求 |
| `device-tags-request` | DeviceTagsRequest | 标签发现请求 |
| `device-tags-subscribe` | Subscribe | 订阅标签变化 |
| `device-tags-unsubscribe` | Unsubscribe | 取消订阅 |
| `device-enable` | DeviceEnable | 启用/禁用设备 |
| `device-restart` | DeviceRestart | 重启设备 |
| `daq-query` | DaqQuery | DAQ 数据查询 |
| `alarms-status` | GetAlarmStatus | 报警状态 |
| `host-interfaces` | GetHostInterfaces | 获取主机网络接口 |
| `script-console` | — | 脚本控制台 |
| `script-command` | — | 脚本命令 |
| `heartbeat` | — | 心跳 |

### 新增事件时的同步步骤

1. **Socket.IO 端**：在 `server/runtime/` 相关模块中添加 `events.emit('event-name', data)`
2. **SignalR 端**：
   - 在 `Core/Const/IoEventTypes.cs` 中添加常量
   - 在 `Runtime/DataHub.cs` 中添加 `[HubMethodName]` 方法
3. **客户端**：
   - 在 `hmi.service.ts`（Socket.IO）中添加事件监听
   - 在 `hmi.service.signalr.ts`（SignalR）中添加 Hub 调用

## 数据模型字段对齐

新增或修改模型字段时，需同步：
1. Node.js 端的 JS 对象结构
2. .NET 端的 C# 类（`Core/Models/`）
3. 前端 TypeScript 接口（`client/src/app/_models/`）

**注意**：Node.js 端字段用 camelCase，.NET 端用 PascalCase，JSON 序列化时 .NET 自动转 camelCase。

## 验证策略

### compare-api.mjs 脚本

项目根目录提供 `compare-api.mjs` 对比验证脚本：

```bash
# 仅读操作对比
node compare-api.mjs

# 包含写入操作
node compare-api.mjs --write

# 自定义端口和凭据
node compare-api.mjs --node-port 1881 --dotnet-port 1882 --user admin --pass 123456
```

该脚本同时调用两个后端的 API，对比返回结果是否一致。

### 手动验证

1. 同时启动两个后端（不同端口）
2. 分别用 Socket.IO 和 SignalR 客户端连接
3. 对比实时事件推送的数据格式

## 注意事项

- Node.js 端有 15 种设备驱动，.NET 端仅 5 种，部分功能为单端独有
- 新增功能时需明确是"两端共有"还是"单端独有"
- 两端共有的 API 必须保持请求/响应结构完全一致
- API 文档需同步维护：`server/API.md` 和 `server-dotnet/API.md`

## 关键源文件

| 文件 | 内容 |
|------|------|
| `server/api/index.js` | Node.js API 统一入口 + 路由注册 |
| `server-dotnet/Server/Controllers/` | .NET API Controller 目录 |
| `server-dotnet/Runtime/DataHub.cs` | SignalR Hub 方法定义 |
| `server-dotnet/Core/Const/IoEventTypes.cs` | SignalR 事件名常量 |
| `client/angular.json` | fileReplacements 切换配置 |
| `client/src/app/_services/hmi.service.ts` | Socket.IO 客户端实现 |
| `client/src/app/_services/hmi.service.signalr.ts` | SignalR 客户端实现 |
| `compare-api.mjs` | API 对比验证脚本 |
