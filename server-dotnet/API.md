# FUXA .NET 后端 API 接口文档

> 基础路径：`http://<host>:<port>`（默认端口见 `appsettings.json`）
> 所有接口默认 JSON 通信。
> 实时通信使用 **SignalR** Hub，端点 `/DataHub`

错误响应通用格式：
```json
{ "error": "<code>", "message": "<text>" }
```

---

## 目录

- [系统](#系统)
- [认证 Auth](#认证-auth)
- [项目 Projects](#项目-projects)
- [设备与标签 Devices & Tags](#设备与标签-devices--tags)
- [告警 Alarms](#告警-alarms)
- [API 密钥 ApiKeys](#api-密钥-apikeys)
- [用户与角色 Users & Roles](#用户与角色-users--roles)
- [资源 Resources](#资源-resources)
- [DAQ 历史数据](#daq-历史数据)
- [调度器 Scheduler](#调度器-scheduler)
- [命令 Command](#命令-command)
- [脚本 Scripts](#脚本-scripts)
- [插件 Plugins](#插件-plugins)
- [诊断 Diagnose](#诊断-diagnose)
- [报告 Reports](#报告-reports)
- [SignalR DataHub](#signalr-datahub)

---

## 系统

### GET `/api/version`
返回服务版本号。
- Controller: `ProjectController.GetVersion`
- 响应：`200` text/plain — `"1.0.0"`

### GET `/api/settings`
获取应用 settings（`language`、`uiPort`、`secureEnabled`、`tokenExpiresIn`、`broadcastAll`、`logFull`、`userRole`、`alarms`、`stmp`（隐藏密码字段）、`daqStore`）。
- Controller: `ProjectController.GetSettings`
- 响应：`200` JSON

### POST `/api/settings`
保存应用 settings。
- Controller: `ProjectController.SaveSettings`
- Body：`Settings` 对象（写入 `mysettings.json`）
- 响应：`200`

### POST `/api/heartbeat`
心跳。
- Controller: `ProjectController.Heartbeat`
- Body：`{ message? }`
- 响应：`200` `{ message, token? }`

---

## 认证 Auth

### POST `/api/signin`
用户登录。
- Controller: `AuthController.Singin`
- Body：`{ username, password }`
- 响应：
```json
{
  "status": "success",
  "message": "user found!!!",
  "data": { "userName", "fullName", "groups", "info", "token" }
}
```
- 当 `secureEnabled=true` 时，同时写入 HttpOnly Cookie `fuxa_refresh`（Path=`/api/refresh`，7 天有效）

### POST `/api/refresh`
使用 HttpOnly refresh cookie 续签 access token。
- Controller: `AuthController.Refresh`
- 前置：`secureEnabled=true`
- 响应：
  - `200` `{ status, message, data: { userName, fullName, groups, info, token } }`
  - `401` token 缺失或非法
  - `204` 安全未启用

### POST `/api/signout`
清除 refresh cookie。
- Controller: `AuthController.Signout`
- 响应：`204`

---

## 项目 Projects

### GET `/api/project`
获取整个项目对象。
- Controller: `ProjectController.GetProject`
- 响应：`200` `ProjectData`

### GET `/api/projectVersion`
获取项目版本号（时间戳）。
- Controller: `ProjectController.ProjectVersion`
- 响应：`200` `DateTime`

### POST `/api/project`
保存整个项目。
- Controller: `ProjectController.SaveProject`
- Body：`ProjectData` JSON
- 响应：`200`

### POST `/api/projectData`
分项保存。
- Controller: `ProjectController.ProjectData`
- Body：`{ cmd: <ProjectDataCmdType>, data: {...} }`
- `cmd` 取值：`set-views`/`set-device`/`del-device`/`set-alarm`/`del-alarm`/`set-notification`/`del-notification` 等

### GET `/api/projectdemo`
获取演示项目（来自 `project.demo.fuxap` 文件）。
- Controller: `ProjectController.GetProjectDemo`
- 响应：`200` JSON / `404`

### GET `/api/device?query=security&name=<deviceName>`
获取设备安全属性。
- Controller: `ProjectController.GetDeviceProperty`
- 响应：`200` JSON

### POST `/api/device`
设置设备安全属性。
- Controller: `ProjectController.SetDeviceProperty`
- Body：`{ params: { query: 'security', name, value } }`

### POST `/api/upload`
上传文件资源。
- Controller: `ProjectController.Upload`
- Body：`{ resource: { name, fullPath?, type, data(base64) }, destination? }`
- 响应：`200` `{ location }`

### POST `/api/getTagValues`
批量获取标签当前值。
- Controller: `ProjectController.GetTagValues`
- Body：`string[]`
- 响应：`200` `{ <tagId>: value | null }`

### GET `/api/getDevices`
获取所有设备列表。
- Controller: `ProjectController.GetDevices`
- 响应：`200` `Device[]`

---

## 告警 Alarms

### GET|POST `/api/alarms`
获取当前活动告警。
- Controller: `AlarmController.Alarms`

### GET|POST `/api/alarmsHistory`
获取历史告警。
- Controller: `AlarmController.AlarmsHistory`
- Query：`start`、`end`

### POST `/api/alarmack`
确认告警。
- Controller: `AlarmController.AlarmAck`
- Body：`{ params: { name, type, ack: { username, time } } }`

### POST `/api/alarmsClear`
清空所有告警。
- Controller: `AlarmController.ClearAlarms`

### GET `/api/getAlarms`
对外简化接口，返回格式化告警列表。
- Controller: `AlarmController.GetAlarms`

---

## API 密钥 ApiKeys

### GET `/api/apikeys`
获取 API 密钥列表。
- Controller: `ApiKeysController.GetApiKeys`
- 响应：`200` `ApiKey[]`

### POST `/api/apikeys`
创建/更新 API 密钥。
- Controller: `ApiKeysController.SetApiKeys`
- Body：`{ params: ApiKey[] }`
- `ApiKey`：`{ id, name, key, groups, expire?, info? }`

### DELETE `/api/apikeys?apikeys=<JSON>`
删除 API 密钥。
- Controller: `ApiKeysController.RemoveApiKeys`
- Query `apikeys`：URL 编码的 JSON 数组 `[{ id }, ...]`

---

## 用户与角色 Users & Roles

### GET `/api/users`
列出用户（不含 password 字段）。
- Controller: `UserController.GetUsers`

### POST `/api/users`
新增/修改用户。
- Controller: `UserController.SetUser`
- Body：`{ params: User }`

### DELETE `/api/users?param=<username>`
删除用户。
- Controller: `UserController.RemoveUser`

### GET `/api/roles`
角色列表。
- Controller: `UserController.GetRoles`

### POST `/api/roles`
新增/修改角色。
- Controller: `UserController.SetRole`
- Body：`{ params: Role }`

### DELETE `/api/roles?roles=<JSON>`
删除角色。
- Controller: `UserController.RemoveRoles`
- Query `roles`：JSON 字符串化的 `string[]`

---

## 资源 Resources

### GET `/api/resources/images`
列出已上传图片资源。
- Controller: `ResourceController.GetImages`

### GET `/api/resources/resources`
列出非图片资源（字体等）。
- Controller: `ResourceController.GetResources`

### POST `/api/resources/remove`
删除图片资源。
- Controller: `ResourceController.RemoveResource`
- Body：`{ file: <fileName> }`

### GET `/api/resources/generateImage`
生成图表图片。
- Controller: `ResourceController.GenerateImage`
- 当前返回 **`501 Not Implemented`**

### GET `/api/resources/templates`
列出模板。
- Controller: `ResourceController.GetTemplates`

### POST `/api/resources/template`
保存模板。
- Controller: `ResourceController.SaveTemplate`
- Body：`{ template: { ... } }`

### DELETE `/api/resources/templates?templates=<JSON>`
删除模板。
- Controller: `ResourceController.RemoveTemplates`

### GET `/api/resources/widgets`
列出 widget。
- Controller: `ResourceController.GetWidgets`

### POST `/api/resources/removeWidget`
删除 widget 文件。
- Controller: `ResourceController.RemoveWidget`
- Body：`{ path: <relativePath> }`

---

## DAQ 历史数据

### GET `/api/daq?query=<JSON>`
查询 DAQ 时序数据。
- Controller: `DaqController.GetDaq`
- Query `query`（JSON）：`{ sids, gid?, from, to, event? }`

---

## 调度器 Scheduler

### GET `/api/scheduler?id=<schedulerId>`
读取调度器配置。
- Controller: `SchedulerController.GetScheduler`

### POST `/api/scheduler`
保存调度器配置。
- Controller: `SchedulerController.SaveScheduler`
- Body：`{ id, data: SchedulerData }`

### DELETE `/api/scheduler?id=<schedulerId>`
删除调度器配置。
- Controller: `SchedulerController.DeleteScheduler`

---

## 命令 Command

### GET `/api/download?param=<fileName>`
下载报告文件。
- Controller: `CommandController.Download`

### GET `/api/getTagValue?id=<tagId>`
获取单个标签值。
- Controller: `CommandController.GetTagValue`

### POST `/api/setTagValue`
设置标签值。
- Controller: `CommandController.SetTagValue`
- Body：`{ params: { variableId, value } }`

---

## 脚本 Scripts

### POST `/api/runscript`
执行脚本。
- Controller: `ScriptController.RunScript`
- Body：`{ params: { script, parameters? } }`

### POST `/api/runSysFunction`
执行系统内置函数。
- Controller: `ScriptController.RunSysFunction`
- Body：`{ params: { name, parameters? } }`

---

## 插件 Plugins

### GET `/api/plugins`
列出已安装插件。
- Controller: `PluginController.GetPlugins`

### POST `/api/plugins`
安装插件。
- Controller: `PluginController.InstallPlugin`
- Body：`{ params: { name, version? } }`

### DELETE `/api/plugins?param=<name>`
卸载插件。
- Controller: `PluginController.Uninstall`

---

## 诊断 Diagnose

### GET|POST `/api/logsdir`
列出日志目录文件。
- Controller: `DiagnoseController.GetLogsdir`

### GET|POST `/api/logs`
读取日志文件内容。
- Controller: `DiagnoseController.GetLogs`
- Query：`param=<fileName>`

### GET|POST `/api/reportsdir`
列出报告目录文件。
- Controller: `DiagnoseController.GetReportsDir`

### POST `/api/sendmail`
测试发送邮件。
- Controller: `DiagnoseController.SendMail`
- Body：`{ params: { to, subject, body } }`

---

## 报告 Reports

### GET `/api/reportsQuery?query=<JSON>`
查询报告文件列表。
- Controller: `ReportsController.GetReportsQuery`
- Query `query`（JSON）：`{ name?: string, count?: number }`
  - `name` 模糊匹配文件名
  - `count` 取最近 N 个，按创建时间倒序
- 响应：`200`
```json
[{ "fileName": "...", "reportName": "...", "created": "2024-01-15T10:30:00" }]
```

### POST `/api/reportBuild`
触发构建报告。
- Controller: `ReportsController.ReportBuild`
- Body：`{ params: <ReportItem> }`
- 当前返回 **`501 Not Implemented`**（待 JobManager 集成 `forceReport`）

### POST `/api/reportRemoveFile`
删除报告文件。
- Controller: `ReportsController.ReportRemoveFile`
- Body：`{ params: { fileName: <name> } }`
- 含路径遍历防护

---

## SignalR DataHub

实时通信使用 SignalR Hub，端点 `/DataHub`。

### Hub 方法

| HubMethodName | 方向 | 用途 |
|---------------|------|------|
| `DEVICE_TAGS_SUBSCRIBE` | C→S | 订阅标签值变化 |
| `DEVICE_TAGS_UNSUBSCRIBE` | C→S | 取消订阅 |
| `DEVICE_VALUES` | C→S | GET/SET 设备标签值 |
| `DEVICE_STATUS` | C→S | 获取设备连接状态 |
| `DEVICE_BROWSE` | C→S | OPC UA 节点浏览 |
| `DEVICE_NODE_ATTRIBUTE` | C→S | OPC UA 节点属性 |
| `DEVICE_PROPERTY` | C→S | 测试设备连接 |
| `DEVICE_WEBAPI_REQUEST` | C→S | WebAPI 设备测试请求 |
| `DEVICE_TAGS_REQUEST` | C→S | 获取设备标签列表 |
| `DEVICE_ENABLE` | C→S | 启用/禁用设备 |
| `ALARMS_STATUS` | C→S | 获取告警状态 |
| `DAQ_QUERY` | C→S | DAQ 历史查询 |
| `HOST_INTERFACES` | C→S | 主机网络接口枚举 |

### 服务端推送

| 事件方法 | 方向 | 用途 |
|----------|------|------|
| `device_values` | S→C | 推送设备最新值 |
| `device_status` | S→C | 推送设备状态变化 |
| `alarms_status` | S→C | 推送告警状态 |

---

## 附录：常用枚举

### `ProjectDataCmdType`
| 值 | 说明 |
|----|------|
| `set-views` | 保存视图列表 |
| `set-device` | 新增/更新设备 |
| `del-device` | 删除设备 |
| `set-alarm` | 新增/更新告警 |
| `del-alarm` | 删除告警 |
| `set-notification` | 新增/更新通知 |
| `del-notification` | 删除通知 |

### 数据存储
| 表/数据库 | 路径 | 用途 |
|-----------|------|------|
| `users.fuxap.db` | `_appdata/` | 用户与角色 |
| `apikeys.fuxap.db` | `_appdata/` | API 密钥 |
| `scheduler.fuxap.db` | `_appdata/` | 调度器配置 |
| `project.fuxap.db` | 工作目录 | 项目数据 |
| `daq.fuxap.db` | `_db/` | DAQ 时序存储 |
| `alarms.fuxap.db` | `_db/` | 告警历史 |
| `mysettings.json` | `_appdata/` | 用户 settings |

---

## 对比 Node.js 后端的差异

| 功能 | .NET 状态 | 备注 |
|------|-----------|------|
| `/api/resources/generateImage` | `501` 桩 | 图表图片生成未实现 |
| `/api/reportBuild` | `501` 桩 | 等待 JobManager 集成 |
| Socket.IO → SignalR | 已迁移 | 命令字映射已对齐 |
| `x-api-key` 验证中间件 | 未实现 | 待补充 `x-api-key` 中间件 |

---

> 文档版本：基于 `server-dotnet/Server/Controllers/` 当前实现整理。如新增端点，请在对应 Controller 同步更新本文档。
