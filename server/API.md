# FUXA Node.js 后端 API 接口文档

> 基础路径：`http://<host>:1881`
> 所有接口默认 JSON 通信。鉴权方式：
> - 大部分写操作要求请求头 `x-access-token: <JWT>`（由 `/api/signin` 颁发），管理员级接口还要求 token 中携带管理员 `groups`
> - 部分接口（如 `/api/getTagValue`、`/api/setTagValue`）支持 `x-api-key` 头（详见 [verify-api-or-token.js](api/apikeys/verify-api-or-token.js)）
> - 配置 `secureEnabled=false` 时全局放行

错误响应通用格式：
```json
{ "error": "<code>", "message": "<text>" }
```
鉴权失败返回 `401 unauthorized_error`，Token 过期返回 `403`。

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

---

## 系统

### GET `/api/version`
返回服务版本号。
- 响应：`200` text/plain — 如 `"1.7.5"`

### GET `/api/settings`
获取应用 settings（含 `language`、`uiPort`、`secureEnabled`、`tokenExpiresIn`、`broadcastAll`、`alarms`、`stmp`（隐藏密码字段）、`daqStore` 等）。
- 响应：`200` JSON

### POST `/api/settings`
保存应用 settings。**管理员权限**。
- Body：`Settings` 对象
- 响应：`200`

### POST `/api/heartbeat`
心跳。若启用安全，返回最新 token（如即将过期会刷新）。
- 响应：`200` `{ message, token? }`

---

## 认证 Auth

### POST `/api/signin`
用户登录。
- Body：`{ username, password }`
- 响应：
```json
{
  "status": "success",
  "data": { "username", "fullname", "groups", "info", "token" }
}
```
- 当 `enableRefreshCookieAuth=true` 时，同时写入 HttpOnly Cookie `fuxa_refresh`（Path=`/api/refresh`，7 天有效）

### POST `/api/refresh`
使用 HttpOnly refresh cookie 续签 access token。
- 前置：`secureEnabled=true` 且 `enableRefreshCookieAuth=true`
- 响应：
  - `200` `{ status, message, data: { ...userInfo, token } }`
  - `401` token 缺失或非法
  - `204` 安全未启用

### POST `/api/signout`
清除 refresh cookie。
- 响应：`204`

---

## 项目 Projects

### GET `/api/project`
获取整个项目对象。
- 响应：`200` `ProjectData`

### GET `/api/projectVersion`
获取项目版本号（时间戳）。
- 响应：`200` `number`

### POST `/api/project`
保存整个项目。**管理员权限**。
- Body：`ProjectData`
- 响应：`200`

### POST `/api/projectData`
分项保存。**管理员权限**。
- Body：`{ cmd: <ProjectDataCmdType>, data: {...} }`
- `cmd` 取值（见 server/runtime）：`set-views`/`set-device`/`del-device`/`set-tag`/`del-tag`/`set-alarm`/`del-alarm`/`set-notification`/`del-notification` 等

### GET `/api/projectdemo`
获取演示项目（来自 `project.demo.fuxap`）。
- 响应：`200` JSON

### GET `/api/device?query=security&name=<deviceName>`
获取设备安全属性。
- 响应：`200` 任意 JSON

### POST `/api/device`
设置设备安全属性。**管理员权限**。
- Body：`{ params: { query: 'security', name, value } }`

### POST `/api/upload`
上传文件资源（图片/SVG 等）。
- Body：`{ resource: { name, fullPath?, type, data(base64) }, destination? }`
- 响应：`200` `{ location: '/_upload_files/<name>' }`

### POST `/api/getTagValues`
批量获取标签当前值。
- Body：`string[]`（tag id 列表）
- 响应：`200` `{ <tagId>: value | null }`

### GET `/api/getDevices`
获取所有设备列表。
- 响应：`200` `Device[]`

---

## 设备与标签 Devices & Tags

> 实时设备订阅、状态变化、节点浏览等通过 **Socket.IO** 实现，命令字详见 server `runtime/devices.js`。

---

## 告警 Alarms

### GET `/api/alarms`
获取当前活动告警。
- Query：可选 `subtype`
- 响应：`200` `Alarm[]`

### GET `/api/alarmsHistory`
获取历史告警。
- Query：`start`、`end`（毫秒时间戳）
- 响应：`200` `AlarmHistory[]`

### POST `/api/alarmack`
确认告警。**管理员权限**。
- Body：`{ params: { name, type, ack: { username, time } } }`

### POST `/api/alarmsClear`
清空所有告警。**管理员权限**。

### GET `/api/getAlarms`
对外简化接口，返回格式化告警列表。

---

## API 密钥 ApiKeys

### GET `/api/apikeys`
获取 API 密钥列表。**管理员权限**。
- 响应：`200` `ApiKey[]`

### POST `/api/apikeys`
创建/更新 API 密钥。**管理员权限**。
- Body：`{ params: ApiKey[] }`
- `ApiKey`：`{ id, name, key, groups, expire?, info? }`

### DELETE `/api/apikeys?apikeys=<JSON>`
删除 API 密钥。**管理员权限**。
- Query `apikeys`：URL 编码的 JSON 数组 `[{ id }, ...]`

---

## 用户与角色 Users & Roles

### GET `/api/users`
列出用户。**管理员权限**。
- 响应：`200` `User[]`（不含 password）

### POST `/api/users`
新增/修改用户。**管理员权限**。
- Body：`{ params: User }`

### DELETE `/api/users?param=<username>`
删除用户。**管理员权限**。

### GET `/api/roles`
角色列表。

### POST `/api/roles`
新增/修改角色。**管理员权限**。
- Body：`{ params: Role }`

### DELETE `/api/roles?roles=<JSON>`
删除角色。**管理员权限**。
- Query `roles`：JSON 字符串化的 `string[]`

---

## 资源 Resources

### GET `/api/resources/images`
列出已上传图片资源。

### GET `/api/resources/resources`
列出非图片资源（字体等）。

### POST `/api/resources/remove`
删除图片资源。**管理员权限**。
- Body：`{ file: <fileName> }`

### GET `/api/resources/generateImage?type=<chartType>&...`
生成图表图片（用于报表）。

### GET `/api/resources/templates`
列出模板。

### POST `/api/resources/template`
保存模板。**管理员权限**。
- Body：`{ template: { id, name, data, ... } }`

### DELETE `/api/resources/templates?templates=<JSON>`
删除模板。**管理员权限**。

### GET `/api/resources/widgets`
列出 widget。

### POST `/api/resources/removeWidget`
删除 widget 文件。**管理员权限**。
- Body：`{ path: <relativePath> }`

---

## DAQ 历史数据

### GET `/api/daq?query=<JSON>`
查询 DAQ 时序数据。
- Query `query`（JSON）：`{ sids: string[], gid?: string, from: number, to: number, event?: boolean }`
- 响应：`200` `{ [sid]: [{ dt, value }] }`

---

## 调度器 Scheduler

### GET `/api/scheduler?id=<schedulerId>`
读取调度器配置。

### POST `/api/scheduler`
保存调度器配置。**管理员权限**。
- Body：`{ id, data: SchedulerData }`

### DELETE `/api/scheduler?id=<schedulerId>`
删除调度器配置。**管理员权限**。

---

## 命令 Command

### GET `/api/download?param=<fileName>`
下载报告文件。

### GET `/api/getTagValue?id=<tagId>`
获取单个标签值。**支持 `x-api-key`**。

### POST `/api/setTagValue`
设置标签值。**支持 `x-api-key`**。
- Body：`{ params: { variableId, value } }`

---

## 脚本 Scripts

### POST `/api/runscript`
执行脚本。**管理员权限**。
- Body：`{ params: { script: <Script>, parameters?: [...] } }`

### POST `/api/runSysFunction`
执行系统内置函数。**管理员权限**。
- Body：`{ params: { name, parameters?: [...] } }`

---

## 插件 Plugins

### GET `/api/plugins`
列出已安装插件。**管理员权限**。

### POST `/api/plugins`
安装插件。**管理员权限**。
- Body：`{ params: { name, version? } }`

### DELETE `/api/plugins?param=<name>`
卸载插件。**管理员权限**。

---

## 诊断 Diagnose

### GET `/api/logsdir`
列出日志目录文件。

### GET `/api/logs?param=<fileName>`
读取日志文件内容。

### GET `/api/reportsdir`
列出报告目录文件（诊断用）。

### POST `/api/sendmail`
测试发送邮件。**管理员权限**。
- Body：`{ params: { to, subject, body } }`

---

## 报告 Reports

### GET `/api/reportsQuery?query=<JSON>`
查询报告文件列表。
- Query `query`（JSON）：`{ name?: string, count?: number }`
  - `name` 模糊匹配文件名
  - `count` 取最近 N 个，按创建时间倒序
- 响应：`200`
```json
[{ "fileName": "...", "reportName": "...", "created": "YYYY-MM-DDTHH:mm:ss" }]
```

### POST `/api/reportBuild`
触发构建报告。**管理员权限**。
- Body：`{ params: <ReportItem> }`
- 实际调用 `runtime.jobsMgr.forceReport()`

### POST `/api/reportRemoveFile`
删除报告文件。**管理员权限**。
- Body：`{ params: { fileName: <name> } }`
- 含路径遍历防护（[path-helper.js](api/path-helper.js)）

---

## Socket.IO 实时通道

Node.js 端基于 `socket.io` 暴露实时通道（同口 `1881`，命名空间 `/`），主要事件：

| 事件 | 方向 | 用途 |
|------|------|------|
| `device-property` | C→S | 测试设备连接属性 |
| `device-values` | S→C | 推送设备最新值 |
| `device-status` | S→C | 推送设备连接状态 |
| `device-browse` | C↔S | OPC UA 节点浏览 |
| `device-node-attribute` | C↔S | OPC UA 节点属性 |
| `device-webapi-request` | C↔S | WebAPI 设备测试请求 |
| `device-tags-request` | C↔S | 主动获取设备标签 |
| `device-enabled` | C→S | 启用/禁用设备 |
| `alarms-status` | S→C | 告警状态推送 |
| `daq-query` | C↔S | DAQ 历史查询 |
| `host-interfaces` | C↔S | 主机网络接口枚举 |

具体消息体定义可在 `server/runtime/devices.js` 中查阅。

---

## 附录：常用枚举

### `ProjectDataCmdType`（部分）
| 值 | 说明 |
|----|------|
| `set-views` | 保存视图列表 |
| `set-device` | 新增/更新设备 |
| `del-device` | 删除设备 |
| `set-tag` | 新增/更新标签 |
| `del-tag` | 删除标签 |
| `set-alarm` | 新增/更新告警 |
| `del-alarm` | 删除告警 |
| `set-notification` | 新增/更新通知 |
| `del-notification` | 删除通知 |
| `set-text` | 翻译文本资源 |
| `set-chart` | 新增/更新图表配置 |
| `set-report` | 新增/更新报告配置 |
| `set-scheduler` | 新增/更新调度器 |

### 权限组（groups）
- `-1` 管理员
- 其他位运算掩码用于按组授权资源访问

---

> 文档版本：基于 [server/api/index.js](api/index.js) 当前实现整理。如新增端点，请在对应模块同步更新本文档。
