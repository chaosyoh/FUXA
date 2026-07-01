---
name: architecture-overview
description: FUXA项目的完整架构参考，涵盖目录结构、模块关系、数据流和存储架构。当新成员加入项目、需要理解系统架构、进行跨模块修改或排查问题时使用此Skill。
---

# FUXA 项目架构总览

## 项目定位

FUXA 是一个开源 Web SCADA/HMI/仪表盘可视化平台，面向工业自动化、IoT 和实时过程可视化。支持多协议设备接入、内置数据历史记录（DAQ）、全功能 Web 端工程编辑器。

## 顶层目录结构

```
Fuxa/
├── client/            # Angular 18 前端 SPA
├── server/            # Node.js/Express 后端（Socket.IO 通信）
├── server-dotnet/     # ASP.NET Core 后端（SignalR 通信，替代实现）
├── app/electron/      # Electron 桌面应用壳
├── app/headless/      # 无头模式入口
├── node-red/          # Node-RED 集成插件
├── docs/              # MkDocs 文档
├── odbc/              # ODBC 驱动安装脚本
├── Dockerfile         # 多阶段 Docker 构建
└── compose.yml        # Docker Compose 部署
```

## 通信架构

```
浏览器 ←→ Socket.IO (实时双向) ←→ server/runtime (Node.js)
浏览器 ←→ REST API (HTTP)      ←→ server/api (Node.js)
浏览器 ←→ SignalR (可选)       ←→ server-dotnet/Runtime (.NET)
```

- **Node.js Server**: Socket.IO + Express REST，默认端口 1881
- **.NET Server**: SignalR + ASP.NET Core Web API
- 客户端 `hmi.service.ts`（Socket.IO）与 `hmi.service.signalr.ts`（SignalR）通过 Angular `fileReplacements` 切换
- 构建命令：`ng serve`（Socket.IO）/ `ng serve -c signalr`（SignalR）

## 客户端架构 (`client/src/app/`)

### 核心模块

| 目录 | 用途 |
|------|------|
| `_services/` | 服务层（~23个）：HmiService, ProjectService, AuthService, SettingsService, DeviceService 等 |
| `_models/` | 数据模型（~18个）：device, hmi, alarm, chart, script, report, settings 等 |
| `_helpers/` | 工具类（~11个）：define, utils, svg-utils, auth-interceptor, calc 等 |
| `_directives/` | 自定义指令（7个）：拖拽、resize、数字输入等 |

### 功能模块

| 目录 | 用途 |
|------|------|
| `editor/` | SCADA 编辑器（核心）：视图属性、图表/图形/卡片/报警/脚本配置 |
| `device/` | 设备管理：设备列表/树/属性、Tag 配置/写入/选项 |
| `gauges/` | 仪表/图形控件：gauge-base, controls(19个), shapes(5个) |
| `alarms/` | 报警管理 |
| `scripts/` | 脚本编辑/调度 |
| `reports/` | 报表 |
| `language/` | 多语言文本管理 |
| `home/` | 主页/HMI 视图 |
| `view/` | 独立视图渲染 |
| `header/` + `sidenav/` | 顶部导航 + 侧边栏 |

### 路由页面

| 路由 | 组件 |
|------|------|
| `/home` | HomeComponent |
| `/editor` | EditorComponent（SCADA 编辑器，77KB）|
| `/device` | DeviceComponent |
| `/view` | ViewComponent |
| `/users` | UsersComponent |
| `/alarms` | AlarmViewComponent |
| `/scripts` | ScriptListComponent |
| `/reports` | ReportListComponent |
| `/logs` | LogsViewComponent |

### SVG 编辑器

- 位于 `client/src/assets/lib/svgeditor/`，自定义 SVG 编辑器核心
- 形状库 `shapes/`：my-shapes.js, ape-shapes.js, proc-shapes.js 等

### 关键文件

- `app.module.ts`：根模块（所有组件声明）
- `app.routing.ts`：路由配置
- `material.module.ts`：Angular Material 模块
- `auth.guard.ts`：认证守卫
- `theme.scss`：主题配置（亮/暗色）

## Node.js 服务端架构 (`server/`)

### 入口链

```
main.js → fuxa.js → runtime/ (运行时引擎)
                   → api/     (REST API)
```

- `main.js`：配置加载、Express/Socket.IO 服务器创建、静态路由、启动监听
- `fuxa.js`：协调层，初始化 runtime + api，暴露 init/start/stop 生命周期

### API 层 (`server/api/`) — 14 个 REST 模块

| 模块 | 用途 |
|------|------|
| `projects/` | 项目 CRUD、导入导出 |
| `devices/` | 设备配置管理 |
| `auth/` | 认证（JWT）|
| `users/` | 用户管理 |
| `apikeys/` | API Key 管理 |
| `alarms/` | 报警查询 |
| `scripts/` | 脚本管理 |
| `scheduler/` | 调度任务 |
| `daq/` | 数据采集查询 |
| `resources/` | 资源文件管理 |
| `reports/` | 报表 |
| `plugins/` | 插件 |
| `diagnose/` | 诊断 |
| `command/` | 命令下发 |

API 入口：`api/index.js` — 统一注册所有子路由，JWT 中间件 + rate-limit

### 运行时层 (`server/runtime/`)

| 目录/文件 | 用途 |
|------|------|
| `index.js` | 运行时主控制器，协调所有子模块 |
| `devices/` | 设备驱动管理（15种协议），状态机：INIT→IDLE→POLLING |
| `alarms/` | 报警引擎 |
| `scripts/` | 服务端脚本执行 |
| `scheduler/` | 定时任务调度 |
| `notificator/` | 通知系统（邮件等）|
| `storage/` | DAQ 数据存储（InfluxDB/QuestDB/TDEngine）|
| `project/` | 项目运行时加载 |
| `users/` | 用户运行时管理 |
| `plugins/` | 插件系统 |
| `events.js` | EventEmitter 事件总线 |
| `logger.js` | Winston 日志 |
| `utils.js` | 通用工具函数 |

### 支持的设备协议（`runtime/devices/`）

S7, OPC-UA, ModbusRTU, ModbusTCP, MQTT, BACnet, ADS, Ethernet/IP, Melsec, ODBC, Redis, GPIO, HTTP Request, Webcam, FuxaServer

驱动注册：`device.js` 中的 DeviceEnum + createDevice 分支

## .NET 服务端架构 (`server-dotnet/`)

解决方案：`SmartScada.slnx`

| 项目 | 用途 |
|------|------|
| `Core/` | 核心模型（Tag, Device, TagGroup 等）、常量、认证、扩展、Quartz 调度 |
| `Runtime/` | DataHub (SignalR)、IDevice/DriverBase、报警、存储、脚本、调度、通知 |
| `Server/` | ASP.NET Core：14 个 Controller + DeviceFactory + 服务（KepServer 转换、Excel 导入导出）|
| `Device.ModbusTCP/` | Modbus TCP 驱动 |
| `Device.MQTT/` | MQTT 驱动 |
| `Device.OpcUa/` | OPC-UA 驱动 |
| `Device.S7/` | Siemens S7 驱动 |
| `Device.WebAPI/` | Web API 设备驱动 |
| `OpcUA/` | OPC-UA 基础库 |
| `Test/` | 测试项目 |

入口：`Server/Program.cs`

Controller 与 Node.js API 模块一一对应（Alarm, Auth, Command, Daq, Device, Diagnose, Plugin, Project, Reports, Resource, Scheduler, Script, User, ApiKeys）

## 数据流

```
设备/PLC → 驱动 polling → events.emit('device-value:changed') → runtime → Socket.IO/SignalR → 浏览器
浏览器 → Socket.IO/SignalR → API/Hub → DeviceManager → 驱动.setValue → 设备/PLC
DAQ: 驱动 → addDaq → daqstorage → 时序数据库（InfluxDB/QuestDB/TDEngine）
```

## 存储架构

| 存储 | 用途 |
|------|------|
| SQLite (`_db/`) | 项目数据（project.fuxap.db）、用户（users.fuxap.db）、API Keys、报警 |
| InfluxDB / QuestDB / TDEngine | 时序 DAQ 数据（可选）|
| 文件系统 (`_appdata/`) | 设置（settings.js）、上传文件、图片、报表、日志 |

## 配置

- `server/settings.default.js`：默认配置（端口、日志、DAQ、CORS、安全、Node-RED 等）
- `server/_appdata/settings.js`：运行时配置（首次从 default 复制）
- `server/_appdata/mysettings.json`：用户自定义设置
