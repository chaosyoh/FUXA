---
name: build-and-deploy
description: FUXA项目的构建与部署完整指南，覆盖本地开发环境搭建、Docker构建和生产部署。当搭建开发环境、打包发布、配置Docker或排查构建问题时使用此Skill。
---

# 构建与部署流程

## 本地开发环境

### 前置要求

- Node.js 18+
- .NET SDK 8+（如需使用 .NET 后端）
- Angular CLI（`npm install -g @angular/cli`）

### 方式 1：Node.js 后端 + Socket.IO（默认）

**启动服务端**：
```bash
cd server
npm install
npm start    # 默认端口 1881
```

**启动客户端**：
```bash
cd client
npm install
ng serve     # 默认端口 4200，代理到 localhost:1881
```

### 方式 2：.NET 后端 + SignalR

**启动服务端**：
```bash
cd server-dotnet
dotnet run --project Server
```

**启动客户端**（SignalR 模式）：
```bash
cd client
ng serve -c signalr
```

### 方式 3：Node.js 后端（LAN 访问）

```bash
cd client
ng serve --host 0.0.0.0    # 允许局域网访问
```

### 其他客户端构建配置

| 配置 | 命令 | 说明 |
|------|------|------|
| 默认开发 | `ng serve` | Socket.IO 模式 |
| SignalR 开发 | `ng serve -c signalr` | SignalR 模式（替换 hmi.service.ts）|
| 生产构建 | `ng build -c production` | 优化 + AOT |
| SignalR 生产 | `ng build -c signalr-production` | SignalR + 生产优化 |
| Demo 构建 | `ng build -c demo` | Demo 环境配置 |

## Angular fileReplacements 机制

客户端通过 `angular.json` 中的 `fileReplacements` 切换后端通信模式：

- `signalr`：替换 `hmi.service.ts` → `hmi.service.signalr.ts`
- `signalr-production`：同时替换 environment + hmi.service
- `production`：仅替换 environment
- `demo`：替换为 demo 环境

## Docker 构建

### 三阶段构建（Dockerfile）

**阶段 1：Client Builder**（node:18-bookworm）
- 安装前端依赖 → `npm run build --configuration production`
- 产物：`client/dist/`

**阶段 2：Server Builder**（node:18-bookworm）
- 安装构建工具（python3, build-essential, libsqlite3-dev）
- 安装服务端依赖 → `npm prune --production`
- 可选安装 node-snap7（Siemens S7）
- 强制重建 sqlite3 原生模块
- 可选安装 ODBC 驱动
- 编译 TypeScript → `npm run build`

**阶段 3：Runner**（node:18-bookworm-slim）
- 仅安装运行时库（sqlite3, 可选 unixodbc）
- 复制服务端 + 客户端产物
- 设置 `NODE_ENV=production`
- 暴露端口 1881
- 启动命令：`node main.js`

### 构建参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `NODE_SNAP` | `false` | 是否安装 node-snap7（Siemens S7 驱动）|
| `INSTALL_ODBC` | `true` | 是否安装 ODBC 驱动 |

### 构建命令

```bash
# 默认构建
docker build -t fuxa .

# 启用 S7 + ODBC
docker build --build-arg NODE_SNAP=true --build-arg INSTALL_ODBC=true -t fuxa .

# 精简构建（无 S7、无 ODBC）
docker build --build-arg NODE_SNAP=false --build-arg INSTALL_ODBC=false -t fuxa .
```

## Docker Compose 部署

```yaml
# compose.yml
services:
  fuxa:
    image: frangoteam/fuxa:latest
    restart: unless-stopped
    volumes:
      - './appdata:/usr/src/app/FUXA/server/_appdata'
      - './db:/usr/src/app/FUXA/server/_db'
      - './logs:/usr/src/app/FUXA/server/_logs'
      - './images:/usr/src/app/FUXA/server/_images'
    ports:
      - '1881:1881'
```

### 数据卷说明

| 卷 | 容器路径 | 内容 |
|----|----------|------|
| appdata | `_appdata/` | 设置文件、用户数据、上传文件 |
| db | `_db/` | SQLite 数据库文件 |
| logs | `_logs/` | 日志文件 |
| images | `_images/` | 图片资源 |

## 生产部署要点

- 环境变量：`NODE_ENV=production`
- 客户端构建产物（`client/dist/`）由 Express 静态服务
- 端口：默认 1881（可通过 `settings.js` 或启动参数 `--port` 修改）
- 数据存储：`_appdata/` 目录（SQLite），可选 InfluxDB/QuestDB/TDEngine
- HTTPS：在 `settings.js` 中配置 `https` 对象

## 服务端构建（esbuild）

```bash
cd server
npm run build    # 使用 esbuild 打包
```

配置文件：`server/esbuild.config.js`

## Electron 桌面应用

```bash
cd app/electron
npm install
npm start
```

## 常见问题排查

| 问题 | 解决方案 |
|------|----------|
| sqlite3 原生编译失败 | `npm install --build-from-source --sqlite=/usr/bin sqlite3` |
| ODBC 驱动缺失 | 运行 `odbc/install_odbc_drivers.sh` |
| node-snap7 编译失败（Windows） | 需要安装 node-gyp + Visual Studio Build Tools |
| node-snap7 编译失败（Python 3.14+） | 先安装 `pip install setuptools` |
| 端口 1881 被占用 | 修改 `settings.js` 中 `uiPort` 或使用 `--port` 参数 |
| 前端代理不生效 | 检查 `client/proxy.conf.json` 配置 |
| node_modules 缓存问题 | 删除 `node_modules` + `package-lock.json` 后重新 `npm install` |

## 关键源文件

| 文件 | 内容 |
|------|------|
| `Dockerfile` | 三阶段 Docker 构建定义 |
| `compose.yml` | Docker Compose 部署配置 |
| `client/angular.json` | Angular 构建配置 + fileReplacements |
| `client/package.json` | 前端脚本和依赖 |
| `server/package.json` | 服务端脚本和依赖 |
| `server/esbuild.config.js` | 服务端打包配置 |
| `server/settings.default.js` | 默认服务端配置 |
