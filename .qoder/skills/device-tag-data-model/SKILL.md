---
name: device-tag-data-model
description: FUXA项目中Device、Tag、TagGroup核心数据模型的完整字段参考，覆盖Node.js和.NET双后端。当修改设备配置、Tag属性、进行数据导入导出或前后端数据对齐时使用此Skill。
---

# Device/Tag 数据模型参考

## Device 模型

### .NET 端定义（`Core/Models/Device.cs`）

| 字段 | 类型 | 说明 |
|------|------|------|
| `Id` | string | 主键（前缀 `d_`）|
| `Name` | string | 设备名称 |
| `Enabled` | bool | 是否启用 |
| `Property` | DeviceProperty (JSON) | 协议特定连接配置 |
| `Type` | string | 设备类型（对应 DeviceType 常量）|
| `Polling` | int | 轮询间隔（毫秒，默认 3000）|
| `DegradeEnabled` | bool? | 启用降级重连（默认 true）|
| `DegradeRetryCount` | int? | 降级前重试次数（默认 2）|
| `DegradePeriod` | int? | 降级周期秒数（默认 60）|
| `Tags` | Dict\<string, Tag\> | 标签集合（内存中，不入库）|
| `TagGroups` | Dict\<string, TagGroup\> | 标签分组（内存中，不入库）|

### DeviceProperty 字段（按协议使用不同字段）

| 字段 | 适用协议 |
|------|----------|
| `Address` | 所有协议 |
| `Port` | ModbusTCP, BACnet, OPC-UA |
| `Slot` | S7 |
| `Rack` | S7 |
| `CpuType` | S7（S7200, S7200Smart, S7300, S7400, S71200, S71500）|
| `SlaveId` | ModbusRTU |
| `Baudrate` | ModbusRTU |
| `Databits` | ModbusRTU |
| `Stopbits` | ModbusRTU |
| `ClientId` | MQTT |
| `Username` | MQTT, OPC-UA |
| `Password` | MQTT, OPC-UA |
| `GetUrl` | WebAPI |
| `PostUrl` | WebAPI |

### Node.js 端

Node.js 端 Device 为纯 JS 对象，字段名使用 **camelCase**：
- `id`, `name`, `type`, `enabled`, `polling`, `property`, `tags`
- 降级字段：`degradeEnabled`, `degradeRetryCount`, `degradePeriod`
- `property` 为嵌套对象，字段名同 .NET 但使用 camelCase

## Tag 模型

### .NET 端定义（`Core/Models/Tag.cs`）— 权威定义

| 字段 | 类型 | 说明 |
|------|------|------|
| `Id` | string | 主键（前缀 `t_`）|
| `DeviceId` | string | 所属设备 ID |
| `Name` | string | 标签名称 |
| `Label` | string | 显示标签 |
| `Type` | string | 变量类型（协议相关，如 BOOL/INT/REAL/Float32 等）|
| `Memaddress` | string | 内存地址（Modbus/WebAPI 使用）|
| `Address` | string | 协议地址 |
| `Divisor` | int? | 除数缩放（Modbus 使用）|
| `Options` | string | 扩展选项 |
| `Format` | string? | 小数位数 |
| `Daq` | Daq (JSON) | 数据采集设置 |
| `Init` | string | 初始值 |
| `Scale` | Scale? (JSON) | 缩放设置 |
| `ScaleReadFunction` | string? | 读取缩放函数名 |
| `ScaleReadParams` | string? | 读取缩放参数 |
| `ScaleWriteFunction` | string? | 写入缩放函数名 |
| `ScaleWriteParams` | string? | 写入缩放参数 |
| `Description` | string? | 描述 |
| `Deadband` | TagDeadband? (JSON) | 死区设置 |
| `SysType` | int? | 系统标签类型（1=deviceConnectionStatus）|
| `Direction` | string? | GPIO 方向 |
| `Edge` | string? | GPIO 边沿 |
| `GroupId` | string? | 所属分组 ID |
| `Access` | string? | 读写权限：`rw`（默认）/ `ro` |
| `Value` | object? | 运行时值（不入库）|

### Daq 子模型

| 字段 | 类型 | 说明 |
|------|------|------|
| `Enabled` | bool | 是否启用 DAQ |
| `Changed` | bool | 值变化时记录 |
| `Interval` | int | 记录间隔（秒）|

### Scale 子模型

| 字段 | 类型 | 说明 |
|------|------|------|
| `Mode` | string | 缩放模式：`linear` / `convertDateTime` / `convertTickTime` / `expression` |
| `RawLow` / `RawHigh` | decimal | 原始范围 |
| `ScaledLow` / `ScaledHigh` | decimal | 缩放范围 |
| `DateTimeFormat` | string | 日期格式 |
| `ReadExpression` / `WriteExpression` | string | 自定义表达式 |

### TagDeadband 子模型

| 字段 | 类型 | 说明 |
|------|------|------|
| `Value` | decimal | 死区值 |
| `Mode` | string | 死区模式 |

## TagGroup 模型（`Core/Models/TagGroup.cs`）

| 字段 | 类型 | 说明 |
|------|------|------|
| `Id` | string | 主键 |
| `ParentId` | string? | 父组 ID（空=根级别）|
| `DeviceId` | string | 所属设备 ID |
| `Name` | string | 组名称 |

## Node.js 端 Tag 差异

Node.js 端 Tag 为纯 JS 对象，**字段名使用 camelCase**：
- `id`, `deviceId`, `name`, `label`, `type`, `address`, `memaddress`
- `divisor`, `options`, `format`, `init`, `scale`, `daq`
- `scaleReadFunction`, `scaleReadParams`, `scaleWriteFunction`, `scaleWriteParams`
- `description`, `deadband`, `sysType`, `direction`, `edge`
- `groupId`, `access`

**关键差异**：
- Node.js 端字段名全小写/驼峰，.NET 端为 PascalCase
- JSON 序列化时 .NET 默认转为 camelCase（JsonSerializerOptions），与前端一致

## 设备类型枚举对照表

| Node.js DeviceEnum | .NET DeviceType | 说明 |
|---------------------|-----------------|------|
| `S7` → `'SiemensS7'` | `SiemensS7` | Siemens S7 |
| `OPCUA` → `'OPCUA'` | `OPCUA` | OPC UA |
| `ModbusTCP` → `'ModbusTCP'` | `ModbusTCP` | Modbus TCP |
| `ModbusRTU` → `'ModbusRTU'` | — | Modbus RTU（仅 Node.js）|
| `MQTTclient` → `'MQTTclient'` | `MQTTclient` | MQTT |
| `WebAPI` → `'WebAPI'` | `WebAPI` | HTTP/Web API |
| `BACnet` → `'BACnet'` | — | BACnet（仅 Node.js）|
| `EthernetIP` → `'EthernetIP'` | — | Ethernet/IP（仅 Node.js）|
| `FuxaServer` → `'FuxaServer'` | — | FuxaServer 内部（仅 Node.js）|
| `ODBC` → `'ODBC'` | — | ODBC（仅 Node.js）|
| `ADSclient` → `'ADSclient'` | — | Beckhoff ADS（仅 Node.js）|
| `GPIO` → `'GPIO'` | — | GPIO（仅 Node.js）|
| `WebCam` → `'WebCam'` | — | 摄像头（仅 Node.js）|
| `MELSEC` → `'MELSEC'` | — | 三菱 MELSEC（仅 Node.js）|
| `REDIS` → `'REDIS'` | — | Redis（仅 Node.js）|

**注意**：.NET 后端目前仅支持 5 种协议，Node.js 支持 15 种。

## 数据导入/导出

- **KepServer JSON 导入**：`server/api/devices/kepserver-converter.js`（Node.js）/ `Server/Services/KepserverConverterService.cs`（.NET）
- **Excel 导入/导出**：`Server/Services/DeviceXlsService.cs`（.NET）
- **项目文件**：`server/project.default.json` / `server/project.demo.fuxap`

## 关键源文件

| 文件 | 内容 |
|------|------|
| `server-dotnet/Core/Models/Device.cs` | Device + DeviceProperty 定义 |
| `server-dotnet/Core/Models/Tag.cs` | Tag + Daq + Scale + TagDeadband 定义 |
| `server-dotnet/Core/Models/TagGroup.cs` | TagGroup 定义 |
| `server-dotnet/Core/Const/DeviceType.cs` | 设备类型常量 |
| `server/runtime/devices/device.js` | DeviceEnum + 驱动注册（第 621-639 行）|
| `server/runtime/devices/device-utils.js` | Tag 值处理工具函数 |
