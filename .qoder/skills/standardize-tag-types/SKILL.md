---
name: standardize-tag-types
description: FUXA项目中各设备驱动的Tag类型定义、映射和标准化规范。当新增设备驱动、修复类型不一致问题、处理KepServer导入或进行跨协议类型对齐时使用此Skill。
---

# Tag 类型标准化

## 各协议类型全景图

### S7 类型（标准类型键）

| 标准类型 | 别名（输入时可识别）| 说明 |
|----------|---------------------|------|
| `BOOL` | BOOLEAN | 布尔 |
| `BYTE` | UINT8 | 字节 |
| `CHAR` | — | 字符 |
| `INT` | INT16, INTEGER | 16位有符号整数 |
| `WORD` | UINT16 | 16位无符号整数 |
| `DINT` | INT32 | 32位有符号整数 |
| `DWORD` | UINT32 | 32位无符号整数 |
| `REAL` | FLOAT, FLOAT32 | 32位浮点数 |

S7 驱动的 `_normalizeType()` 函数（`server/runtime/devices/s7/index.js` 第 750-782 行）负责将各种别名统一为标准类型。

### Modbus 类型

| 类型 | 说明 |
|------|------|
| `Boolean` | 布尔（线圈）|
| `Int16` | 16位有符号整数 |
| `UInt16` | 16位无符号整数 |
| `Int32` | 32位有符号整数 |
| `UInt32` | 32位无符号整数 |
| `Float32` | 32位浮点数 |
| `Float64` | 64位浮点数 |
| `Int16MLE` / `UInt16MLE` | Mid-Little-Endian 字节序变体 |
| `Int32MLE` / `UInt32MLE` | Mid-Little-Endian 字节序变体 |
| `Float32MLE` / `Float64MLE` | Mid-Little-Endian 字节序变体 |

**MLE（Mid-Little-Endian）**：字节序变体，用于 KepServer 等使用中间字节序的设备。

### OPC-UA 类型

OPC-UA 类型映射到 `opcua.DataType` 枚举，包括 Boolean, SByte, Byte, Int16, UInt16, Int32, UInt32, Int64, UInt64, Float, Double, String, DateTime 等。

## Node.js 端类型处理工具

### device-utils.js（`server/runtime/devices/device-utils.js`）

#### `tagValueCompose(value, oldValue, tag, runtime)`
统一的值组合函数，读取时调用：
1. 执行 `scaleReadFunction`（如有缩放脚本）
2. 数值类型判断 + 解析
3. 应用死区过滤（deadband）
4. 应用缩放（linear/convertDateTime/convertTickTime/expression）
5. 应用格式化（format 小数位数）

#### `tagRawCalculator(value, tag, runtime)`
写入值的反向计算：
1. 反向线性缩放
2. 反向表达式计算
3. 执行 `scaleWriteFunction`

#### `parseValue(value, type)`
原始值按类型解析：
- `number`：parseFloat
- `boolean`：字符串 'true'/'1' → true
- `string`：直接返回
- 其他：尝试 parseFloat，失败则 Number，再失败返回原值

#### `tagDaqToSave(tag, timestamp)`
判断 DAQ 是否需要存储：
- 值变化 + `daq.changed` → 存储
- 时间间隔超过 `daq.interval` → 存储

## .NET 端类型处理

### ModbusTCPClient 寄存器跨度

Modbus 驱动根据 Tag 类型确定读取的寄存器数量：
- Boolean/Int16/UInt16 → 1 个寄存器（16位）
- Int32/UInt32/Float32 → 2 个寄存器（32位）
- Float64 → 4 个寄存器（64位）

### KepServer 类型映射（`.NET` 和 `Node.js`）

KepServer JSON 导出中的 `TAG_DATA_TYPE` 编码需映射到 FUXA 类型：

| KepServer TAG_DATA_TYPE | FUXA S7 类型 | FUXA Modbus 类型 |
|-------------------------|--------------|------------------|
| Boolean | BOOL | Boolean |
| Byte | BYTE | UInt16 |
| Char | CHAR | — |
| Word | WORD | UInt16 |
| Short | INT | Int16 |
| DWord | DWORD | UInt32 |
| Long | DINT | Int32 |
| Float | REAL | Float32 |
| Double | — | Float64 |
| String | — | String |

映射代码位置：
- Node.js：`server/api/devices/kepserver-converter.js`
- .NET：`server-dotnet/Server/Services/KepserverConverterService.cs`

## 新增驱动的 normalizeType() 规范

**必须实现**：每个新驱动应在内部提供 `_normalizeType()` 函数，将前端传入的类型字符串映射为驱动内部使用的类型标识。

```js
var _normalizeType = function (typeStr) {
    if (!typeStr) return '<default-type>';
    var t = typeStr.toUpperCase();
    switch (t) {
        case 'BOOL':
        case 'BOOLEAN':
            return '<internal-bool-type>';
        // ... 其他类型映射
        default:
            return '<default-type>';
    }
}
```

## 类型字符串前后端对应关系

| 前端 UI 类型 | Node.js 驱动类型 | .NET 驱动类型 |
|-------------|-----------------|--------------|
| BOOL / Boolean | BOOL / Boolean | BOOL / Boolean |
| INT / Int16 | INT / Int16 | INT / Int16 |
| WORD / UInt16 | WORD / UInt16 | WORD / UInt16 |
| DINT / Int32 | DINT / Int32 | DINT / Int32 |
| REAL / Float32 | REAL / Float32 | REAL / Float32 |
| String | String | String |

## 注意事项

- S7 驱动的默认类型为 `WORD`（当类型无法识别时）
- Modbus MLE 变体类型命名以 `MLE` 后缀标识
- .NET 端 `Tag.Type` 字段为字符串，不做枚举约束
- Bool 类型在前端显示需要特殊处理（true/false → 1/0 或 True/False 转换）
- KepServer JSON 文件可能包含 BOM，解析时需处理

## 关键源文件

| 文件 | 内容 |
|------|------|
| `server/runtime/devices/s7/index.js` | S7 `_normalizeType()` + datatypes 定义 |
| `server/runtime/devices/device-utils.js` | tagValueCompose / tagRawCalculator / parseValue / tagDaqToSave |
| `server/runtime/devices/modbus/index.js` | Modbus 类型处理 |
| `server/api/devices/kepserver-converter.js` | KepServer 类型映射（Node.js）|
| `server-dotnet/Server/Services/KepserverConverterService.cs` | KepServer 类型映射（.NET）|
| `server-dotnet/Device.ModbusTCP/ModbusTcpClient.cs` | Modbus 寄存器跨度逻辑 |
