using Core.Const;
using Core.Models;
using Microsoft.AspNetCore.SignalR;
using Runtime;
using System.IO.Ports;

namespace DeviceModbusRTU;

/// <summary>
/// Modbus RTU 客户端驱动（串口通信）
/// </summary>
public class ModbusRtuClient : DeviceBase, IDisposable
{
    private SerialPort? _serialPort;
    private byte _unitId = 1;
    private readonly object _portLock = new();

    // 内存区域分组: key = area prefix (0/1/3/4), value = list of (address, tag)
    private readonly Dictionary<int, List<ModbusTagItem>> _memoryMap = new();
    private const int TokenLimit = 100; // 每批最多读取的寄存器数

    // RTU 帧间间隔 (ms)
    private const int FrameDelay = 5;

    public ModbusRtuClient(IHubContext<DataHub> hubCtx) : base(hubCtx)
    {
    }

    public override void Load(Device data)
    {
        base.Load(data);
        OrganizeMemory();
    }

    /// <summary>
    /// 将 Tag 按 Modbus 内存区域分组
    /// Memaddress 格式: 0xxxxx=Coils, 1xxxxx=DiscreteInputs, 3xxxxx=InputRegisters, 4xxxxx=HoldingRegisters
    /// </summary>
    private void OrganizeMemory()
    {
        _memoryMap.Clear();
        foreach (var tag in Data.Tags.Values)
        {
            if (string.IsNullOrEmpty(tag.Memaddress)) continue;
            if (!int.TryParse(tag.Memaddress, out var fullAddr)) continue;

            int area;
            int offset;
            if (fullAddr >= 400000)
            {
                area = 4; offset = fullAddr - 400001;
            }
            else if (fullAddr >= 300000)
            {
                area = 3; offset = fullAddr - 300001;
            }
            else if (fullAddr >= 100000)
            {
                area = 1; offset = fullAddr - 100001;
            }
            else
            {
                area = 0; offset = fullAddr - 1;
            }

            if (offset < 0) offset = 0;

            if (!_memoryMap.TryGetValue(area, out var list))
            {
                list = [];
                _memoryMap[area] = list;
            }
            list.Add(new ModbusTagItem { Tag = tag, Offset = (ushort)offset });
        }

        // 按偏移排序
        foreach (var list in _memoryMap.Values)
            list.Sort((a, b) => a.Offset.CompareTo(b.Offset));
    }

    public override async Task<bool> Connect()
    {
        await NotifyStatus(DeviceStatus.Busy);
        try
        {
            _serialPort?.Close();
            _serialPort?.Dispose();

            var portName = Data.Property.Address ?? string.Empty;
            if (string.IsNullOrEmpty(portName))
            {
                connected = false;
                await NotifyStatus(DeviceStatus.Error);
                return false;
            }

            var baudRate = 9600;
            var dataBits = 8;
            var stopBits = StopBits.One;
            var parity = Parity.None;

            if (!string.IsNullOrEmpty(Data.Property.Baudrate) && int.TryParse(Data.Property.Baudrate, out var br))
                baudRate = br;
            if (!string.IsNullOrEmpty(Data.Property.Databits) && int.TryParse(Data.Property.Databits, out var db))
                dataBits = db;
            if (!string.IsNullOrEmpty(Data.Property.Stopbits))
            {
                if (double.TryParse(Data.Property.Stopbits, System.Globalization.NumberStyles.Any,
                    System.Globalization.CultureInfo.InvariantCulture, out var sb))
                {
                    stopBits = sb switch
                    {
                        1.0 => StopBits.One,
                        1.5 => StopBits.OnePointFive,
                        2.0 => StopBits.Two,
                        _ => StopBits.One
                    };
                }
            }
            if (!string.IsNullOrEmpty(Data.Property.Parity))
            {
                parity = Data.Property.Parity.ToLowerInvariant() switch
                {
                    "none" => Parity.None,
                    "even" => Parity.Even,
                    "odd" => Parity.Odd,
                    "mark" => Parity.Mark,
                    "space" => Parity.Space,
                    _ => Parity.None
                };
            }
            if (!string.IsNullOrEmpty(Data.Property.SlaveId) && byte.TryParse(Data.Property.SlaveId, out var sid))
                _unitId = sid;

            _serialPort = new SerialPort(portName, baudRate, parity, dataBits, stopBits)
            {
                ReadTimeout = 2000,
                WriteTimeout = 2000,
                ReadBufferSize = 4096,
                WriteBufferSize = 4096
            };

            await Task.Run(() => _serialPort.Open());
            _serialPort.DiscardInBuffer();
            _serialPort.DiscardOutBuffer();

            connected = true;
            await NotifyStatus(DeviceStatus.Ok);
            return true;
        }
        catch (Exception)
        {
            connected = false;
            ClearVarsValue();
            await NotifyStatus(DeviceStatus.Error);
            return false;
        }
    }

    public override async Task<bool> Disconnect()
    {
        try
        {
            connected = false;
            if (_serialPort != null && _serialPort.IsOpen)
            {
                await Task.Run(() => _serialPort.Close());
            }
            ClearVarsValue();
            await NotifyStatus(DeviceStatus.Off);
            return true;
        }
        catch
        {
            return false;
        }
    }

    public override async Task<bool> Polling()
    {
        if (!connected || _serialPort == null || !_serialPort.IsOpen) return false;
        if (!CheckWorking(true))
            return false;

        try
        {
            foreach (var (area, items) in _memoryMap)
            {
                var batches = CreateBatches(items);
                foreach (var batch in batches)
                {
                    await ReadBatch(area, batch);
                    await Task.Delay(FrameDelay);
                }
            }
            lastReadTimestamp = DateTime.Now;
            AddDaq();
            CheckWorking(false);
            return true;
        }
        catch (Exception)
        {
            CheckWorking(false);
            connected = false;
            return false;
        }
    }

    private List<List<ModbusTagItem>> CreateBatches(List<ModbusTagItem> items)
    {
        var batches = new List<List<ModbusTagItem>>();
        if (items.Count == 0) return batches;

        var currentBatch = new List<ModbusTagItem> { items[0] };
        for (var i = 1; i < items.Count; i++)
        {
            var span = items[i].Offset - currentBatch[0].Offset;
            if (span >= TokenLimit || currentBatch.Count >= TokenLimit)
            {
                batches.Add(currentBatch);
                currentBatch = [];
            }
            currentBatch.Add(items[i]);
        }
        if (currentBatch.Count > 0) batches.Add(currentBatch);
        return batches;
    }

    private async Task ReadBatch(int area, List<ModbusTagItem> batch)
    {
        var startAddr = batch[0].Offset;
        var endAddr = batch[^1].Offset;
        var regCount = (ushort)(endAddr - startAddr + GetRegisterSpan(batch[^1].Tag.Type));
        if (regCount < 1) regCount = 1;

        switch (area)
        {
            case 0: // Coils (FC01)
                {
                    var coils = await ReadCoilsAsync(_unitId, startAddr, regCount);
                    foreach (var item in batch)
                    {
                        var idx = item.Offset - startAddr;
                        if (idx >= 0 && idx < coils.Length)
                            item.Tag.Value = coils[idx];
                    }
                    break;
                }
            case 1: // Discrete Inputs (FC02)
                {
                    var inputs = await ReadDiscreteInputsAsync(_unitId, startAddr, regCount);
                    foreach (var item in batch)
                    {
                        var idx = item.Offset - startAddr;
                        if (idx >= 0 && idx < inputs.Length)
                            item.Tag.Value = inputs[idx];
                    }
                    break;
                }
            case 3: // Input Registers (FC04)
                {
                    var regs = await ReadInputRegistersAsync(_unitId, startAddr, regCount);
                    foreach (var item in batch)
                    {
                        var idx = item.Offset - startAddr;
                        item.Tag.Value = ParseRegisterValue(regs, idx, item.Tag);
                    }
                    break;
                }
            case 4: // Holding Registers (FC03)
                {
                    var regs = await ReadHoldingRegistersAsync(_unitId, startAddr, regCount);
                    foreach (var item in batch)
                    {
                        var idx = item.Offset - startAddr;
                        item.Tag.Value = ParseRegisterValue(regs, idx, item.Tag);
                    }
                    break;
                }
        }
    }

    private static int GetRegisterSpan(string tagType)
    {
        return tagType?.ToLowerInvariant() switch
        {
            "int32" or "uint32" or "float32" or "float" or "real" => 2,
            "float64" or "double" or "int64" or "uint64" => 4,
            _ => 1
        };
    }

    private static object? ParseRegisterValue(ushort[] registers, int index, Tag tag)
    {
        if (index < 0 || index >= registers.Length) return null;

        var type = tag.Type?.ToLowerInvariant() ?? "uint16";
        switch (type)
        {
            case "bool":
                return registers[index] != 0;
            case "int16":
                return (short)registers[index];
            case "uint16":
                return registers[index];
            case "int32":
                if (index + 1 >= registers.Length) return null;
                return (int)((registers[index] << 16) | registers[index + 1]);
            case "uint32":
                if (index + 1 >= registers.Length) return null;
                return (uint)((registers[index] << 16) | registers[index + 1]);
            case "float32":
            case "float":
            case "real":
                if (index + 1 >= registers.Length) return null;
                var bytes = new byte[4];
                bytes[0] = (byte)(registers[index] >> 8);
                bytes[1] = (byte)(registers[index] & 0xFF);
                bytes[2] = (byte)(registers[index + 1] >> 8);
                bytes[3] = (byte)(registers[index + 1] & 0xFF);
                if (BitConverter.IsLittleEndian) Array.Reverse(bytes);
                return BitConverter.ToSingle(bytes, 0);
            default:
                if (tag.Divisor > 0)
                    return (double)registers[index] / tag.Divisor;
                return registers[index];
        }
    }

    public override async Task<bool> SetValue(string id, object value)
    {
        if (!connected || _serialPort == null || !_serialPort.IsOpen) return false;
        if (!Data.Tags.TryGetValue(id, out var tag)) return false;
        if (string.IsNullOrEmpty(tag.Memaddress)) return false;
        if (!int.TryParse(tag.Memaddress, out var fullAddr)) return false;

        try
        {
            value = PrepareWriteValue(value, tag);

            if (fullAddr < 100000)
            {
                // Coil area (FC05)
                var offset = (ushort)(fullAddr - 1);
                var boolVal = Convert.ToBoolean(value);
                await WriteSingleCoilAsync(_unitId, offset, boolVal);
                tag.Value = boolVal;
                return true;
            }
            else if (fullAddr >= 400000)
            {
                // Holding Register (FC06)
                var offset = (ushort)(fullAddr - 400001);
                var regVal = Convert.ToUInt16(value);
                await WriteSingleRegisterAsync(_unitId, offset, regVal);
                tag.Value = regVal;
                return true;
            }
            // Discrete Inputs (1xxxxx) and Input Registers (3xxxxx) are read-only
            return false;
        }
        catch (Exception)
        {
            return false;
        }
    }

    #region Modbus RTU Protocol Methods

    private async Task<bool[]> ReadCoilsAsync(byte unitId, ushort startAddress, ushort quantity)
    {
        if (quantity < 1 || quantity > 2000)
            throw new ArgumentOutOfRangeException(nameof(quantity));

        byte[] request = BuildRtuFrame(unitId, 0x01, startAddress, quantity);
        byte[] response = await SendReceiveAsync(request, quantity);
        ValidateRtuResponse(response, unitId, 0x01);

        byte dataLength = response[2];
        byte[] data = new byte[dataLength];
        Array.Copy(response, 3, data, 0, dataLength);

        bool[] coils = new bool[quantity];
        for (int i = 0; i < quantity; i++)
            coils[i] = (data[i / 8] & (1 << (i % 8))) != 0;
        return coils;
    }

    private async Task<bool[]> ReadDiscreteInputsAsync(byte unitId, ushort startAddress, ushort quantity)
    {
        if (quantity < 1 || quantity > 2000)
            throw new ArgumentOutOfRangeException(nameof(quantity));

        byte[] request = BuildRtuFrame(unitId, 0x02, startAddress, quantity);
        byte[] response = await SendReceiveAsync(request, quantity);
        ValidateRtuResponse(response, unitId, 0x02);

        byte dataLength = response[2];
        byte[] data = new byte[dataLength];
        Array.Copy(response, 3, data, 0, dataLength);

        bool[] inputs = new bool[quantity];
        for (int i = 0; i < quantity; i++)
            inputs[i] = (data[i / 8] & (1 << (i % 8))) != 0;
        return inputs;
    }

    private async Task<ushort[]> ReadHoldingRegistersAsync(byte unitId, ushort startAddress, ushort quantity)
    {
        if (quantity < 1 || quantity > 125)
            throw new ArgumentOutOfRangeException(nameof(quantity));

        byte[] request = BuildRtuFrame(unitId, 0x03, startAddress, quantity);
        byte[] response = await SendReceiveAsync(request, quantity);
        ValidateRtuResponse(response, unitId, 0x03);

        byte dataLength = response[2];
        ushort[] registers = new ushort[quantity];
        for (int i = 0; i < quantity; i++)
            registers[i] = (ushort)((response[3 + i * 2] << 8) | response[4 + i * 2]);
        return registers;
    }

    private async Task<ushort[]> ReadInputRegistersAsync(byte unitId, ushort startAddress, ushort quantity)
    {
        if (quantity < 1 || quantity > 125)
            throw new ArgumentOutOfRangeException(nameof(quantity));

        byte[] request = BuildRtuFrame(unitId, 0x04, startAddress, quantity);
        byte[] response = await SendReceiveAsync(request, quantity);
        ValidateRtuResponse(response, unitId, 0x04);

        byte dataLength = response[2];
        ushort[] registers = new ushort[quantity];
        for (int i = 0; i < quantity; i++)
            registers[i] = (ushort)((response[3 + i * 2] << 8) | response[4 + i * 2]);
        return registers;
    }

    private async Task WriteSingleCoilAsync(byte unitId, ushort address, bool value)
    {
        ushort data = value ? (ushort)0xFF00 : (ushort)0x0000;
        byte[] request = BuildRtuFrame(unitId, 0x05, address, data);
        byte[] response = await SendReceiveAsync(request, 1);
        ValidateRtuResponse(response, unitId, 0x05);
    }

    private async Task WriteSingleRegisterAsync(byte unitId, ushort address, ushort value)
    {
        byte[] request = BuildRtuFrame(unitId, 0x06, address, value);
        byte[] response = await SendReceiveAsync(request, 1);
        ValidateRtuResponse(response, unitId, 0x06);
    }

    /// <summary>
    /// 构建 Modbus RTU 帧: [SlaveAddr(1)] [FC(1)] [StartAddr(2)] [Data(2)] [CRC(2)]
    /// </summary>
    private static byte[] BuildRtuFrame(byte unitId, byte functionCode, ushort startAddress, ushort data)
    {
        byte[] frame = new byte[8];
        frame[0] = unitId;
        frame[1] = functionCode;
        frame[2] = (byte)(startAddress >> 8);
        frame[3] = (byte)(startAddress & 0xFF);
        frame[4] = (byte)(data >> 8);
        frame[5] = (byte)(data & 0xFF);
        var crc = Crc16Modbus(frame, 0, 6);
        frame[6] = (byte)(crc & 0xFF);        // CRC Low
        frame[7] = (byte)((crc >> 8) & 0xFF); // CRC High
        return frame;
    }

    /// <summary>
    /// 发送请求并接收响应
    /// </summary>
    private async Task<byte[]> SendReceiveAsync(byte[] request, ushort expectedQuantity)
    {
        if (_serialPort == null || !_serialPort.IsOpen)
            throw new InvalidOperationException("串口未打开");

        lock (_portLock)
        {
            _serialPort.DiscardInBuffer();
            _serialPort.Write(request, 0, request.Length);
        }

        // 计算期望的响应长度
        int expectedLength = CalculateResponseLength(request, expectedQuantity);

        byte[] response = await ReadResponseAsync(expectedLength);
        return response;
    }

    /// <summary>
    /// 根据请求类型计算期望的响应帧长度
    /// </summary>
    private static int CalculateResponseLength(byte[] request, ushort quantity)
    {
        byte fc = request[1];
        return fc switch
        {
            // Read Coils / Read Discrete Inputs: addr(1) + fc(1) + byteCount(1) + data(N) + crc(2)
            0x01 or 0x02 => 5 + (quantity + 7) / 8,
            // Read Holding/Input Registers: addr(1) + fc(1) + byteCount(1) + data(N*2) + crc(2)
            0x03 or 0x04 => 5 + quantity * 2,
            // Write Single Coil / Write Single Register: echo of request (8 bytes)
            0x05 or 0x06 => 8,
            _ => 256 // fallback, read until timeout
        };
    }

    /// <summary>
    /// 从串口异步读取指定长度的响应
    /// </summary>
    private async Task<byte[]> ReadResponseAsync(int expectedLength)
    {
        if (_serialPort == null) throw new InvalidOperationException("串口未初始化");

        byte[] buffer = new byte[expectedLength + 16]; // 额外空间
        int totalRead = 0;
        var cts = new CancellationTokenSource(_serialPort.ReadTimeout);

        while (totalRead < expectedLength && !cts.Token.IsCancellationRequested)
        {
            try
            {
                int n = await Task.Run(() =>
                {
                    try
                    {
                        return _serialPort.Read(buffer, totalRead, expectedLength - totalRead);
                    }
                    catch (TimeoutException)
                    {
                        return 0;
                    }
                }, cts.Token);

                if (n == 0) break;
                totalRead += n;
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        if (totalRead < 4) // 最小响应长度: addr(1) + fc(1) + crc(2)
            throw new Exception($"响应数据不完整，期望 {expectedLength} 字节，收到 {totalRead} 字节");

        byte[] response = new byte[totalRead];
        Array.Copy(buffer, response, totalRead);
        return response;
    }

    /// <summary>
    /// 验证 RTU 响应: 检查从站地址、功能码和 CRC
    /// </summary>
    private static void ValidateRtuResponse(byte[] response, byte expectedUnitId, byte expectedFunctionCode)
    {
        if (response.Length < 4)
            throw new Exception("响应帧长度不足");

        // 检查从站地址
        if (response[0] != expectedUnitId)
            throw new Exception($"从站地址不匹配，期望 {expectedUnitId}，收到 {response[0]}");

        byte functionCode = response[1];

        // 检查异常响应 (功能码最高位为1)
        if ((functionCode & 0x80) != 0)
        {
            byte exceptionCode = response.Length > 2 ? response[2] : (byte)0;
            throw new Exception($"Modbus 异常: 功能码 {functionCode & 0x7F}，异常码 {exceptionCode}");
        }

        if (functionCode != expectedFunctionCode)
            throw new Exception($"功能码不匹配，期望 {expectedFunctionCode}，收到 {functionCode}");

        // 验证 CRC
        var receivedCrc = (ushort)(response[^2] | (response[^1] << 8));
        var calculatedCrc = Crc16Modbus(response, 0, response.Length - 2);
        if (receivedCrc != calculatedCrc)
            throw new Exception($"CRC校验失败，期望 0x{calculatedCrc:X4}，收到 0x{receivedCrc:X4}");
    }

    /// <summary>
    /// 计算 CRC16-Modbus 校验和
    /// </summary>
    private static ushort Crc16Modbus(byte[] data, int offset, int length)
    {
        ushort crc = 0xFFFF;
        for (int i = offset; i < offset + length; i++)
        {
            crc ^= data[i];
            for (int j = 0; j < 8; j++)
            {
                if ((crc & 0x0001) != 0)
                {
                    crc >>= 1;
                    crc ^= 0xA001;
                }
                else
                {
                    crc >>= 1;
                }
            }
        }
        return crc;
    }

    #endregion

    public void Dispose()
    {
        try { Disconnect().Wait(); } catch { }
        _serialPort?.Dispose();
    }

    private class ModbusTagItem
    {
        public Tag Tag { get; set; } = null!;
        public ushort Offset { get; set; }
    }
}
