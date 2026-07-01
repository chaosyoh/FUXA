using Core.Const;
using Core.Models;
using Microsoft.AspNetCore.SignalR;
using Runtime;
using System.Buffers.Binary;
using System.Net.Sockets;

/// <summary>
/// Modbus TCP 客户端驱动
/// </summary>
public class ModbusTcpClient : DeviceBase, IDisposable
{
    private TcpClient? _tcpClient;
    private NetworkStream? _stream;
    private ushort _transactionId = 0;
    private byte _unitId = 1;

    // 内存区域分组: key = area prefix (0/1/3/4), value = list of (address, tag)
    private readonly Dictionary<int, List<ModbusTagItem>> _memoryMap = new();
    private const int TokenLimit = 100; // 每批最多读取的寄存器数

    public ModbusTcpClient(IHubContext<DataHub> hubCtx) : base(hubCtx)
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
            _tcpClient?.Dispose();
            _tcpClient = new TcpClient();

            var address = Data.Property.Address ?? string.Empty;
            var port = 502;
            if (!string.IsNullOrEmpty(Data.Property.Port) && int.TryParse(Data.Property.Port, out var p))
                port = p;
            if (!string.IsNullOrEmpty(Data.Property.SlaveId) && byte.TryParse(Data.Property.SlaveId, out var sid))
                _unitId = sid;

            await _tcpClient.ConnectAsync(address, port);
            _stream = _tcpClient.GetStream();
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
            _stream?.Close();
            _tcpClient?.Close();
            _stream = null;
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
        if (!connected || _stream == null) return false;
        if (!CheckWorking(true))
            return false;

        try
        {
            foreach (var (area, items) in _memoryMap)
            {
                // 分批读取
                var batches = CreateBatches(items);
                foreach (var batch in batches)
                {
                    await ReadBatch(area, batch);
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
        // 对于寄存器类型，需要考虑数据类型占用的寄存器数
        var regCount = (ushort)(endAddr - startAddr + GetRegisterSpan(batch[^1].Tag.Type));
        if (regCount < 1) regCount = 1;

        switch (area)
        {
            case 0: // Coils
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
            case 1: // Discrete Inputs
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
            case 3: // Input Registers
                {
                    var regs = await ReadInputRegistersAsync(_unitId, startAddr, regCount);
                    foreach (var item in batch)
                    {
                        var idx = item.Offset - startAddr;
                        item.Tag.Value = ParseRegisterValue(regs, idx, item.Tag);
                    }
                    break;
                }
            case 4: // Holding Registers
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
                // 应用 Divisor
                if (tag.Divisor > 0)
                    return (double)registers[index] / tag.Divisor;
                return registers[index];
        }
    }

    public override async Task<bool> SetValue(string id, object value)
    {
        if (!connected || _stream == null) return false;
        if (!Data.Tags.TryGetValue(id, out var tag)) return false;
        if (string.IsNullOrEmpty(tag.Memaddress)) return false;
        if (!int.TryParse(tag.Memaddress, out var fullAddr)) return false;

        try
        {
            if (fullAddr < 100000)
            {
                // Coil area
                var offset = (ushort)(fullAddr - 1);
                var boolVal = Convert.ToBoolean(value);
                await WriteSingleCoilAsync(_unitId, offset, boolVal);
                tag.Value = boolVal;
                return true;
            }
            else if (fullAddr >= 400000)
            {
                // Holding Register
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

    #region Modbus Protocol Methods

    public async Task<bool[]> ReadCoilsAsync(byte unitId, ushort startAddress, ushort quantity, CancellationToken ct = default)
    {
        if (quantity < 1 || quantity > 2000)
            throw new ArgumentOutOfRangeException(nameof(quantity));

        byte[] request = BuildRequest(unitId, 0x01, startAddress, quantity);
        byte[] response = await SendReceiveAsync(request, ct);
        ValidateResponse(response, 0x01);

        byte dataLength = response[8];
        byte[] data = new byte[dataLength];
        Array.Copy(response, 9, data, 0, dataLength);

        bool[] coils = new bool[quantity];
        for (int i = 0; i < quantity; i++)
            coils[i] = (data[i / 8] & (1 << (i % 8))) != 0;
        return coils;
    }

    public async Task<bool[]> ReadDiscreteInputsAsync(byte unitId, ushort startAddress, ushort quantity, CancellationToken ct = default)
    {
        if (quantity < 1 || quantity > 2000)
            throw new ArgumentOutOfRangeException(nameof(quantity));

        byte[] request = BuildRequest(unitId, 0x02, startAddress, quantity);
        byte[] response = await SendReceiveAsync(request, ct);
        ValidateResponse(response, 0x02);

        byte dataLength = response[8];
        byte[] data = new byte[dataLength];
        Array.Copy(response, 9, data, 0, dataLength);

        bool[] inputs = new bool[quantity];
        for (int i = 0; i < quantity; i++)
            inputs[i] = (data[i / 8] & (1 << (i % 8))) != 0;
        return inputs;
    }

    public async Task<ushort[]> ReadHoldingRegistersAsync(byte unitId, ushort startAddress, ushort quantity, CancellationToken ct = default)
    {
        if (quantity < 1 || quantity > 125)
            throw new ArgumentOutOfRangeException(nameof(quantity));

        byte[] request = BuildRequest(unitId, 0x03, startAddress, quantity);
        byte[] response = await SendReceiveAsync(request, ct);
        ValidateResponse(response, 0x03);

        byte dataLength = response[8];
        ushort[] registers = new ushort[quantity];
        for (int i = 0; i < quantity; i++)
            registers[i] = (ushort)((response[9 + i * 2] << 8) | response[10 + i * 2]);
        return registers;
    }

    public async Task<ushort[]> ReadInputRegistersAsync(byte unitId, ushort startAddress, ushort quantity, CancellationToken ct = default)
    {
        if (quantity < 1 || quantity > 125)
            throw new ArgumentOutOfRangeException(nameof(quantity));

        byte[] request = BuildRequest(unitId, 0x04, startAddress, quantity);
        byte[] response = await SendReceiveAsync(request, ct);
        ValidateResponse(response, 0x04);

        byte dataLength = response[8];
        ushort[] registers = new ushort[quantity];
        for (int i = 0; i < quantity; i++)
            registers[i] = (ushort)((response[9 + i * 2] << 8) | response[10 + i * 2]);
        return registers;
    }

    public async Task WriteSingleCoilAsync(byte unitId, ushort address, bool value, CancellationToken ct = default)
    {
        ushort data = value ? (ushort)0xFF00 : (ushort)0x0000;
        byte[] request = BuildRequest(unitId, 0x05, address, data);
        byte[] response = await SendReceiveAsync(request, ct);
        ValidateResponse(response, 0x05);
    }

    public async Task WriteSingleRegisterAsync(byte unitId, ushort address, ushort value, CancellationToken ct = default)
    {
        byte[] request = BuildRequest(unitId, 0x06, address, value);
        byte[] response = await SendReceiveAsync(request, ct);
        ValidateResponse(response, 0x06);
    }

    private byte[] BuildRequest(byte unitId, byte functionCode, ushort startAddress, ushort data)
    {
        byte[] buffer = new byte[12];
        buffer[0] = (byte)(_transactionId >> 8);
        buffer[1] = (byte)(_transactionId);
        buffer[2] = 0; buffer[3] = 0;
        buffer[4] = 0; buffer[5] = 6;
        buffer[6] = unitId;
        buffer[7] = functionCode;
        buffer[8] = (byte)(startAddress >> 8);
        buffer[9] = (byte)(startAddress);
        buffer[10] = (byte)(data >> 8);
        buffer[11] = (byte)(data);
        _transactionId++;
        return buffer;
    }

    private async Task<byte[]> SendReceiveAsync(byte[] request, CancellationToken ct)
    {
        if (_tcpClient == null || !_tcpClient.Connected || _stream == null)
            throw new InvalidOperationException("未连接到服务器");

        await _stream.WriteAsync(request, ct);

        byte[] header = new byte[7];
        int bytesRead = 0;
        while (bytesRead < 7)
        {
            int n = await _stream.ReadAsync(header, bytesRead, 7 - bytesRead, ct);
            if (n == 0) throw new Exception("连接关闭");
            bytesRead += n;
        }

        int remainingLength = ((header[4] << 8) | header[5]) - 1;
        if (remainingLength < 2)
            throw new Exception("无效的响应长度");

        byte[] body = new byte[remainingLength];
        bytesRead = 0;
        while (bytesRead < remainingLength)
        {
            int n = await _stream.ReadAsync(body, bytesRead, remainingLength - bytesRead, ct);
            if (n == 0) throw new Exception("连接关闭");
            bytesRead += n;
        }

        byte[] response = new byte[7 + remainingLength];
        Array.Copy(header, 0, response, 0, 7);
        Array.Copy(body, 0, response, 7, remainingLength);

        if (header[0] != request[0] || header[1] != request[1])
            throw new Exception("事务ID不匹配");

        return response;
    }

    private void ValidateResponse(byte[] response, byte expectedFunctionCode)
    {
        byte functionCode = response[7];
        if ((functionCode & 0x80) != 0)
        {
            byte exceptionCode = response[8];
            throw new Exception($"Modbus 异常: 功能码 {functionCode & 0x7F}，异常码 {exceptionCode}");
        }
        if (functionCode != expectedFunctionCode)
            throw new Exception($"功能码不匹配，期望 {expectedFunctionCode}，收到 {functionCode}");
    }

    #endregion

    public void Dispose()
    {
        try { Disconnect().Wait(); } catch { }
        _tcpClient?.Dispose();
        _stream?.Dispose();
    }

    private class ModbusTagItem
    {
        public Tag Tag { get; set; } = null!;
        public ushort Offset { get; set; }
    }
}
