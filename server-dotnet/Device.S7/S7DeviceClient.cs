using Core.Const;
using Core.Models;
using Microsoft.AspNetCore.SignalR;
using Runtime;
using S7.Net;

namespace DeviceS7;

public class S7DeviceClient : DeviceBase, IDisposable
{
    private Plc? _plc;

    // DB tags grouped by DB number: dbNum -> list of items
    private readonly Dictionary<int, List<S7TagItem>> _dbItems = new();
    // Non-DB tags (E/I/A/Q/M areas)
    private readonly List<S7TagItem> _mixItems = new();
    // Pre-computed batch read groups for mix items, grouped by area with gap splitting
    private readonly List<MixBatch> _mixBatches = new();
    // Gap threshold in bytes: adjacent tags with gap > this are split into separate batches
    private const int GapThreshold = 100;
    // Max bytes per single PLC read request (conservative for all S7 CPUs, PDU limit is typically 240-960)
    private const int MaxPduData = 200;

    private record MixBatch(DataType Area, List<S7TagItem> Items);

    public S7DeviceClient(IHubContext<DataHub> hubCtx) : base(hubCtx)
    {
    }

    public override void Load(Device data)
    {
        base.Load(data);
        OrganizeTags();
    }

    private void OrganizeTags()
    {
        _dbItems.Clear();
        _mixItems.Clear();

        foreach (var tag in Data.Tags.Values)
        {
            if (string.IsNullOrEmpty(tag.Address)) continue;
            var item = S7AddressParser.Parse(tag);
            if (item == null) continue;

            if (item.IsDb)
            {
                if (!_dbItems.TryGetValue(item.DbNumber, out var list))
                {
                    list = [];
                    _dbItems[item.DbNumber] = list;
                }
                list.Add(item);
            }
            else
            {
                _mixItems.Add(item);
            }
        }

        // Sort by start offset for efficient block reads
        foreach (var list in _dbItems.Values)
            list.Sort((a, b) => a.Start.CompareTo(b.Start));

        // Build batch read groups for mix items: group by area, sort, split by gap
        _mixBatches.Clear();
        var areaGroups = _mixItems.GroupBy(i => i.Area);
        foreach (var group in areaGroups)
        {
            var sorted = group.OrderBy(i => i.Start).ToList();
            _mixBatches.AddRange(CreateMixBatches(group.Key, sorted));
        }
    }

    /// <summary>
    /// Split sorted items within the same area into batches based on gap threshold.
    /// Adjacent items with gap > GapThreshold bytes are placed in separate batches.
    /// </summary>
    private static List<MixBatch> CreateMixBatches(DataType area, List<S7TagItem> sortedItems)
    {
        var batches = new List<MixBatch>();
        if (sortedItems.Count == 0) return batches;

        var currentBatch = new List<S7TagItem> { sortedItems[0] };
        for (var i = 1; i < sortedItems.Count; i++)
        {
            var prev = sortedItems[i - 1];
            var curr = sortedItems[i];
            var prevEnd = prev.Start + prev.ByteLength;
            var gap = curr.Start - prevEnd;

            if (gap > GapThreshold)
            {
                batches.Add(new MixBatch(area, currentBatch));
                currentBatch = [curr];
            }
            else
            {
                currentBatch.Add(curr);
            }
        }
        if (currentBatch.Count > 0) batches.Add(new MixBatch(area, currentBatch));
        return batches;
    }

    public override async Task<bool> Connect()
    {
        await NotifyStatus(DeviceStatus.Busy);
        try
        {
            var address = Data.Property.Address ?? "127.0.0.1";
            short.TryParse(Data.Property.Rack, out var rack);
            short.TryParse(Data.Property.Slot, out var slot);

            var cpuType = ResolveCpuType(Data.Property.CpuType, rack, slot);
            // For S7-200/S7-200 Smart, force rack=0, slot=0 (library handles TSAP internally)
            if (string.Equals(Data.Property.CpuType, "S7200Smart", StringComparison.OrdinalIgnoreCase)
                || string.Equals(Data.Property.CpuType, "S7200", StringComparison.OrdinalIgnoreCase))
            {
                rack = 0;
                slot = 0;
            }

            _plc = new Plc(cpuType, address, rack, slot);
            await _plc.OpenAsync();

            if (!_plc.IsConnected)
            {
                connected = false;
                await NotifyStatus(DeviceStatus.Error);
                return false;
            }

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
            _plc?.Close();
            connected = false;
            monitored = false;
            ClearVarsValue();
            await NotifyStatus(DeviceStatus.Off);
            return true;
        }
        catch
        {
            connected = false;
            return false;
        }
    }

    public override async Task<bool> Polling()
    {
        if (!connected || _plc == null || !_plc.IsConnected) return false;
        if (!CheckWorking(true)) return false;

        try
        {
            // Read DB blocks
            foreach (var (dbNum, items) in _dbItems)
            {
                await ReadDbAsync(dbNum, items);
            }

            // Read non-DB tags (batched by area with gap splitting)
            foreach (var batch in _mixBatches)
            {
                await ReadMixBatchAsync(batch);
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

    private async Task ReadDbAsync(int dbNumber, List<S7TagItem> items)
    {
        if (items.Count == 0 || _plc == null) return;

        var minStart = items[0].Start;
        var maxEnd = items.Max(i => i.Start + i.ByteLength);
        var size = maxEnd - minStart;
        if (size <= 0) return;

        var buffer = await ReadBytesChunkedAsync(DataType.DataBlock, dbNumber, minStart, size);
        if (buffer == null || buffer.Length == 0) return;

        foreach (var item in items)
        {
            var offset = item.Start - minStart;
            item.Tag.Value = ParseValue(buffer, offset, item);
        }
    }

    /// <summary>
    /// Batch read mix items (I/Q/M areas) from the same area group.
    /// Reads the entire byte range [minStart, maxEnd) in one PLC call, then parses individual values.
    /// </summary>
    private async Task ReadMixBatchAsync(MixBatch batch)
    {
        if (batch.Items.Count == 0 || _plc == null) return;

        var items = batch.Items;
        var minStart = items[0].Start; // already sorted
        var maxEnd = items.Max(i => i.Start + i.ByteLength);
        var size = maxEnd - minStart;
        if (size <= 0) return;

        var buffer = await ReadBytesChunkedAsync(batch.Area, 0, minStart, size);
        if (buffer == null || buffer.Length == 0) return;

        foreach (var item in items)
        {
            var offset = item.Start - minStart;
            item.Tag.Value = ParseValue(buffer, offset, item);
        }
    }

    /// <summary>
    /// Read bytes from PLC, automatically splitting into PDU-sized chunks if needed.
    /// Returns the combined buffer as if it were a single read.
    /// </summary>
    private async Task<byte[]> ReadBytesChunkedAsync(DataType area, int dbNumber, int start, int size)
    {
        if (_plc == null) return [];
        if (size <= MaxPduData)
        {
            return await _plc.ReadBytesAsync(area, dbNumber, start, size);
        }

        // Split into chunks respecting PDU limit
        var result = new byte[size];
        var offset = 0;
        while (offset < size)
        {
            var chunkSize = Math.Min(MaxPduData, size - offset);
            var chunk = await _plc.ReadBytesAsync(area, dbNumber, start + offset, chunkSize);
            if (chunk != null && chunk.Length > 0)
            {
                Array.Copy(chunk, 0, result, offset, chunk.Length);
            }
            offset += chunkSize;
        }
        return result;
    }

    private static object? ParseValue(byte[] buffer, int offset, S7TagItem item)
    {
        if (offset < 0 || offset + item.ByteLength > buffer.Length) return null;

        return item.DataType switch
        {
            S7DataType.Bool => (buffer[offset] & (1 << item.Bit)) != 0,
            S7DataType.Byte => buffer[offset],
            S7DataType.Char => (char)buffer[offset],
            S7DataType.Word => (ushort)((buffer[offset] << 8) | buffer[offset + 1]),
            S7DataType.Int => (short)((buffer[offset] << 8) | buffer[offset + 1]),
            S7DataType.DWord => (uint)((buffer[offset] << 24) | (buffer[offset + 1] << 16) | (buffer[offset + 2] << 8) | buffer[offset + 3]),
            S7DataType.DInt => (buffer[offset] << 24) | (buffer[offset + 1] << 16) | (buffer[offset + 2] << 8) | buffer[offset + 3],
            S7DataType.Real => BitConverter.ToSingle(BitConverter.IsLittleEndian
                ? new[] { buffer[offset + 3], buffer[offset + 2], buffer[offset + 1], buffer[offset] }
                : buffer[offset..(offset + 4)], 0),
            _ => null
        };
    }

    public override async Task<bool> SetValue(string id, object value)
    {
        if (!connected || _plc == null || !_plc.IsConnected) return false;
        if (!Data.Tags.TryGetValue(id, out var tag)) return false;

        var item = S7AddressParser.Parse(tag);
        if (item == null) return false;

        try
        {
            if (item.DataType == S7DataType.Bool)
            {
                return await WriteBoolAsync(item, Convert.ToBoolean(value));
            }

            var buffer = FormatValue(value, item);
            if (buffer == null) return false;

            if (item.IsDb)
            {
                await _plc.WriteBytesAsync(DataType.DataBlock, item.DbNumber, item.Start, buffer);
            }
            else
            {
                await _plc.WriteBytesAsync(item.Area, 0, item.Start, buffer);
            }

            tag.Value = value;
            return true;
        }
        catch
        {
            return false;
        }
    }

    private async Task<bool> WriteBoolAsync(S7TagItem item, bool value)
    {
        if (_plc == null) return false;

        // Read the current byte to preserve other bits
        byte[] current;
        if (item.IsDb)
        {
            current = await _plc.ReadBytesAsync(DataType.DataBlock, item.DbNumber, item.Start, 1);
        }
        else
        {
            current = await _plc.ReadBytesAsync(item.Area, 0, item.Start, 1);
        }

        if (current == null || current.Length == 0) return false;

        // Set or clear the specific bit
        if (value)
            current[0] |= (byte)(1 << item.Bit);
        else
            current[0] &= (byte)~(1 << item.Bit);

        // Write back
        if (item.IsDb)
        {
            await _plc.WriteBytesAsync(DataType.DataBlock, item.DbNumber, item.Start, current);
        }
        else
        {
            await _plc.WriteBytesAsync(item.Area, 0, item.Start, current);
        }

        item.Tag.Value = value;
        return true;
    }

    private static byte[]? FormatValue(object value, S7TagItem item)
    {
        try
        {
            return item.DataType switch
            {
                S7DataType.Byte => [Convert.ToByte(value)],
                S7DataType.Char => [(byte)Convert.ToChar(value)],
                S7DataType.Word => FormatUInt16(Convert.ToUInt16(value)),
                S7DataType.Int => FormatInt16(Convert.ToInt16(value)),
                S7DataType.DWord => FormatUInt32(Convert.ToUInt32(value)),
                S7DataType.DInt => FormatInt32(Convert.ToInt32(value)),
                S7DataType.Real => FormatFloat(Convert.ToSingle(value)),
                _ => null
            };
        }
        catch { return null; }
    }

    private static byte[] FormatUInt16(ushort v) =>
        [(byte)(v >> 8), (byte)v];

    private static byte[] FormatInt16(short v) =>
        [(byte)(v >> 8), (byte)v];

    private static byte[] FormatUInt32(uint v) =>
        [(byte)(v >> 24), (byte)(v >> 16), (byte)(v >> 8), (byte)v];

    private static byte[] FormatInt32(int v) =>
        [(byte)(v >> 24), (byte)(v >> 16), (byte)(v >> 8), (byte)v];

    private static byte[] FormatFloat(float v)
    {
        var bytes = BitConverter.GetBytes(v);
        if (BitConverter.IsLittleEndian) Array.Reverse(bytes);
        return bytes;
    }

    /// <summary>
    /// 根据 cpuType 属性推断 CpuType 枚举，如未指定则根据 slot 推断
    /// </summary>
    private static CpuType ResolveCpuType(string? cpuTypeStr, short rack, short slot)
    {
        if (!string.IsNullOrEmpty(cpuTypeStr))
        {
            return cpuTypeStr.ToUpperInvariant() switch
            {
                "S7200" => CpuType.S7200,
                "S7200SMART" => CpuType.S7200Smart,
                "S7300" => CpuType.S7300,
                "S7400" => CpuType.S7400,
                "S71200" => CpuType.S71200,
                "S71500" => CpuType.S71500,
                _ => CpuType.S71200
            };
        }
        // Fallback: infer from slot
        return slot >= 2 ? CpuType.S7300 : CpuType.S71200;
    }

    public void Dispose()
    {
        try { _plc?.Close(); } catch { }
    }
}

public enum S7DataType
{
    Bool, Byte, Char, Word, Int, DWord, DInt, Real
}

public class S7TagItem
{
    public Tag Tag { get; set; } = null!;
    public bool IsDb { get; set; }
    public int DbNumber { get; set; }
    public DataType Area { get; set; } // S7.Net DataType enum
    public int Start { get; set; } // Byte offset
    public int Bit { get; set; } // Bit offset for BOOL
    public S7DataType DataType { get; set; }

    public int ByteLength => DataType switch
    {
        S7DataType.Bool => 1,
        S7DataType.Byte => 1,
        S7DataType.Char => 1,
        S7DataType.Word => 2,
        S7DataType.Int => 2,
        S7DataType.DWord => 4,
        S7DataType.DInt => 4,
        S7DataType.Real => 4,
        _ => 1
    };
}

/// <summary>
/// S7 地址解析器
/// 支持格式: DB10.DBX0.0, DB10.DBB2, DB10.DBW4, DB10.DBD8, EB0, IB0, EW2, IW2, AB0, QB0, MB0, MW2, MD4, E0.0, I0.0, M0.0
/// </summary>
public static class S7AddressParser
{
    public static S7TagItem? Parse(Tag tag)
    {
        var addr = tag.Address?.Trim()?.ToUpperInvariant();
        if (string.IsNullOrEmpty(addr)) return null;

        var item = new S7TagItem { Tag = tag };

        // DB addresses: DB10.DBX0.0, DB10.DBB2, DB10.DBW4, DB10.DBD8
        if (addr.StartsWith("DB"))
        {
            return ParseDb(addr, item);
        }

        // S7-200/S7-200 Smart V area: VB, VW, VD, VX mapped to DB1
        if (addr.StartsWith('V'))
        {
            return ParseVArea(addr, item);
        }

        // Input area: EB0, IB0, EW2, IW2, ED4, ID4, E0.0, I0.0
        if (addr.StartsWith('E') || addr.StartsWith('I'))
        {
            item.Area = S7.Net.DataType.Input;
            return ParseNonDb(addr, item);
        }

        // Output area: AB0, QB0, AW2, QW2, AD4, QD4, A0.0, Q0.0
        if (addr.StartsWith('A') || addr.StartsWith('Q'))
        {
            item.Area = S7.Net.DataType.Output;
            return ParseNonDb(addr, item);
        }

        // Memory area: MB0, MW2, MD4, M0.0
        if (addr.StartsWith('M'))
        {
            item.Area = S7.Net.DataType.Memory;
            return ParseMemory(addr, item);
        }

        return null;
    }

    private static S7TagItem? ParseDb(string addr, S7TagItem item)
    {
        // Format: DB{n}.DB{X|B|W|D}{offset}[.{bit}]
        var dotIdx = addr.IndexOf('.');
        if (dotIdx < 0) return null;

        var dbPart = addr[..dotIdx]; // "DB10"
        var fieldPart = addr[(dotIdx + 1)..]; // "DBX0.0" or "DBB2" etc

        if (!int.TryParse(dbPart[2..], out var dbNum)) return null;

        item.IsDb = true;
        item.DbNumber = dbNum;

        if (!fieldPart.StartsWith("DB") || fieldPart.Length < 4) return null;
        var typeChar = fieldPart[2];
        var rest = fieldPart[3..];

        switch (typeChar)
        {
            case 'X': // Bit: DBX0.0
                var bitDotIdx = rest.IndexOf('.');
                if (bitDotIdx < 0) return null;
                if (!int.TryParse(rest[..bitDotIdx], out var byteOff)) return null;
                if (!int.TryParse(rest[(bitDotIdx + 1)..], out var bitOff)) return null;
                item.Start = byteOff;
                item.Bit = bitOff;
                item.DataType = S7DataType.Bool;
                break;
            case 'B': // Byte
                if (!int.TryParse(rest, out var bOff)) return null;
                item.Start = bOff;
                item.DataType = S7DataType.Byte;
                break;
            case 'W': // Word (2 bytes)
                if (!int.TryParse(rest, out var wOff)) return null;
                item.Start = wOff;
                item.DataType = MapTagTypeToS7Word(item.Tag.Type);
                break;
            case 'D': // DWord (4 bytes)
                if (!int.TryParse(rest, out var dOff)) return null;
                item.Start = dOff;
                item.DataType = MapTagTypeToS7DWord(item.Tag.Type);
                break;
            default:
                return null;
        }

        return item;
    }

    private static S7TagItem? ParseNonDb(string addr, S7TagItem item)
    {
        item.IsDb = false;
        var rest = addr[1..]; // remove E/I/A/Q prefix

        // Check for B/W/D suffix: EB0, EW2, ED4
        if (rest.Length > 0 && rest[0] == 'B')
        {
            if (!int.TryParse(rest[1..], out var off)) return null;
            item.Start = off;
            item.DataType = S7DataType.Byte;
            return item;
        }
        if (rest.Length > 0 && rest[0] == 'W')
        {
            if (!int.TryParse(rest[1..], out var off)) return null;
            item.Start = off;
            item.DataType = MapTagTypeToS7Word(item.Tag.Type);
            return item;
        }
        if (rest.Length > 0 && rest[0] == 'D')
        {
            if (!int.TryParse(rest[1..], out var off)) return null;
            item.Start = off;
            item.DataType = MapTagTypeToS7DWord(item.Tag.Type);
            return item;
        }

        // Bit notation: E0.0, I1.3
        var dotIdx = rest.IndexOf('.');
        if (dotIdx >= 0)
        {
            if (!int.TryParse(rest[..dotIdx], out var byteOff)) return null;
            if (!int.TryParse(rest[(dotIdx + 1)..], out var bitOff)) return null;
            item.Start = byteOff;
            item.Bit = bitOff;
            item.DataType = S7DataType.Bool;
            return item;
        }

        return null;
    }

    private static S7TagItem? ParseMemory(string addr, S7TagItem item)
    {
        item.IsDb = false;
        var rest = addr[1..]; // remove M prefix

        if (rest.Length > 0 && rest[0] == 'B')
        {
            if (!int.TryParse(rest[1..], out var off)) return null;
            item.Start = off;
            item.DataType = S7DataType.Byte;
            return item;
        }
        if (rest.Length > 0 && rest[0] == 'W')
        {
            if (!int.TryParse(rest[1..], out var off)) return null;
            item.Start = off;
            item.DataType = MapTagTypeToS7Word(item.Tag.Type);
            return item;
        }
        if (rest.Length > 0 && rest[0] == 'D')
        {
            if (!int.TryParse(rest[1..], out var off)) return null;
            item.Start = off;
            item.DataType = MapTagTypeToS7DWord(item.Tag.Type);
            return item;
        }

        // Bit notation: M0.0, M1.5
        var dotIdx = rest.IndexOf('.');
        if (dotIdx >= 0)
        {
            if (!int.TryParse(rest[..dotIdx], out var byteOff)) return null;
            if (!int.TryParse(rest[(dotIdx + 1)..], out var bitOff)) return null;
            item.Start = byteOff;
            item.Bit = bitOff;
            item.DataType = S7DataType.Bool;
            return item;
        }

        return null;
    }

    /// <summary>
    /// S7-200/S7-200 Smart V area parser
    /// V area maps to DB1: VB10=DB1.DBB10, VW100=DB1.DBW100, VD358=DB1.DBD358, VX0.0=DB1.DBX0.0, V0.0=DB1.DBX0.0
    /// </summary>
    private static S7TagItem? ParseVArea(string addr, S7TagItem item)
    {
        item.IsDb = true;
        item.DbNumber = 1; // V area = DB1

        var rest = addr[1..]; // remove 'V' prefix
        if (rest.Length == 0) return null;

        var typeChar = rest[0];
        switch (typeChar)
        {
            case 'B': // VB10
                if (!int.TryParse(rest[1..], out var bOff)) return null;
                item.Start = bOff;
                item.DataType = S7DataType.Byte;
                return item;
            case 'W': // VW100
                if (!int.TryParse(rest[1..], out var wOff)) return null;
                item.Start = wOff;
                item.DataType = MapTagTypeToS7Word(item.Tag.Type);
                return item;
            case 'D': // VD358
                if (!int.TryParse(rest[1..], out var dOff)) return null;
                item.Start = dOff;
                item.DataType = MapTagTypeToS7DWord(item.Tag.Type);
                return item;
            case 'X': // VX0.0
                var xRest = rest[1..];
                var xDot = xRest.IndexOf('.');
                if (xDot < 0) return null;
                if (!int.TryParse(xRest[..xDot], out var xByte)) return null;
                if (!int.TryParse(xRest[(xDot + 1)..], out var xBit)) return null;
                item.Start = xByte;
                item.Bit = xBit;
                item.DataType = S7DataType.Bool;
                return item;
        }

        // V0.0 format (bit without X prefix) or V358 (no type char, infer from tag type)
        var dotIdx = rest.IndexOf('.');
        if (dotIdx >= 0)
        {
            // Bit notation: V0.0
            if (!int.TryParse(rest[..dotIdx], out var byteOff)) return null;
            if (!int.TryParse(rest[(dotIdx + 1)..], out var bitOff)) return null;
            item.Start = byteOff;
            item.Bit = bitOff;
            item.DataType = S7DataType.Bool;
            return item;
        }

        // Plain offset: V358 - infer data type from tag type
        if (int.TryParse(rest, out var plainOff))
        {
            item.Start = plainOff;
            item.DataType = MapTagTypeFromString(item.Tag.Type);
            return item;
        }

        return null;
    }

    private static S7DataType MapTagTypeFromString(string? tagType)
    {
        return tagType?.ToUpperInvariant() switch
        {
            "BOOL" or "BOOLEAN" => S7DataType.Bool,
            "BYTE" or "UINT8" or "CHAR" => S7DataType.Byte,
            "INT" or "INT16" or "INTEGER" => S7DataType.Int,
            "WORD" or "UINT16" => S7DataType.Word,
            "DINT" or "INT32" => S7DataType.DInt,
            "DWORD" or "UINT32" => S7DataType.DWord,
            "REAL" or "FLOAT" or "FLOAT32" => S7DataType.Real,
            _ => S7DataType.DWord
        };
    }

    private static S7DataType MapTagTypeToS7Word(string? tagType)
    {
        return tagType?.ToUpperInvariant() switch
        {
            "INT" or "INT16" or "INTEGER" => S7DataType.Int,
            "WORD" or "UINT16" => S7DataType.Word,
            _ => S7DataType.Word
        };
    }

    private static S7DataType MapTagTypeToS7DWord(string? tagType)
    {
        return tagType?.ToUpperInvariant() switch
        {
            "DINT" or "INT32" => S7DataType.DInt,
            "DWORD" or "UINT32" => S7DataType.DWord,
            "REAL" or "FLOAT" or "FLOAT32" => S7DataType.Real,
            _ => S7DataType.DInt
        };
    }
}
