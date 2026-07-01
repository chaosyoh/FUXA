using S7.Net;

Console.WriteLine("=== S7-200 Smart PLC 连接测试 ===");
Console.WriteLine($"目标: 192.168.5.75, 地址: VD358 (无符号双字)");
Console.WriteLine();

var plc = new Plc(CpuType.S7200Smart, "192.168.5.75", 0, 0);

try
{
    Console.WriteLine("正在连接...");
    await plc.OpenAsync();

    if (plc.IsConnected)
    {
        Console.WriteLine("连接成功!");

        // VD358: V区双字, 在S7.Net中 V区 = DB1
        // VD358 -> DataType.DataBlock, db=1, startByte=358, count=4
        var bytes = await plc.ReadBytesAsync(DataType.DataBlock, 1, 358, 4);

        if (bytes != null && bytes.Length == 4)
        {
            // S7 PLC 使用大端字节序
            var value = (uint)((bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3]);
            Console.WriteLine($"VD358 (UInt32) = {value}");
            Console.WriteLine($"原始字节: [{string.Join(", ", bytes.Select(b => $"0x{b:X2}"))}]");
        }
        else
        {
            Console.WriteLine("读取失败: 未获取到有效数据");
        }
    }
    else
    {
        Console.WriteLine("连接失败: PLC 未连接");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"错误: {ex.Message}");
}
finally
{
    plc.Close();
    Console.WriteLine();
    Console.WriteLine("连接已关闭");
}
