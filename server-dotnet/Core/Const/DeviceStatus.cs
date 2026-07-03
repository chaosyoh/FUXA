using System;
using System.Collections.Generic;
using System.Text;

namespace Core.Const;

public struct DeviceStatus
{
    public const string Off = "connect-off";

    public const string Ok = "connect-ok";

    public const string Error = "connect-error";

    public const string Busy = "connect-busy";

}

/// <summary>
/// 设备连接状态数值，写入 FuxaServer 的 Connection Status 内部变量
/// 与 Node.js 端 ConnectionStatusEnum 保持一致
/// </summary>
public struct ConnectionStatus
{
    /// <summary>超过 5 倍轮询间隔无响应</summary>
    public const int Off = 0;
    /// <summary>超过 2 倍轮询间隔无响应</summary>
    public const int Warning = 3;
    /// <summary>正常在线</summary>
    public const int On = 1;
}

