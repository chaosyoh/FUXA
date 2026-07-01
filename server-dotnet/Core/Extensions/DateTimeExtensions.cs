using System;
using System.Collections.Generic;
using System.Text;

namespace Core.Extensions;

public static class DateTimeExtensions
{
    /// <summary>
    /// 将日期转换为UNIX时间戳字符串
    /// </summary>
    /// <param name="dateTime"></param>
    /// <returns></returns>
    public static string ToUnixTimeStamp(this DateTime dateTime)
    {
        DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        TimeSpan timeSpan = dateTime.ToUniversalTime() - unixEpoch;
        return timeSpan.TotalSeconds.ToString("0");
    }
}

