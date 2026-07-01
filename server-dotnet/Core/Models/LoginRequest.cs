using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Serialization;

namespace Core.Models;
/// <summary>
/// 登录请求
/// </summary>
public class LoginRequest
{
    /// <summary>
    /// 用户名
    /// </summary>
    public string Username { get; set; } = string.Empty;
    /// <summary>
    /// 密码
    /// </summary>
    public string Password { get; set; } = string.Empty;
}

public class LoginResponse
{
    [JsonPropertyName("username")]
    public string Username { get; set; } = string.Empty;

    [JsonPropertyName("fullname")]
    public string Fullname { get; set; } = string.Empty;


    public int Groups { get; set; } = -1;

    public object? Info { get; set; }

    public string Token { get; set; } = string.Empty;
}