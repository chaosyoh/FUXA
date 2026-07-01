using Core.Const;
using Core.Entity;
using Core.Extensions;
using Core.Settings;
using Microsoft.IdentityModel.Tokens;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Text;

namespace Core.Utils;

/// <summary>
/// JWT帮助类
/// </summary>
public static class JwtHelper
{

    /// <summary>
    /// 生成JWT
    /// </summary>
    /// <param name="userInfo"></param>
    /// <returns></returns>
    public static string ToJwtToken(this User userInfo)
    {
        var settings = AppSettings.GetSettings();
        var notBefore = DateTime.Now;
        //TODO
        var expDate = notBefore.AddMinutes(1440);
        //string exp = $"{new DateTimeOffset(DateTime.Now.AddMinutes(ManageUser.UserContext.MenuType == 1 ? 43200 : AppSetting.ExpMinutes)).ToUnixTimeSeconds()}";
        var claims = new List<Claim>
            {
                //new Claim(JwtClaimTypes.User_Id,userInfo.User_Id.ToString()),
                //new Claim(JwtClaimTypes.UserTrueName,userInfo.UserTrueName.ToString()),
                new Claim(JwtClaimTypes.Exp,expDate.ToUnixTimeStamp()),
            };
        return CteateJwtToken(claims, notBefore, expDate);
    }
    /// <summary>
    /// 生成长期token
    /// </summary>
    /// <param name="userInfo"></param>
    /// <returns></returns>
    public static string CreateLTSToken(this User userInfo)
    {
        var notBefore = DateTime.Parse("1900-1-1");
        var expDate = DateTime.MaxValue;
        //string exp = $"{new DateTimeOffset(DateTime.Now.AddMinutes(ManageUser.UserContext.MenuType == 1 ? 43200 : AppSetting.ExpMinutes)).ToUnixTimeSeconds()}";
        var claims = new List<Claim>
            {
                //new Claim(JwtClaimTypes.UserTrueName,userInfo.UserTrueName.ToString()),
                new Claim(JwtClaimTypes.Exp,expDate.ToUnixTimeStamp()),
            };
        var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes("BB3647441FFA4B5DB4E64A29B53CE525"));
        var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);
        JwtSecurityToken securityToken = new JwtSecurityToken("sia", "sia.core", claims, notBefore, expDate, creds);
        string jwt = new JwtSecurityTokenHandler().WriteToken(securityToken);
        return jwt;
    }

    private static string CteateJwtToken(List<Claim> claims, DateTime notBefore, DateTime expDate)
    {
        var settings = AppSettings.GetSettings();
        //秘钥16位
        //TODO：从配置加载
        var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes("BB3647441FFA4B5DB4E64A29B53CE525"));
        var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);
        JwtSecurityToken securityToken = new JwtSecurityToken("SIA", "SIA", claims, notBefore, expDate, creds);
        string jwt = new JwtSecurityTokenHandler().WriteToken(securityToken);
        return jwt;
    }

    /// <summary>
    /// 生成刷新Token（长期）
    /// </summary>
    /// <param name="username"></param>
    /// <param name="groups"></param>
    /// <param name="expireDays"></param>
    /// <returns></returns>
    public static string CreateRefreshToken(string username, int groups, int expireDays = 7)
    {
        var notBefore = DateTime.Now;
        var expDate = notBefore.AddDays(expireDays);
        var claims = new List<Claim>
        {
            new Claim(JwtClaimTypes.User_Id, username),
            new Claim("groups", groups.ToString()),
            new Claim("type", "refresh"),
            new Claim(JwtClaimTypes.Exp, expDate.ToUnixTimeStamp()),
        };
        return CteateJwtToken(claims, notBefore, expDate);
    }

    /// <summary>
    /// 校验Token签名和有效期，并返回解析后的 JwtSecurityToken
    /// </summary>
    /// <param name="token"></param>
    /// <param name="securityToken"></param>
    /// <returns></returns>
    public static bool TryValidateToken(string token, out JwtSecurityToken? securityToken)
    {
        securityToken = null;
        if (string.IsNullOrEmpty(token)) return false;
        try
        {
            var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes("BB3647441FFA4B5DB4E64A29B53CE525"));
            var handler = new JwtSecurityTokenHandler();
            var parameters = new TokenValidationParameters
            {
                ValidateIssuer = false,
                ValidateAudience = false,
                ValidateLifetime = true,
                ValidateIssuerSigningKey = true,
                IssuerSigningKey = key,
                ClockSkew = TimeSpan.Zero,
            };
            handler.ValidateToken(token, parameters, out var validated);
            securityToken = validated as JwtSecurityToken;
            return securityToken != null;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// 获取JWT token的过期时间
    /// </summary>
    /// <param name="token"></param>
    /// <returns></returns>
    public static DateTime GetExpireTime(string token)
    {
        JwtSecurityToken jwtSecurityToken = new JwtSecurityTokenHandler().ReadJwtToken(token);
        return jwtSecurityToken.ValidTo;
    }

    /// <summary>
    /// 获取Token中的Claims
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="token"></param>
    /// <param name="jwtClaimsType"></param>
    /// <returns></returns>
    public static T? GetClaimsValue<T>(string token, string jwtClaimsType)
    {
        JwtSecurityToken jwtSecurityToken = new JwtSecurityTokenHandler().ReadJwtToken(token);
        IEnumerable<Claim> claims = jwtSecurityToken.Claims;
        return GetClaimsValue<T>(claims, jwtClaimsType);
    }

    /// <summary>
    /// 查找指定Claim
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="claims"></param>
    /// <param name="jwtClaimsType"></param>
    /// <returns></returns>
    public static T? GetClaimsValue<T>(this IEnumerable<Claim> claims, string jwtClaimsType)
    {
        var value = claims.FirstOrDefault(x => x.Type == jwtClaimsType)?.Value;
        if (value != null)
        {
            return (T)Convert.ChangeType(value, typeof(T));
        }
        else
        {
            return default;
        }
    }
    /// <summary>
    /// 查找指定Claim
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="principal"></param>
    /// <param name="jwtClaimsType"></param>
    /// <returns></returns>
    public static T? GetClaimsValue<T>(this ClaimsPrincipal? principal, string jwtClaimsType)
    {
        var value = principal?.Claims.FirstOrDefault(x => x.Type == jwtClaimsType)?.Value;
        if (value != null)
        {
            return (T)Convert.ChangeType(value, typeof(T));
        }
        else
        {
            return default;
        }
    }

}
