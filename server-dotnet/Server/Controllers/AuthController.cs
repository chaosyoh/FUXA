using Core.Models;
using Core.Response;
using Core.Settings;
using Core.Utils;
using Microsoft.AspNetCore.Mvc;
using Runtime.Users;

namespace Server.Controllers;

[ApiController]
public class AuthController : ControllerBase
{
    private readonly ILogger<AuthController> _logger;
    private readonly UserService _service;
    private const string RefreshCookieName = "fuxa_refresh";

    public AuthController(ILogger<AuthController> logger, UserService service)
    {
        _logger = logger;
        _service = service;
    }

    /// <summary>
    /// POST /api/signin - 用户登录
    /// </summary>
    [HttpPost]
    [Route("/api/signin")]
    public async Task<IActionResult> Singin(LoginRequest req)
    {
        var response = await _service.Singin(req);
        if (response.Status != "success" || response.Data == null)
        {
            return Unauthorized(response);
        }

        var settings = AppSettings.GetSettings();
        if (settings.SecureEnabled)
        {
            var refreshToken = JwtHelper.CreateRefreshToken(response.Data.Username, response.Data.Groups);
            SetRefreshCookie(refreshToken);
        }

        return Ok(response);
    }

    /// <summary>
    /// POST /api/refresh - 使用 HttpOnly refresh cookie 续签 access token
    /// </summary>
    [HttpPost]
    [Route("/api/refresh")]
    public async Task<IActionResult> Refresh()
    {
        var settings = AppSettings.GetSettings();
        if (!settings.SecureEnabled)
        {
            return NoContent();
        }

        var refreshToken = Request.Cookies[RefreshCookieName];
        if (string.IsNullOrEmpty(refreshToken))
        {
            return Unauthorized(new { status = "error", message = "Refresh token missing" });
        }

        if (!JwtHelper.TryValidateToken(refreshToken, out var jwt))
        {
            ClearRefreshCookie();
            return Unauthorized(new { status = "error", message = "Invalid refresh token" });
        }

        var tokenType = jwt!.Claims.FirstOrDefault(c => c.Type == "type")?.Value;
        if (tokenType != "refresh")
        {
            ClearRefreshCookie();
            return Unauthorized(new { status = "error", message = "Invalid refresh token" });
        }

        var username = jwt.Claims.FirstOrDefault(c => c.Type == "id")?.Value;
        var groupsStr = jwt.Claims.FirstOrDefault(c => c.Type == "groups")?.Value;
        int.TryParse(groupsStr, out var groups);

        string? fullname = null;
        string? info = null;
        if (!string.IsNullOrEmpty(username))
        {
            try
            {
                var users = await _service.GetUsers(username);
                if (users.Count > 0)
                {
                    fullname = users[0].FullName;
                    groups = users[0].Groups;
                    info = users[0].Info;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "api refresh: user lookup failed");
            }
        }

        var user = new Core.Entity.User
        {
            Username = username ?? string.Empty,
            FullName = fullname ?? string.Empty,
            Groups = groups,
        };
        var newAccessToken = user.ToJwtToken();
        var newRefreshToken = JwtHelper.CreateRefreshToken(user.Username, user.Groups);
        SetRefreshCookie(newRefreshToken);

        return Ok(new ResponseBase<LoginResponse>
        {
            Status = "success",
            Message = "token refreshed",
            Data = new LoginResponse
            {
                Username = user.Username,
                Fullname = user.FullName,
                Groups = user.Groups,
                Info = info,
                Token = newAccessToken,
            }
        });
    }

    /// <summary>
    /// POST /api/signout - 清除 refresh cookie
    /// </summary>
    [HttpPost]
    [Route("/api/signout")]
    public IActionResult Signout()
    {
        ClearRefreshCookie();
        return NoContent();
    }

    private void SetRefreshCookie(string token)
    {
        Response.Cookies.Append(RefreshCookieName, token, new CookieOptions
        {
            HttpOnly = true,
            SameSite = SameSiteMode.Lax,
            Secure = false,
            Path = "/api/refresh",
            MaxAge = TimeSpan.FromDays(7),
        });
    }

    private void ClearRefreshCookie()
    {
        Response.Cookies.Delete(RefreshCookieName, new CookieOptions
        {
            Path = "/api/refresh",
        });
    }
}
