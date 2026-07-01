using Core.Entity;
using Core.Models.Requests;
using Microsoft.AspNetCore.Mvc;
using Runtime.Users;

namespace Server.Controllers;

[ApiController]
public class UserController : ControllerBase
{
    private readonly ILogger<UserController> _logger;
    private readonly UserService _userService;

    public UserController(ILogger<UserController> logger, UserService userService)
    {
        _logger = logger;
        _userService = userService;
    }

    [HttpGet]
    [Route("/api/users")]
    public async Task<IActionResult> GetUsers()
    {
        var users = await _userService.GetUsers(null);
        var result = users.Select(u => new
        {
            username = u.Username,
            fullname = u.FullName,
            password = u.Password,
            groups = u.Groups,
            info = u.Info,
        }).ToList();
        return Ok(result);
    }

    [HttpPost]
    [Route("/api/users")]
    public async Task<IActionResult> SetUser([FromBody] UserSaveRequest? req)
    {
        if (req?.Params == null)
        {
            return BadRequest(new { error = "invalid_request", message = "Missing user params" });
        }
        await _userService.SetUser(req.Params);
        return Ok();
    }

    [HttpDelete]
    [Route("/api/users")]
    public async Task<IActionResult> RemoveUser()
    {
        var username = Request.Query["param"].FirstOrDefault();
        if (string.IsNullOrEmpty(username))
        {
            return BadRequest(new { error = "invalid_request", message = "Missing param" });
        }
        await _userService.RemoveUser(username);
        return Ok();
    }

    [HttpGet]
    [Route("/api/roles")]
    public async Task<IActionResult> GetRoles()
    {
        var roles = await _userService.GetRoles();
        return Ok(roles);
    }

    [HttpPost]
    [Route("/api/roles")]
    public async Task<IActionResult> SetRole([FromBody] RoleSaveRequest? req)
    {
        if (req?.Params == null)
        {
            return BadRequest(new { error = "invalid_request", message = "Missing role params" });
        }
        await _userService.SetRole(req.Params);
        return Ok();
    }

    [HttpDelete]
    [Route("/api/roles")]
    public async Task<IActionResult> RemoveRoles()
    {
        var rolesStr = Request.Query["roles"].FirstOrDefault();
        if (string.IsNullOrEmpty(rolesStr))
        {
            return BadRequest(new { error = "invalid_request", message = "Missing roles" });
        }
        var roleNames = System.Text.Json.JsonSerializer.Deserialize<List<string>>(rolesStr, Core.Utils.JsonHelper.Default);
        if (roleNames != null && roleNames.Count > 0)
        {
            await _userService.RemoveRoles(roleNames);
        }
        return Ok();
    }
}
