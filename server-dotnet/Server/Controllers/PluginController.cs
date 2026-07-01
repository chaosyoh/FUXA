using Core.Models;
using Microsoft.AspNetCore.Mvc;
using Runtime.Plugins;

namespace Server.Controllers;

[ApiController]
public class PluginController : ControllerBase
{
    private readonly IPluginService _pluginService;

    public PluginController(IPluginService pluginService)
    {
        _pluginService = pluginService;
    }

    [HttpGet]
    [Route("/api/plugins")]
    public List<Plugin> GetPlugins()
    {
        return _pluginService.GetPlugins();
    }

    [HttpDelete]
    [Route("/api/plugins")]
    public IActionResult Uninstall()
    {
        // In .NET, plugins are compile-time assemblies; no runtime uninstall needed
        return Ok();
    }

    [HttpPost]
    [Route("/api/plugins")]
    public IActionResult InstallPlugin([FromBody] PluginInstallRequest? req)
    {
        if (req?.Params == null || string.IsNullOrEmpty(req.Params.Type))
        {
            return BadRequest(new { error = "invalid_request", message = "Missing plugin type" });
        }

        _pluginService.ActivatePlugin(req.Params.Type);
        return Ok();
    }
}

public class PluginInstallRequest
{
    public PluginInstallParams? Params { get; set; }
}

public class PluginInstallParams
{
    public string? Name { get; set; }
    public string? Type { get; set; }
    public string? Module { get; set; }
    public bool Pkg { get; set; }
}
