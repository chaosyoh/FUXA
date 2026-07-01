using Core.Models;
using Core.Models.Requests;
using Core.Settings;
using Core.Utils;
using Microsoft.AspNetCore.Mvc;
using Runtime;
using Runtime.Project;
using System.Text.Json;

namespace Server.Controllers;

[ApiController]
public class CommandController : ControllerBase
{
    private readonly ILogger<CommandController> _logger;
    private readonly IProjectService _projectService;
    private readonly IDeviceRegistry _registry;

    public CommandController(ILogger<CommandController> logger, IProjectService projectService, IDeviceRegistry registry)
    {
        _logger = logger;
        _projectService = projectService;
        _registry = registry;
    }

    [HttpGet]
    [Route("/api/download")]
    public IActionResult Download()
    {
        var cmd = Request.Query["cmd"].FirstOrDefault();
        var name = Request.Query["name"].FirstOrDefault();

        if (cmd != "REPORT-DOWNLOAD" || string.IsNullOrEmpty(name))
        {
            return BadRequest(new { error = "invalid_request", message = "Invalid command or missing name" });
        }

        // Sanitize filename - prevent directory traversal
        name = name.Replace("..", "").Replace("/", "").Replace("\\", "");
        var settings = AppSettings.GetSettings();
        var filePath = Path.Combine(settings.ReoprtsDir, name);

        if (!System.IO.File.Exists(filePath))
        {
            return NotFound(new { error = "not_found", message = "File not found" });
        }

        return PhysicalFile(filePath, "application/octet-stream", name);
    }

    [HttpGet]
    [Route("/api/getTagValue")]
    public IActionResult GetTagValue()
    {
        var idsStr = Request.Query["ids"].FirstOrDefault();
        if (string.IsNullOrEmpty(idsStr))
        {
            return BadRequest(new { error = "invalid_request", message = "Missing ids parameter" });
        }

        var ids = JsonSerializer.Deserialize<List<string>>(idsStr, JsonHelper.Default);
        if (ids == null || ids.Count == 0)
        {
            return Ok(new List<object>());
        }

        var projectData = _projectService.GetProject();
        var result = new List<object>();
        foreach (var id in ids)
        {
            if (projectData.Tags.TryGetValue(id, out var tag))
            {
                result.Add(new { id, value = tag.Value });
            }
            else
            {
                result.Add(new { id, value = (object?)null });
            }
        }
        return Ok(result);
    }

    [HttpPost]
    [Route("/api/setTagValue")]
    public async Task<IActionResult> SetTagValue([FromBody] SetTagValueRequest? req)
    {
        if (req?.Tags == null || req.Tags.Count == 0)
        {
            return BadRequest(new { error = "invalid_request", message = "Missing tags" });
        }

        var errors = new List<string>();

        foreach (var item in req.Tags)
        {
            try
            {
                // Check access permission
                var projectData = _projectService.GetProject();
                if (projectData.Tags.TryGetValue(item.Id, out var tag))
                {
                    if (tag.Access == "ro")
                    {
                        errors.Add($"{item.Id}: read-only tag, write rejected");
                        continue;
                    }
                }

                // Write to physical device via driver
                if (!await _registry.SetTagValue(item.Id, item.Value!))
                {
                    errors.Add($"{item.Id}: tag not found or write failed");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "setTagValue failed for tag {TagId}", item.Id);
                errors.Add($"{item.Id}: {ex.Message}");
            }
        }

        return Ok(errors);
    }
}
