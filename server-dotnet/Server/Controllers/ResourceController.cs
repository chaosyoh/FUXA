using Core.Models;
using Core.Models.Requests;
using Core.Settings;
using Microsoft.AspNetCore.Mvc;
using Runtime.Resources;

namespace Server.Controllers;

[ApiController]
public class ResourceController : ControllerBase
{
    private readonly ILogger<ResourceController> _logger;
    private readonly IResourceService _resourceService;

    public ResourceController(ILogger<ResourceController> logger, IResourceService resourceService)
    {
        _logger = logger;
        _resourceService = resourceService;
    }

    [HttpGet]
    [Route("/api/resources/images")]
    public IActionResult GetImages()
    {
        var result = _resourceService.GetImages();
        return Ok(result);
    }

    [HttpGet]
    [Route("/api/resources/resources")]
    public IActionResult GetResources()
    {
        var result = _resourceService.GetResources();
        return Ok(result);
    }

    [HttpPost]
    [Route("/api/resources/remove")]
    public IActionResult RemoveResource([FromBody] RemoveResourceRequest? req)
    {
        if (req == null || string.IsNullOrEmpty(req.File))
        {
            return BadRequest(new { error = "invalid_request", message = "Missing file path" });
        }

        var settings = AppSettings.GetSettings();
        _resourceService.RemoveFile(settings.ImagesFileDir, req.File);
        return Ok();
    }

    [HttpGet]
    [Route("/api/resources/widgets")]
    public IActionResult GetWidgets()
    {
        var result = _resourceService.GetWidgets();
        return Ok(result);
    }

    [HttpPost]
    [Route("/api/resources/removeWidget")]
    public IActionResult RemoveWidget([FromBody] RemoveWidgetRequest? req)
    {
        if (req == null || string.IsNullOrEmpty(req.Path))
        {
            return BadRequest(new { error = "invalid_request", message = "Missing path" });
        }

        var settings = AppSettings.GetSettings();
        _resourceService.RemoveFile(settings.WidgetsFileDir, req.Path);
        return Ok();
    }

    [HttpGet]
    [Route("/api/resources/templates")]
    public async Task<IActionResult> GetTemplates()
    {
        var result = await _resourceService.GetTemplates();
        return Ok(result);
    }

    [HttpPost]
    [Route("/api/resources/template")]
    public async Task<IActionResult> SaveTemplate([FromBody] SaveTemplateRequest? req)
    {
        if (req?.Template == null)
        {
            return BadRequest(new { error = "invalid_request", message = "Missing template" });
        }

        await _resourceService.SetTemplate(req.Template);
        return Ok();
    }

    [HttpDelete]
    [Route("/api/resources/templates")]
    public async Task<IActionResult> RemoveTemplates()
    {
        var templates = Request.Query["templates"].FirstOrDefault();
        if (string.IsNullOrEmpty(templates))
        {
            return BadRequest(new { error = "invalid_request", message = "Missing templates" });
        }

        await _resourceService.RemoveTemplates(templates);
        return Ok();
    }

    [HttpGet]
    [Route("/api/resources/generateImage")]
    public IActionResult GenerateImage()
    {
        return StatusCode(501, new { error = "not_implemented", message = "Chart image generation is not yet available in the .NET backend" });
    }
}
