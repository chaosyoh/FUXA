using Core.Models;
using Microsoft.AspNetCore.Mvc;
using Runtime.Project;
using Server.Services;
using System.Text.Json.Nodes;

namespace Server.Controllers;

[ApiController]
public class DeviceImportExportController : ControllerBase
{
    private readonly ILogger<DeviceImportExportController> _logger;
    private readonly IProjectService _projectService;
    private readonly DeviceXlsService _xlsService;
    private readonly KepserverConverterService _kepConverterService;

    public DeviceImportExportController(
        ILogger<DeviceImportExportController> logger,
        IProjectService projectService,
        DeviceXlsService xlsService,
        KepserverConverterService kepConverterService)
    {
        _logger = logger;
        _projectService = projectService;
        _xlsService = xlsService;
        _kepConverterService = kepConverterService;
    }

    /// <summary>
    /// Export devices as xlsx file
    /// </summary>
    [HttpGet]
    [Route("/api/devices/export")]
    public IActionResult ExportDevices([FromQuery] string type = "xls")
    {
        try
        {
            var projectData = _projectService.GetProject();
            var devices = projectData.Devices;
            var scripts = projectData.Scripts ?? new List<JsonNode?>();
            var deviceFolders = projectData.DeviceFolders;

            var xlsBytes = _xlsService.GenerateXls(devices, scripts, deviceFolders);

            var filename = "fuxa-devices.xlsx";
            return File(xlsBytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to export devices");
            return StatusCode(500, new { error = "export_failed", message = ex.Message });
        }
    }

    /// <summary>
    /// Import devices from uploaded xlsx file
    /// </summary>
    [HttpPost]
    [Route("/api/devices/import")]
    [RequestSizeLimit(100 * 1024 * 1024)] // 100MB
    public IActionResult ImportDevices(IFormFile file, [FromQuery] bool isTemplate = false)
    {
        if (file == null || file.Length == 0)
        {
            return BadRequest(new { error = "invalid_request", message = "No file uploaded" });
        }

        try
        {
            var projectData = _projectService.GetProject();
            var scripts = projectData.Scripts ?? new List<JsonNode?>();

            using var stream = file.OpenReadStream();
            var (devices, deviceFolders) = _xlsService.ParseXls(stream, isTemplate, scripts);

            return Ok(new { devices, deviceFolders });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to import devices");
            return BadRequest(new { error = "import_failed", message = ex.Message });
        }
    }

    /// <summary>
    /// Import devices from uploaded KepServer JSON file
    /// Merge logic: same name + same type → update device property & merge tags by name
    /// </summary>
    [HttpPost]
    [Route("/api/devices/import-kepserver")]
    [RequestSizeLimit(100 * 1024 * 1024)] // 100MB
    public IActionResult ImportKepserver(IFormFile file)
    {
        if (file == null || file.Length == 0)
        {
            return BadRequest(new { error = "invalid_request", message = "No file uploaded" });
        }

        try
        {
            // Read file content
            using var reader = new StreamReader(file.OpenReadStream());
            var rawText = reader.ReadToEnd();

            // Strip BOM and comments
            var cleanText = KepserverConverterService.StripJsonComments(rawText);

            // Parse JSON
            var kepJson = JsonNode.Parse(cleanText);
            if (kepJson == null)
            {
                return BadRequest(new { error = "import_failed", message = "Invalid JSON content" });
            }

            // Convert to FUXA devices
            var convertedDevices = _kepConverterService.ConvertKepserverToFuxa(kepJson);

            // Get existing devices for merge
            var projectData = _projectService.GetProject();
            var existingDevices = projectData.Devices;

            // Merge
            var mergedDevices = _kepConverterService.MergeDevices(convertedDevices, existingDevices);

            return Ok(new { devices = mergedDevices });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to import KepServer config");
            return BadRequest(new { error = "import_failed", message = ex.Message });
        }
    }
}
