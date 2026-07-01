using Core.Models.Requests;
using Core.Settings;
using Microsoft.AspNetCore.Mvc;
using Runtime.Notificator;

namespace Server.Controllers;

[ApiController]
public class DiagnoseController : ControllerBase
{
    private readonly ILogger<DiagnoseController> _logger;
    private readonly INotificatorService _notificatorService;

    public DiagnoseController(ILogger<DiagnoseController> logger, INotificatorService notificatorService)
    {
        _logger = logger;
        _notificatorService = notificatorService;
    }

    [HttpGet, HttpPost]
    [Route("/api/logsdir")]
    public IActionResult GetLogsdir()
    {
        var settings = AppSettings.GetSettings();
        if (!Directory.Exists(settings.LogDir))
        {
            return Ok(new List<string>());
        }
        var files = Directory.GetFiles(settings.LogDir)
            .Select(Path.GetFileName)
            .Where(f => f != null)
            .ToList();
        return Ok(files);
    }

    [HttpGet, HttpPost]
    [Route("/api/logs")]
    public IActionResult GetLogs()
    {
        var fileName = Request.Query["file"].FirstOrDefault() ?? "fuxa.log";
        // Sanitize filename - prevent directory traversal
        fileName = fileName.Replace("..", "").Replace("/", "").Replace("\\", "");

        var settings = AppSettings.GetSettings();
        var filePath = Path.Combine(settings.LogDir, fileName);

        if (!System.IO.File.Exists(filePath))
        {
            return NotFound(new { error = "not_found", message = "Log file not found" });
        }

        return PhysicalFile(filePath, "text/plain", fileName);
    }

    [HttpGet, HttpPost]
    [Route("/api/reportsdir")]
    public IActionResult GetReportsDir()
    {
        var name = Request.Query["name"].FirstOrDefault() ?? string.Empty;
        var settings = AppSettings.GetSettings();

        if (!Directory.Exists(settings.ReoprtsDir))
        {
            return Ok(new List<string>());
        }

        var files = Directory.GetFiles(settings.ReoprtsDir)
            .Select(Path.GetFileName)
            .Where(f => f != null && (string.IsNullOrEmpty(name) || f.StartsWith($"{name}_")))
            .ToList();
        return Ok(files);
    }

    [HttpPost]
    [Route("/api/sendmail")]
    public async Task<IActionResult> SendMail([FromBody] MailRequest? req)
    {
        if (req?.Params?.Msg == null)
        {
            return BadRequest(new { error = "invalid_request", message = "Missing mail message" });
        }

        try
        {
            await _notificatorService.SendMail(req.Params.Msg, req.Params.Smtp);
            return Ok();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send mail");
            return StatusCode(500, new { error = "send_failed", message = ex.Message });
        }
    }
}
