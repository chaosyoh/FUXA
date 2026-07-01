using Core.Models.Requests;
using Core.Settings;
using Microsoft.AspNetCore.Mvc;
using System.Text.RegularExpressions;

namespace Server.Controllers;

[ApiController]
public class ReportsController : ControllerBase
{
    private readonly ILogger<ReportsController> _logger;

    public ReportsController(ILogger<ReportsController> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// GET /api/reportsQuery - 查询报告文件列表（支持按名称过滤和数量限制）
    /// </summary>
    [HttpGet]
    [Route("/api/reportsQuery")]
    public IActionResult GetReportsQuery()
    {
        try
        {
            var queryStr = Request.Query["query"].FirstOrDefault();
            var filter = new ReportsQueryFilter();
            if (!string.IsNullOrEmpty(queryStr))
            {
                filter = System.Text.Json.JsonSerializer.Deserialize<ReportsQueryFilter>(queryStr, Core.Utils.JsonHelper.Default)
                         ?? new ReportsQueryFilter();
            }

            var settings = AppSettings.GetSettings();
            var reportPath = settings.ReoprtsDir;
            if (!Directory.Exists(reportPath))
            {
                return Ok(Array.Empty<object>());
            }

            var reportFiles = Directory.GetFiles(reportPath);
            var result = new List<ReportFileInfo>();
            var datePattern = new Regex(@"_(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})$");

            foreach (var filePath in reportFiles)
            {
                try
                {
                    var file = Path.GetFileName(filePath);
                    var fileNameWithoutExt = Path.GetFileNameWithoutExtension(file);

                    if (!string.IsNullOrEmpty(filter.Name) && !fileNameWithoutExt.Contains(filter.Name))
                    {
                        continue;
                    }

                    var reportName = Regex.Replace(fileNameWithoutExt, @"_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$", "");
                    DateTime? created = null;
                    var match = datePattern.Match(fileNameWithoutExt);
                    if (match.Success)
                    {
                        var year = int.Parse(match.Groups[1].Value);
                        var month = int.Parse(match.Groups[2].Value);
                        var day = int.Parse(match.Groups[3].Value);
                        var hour = int.Parse(match.Groups[4].Value);
                        var minute = int.Parse(match.Groups[5].Value);
                        var second = int.Parse(match.Groups[6].Value);
                        created = new DateTime(year, month, day, hour, minute, second);
                    }

                    result.Add(new ReportFileInfo
                    {
                        FileName = file,
                        ReportName = reportName,
                        Created = created,
                    });
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Parsing {File} failed", filePath);
                }
            }

            if (filter.Count.HasValue && filter.Count.Value > 0)
            {
                result = result
                    .Where(r => r.Created != null)
                    .OrderByDescending(r => r.Created)
                    .Take(filter.Count.Value)
                    .ToList();
            }

            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "api get reportsQuery failed");
            return BadRequest(new { error = ex.StackTrace, message = ex.Message });
        }
    }

    /// <summary>
    /// POST /api/reportBuild - 触发构建报告
    /// </summary>
    [HttpPost]
    [Route("/api/reportBuild")]
    public IActionResult ReportBuild([FromBody] ReportBuildRequest? req)
    {
        // TODO: integrate with job manager's forceReport once implemented
        return StatusCode(501, new { error = "not_implemented", message = "Report build is not yet available in the .NET backend" });
    }

    /// <summary>
    /// POST /api/reportRemoveFile - 删除指定报告文件
    /// </summary>
    [HttpPost]
    [Route("/api/reportRemoveFile")]
    public IActionResult ReportRemoveFile([FromBody] ReportRemoveFileRequest? req)
    {
        if (req?.Params == null || string.IsNullOrEmpty(req.Params.FileName))
        {
            return BadRequest(new { error = "invalid_request", message = "Missing fileName" });
        }

        try
        {
            var settings = AppSettings.GetSettings();
            var reportPath = settings.ReoprtsDir;
            if (!Directory.Exists(reportPath))
            {
                return NotFound(new { error = "not_found", message = "Reports directory not found" });
            }

            // Sanitize file name to prevent path traversal
            var fileName = req.Params.FileName.Replace("..", "").TrimStart('/').TrimStart('\\');
            var filePath = Path.Combine(reportPath, fileName);
            var fullPath = Path.GetFullPath(filePath);
            var fullReportDir = Path.GetFullPath(reportPath);
            if (!fullPath.StartsWith(fullReportDir))
            {
                return BadRequest(new { error = "invalid_path", message = "Invalid report file path" });
            }

            if (!System.IO.File.Exists(fullPath))
            {
                return NotFound(new { error = "not_found", message = "Report file not found" });
            }

            System.IO.File.Delete(fullPath);
            _logger.LogInformation("Report file '{FilePath}' deleted", fullPath);
            return Ok();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "api post reportRemoveFile failed");
            return BadRequest(new { error = "error", message = ex.Message });
        }
    }

    private class ReportFileInfo
    {
        public string FileName { get; set; } = string.Empty;
        public string ReportName { get; set; } = string.Empty;
        public DateTime? Created { get; set; }
    }
}
