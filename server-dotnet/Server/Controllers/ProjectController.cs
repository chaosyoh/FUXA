using Core.Const;
using Core.Models;
using Core.Models.Requests;
using Core.Settings;
using Core.Utils;
using Microsoft.AspNetCore.Mvc;
using Runtime.Alarms;
using Runtime.Notificator;
using Runtime.Project;
using Runtime.Users;
using System.Text.Json;

namespace Server.Controllers;

[ApiController]
public class ProjectController : ControllerBase
{
    private readonly ILogger<ProjectController> _logger;
    private readonly IProjectService _projectService;
    private readonly ProjectStorage _prjStorage;
    private readonly UserService _userService;
    private readonly IAlarmService _alarmService;
    private readonly INotificatorService _notificatorService;

    public ProjectController(
        ILogger<ProjectController> logger,
        IProjectService projectService,
        ProjectStorage prjStorage,
        UserService userService,
        IAlarmService alarmService,
        INotificatorService notificatorService)
    {
        _logger = logger;
        _projectService = projectService;
        _prjStorage = prjStorage;
        _userService = userService;
        _alarmService = alarmService;
        _notificatorService = notificatorService;
    }

    [HttpGet]
    [Route("/api/project")]
    public IActionResult GetProject()
    {
        var data = _projectService.GetProject();
        // Build response matching Node.js format:
        // - Include server field (only stored properties, omit defaults)
        // - Omit empty arrays (Node.js doesn't include them when empty)
        // - Omit layout/mobileLayout when not stored in DB
        var result = new Dictionary<string, object?>();
        result["version"] = data.Version;

        // server device - only include fields that were actually stored (omit defaults)
        var serverDict = new Dictionary<string, object?>();
        serverDict["id"] = data.Server.Id;
        serverDict["name"] = data.Server.Name;
        serverDict["type"] = data.Server.Type;
        serverDict["property"] = data.Server.Property;
        result["server"] = serverDict;

        result["devices"] = data.Devices;

        // hmi - only include layout/mobileLayout if they have meaningful content
        var hmiDict = new Dictionary<string, object?>();
        hmiDict["views"] = data.Hmi.Views;
        if (data.Hmi.Layout != null && !string.IsNullOrEmpty(data.Hmi.Layout.Start))
        {
            hmiDict["layout"] = data.Hmi.Layout;
        }
        if (data.Hmi.MobileLayout != null && !string.IsNullOrEmpty(data.Hmi.MobileLayout.Start))
        {
            hmiDict["mobileLayout"] = data.Hmi.MobileLayout;
        }
        result["hmi"] = hmiDict;

        if (data.Charts.Count > 0) result["charts"] = data.Charts;
        if (data.Alarms.Count > 0) result["alarms"] = data.Alarms;
        if (data.Notifications.Count > 0) result["notifications"] = data.Notifications;
        if (data.Scripts.Count > 0) result["scripts"] = data.Scripts;
        if (data.Texts.Count > 0) result["texts"] = data.Texts;
        if (data.Reports.Count > 0) result["reports"] = data.Reports;
        if (data.MapsLocations.Count > 0) result["mapsLocations"] = data.MapsLocations;
        if (data.Languages != null) result["languages"] = data.Languages;
        if (data.Graphs.Count > 0) result["graphs"] = data.Graphs;
        if (data.ClientAccess != null) result["clientAccess"] = data.ClientAccess;
        result["timestamp"] = data.Timestamp?.ToString("yyyy/M/d HH:mm:ss")
            ?? DateTime.Now.ToString("yyyy/M/d HH:mm:ss");

        return Ok(result);
    }

    [HttpPost]
    [Route("/api/project")]
    public async Task<IActionResult> SaveProject([FromBody] object? projectJson)
    {
        if (projectJson == null)
        {
            return BadRequest(new { error = "invalid_request", message = "Missing project data" });
        }

        try
        {
            await _projectService.SetProject(projectJson);
            _alarmService.Reset();
            _notificatorService.Reset();
            return Ok();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save project");
            return StatusCode(500, new { error = "save_failed", message = ex.Message });
        }
    }

    [HttpPost]
    [Route("/api/projectData")]
    public async Task<IActionResult> ProjectData([FromBody] ProjectDataRequest? req)
    {
        if (req == null || string.IsNullOrEmpty(req.Cmd))
        {
            return BadRequest(new { error = "invalid_request", message = "Missing cmd" });
        }

        try
        {
            await _projectService.SetProjectData(req.Cmd, req.Data);

            if (req.Cmd is ProjectDataCmdType.SetAlarm or ProjectDataCmdType.DelAlarm
                or ProjectDataCmdType.SetDevice or ProjectDataCmdType.DelDevice)
            {
                _alarmService.Reset();
            }
            if (req.Cmd is ProjectDataCmdType.SetNotification or ProjectDataCmdType.DelNotification
                or ProjectDataCmdType.SetAlarm or ProjectDataCmdType.DelAlarm)
            {
                _notificatorService.Reset();
            }

            return Ok();
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogWarning(ex, "Invalid operation on project data: {Cmd}", req.Cmd);
            return BadRequest(new { error = "invalid_operation", message = ex.Message });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to set project data: {Cmd}", req.Cmd);
            return StatusCode(500, new { error = "set_failed", message = ex.Message });
        }
    }

    [HttpGet]
    [Route("/api/projectdemo")]
    public IActionResult GetProjectDemo()
    {
        try
        {
            var settings = AppSettings.GetSettings();
            var demoFile = Path.Combine(settings.AppDir, "project.demo.fuxap");
            if (!System.IO.File.Exists(demoFile))
            {
                return NotFound();
            }
            var json = System.IO.File.ReadAllText(demoFile);
            return Content(json, "application/json");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to read demo project");
            return StatusCode(500, new { error = "read_failed", message = ex.Message });
        }
    }

    [HttpGet]
    [Route("/api/device")]
    public async Task<IActionResult> GetDeviceProperty([FromQuery] string? query, [FromQuery] string? name)
    {
        if (query != "security" || string.IsNullOrEmpty(name))
        {
            return BadRequest(new { error = "invalid_request", message = "Unsupported query or missing name" });
        }

        try
        {
            var rows = await _prjStorage.GetSection(TableType.DEVICESSECURITY, name);
            if (rows.Count > 0)
            {
                var value = System.Text.Json.JsonSerializer.Deserialize<object>(rows[0].Value, Core.Utils.JsonHelper.Default);
                return Ok(value);
            }
            return Ok();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get device property");
            return StatusCode(500, new { error = "get_failed", message = ex.Message });
        }
    }

    [HttpPost]
    [Route("/api/device")]
    public async Task<IActionResult> SetDeviceProperty([FromBody] DevicePropertyRequest? req)
    {
        if (req?.Params?.Query != "security" || string.IsNullOrEmpty(req.Params.Name))
        {
            return BadRequest(new { error = "invalid_request", message = "Unsupported query or missing name" });
        }

        if (req.Params.Value == null)
        {
            return Ok();
        }

        try
        {
            var section = new SqlSection
            {
                Table = TableType.DEVICESSECURITY,
                Name = req.Params.Name,
                Value = req.Params.Value
            };
            await _prjStorage.SetSection(section);
            return Ok();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to set device property");
            return StatusCode(500, new { error = "set_failed", message = ex.Message });
        }
    }

    [HttpGet]
    [Route("/api/version")]
    public string GetVersion()
    {
        return "1.0.0";
    }

    [HttpGet]
    [Route("/api/settings")]
    public object GetSettings()
    {
        var settings = AppSettings.GetSettings();
        return new
        {
            settings.Version,
            settings.Language,
            settings.HideEditorOnboarding,
            settings.UiPort,
            LogDir = settings.LogDir,
            settings.LogApiLevel,
            DbDir = settings.DbDir,
            settings.DaqEnabled,
            settings.DaqTokenizer,
            Logs = settings.Logs,
            settings.BroadcastAll,
            settings.AllowedOrigins,
            settings.SecureEnabled,
            settings.TokenExpiresIn,
            settings.EnableRefreshCookieAuth,
            settings.RefreshTokenExpiresIn,
            settings.SecureOnlyEditor,
            settings.HeartbeatIntervalSec,
            WebcamSnapShotsDir = settings.WebcamSnapShotsDir,
            settings.WebcamSnapShotsCleanup,
            settings.WebcamSnapShotsRetain,
            settings.SwaggerEnabled,
            settings.NodeRedEnabled,
            settings.NodeRedAuthMode,
            settings.NodeRedUnsafeModules,
            settings.LogFull,
            settings.UserRole,
            Alarms = settings.Alarms,
            Smtp = new
            {
                settings.Stmp.Host,
                settings.Stmp.Port,
                settings.Stmp.Mailsender,
                settings.Stmp.Username,
            },
            Daqstore = settings.DaqStore,
            WorkDir = settings.WorkDir,
            AppDir = settings.AppDir,
            PackageDir = settings.PackageDir,
            SettingsFile = settings.UserSettingsFile,
            Environment = settings.Environment,
            UploadFileDir = settings.UploadFileDir,
            ImagesFileDir = settings.ImagesFileDir,
            WidgetsFileDir = settings.WidgetsFileDir,
            ReportsDir = settings.ReoprtsDir,
            UserSettingsFile = settings.UserSettingsFile,
            HttpUploadFileStatic = settings.HttpUploadFileStatic,
        };
    }

    [HttpPost]
    [Route("/api/settings")]
    public IActionResult SaveSettings([FromBody] object? settingsObj)
    {
        if (settingsObj == null)
        {
            return BadRequest(new { error = "invalid_request", message = "Missing settings" });
        }

        try
        {
            var settings = AppSettings.GetSettings();

            // Update in-memory settings singleton
            var json = System.Text.Json.JsonSerializer.Serialize(settingsObj);
            var parsed = System.Text.Json.JsonSerializer.Deserialize<Settings>(json, Core.Utils.JsonHelper.Default);
            if (parsed != null)
            {
                settings.MergeUserSettings(parsed);
            }

            // Persist to appsettings.json (update the "Fuxa" section, preserve other sections)
            var appSettingsPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "appsettings.json");
            System.Text.Json.Nodes.JsonObject root;
            if (System.IO.File.Exists(appSettingsPath))
            {
                var existingJson = System.IO.File.ReadAllText(appSettingsPath);
                root = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.Nodes.JsonObject>(existingJson)
                    ?? new System.Text.Json.Nodes.JsonObject();
            }
            else
            {
                root = new System.Text.Json.Nodes.JsonObject();
            }

            var fuxaNode = System.Text.Json.JsonSerializer.SerializeToNode(settingsObj);
            root["Fuxa"] = fuxaNode;

            var writeOptions = new System.Text.Json.JsonSerializerOptions { WriteIndented = true };
            System.IO.File.WriteAllText(appSettingsPath, root.ToJsonString(writeOptions));

            return Ok();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save settings");
            return StatusCode(500, new { error = "save_failed", message = ex.Message });
        }
    }

    [HttpGet]
    [Route("/api/projectVersion")]
    public IActionResult ProjectVersion()
    {
        var projectData = _projectService.GetProject();
        var timestamp = projectData.Timestamp?.ToString("yyyy/M/d HH:mm:ss")
            ?? DateTime.Now.ToString("yyyy/M/d HH:mm:ss");
        return Ok(new { timestamp });
    }

    [HttpPost]
    [Route("/api/upload")]
    public IActionResult Upload([FromBody] UploadResourceRequest? req)
    {
        if (req?.Resource == null || string.IsNullOrEmpty(req.Resource.Name))
        {
            return BadRequest(new { error = "invalid_request", message = "Missing resource" });
        }

        try
        {
            var settings = AppSettings.GetSettings();
            var destDir = settings.UploadFileDir;

            if (!string.IsNullOrEmpty(req.Destination))
            {
                var sanitizedDest = req.Destination.Replace("..", "");
                destDir = Path.Combine(settings.AppDir, sanitizedDest.TrimStart('/'));
            }

            if (!Directory.Exists(destDir))
            {
                Directory.CreateDirectory(destDir);
            }

            var fileName = req.Resource.FullPath ?? req.Resource.Name;
            fileName = fileName.Replace("..", "").TrimStart('/');
            var filePath = Path.Combine(destDir, fileName);

            var parentDir = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(parentDir) && !Directory.Exists(parentDir))
            {
                Directory.CreateDirectory(parentDir);
            }

            if (!string.IsNullOrEmpty(req.Resource.Data))
            {
                var data = req.Resource.Data;
                var commaIdx = data.IndexOf(',');
                if (commaIdx >= 0)
                {
                    data = data[(commaIdx + 1)..];
                }

                if (req.Resource.Type == "svg" || req.Resource.Type == "image/svg+xml")
                {
                    System.IO.File.WriteAllText(filePath, req.Resource.Data);
                }
                else
                {
                    var bytes = Convert.FromBase64String(data);
                    System.IO.File.WriteAllBytes(filePath, bytes);
                }
            }

            var location = $"/_upload_files/{fileName}";
            return Ok(new { location });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Upload failed");
            return StatusCode(500, new { error = "upload_failed", message = ex.Message });
        }
    }

    [HttpPost]
    [Route("/api/getTagValues")]
    public IActionResult GetTagValues([FromBody] List<string>? tagIds)
    {
        if (tagIds == null || tagIds.Count == 0)
        {
            return Ok(new Dictionary<string, object?>());
        }

        var projectData = _projectService.GetProject();
        var result = new Dictionary<string, object?>();
        foreach (var id in tagIds)
        {
            if (projectData.Tags.TryGetValue(id, out var tag))
            {
                result[id] = tag.Value;
            }
            else
            {
                result[id] = null;
            }
        }
        return Ok(result);
    }

    [HttpGet]
    [Route("/api/getDevices")]
    public IActionResult GetDevices()
    {
        var projectData = _projectService.GetProject();
        return Ok(projectData.Devices);
    }

    [HttpPost]
    [Route("/api/heartbeat")]
    public IActionResult Heartbeat([FromBody] HeartbeatRequest? req)
    {
        var settings = AppSettings.GetSettings();
        if (!settings.SecureEnabled)
        {
            return Ok(new HeartbeatResponse { Message = "guest" });
        }

        // TODO: Extract user from JWT token and refresh if needed
        return Ok(new HeartbeatResponse { Message = "guest" });
    }
}
