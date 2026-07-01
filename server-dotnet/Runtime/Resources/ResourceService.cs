using Core.Models;
using Core.Settings;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Runtime.Resources;

public class ResourceService : IResourceService
{
    private readonly ILogger<ResourceService> _logger;
    private static readonly HashSet<string> ImageExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".jpg", ".jpeg", ".png", ".gif", ".svg", ".mp4", ".webm", ".ogg", ".ogv"
    };
    private static readonly HashSet<string> WidgetExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".svg"
    };

    public ResourceService(ILogger<ResourceService> logger)
    {
        _logger = logger;
    }

    public ResourceListResult GetImages()
    {
        var settings = AppSettings.GetSettings();
        return ScanDirectory(settings.ImagesFileDir, ImageExtensions, "_images");
    }

    public ResourceListResult GetResources()
    {
        var settings = AppSettings.GetSettings();
        return ScanDirectory(settings.ImagesFileDir, ImageExtensions, "_images");
    }

    public ResourceListResult GetWidgets()
    {
        var settings = AppSettings.GetSettings();
        return ScanDirectory(settings.WidgetsFileDir, WidgetExtensions, "_widgets");
    }

    public void RemoveFile(string basePath, string relativePath)
    {
        // Prevent directory traversal
        var sanitized = relativePath.Replace("..", "").Replace("\\", "/");
        var fullPath = Path.GetFullPath(Path.Combine(basePath, sanitized));
        if (!fullPath.StartsWith(Path.GetFullPath(basePath)))
        {
            _logger.LogWarning("Path traversal attempt blocked: {Path}", relativePath);
            return;
        }
        if (File.Exists(fullPath))
        {
            File.Delete(fullPath);
            _logger.LogInformation("Deleted file: {Path}", fullPath);
        }
    }

    public Task<List<JsonNode?>> GetTemplates()
    {
        var templates = new List<JsonNode?>();
        var templateDir = GetTemplateDir();
        if (!Directory.Exists(templateDir)) return Task.FromResult(templates);

        try
        {
            foreach (var file in Directory.GetFiles(templateDir, "*.json"))
            {
                var json = File.ReadAllText(file);
                var node = JsonNode.Parse(json);
                if (node != null) templates.Add(node);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to read templates");
        }
        return Task.FromResult(templates);
    }

    public Task SetTemplate(object template)
    {
        try
        {
            var templateDir = GetTemplateDir();
            if (!Directory.Exists(templateDir))
            {
                Directory.CreateDirectory(templateDir);
            }

            // [FromBody] object? is bound as System.Text.Json.JsonElement by ASP.NET Core.
            JsonNode? node;
            string rawJson;
            if (template is JsonElement je)
            {
                rawJson = je.GetRawText();
                node = JsonNode.Parse(rawJson);
            }
            else if (template is JsonNode jn)
            {
                node = jn;
                rawJson = jn.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
            }
            else
            {
                rawJson = JsonSerializer.Serialize(template, new JsonSerializerOptions { WriteIndented = true });
                node = JsonNode.Parse(rawJson);
            }

            var id = node?["id"]?.GetValue<string>();
            if (string.IsNullOrEmpty(id))
            {
                _logger.LogWarning("Template missing id, cannot save");
                return Task.CompletedTask;
            }

            var sanitizedId = id.Replace("..", "").Replace("/", "").Replace("\\", "");
            var filePath = Path.Combine(templateDir, $"{sanitizedId}.json");
            File.WriteAllText(filePath, rawJson);
            _logger.LogInformation("Saved template: {Id}", id);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save template");
        }
        return Task.CompletedTask;
    }

    public Task RemoveTemplates(string templateIds)
    {
        try
        {
            var templateDir = GetTemplateDir();
            if (!Directory.Exists(templateDir)) return Task.CompletedTask;

            var ids = templateIds.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            foreach (var id in ids)
            {
                var sanitizedId = id.Replace("..", "").Replace("/", "").Replace("\\", "");
                var filePath = Path.Combine(templateDir, $"{sanitizedId}.json");
                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                    _logger.LogInformation("Deleted template: {Id}", id);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to remove templates");
        }
        return Task.CompletedTask;
    }

    private static string GetTemplateDir()
    {
        var settings = AppSettings.GetSettings();
        return Path.Combine(settings.WorkDir, "_templates");
    }

    private ResourceListResult ScanDirectory(string baseDir, HashSet<string> allowedExtensions, string pathPrefix)
    {
        var result = new ResourceListResult();
        if (!Directory.Exists(baseDir)) return result;

        try
        {
            // Root files
            var rootFiles = Directory.GetFiles(baseDir)
                .Where(f => allowedExtensions.Contains(Path.GetExtension(f)))
                .Select(f => new ResourceItem
                {
                    Path = $"{pathPrefix}/{Path.GetFileName(f)}",
                    Name = Path.GetFileNameWithoutExtension(f),
                })
                .ToList();

            if (rootFiles.Count > 0)
            {
                result.Groups.Add(new ResourceGroup { Name = "", Items = rootFiles });
            }

            // Subdirectories
            foreach (var dir in Directory.GetDirectories(baseDir))
            {
                var dirName = Path.GetFileName(dir);
                var files = Directory.GetFiles(dir, "*.*", SearchOption.AllDirectories)
                    .Where(f => allowedExtensions.Contains(Path.GetExtension(f)))
                    .Select(f =>
                    {
                        var relative = Path.GetRelativePath(baseDir, f).Replace("\\", "/");
                        return new ResourceItem
                        {
                            Path = $"{pathPrefix}/{relative}",
                            Name = Path.GetFileNameWithoutExtension(f),
                        };
                    })
                    .ToList();

                if (files.Count > 0)
                {
                    result.Groups.Add(new ResourceGroup { Name = dirName, Items = files });
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to scan directory: {Dir}", baseDir);
        }

        return result;
    }
}
