using Core.Models;
using System.Text.Json.Nodes;

namespace Runtime.Resources;

public interface IResourceService
{
    ResourceListResult GetImages();
    ResourceListResult GetResources();
    ResourceListResult GetWidgets();
    void RemoveFile(string basePath, string relativePath);
    Task<List<JsonNode?>> GetTemplates();
    Task SetTemplate(object template);
    Task RemoveTemplates(string templateIds);
}
