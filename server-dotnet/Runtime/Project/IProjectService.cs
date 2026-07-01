
using Core.Models;
using System.Text.Json;

namespace Runtime.Project;

public interface IProjectService
{
    ICollection<KeyValuePair<int, List<Tag>>> GetArchiveDic();
    ProjectData GetProject();
    Task Load();
    Task SetProjectData(string cmd, JsonElement value);
    Task SetProject(object projectJson);
}