using Core.Models;

namespace Runtime.Plugins;

public interface IPluginService
{
    List<Plugin> GetPlugins();
    Plugin? GetPlugin(string type);
    void ActivatePlugin(string type);
}
