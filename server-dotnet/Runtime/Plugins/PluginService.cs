using Core.Models;
using Microsoft.Extensions.Logging;

namespace Runtime.Plugins;

public class PluginService : IPluginService
{
    private readonly ILogger<PluginService> _logger;
    private readonly List<Plugin> _plugins;

    public PluginService(ILogger<PluginService> logger)
    {
        _logger = logger;
        _plugins = new List<Plugin>
        {
            // Drivers with implementations compiled into the .NET solution
            new Plugin { Name = "OPC UA", Type = "OPCUA", Version = "1.0.0", Current = "1.0.0", Status = "installed", Group = "connection-device" },
            new Plugin { Name = "Modbus TCP", Type = "ModbusTCP", Version = "1.0.0", Current = "1.0.0", Status = "installed", Group = "connection-device" },
            new Plugin { Name = "Siemens S7", Type = "SiemensS7", Version = "1.0.0", Current = "1.0.0", Status = "installed", Group = "connection-device" },
            new Plugin { Name = "MQTT Client", Type = "MQTTclient", Version = "1.0.0", Current = "1.0.0", Status = "installed", Group = "connection-device" },
            new Plugin { Name = "HTTP Request", Type = "WebAPI", Version = "1.0.0", Current = "1.0.0", Status = "installed", Group = "connection-device" },
            // Drivers not yet implemented in .NET backend
            new Plugin { Name = "BACnet", Type = "BACnet", Version = "", Current = "", Status = "", Group = "connection-device" },
            new Plugin { Name = "EtherNet/IP", Type = "EthernetIP", Version = "", Current = "", Status = "", Group = "connection-device" },
            new Plugin { Name = "ODBC", Type = "ODBC", Version = "", Current = "", Status = "", Group = "connection-database" },
            new Plugin { Name = "ADS Client", Type = "ADSclient", Version = "", Current = "", Status = "", Group = "connection-device" },
            new Plugin { Name = "GPIO", Type = "GPIO", Version = "", Current = "", Status = "", Group = "connection-device" },
            new Plugin { Name = "WebCam", Type = "WebCam", Version = "", Current = "", Status = "", Group = "connection-device" },
            new Plugin { Name = "MELSEC", Type = "MELSEC", Version = "", Current = "", Status = "", Group = "connection-device" },
            new Plugin { Name = "Redis", Type = "REDIS", Version = "", Current = "", Status = "", Group = "connection-device" },
        };
    }

    public List<Plugin> GetPlugins()
    {
        return _plugins;
    }

    public Plugin? GetPlugin(string type)
    {
        return _plugins.FirstOrDefault(p => p.Type.Equals(type, StringComparison.OrdinalIgnoreCase));
    }

    public void ActivatePlugin(string type)
    {
        var plugin = GetPlugin(type);
        if (plugin != null)
        {
            plugin.Status = "installed";
            plugin.Version = "1.0.0";
            plugin.Current = "1.0.0";
            _logger.LogInformation("Plugin activated: {Type}", type);
        }
        else
        {
            _logger.LogWarning("Plugin not found for activation: {Type}", type);
        }
    }
}
