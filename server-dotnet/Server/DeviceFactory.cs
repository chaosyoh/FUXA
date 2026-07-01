using Core.Const;
using Core.Models;
using DeviceMQTT;
using DeviceModbusRTU;
using DeviceS7;
using DeviceWebAPI;
using Microsoft.AspNetCore.SignalR;
using OpcUA;
using Runtime;
using Runtime.Project;

namespace Server;

public static class DeviceFactory
{
    public static IDevice? Create(Device config, IHubContext<DataHub> hubCtx, IServiceProvider serviceProvider)
    {
        return config.Type switch
        {
            DeviceType.OPCUA => new OpcUAClient(hubCtx),
            DeviceType.ModbusTCP => new ModbusTcpClient(hubCtx),
            DeviceType.ModbusRTU => new ModbusRtuClient(hubCtx),
            DeviceType.SiemensS7 => new S7DeviceClient(hubCtx),
            DeviceType.MQTTclient => new MqttDeviceClient(hubCtx),
            DeviceType.WebAPI => new HttpApiClient(hubCtx),
            DeviceType.FuxaServer => new FuxaServerClient(hubCtx, serviceProvider.GetRequiredService<IProjectService>()),
            _ => null
        };
    }
}
