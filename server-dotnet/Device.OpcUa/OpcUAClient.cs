using Core;
using Core.Models;
using Microsoft.Extensions.Logging;
using Opc.Ua;
using Opc.Ua.Client;
using OpcUaHelper;
using SQLitePCL;
using System.Data.OscarClient;
using System.Threading.Tasks;

namespace Device.OpcUa;

public class OpcUAClient : DeviceBase
{
    private OpcUaClient _client;
    public OpcUAClient(Core.Models.Device data) : base(data)
    {
        _client = new OpcUaClient();
        _client.ConnectComplete += _client_ConnectComplete;
    }

    private void _client_ConnectComplete(object? sender, EventArgs e)
    {
        if (sender is OpcUaClient client && client.Connected)
        {
            Console.WriteLine("OPCUA连接成功");
        }
    }

    public override async Task<bool> Connect()
    {
        try
        {
            _client.UserIdentity = new UserIdentity(new AnonymousIdentityToken());
            await _client.ConnectServer(Data.Property.Address);
            var dic = new Dictionary<string, List<Tag>>();
            var nodeIds = new List<string>();
            foreach (var tag in Data.Tags.Values)
            {
                if (dic.TryGetValue(tag.Address, out var list))
                {
                    list.Add(tag);
                }
                else
                {
                    nodeIds.Add(tag.Address);
                    dic.Add(tag.Address, [tag]);
                }
            }
            _client.AddSubscription("A", nodeIds.ToArray(), (key, _tag, e) =>
            {
                var notification = e.NotificationValue as MonitoredItemNotification;
                if (dic.TryGetValue(_tag.ResolvedNodeId.ToString(), out var list))
                {
                    foreach (var tag in list)
                    {
                        Console.WriteLine(tag.Label + ": " + notification?.Value?.WrappedValue.Value?.ToString());
                    }
                }
            });
            return true;

        }
        catch (Exception ex)
        {
            ClearVarsValue();
            return false;
        }
    }

    public override async Task<bool> Disconnect()
    {
        try
        {
            //await client.DisconnectAsync();
            connected = false;
            monitored = false;
            _checkWorking(false);
            ClearVarsValue();
            return true;

        }
        catch (Exception ex)
        {
            return false;
        }

    }

    public override async Task<bool> Polling()
    {
        //List<NodeId> nodeIds = new List<NodeId>();
        //foreach (var tag in Data.Tags.Values)
        //{
        //    var nodeId = new NodeId(tag.Address);
        //    nodeIds.Add(nodeId);
        //}
        //// dataValues按顺序定义的值，每个值里面需要重新判断类型
        //List<DataValue> dataValues = await _client.ReadNodesAsync(nodeIds.ToArray());
        //var dic = new Dictionary<string, List<Tag>>();
        //var nodeIds = new List<string>();
        //foreach (var tag in Data.Tags.Values)
        //{
        //    if (dic.TryGetValue(tag.Address, out var list))
        //    {
        //        list.Add(tag);
        //    }
        //    else
        //    {
        //        nodeIds.Add(tag.Address);
        //        dic.Add(tag.Address, [tag]);
        //    }
        //}
        //_client.AddSubscription("A", nodeIds.ToArray(), (key, _tag, e) =>
        //{
        //    var notification = e.NotificationValue as MonitoredItemNotification;
        //    if (dic.TryGetValue(_tag.ResolvedNodeId.ToString(), out var list))
        //    {
        //        foreach (var tag in list)
        //        {
        //            Console.WriteLine(tag.Label + ": " + notification?.Value?.WrappedValue.Value?.ToString());
        //        }
        //    }
        //});



        return true;
    }

    public override Task<bool> SetValue(string id, object value)
    {
        throw new NotImplementedException();
    }

    private void ClearVarsValue()
    {

    }

    private bool _checkWorking(bool check)
    {
        if (check && working)
        {
            Console.WriteLine($"{Data.Name}连接中");
            return false;
        }
        working = check;
        return true;
    }
}
