using Core.Const;
using Core.Models;
using Microsoft.AspNetCore.SignalR;
using Opc.Ua;
using Opc.Ua.Client;
using OpcUaHelper;
using Runtime;

namespace OpcUA;

public class OpcUAClient : DeviceBase, IBrowsableDevice
{
    private OpcUaClient _client;

    public OpcUAClient(IHubContext<DataHub> hubCtx) : base(hubCtx)
    {
        _client = new OpcUaClient();
        _client.ConnectComplete += OnConnectComplete;
    }

    private void OnConnectComplete(object? sender, EventArgs e)
    {
        if (sender is OpcUaClient client && client.Connected)
        {
            connected = true;
        }
    }

    public override async Task<bool> Connect()
    {
        if (!CheckWorking(true)) return false;
        await NotifyStatus(DeviceStatus.Busy);
        try
        {
            _client.UserIdentity = new UserIdentity(new AnonymousIdentityToken());
            await _client.ConnectServer(Data.Property.Address);
            connected = true;
            CheckWorking(false);
            await NotifyStatus(DeviceStatus.Ok);
            return true;
        }
        catch (Exception)
        {
            connected = false;
            CheckWorking(false);
            ClearVarsValue();
            await NotifyStatus(DeviceStatus.Error);
            return false;
        }
    }

    public override async Task<bool> Disconnect()
    {
        try
        {
            _client.Disconnect();
            connected = false;
            monitored = false;
            CheckWorking(false);
            ClearVarsValue();
            await NotifyStatus(DeviceStatus.Off);
            return true;
        }
        catch (Exception)
        {
            connected = false;
            return false;
        }
    }

    public override async Task<bool> Polling()
    {
        if (!connected) return false;

        if (!monitored)
        {
            // 首次轮询: 建立订阅
            var dic = new Dictionary<string, List<Tag>>();
            var nodeIds = new List<string>();
            foreach (var tag in Data.Tags.Values)
            {
                if (string.IsNullOrEmpty(tag.Address)) continue;
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

            if (nodeIds.Count == 0) return true;

            _client.AddSubscription("A", nodeIds.ToArray(), (key, monitoredItem, e) =>
            {
                var notification = e.NotificationValue as MonitoredItemNotification;
                if (notification == null) return;
                if (dic.TryGetValue(monitoredItem.ResolvedNodeId.ToString(), out var tags))
                {
                    foreach (var tag in tags)
                    {
                        tag.Value = notification.Value?.WrappedValue.Value;
                    }
                }
            });
            monitored = true;
            lastReadTimestamp = DateTime.Now;
        }
        else
        {
            // 后续轮询: 订阅已建立，更新时间戳并触发 DAQ
            lastReadTimestamp = DateTime.Now;
            AddDaq();
        }

        return true;
    }

    public override async Task<bool> SetValue(string id, object value)
    {
        if (!connected) return false;
        if (!Data.Tags.TryGetValue(id, out var tag)) return false;
        if (string.IsNullOrEmpty(tag.Address)) return false;

        try
        {
            var nodeId = tag.Address;
            var writeValue = new WriteValue
            {
                NodeId = new NodeId(nodeId),
                AttributeId = Attributes.Value,
                Value = new DataValue(new Variant(value))
            };
            var writeValues = new WriteValueCollection { writeValue };
            var session = _client.Session;
            if (session == null) return false;

            session.Write(null, writeValues, out var results, out _);
            if (results != null && results.Count > 0 && StatusCode.IsGood(results[0]))
            {
                tag.Value = value;
                return true;
            }
            return false;
        }
        catch (Exception)
        {
            return false;
        }
    }

    protected override void ClearVarsValue()
    {
        base.ClearVarsValue();
        monitored = false;
    }

    #region IBrowsableDevice

    public async Task<object?> Browse(string? nodeId)
    {
        var session = _client.Session;
        if (session == null) throw new InvalidOperationException("OPC UA session not connected");

        // 与 Node.js 端保持一致：默认从 RootFolder 开始浏览
        var startNodeId = string.IsNullOrEmpty(nodeId)
            ? ObjectIds.RootFolder
            : NodeId.Parse(nodeId);

        var browseDesc = new BrowseDescriptionCollection
        {
            new BrowseDescription
            {
                NodeId = startNodeId,
                BrowseDirection = BrowseDirection.Forward,
                ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
                IncludeSubtypes = true,
                NodeClassMask = 0,
                ResultMask = (uint)BrowseResultMask.All
            }
        };

        session.Browse(null, null, 0, browseDesc, out var results, out _);

        if (results == null || results.Count == 0)
            return new List<object>();

        var nodes = new List<object>();
        foreach (var reference in results[0].References)
        {
            nodes.Add(ToOpcNode(reference));
        }

        // 处理 ContinuationPoint（大量子节点时分页）
        var cp = results[0].ContinuationPoint;
        while (cp != null && cp.Length > 0)
        {
            session.BrowseNext(null, false, new ByteStringCollection { cp }, out var nextResults, out _);
            if (nextResults == null || nextResults.Count == 0) break;

            foreach (var reference in nextResults[0].References)
            {
                nodes.Add(ToOpcNode(reference));
            }
            cp = nextResults[0].ContinuationPoint;
        }

        return nodes;
    }

    /// <summary>
    /// 将 OPC UA ReferenceDescription 转换为与 Node.js 端 OpcNode 一致的格式
    /// 客户端期望: { id, name, class, type, value, timestamp }
    /// </summary>
    private static object ToOpcNode(ReferenceDescription reference)
    {
        return new
        {
            id = reference.NodeId.ToString(),
            name = reference.DisplayName?.Text ?? reference.BrowseName.ToString(),
            @class = (int)reference.NodeClass,
            type = "",
            value = "",
            timestamp = ""
        };
    }

    public async Task<object?> ReadNodeAttribute(string? nodeId)
    {
        var session = _client.Session;
        if (session == null) throw new InvalidOperationException("OPC UA session not connected");

        var nid = NodeId.Parse(nodeId);

        // 与 Node.js 端保持一致：只读取 DataType, AccessLevel, UserAccessLevel
        // OPC UA AttributeIds: DataType=14, AccessLevel=13, UserAccessLevel=12
        var nodesToRead = new ReadValueIdCollection
        {
            new ReadValueId { NodeId = nid, AttributeId = Attributes.DataType },
            new ReadValueId { NodeId = nid, AttributeId = Attributes.AccessLevel },
            new ReadValueId { NodeId = nid, AttributeId = Attributes.UserAccessLevel },
        };

        session.Read(null, 0, TimestampsToReturn.Both, nodesToRead, out var results, out _);

        if (results == null || results.Count < 3)
            return new Dictionary<string, string>();

        var attribute = new Dictionary<string, string>();

        // DataType (AttributeId = 14): 解析为类型名称字符串
        if (StatusCode.IsGood(results[0].StatusCode) && results[0].Value is NodeId dtNodeId)
        {
            string? dataTypeName = null;
            try
            {
                var dtNodesToRead = new ReadValueIdCollection
                {
                    new ReadValueId { NodeId = dtNodeId, AttributeId = Attributes.DisplayName }
                };
                session.Read(null, 0, TimestampsToReturn.Neither, dtNodesToRead, out var dtResults, out _);
                if (dtResults != null && dtResults.Count > 0 && StatusCode.IsGood(dtResults[0].StatusCode))
                {
                    dataTypeName = (dtResults[0].Value as LocalizedText)?.Text;
                }
            }
            catch { /* ignore */ }
            if (!string.IsNullOrEmpty(dataTypeName))
            {
                attribute["14"] = dataTypeName;
            }
        }

        // AccessLevel (AttributeId = 13)
        if (StatusCode.IsGood(results[1].StatusCode) && results[1].Value != null)
        {
            var accessLevel = Convert.ToByte(results[1].Value);
            attribute["13"] = FormatAccessLevel(accessLevel);
        }

        // UserAccessLevel (AttributeId = 12)
        if (StatusCode.IsGood(results[2].StatusCode) && results[2].Value != null)
        {
            var userAccessLevel = Convert.ToByte(results[2].Value);
            attribute["12"] = FormatAccessLevel(userAccessLevel);
        }

        return attribute;
    }

    /// <summary>
    /// 将 AccessLevel 字节值格式化为 "R", "W", "R/W" 字符串（与 Node.js 端一致）
    /// </summary>
    private static string FormatAccessLevel(byte level)
    {
        bool canRead = (level & 0x01) != 0;
        bool canWrite = (level & 0x02) != 0;
        if (canRead && canWrite) return "R/W";
        if (canRead) return "R";
        if (canWrite) return "W";
        return "";
    }

    /// <summary>
    /// 静态方法：获取 OPC UA 端点安全策略（无需活跃连接）
    /// </summary>
    public static async Task<object?> GetEndpointsStatic(string endpointUrl)
    {
        var config = new ApplicationConfiguration
        {
            ApplicationName = "FUXA",
            ApplicationType = ApplicationType.Client,
            SecurityConfiguration = new SecurityConfiguration
            {
                ApplicationCertificate = new CertificateIdentifier(),
                AutoAcceptUntrustedCertificates = true,
            },
            ClientConfiguration = new ClientConfiguration { DefaultSessionTimeout = 10000 },
            TransportQuotas = new TransportQuotas { OperationTimeout = 10000 }
        };
        await config.Validate(ApplicationType.Client);
        config.CertificateValidator.CertificateValidation += (s, e) => e.Accept = true;

        using var client = DiscoveryClient.Create(new Uri(endpointUrl), EndpointConfiguration.Create(config));
        var endpoints = client.GetEndpoints(null);

        return endpoints.Select(ep => new
        {
            securityMode = ep.SecurityMode.ToString(),
            securityPolicy = ep.SecurityPolicyUri,
            endpointUrl = ep.EndpointUrl,
            securityLevel = ep.SecurityLevel
        }).ToList();
    }

    #endregion
}
