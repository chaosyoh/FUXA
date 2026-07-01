using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Core.Models
{
    public class ProjectData
    {
        public Device Server { get; set; } = new Device();
        public string Version { get; set; } = "1.0.1";

        public ConcurrentDictionary<string,Device> Devices { get; set; } = new ConcurrentDictionary<string, Device>();

        public ConcurrentDictionary<string, DeviceFolder> DeviceFolders { get; set; } = new ConcurrentDictionary<string, DeviceFolder>();

        public ConcurrentDictionary<string, Tag> Tags { get; set; } = new ConcurrentDictionary<string, Tag>();

        public Hmi Hmi { get; set; } = new Hmi();

        public List<Chart> Charts { get; set; } = [];

        public List<Alarm> Alarms { get; set; } = [];


        public List<Notification> Notifications { get; set; } = [];

        public List<JsonNode?> Scripts { get; set; } = [];

        public List<JsonNode?> Texts { get; set; } = [];

        public List<JsonNode?> Reports { get; set; } = [];

        public List<JsonNode?> MapsLocations { get; set; } = [];

        public Languages? Languages { get; set; }

        public List<Graph> Graphs { get; set; } = [];

        public ClientAccess? ClientAccess { get; set; }

        public DateTime? Timestamp { get; set; }
    }
}
