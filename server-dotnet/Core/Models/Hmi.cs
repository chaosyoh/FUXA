using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Core.Models
{
    public class Hmi
    {
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public LayoutSettings? Layout { get; set; }

        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public LayoutSettings? MobileLayout { get; set; }

        public List<View> Views { get; set; } = new List<View>();

    }
}
