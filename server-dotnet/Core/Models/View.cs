using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Core.Models
{
    public class View
    {
        public string Id { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;

        public DocProfile? Profile { get; set; }

        public ExpandoObject? Items { get; set; }

        public ExpandoObject? Variables { get; set; }

        public string Svgcontent { get; set; } = string.Empty;

        public string Type { get; set; } = string.Empty;

        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public ExpandoObject? ViewProperty { get; set; }
    }
}
