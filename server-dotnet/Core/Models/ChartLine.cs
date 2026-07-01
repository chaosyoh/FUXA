using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Models
{
    public class ChartLine
    {
        public string Id { get; set; } = string.Empty;
        public string Device { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;

        public string Label { get; set; } = string.Empty;

        public string Color { get; set; } = string.Empty;

        public string Fill { get; set; } = string.Empty;

        public int Yaxis { get; set; } = 1;

        public int? LineInterpolation { get; set; }

        public int? LineWidth { get; set; }

        public bool SpanGaps { get; set; } = false;

        public List<ChartLineZone>? Zones { get; set; }

    }
}
