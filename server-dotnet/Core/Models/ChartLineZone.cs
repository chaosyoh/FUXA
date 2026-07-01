using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Models
{
    public class ChartLineZone
    {
        public double Min { get; set; }

        public double Max { get; set; }

        public string Stroke { get; set; } = string.Empty;

        public string Fill { get; set; } = string.Empty;
    }
}
