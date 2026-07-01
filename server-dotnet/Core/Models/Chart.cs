using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Models
{
    public class Chart
    {
        public string Id { get; set; } = string.Empty;

        public string Name { get; set; }  = string.Empty;

        public List<ChartLine> Lines { get; set; } = new List<ChartLine>();



    }
}
