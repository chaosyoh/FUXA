using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Models
{
    public class AlarmSubProperty
    {
        public bool? Enabled { get; set; }
        public string? Text {  get; set; }
        public string? Group { get; set; }
        public string? Ackmode { get; set; }
        public string? bkcolor { get; set; }
        public string? color { get; set; }
        public double? Min { get; set; }
        public double? Max { get; set; }
        public int? Checkdelay { get; set; }
        public int? Timedelay { get; set; }

    }
}
