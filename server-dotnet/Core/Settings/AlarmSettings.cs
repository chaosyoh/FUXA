using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Settings
{
    public class AlarmSettings
    {
        public string Retention { get; set; } = "year1";
        public string RetentionType { get; set; } = "days";
        public int RetentionDays { get; set; } = 365;
    }
}
