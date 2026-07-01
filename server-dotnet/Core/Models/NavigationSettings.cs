using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Models
{
    public class NavigationSettings
    {
        public string Mode { get; set; } = string.Empty;

        public string Type { get; set; } = string.Empty;

        public string Bkcolor { get; set; } = string.Empty;

        public string Fgcolor { get; set; } = string.Empty;

        public List<NaviItem> Items { get; set; } = new List<NaviItem>();

        public bool Logo { get; set; } = false;
    }
}
