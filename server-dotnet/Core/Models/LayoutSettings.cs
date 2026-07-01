using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Models
{
    public class LayoutSettings
    {
        public bool Autoresize { get; set; } = false;

        public string Start { get; set; } = string.Empty;

        public NavigationSettings Navigation { get; set; } = new NavigationSettings();

        public HeaderSettings Header { get; set; } = new HeaderSettings();

        public bool Showdev { get; set; } = true;

        public string Zoom { get; set; } = string.Empty;

        public string Inputdialog { get; set; } = "false";

        public bool  Hidenavigation { get; set; } = false;

        public string Theme { get; set; } = string.Empty;

        public bool Loginonstart { get; set; } = false;

        public string Loginoverlaycolor { get; set; } = "none";

        public bool Show_connection_error { get; set; } = true;

        public string CustomStyles { get; set; } = string.Empty;

    }
}
