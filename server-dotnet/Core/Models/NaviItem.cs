using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Models
{
    public class NaviItem
    {
        public string Text { get; set; } = string.Empty;

        public string Link { get; set; } = string.Empty;

        public string Icon { get; set; } = string.Empty;

        public string Image { get; set; } = string.Empty;

        public int? Permission { get; set; } 

        public PermissionRoles? PermissionRoles { get; set; }


    }
}
