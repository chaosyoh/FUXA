using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Models
{
    public class AlarmProperty
    {
        public string VariableId { get; set; } = string.Empty;

        public int? Permission {  get; set; }

        public PermissionRoles? PermissionRoles { get; set; }

        public int? Bitmask { get; set; }
    }
}
