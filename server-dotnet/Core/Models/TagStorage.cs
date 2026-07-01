using SqlSugar;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Models
{
    [SugarTable("currentValues")]
    public class TagStorage
    {
        [SugarColumn(IsPrimaryKey = true)]
        public string TagId { get; set; } = string.Empty;

        public string DeviceId { get; set; } = string.Empty;

        public string? Value { get; set; }
    }
}
