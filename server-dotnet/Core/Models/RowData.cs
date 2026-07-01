using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqlSugar;

namespace Core.Models
{
    public class RowData
    {
        [SugarColumn(IsPrimaryKey = true)]
        public string Name { get; set; } = string.Empty;

        public string Value { get; set; } = string.Empty;


    }
}
