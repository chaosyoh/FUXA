using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Models
{
    public class SqlSection
    {
        public SqlSection()
        {
        }

        public string Table { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;

        public object? Value { get; set; }

    }
}
