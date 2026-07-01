using Core.Const;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Models
{
    public class DocProfile
    {
        public decimal Width { get; set; } = 1024;

        public decimal Height { get; set; } = 768;

        public string Bkcolor { get; set; } = "#ffffffff";

        public decimal Margin { get; set; } = 10;

        public string Align { get; set; } = DocAlignType.TopCenter;

        public string GridType { get; set; } = Const.GridType.Fixed;

        public int ViewRenderDelay { get; set; } = 0;
    }
}
