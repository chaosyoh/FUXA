using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Models
{
    /// <summary>
    /// 
    /// </summary>
    public class TagValue
    {
        /// <summary>
        /// 
        /// </summary>
        public string Id { get; set; } = string.Empty;
        /// <summary>
        /// 
        /// </summary>
        public string? Value { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string DeviceId { get; set; } = string.Empty;
    }
}
