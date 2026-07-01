using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Settings
{
    public class StmpSettings
    {
        public string Host { get; set; } = string.Empty;

        public int Port { get; set; } = 587;

        public string Mailsender { get; set; } = string.Empty;

        public string Username { get; set;} = string.Empty;

        public string Password { get; set; } = string.Empty;

    }
}
