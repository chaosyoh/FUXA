using Core.Const;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Settings
{
    public class DaqStore
    {
        public string Type { get; set; } = DaqStoreTypeEnum.SQlite;

        public string Version { get; set; } = string.Empty;
        public string Url { get; set; } = string.Empty;

        public string Organization { get; set; } = string.Empty;

        public string Database {get;set;} = string.Empty;

        public string Retention { get; set; } = DaqStoreRetentionType.Year1;

        public StoreCredentials Credentials { get; set; } = new StoreCredentials();

    }

    /// <summary>
    /// 
    /// </summary>
    public class StoreCredentials
    {
        public string Token { get; set; } = string.Empty;
        public string UserName { get; set; } = string.Empty;

        public string Password { get; set;} = string.Empty;

        public override bool Equals(object? obj)
        {
            return obj is StoreCredentials credentials &&
                   Token == credentials.Token &&
                   UserName == credentials.UserName &&
                   Password == credentials.Password;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Token, UserName, Password);
        }
    }
}
