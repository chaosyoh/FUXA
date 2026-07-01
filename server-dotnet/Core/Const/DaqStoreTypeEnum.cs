using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Const
{
    public struct DaqStoreTypeEnum
    {
        public const string SQlite = "SQlite";
        public const string InfluxDB = "influxDB";
        public const string InfluxDB18 = "influxDB18";
        public const string TDengine = "TDengine";
        public const string QuestDB = "QuestDB";
    }
}
