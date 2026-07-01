using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Const
{
    public struct IoEventTypes
    {
        public const string DEVICE_STATUS = "device-status";
        public const string DEVICE_PROPERTY = "device-property";
        public const string DEVICE_VALUES = "device-values";
        public const string DEVICE_BROWSE = "device-browse";
        public const string DEVICE_NODE_ATTRIBUTE = "device-node-attribute";
        public const string DEVICE_WEBAPI_REQUEST = "device-webapi-request";
        public const string DEVICE_TAGS_REQUEST = "device-tags-request";
        public const string DEVICE_TAGS_SUBSCRIBE = "device-tags-subscribe";
        public const string DEVICE_TAGS_UNSUBSCRIBE = "device-tags-unsubscribe";
        public const string DEVICE_ENABLE = "device-enable";
        public const string DEVICE_RESTART = "device-restart";
        public const string DAQ_QUERY = "daq-query";
        public const string DAQ_RESULT = "daq-result";
        public const string DAQ_ERROR = "daq-error";
        public const string ALARMS_STATUS = "alarms-status";
        public const string HOST_INTERFACES = "host-interfaces";
        public const string SCRIPT_CONSOLE = "script-console";
        public const string SCRIPT_COMMAND = "script-command";
        public const string ALIVE = "heartbeat";
        public const string PROJECT_UPDATED = "project-updated";
        public const string PROJECT_NOTIFY_UPDATE = "project-notify-update";

    }
}
