using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OSIsoftPI2AzureEventHub
{
    static class Logs
    {
        public static log4net.ILog log = log4net.LogManager.GetLogger(typeof(Program));
    }
}
