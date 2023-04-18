using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OSIsoftPI2AzureEventHub
{
    class JsonWrap
    {
        public int pointId { get; set; }
        public string name { get; set; }
        public string value { get; set; }
        public string uom { get; set; }
        public string quality { get; set; }
        public int mode { get; set; }
        public int prevEvtact { get; set; }
        public string timestamp { get; set; }
    }
}
