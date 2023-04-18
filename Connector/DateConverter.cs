using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OSIsoftPI2AzureEventHub
{
    class DateConverter
    {
        public static string time = DateTime.Now.ToLongTimeString().Replace(":", "-");
        public static string date = DateTime.Now.ToShortDateString().Replace(@"/", "-");
    }
}
