using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Search;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OSIsoftPI2AzureEventHub
{
    class AfContext
    {
        string sq = System.Configuration.ConfigurationManager.AppSettings["sq"];
        /// <summary>
        /// Details : This method is used to get attributes.
        /// </summary>
        public AFAttributeList GetAttributes(string afsrv, string db, string afTemplate, string[] alist)
        {

            PISystem myAF = new PISystems()[afsrv];
            AFDatabase database = myAF.Databases[db];
            AFAttributeList attributesList = new AFAttributeList();

            foreach (string attrib in alist)
            {
                string attribQuery = "Element:{Template:'" + afTemplate + "'} Name:'" + attrib + "'";
                try
                {
                    AFAttributeSearch aFAttributeSearch = new AFAttributeSearch(database, "search", attribQuery);

                    foreach (var attributes in aFAttributeSearch.FindObjects().ToList())
                    {
                        attributesList.Add(attributes);
                    }

                }
                catch (Exception ex)
                {
                    Logs.log.Error(ex.ToString());
                }
            }
            return attributesList;
        }
    }
}
