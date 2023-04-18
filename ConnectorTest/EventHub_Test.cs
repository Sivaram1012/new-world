using Azure.Core;
using Azure.Identity;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace ConnectorTest
{
    [TestClass]
    public class EventHub_Test
    {
        string ehName = "";
        static string clientId ="";
        static string clientSecret = "";
        static string tenantId ="";
        string ehNameSpace ="" ;
        [TestMethod]
        public void TestMethod1()
        {
            TokenCredential credential = new ClientSecretCredential(tenantId, clientId, clientSecret);
          
        }
    }
}
