using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using Connector;
using System.Threading.Tasks;
using OSIsoft.AF.Data;
using System.Collections.Generic;
using OSIsoft.AF.Asset;
using Azure.Messaging.EventHubs.Producer;

namespace ConnectorTest
{
    [TestClass]
    public class AFDatapipeListener_Test
    {


        [TestMethod]
        public async Task AfDataPipeTask_Test()
        {
            // Arrange
            AFDatapipeListener afD= new AFDatapipeListener();
            AFDataPipe afDataPipe = new AFDataPipe();
           // EventHub eh = new EventHub();
            string aftemp = "temp";
            List<AFAttribute> attributes = new List<AFAttribute>();
            int dataUpdateInterval = 1000;
            EventHubProducerClient epc = new EventHubProducerClient("Endpoint=sb://pidataeh.servicebus.windows.net/;SharedAccessKeyName=pidataowner;SharedAccessKey=t8tuAIA0eR2lKYk/uC1Ya3cE1nCkqiDbV+AEhFahJ0s=;EntityPath=pieh", "pieventhub");

            // Act
            var result = await afD.AfDataPipeTask(afDataPipe, aftemp, attributes, dataUpdateInterval, epc);
            //var DataSent= eh.EventHubIngestionAsync(epc, robj).ConfigureAwait(false);

            // Assert
            Assert.AreEqual(Task.CompletedTask, result);

        }
    }
}
