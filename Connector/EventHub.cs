using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using OSIsoft.AF.Data;
using OSIsoft.AF.PI;
using System.IO;
using Azure.Core;
using Azure.Identity;

namespace OSIsoftPI2AzureEventHub
{
    public class EventHub    //added for unit test case - public
    {
        string logpath = System.Configuration.ConfigurationManager.AppSettings["logpath"];
        string compression = System.Configuration.ConfigurationManager.AppSettings["compression"];
        string tz = System.Configuration.ConfigurationManager.AppSettings["timezone"];
        // Set the Event Hub name, client ID, client secret, and tenant ID
        string ehName = System.Configuration.ConfigurationManager.AppSettings["EventHubName"];
        static string clientId = System.Configuration.ConfigurationManager.AppSettings["ClientId"];
        static string clientSecret = System.Configuration.ConfigurationManager.AppSettings["ClientSecret"];
        static string tenantId = System.Configuration.ConfigurationManager.AppSettings["TenantId"];
        string ehNameSpace = System.Configuration.ConfigurationManager.AppSettings["EhNameSpace"];
        string filePath = @"\\QANOC-FP01.ALS.LOCAL\home-noc-1$\EXSAH101\Desktop\Data\Event.json";
        string folderPath = @"\\QANOC-FP01.ALS.LOCAL\home-noc-1$\EXSAH101\Desktop\Data";

        TokenCredential credential = new ClientSecretCredential(tenantId, clientId, clientSecret);


        public async Task EventHubIngestionAsync( IList<AFDataPipeEvent> pievents)
        {
           // _ = producerClient.CloseAsync();
            var dobj = getobj(pievents);
           var producerClient = new EventHubProducerClient(ehNameSpace, ehName, credential);


            try
            {
                EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
                foreach (var obj in dobj)
                {
                    var strcsv = JsonConvert.SerializeObject(obj);
                    EventData evtData = CreateEvent(JsonConvert.SerializeObject(obj));
                    evtData.Properties.Add("Compression", compression);
                    var flag = eventBatch.TryAdd(evtData);

                    if (flag == false)
                    {
                        var st = DateTime.Now;
                        await producerClient.SendAsync(eventBatch).ConfigureAwait(false);
                        var et = DateTime.Now;
                        var diff = et - st;
                        Console.WriteLine("event entered: " + eventBatch.Count + "Time: " + diff + " Size KB:" + eventBatch.SizeInBytes / 1000 + " " + eventBatch.MaximumSizeInBytes / 1000);
                        Logs.log.Info("event entered: " + eventBatch.Count + "Time: " + diff + " Size KB:" + eventBatch.SizeInBytes / 1000 + " " + eventBatch.MaximumSizeInBytes / 1000);
                        eventBatch.Dispose();

                        eventBatch = default;
                        eventBatch = await producerClient.CreateBatchAsync();
                        flag = eventBatch.TryAdd(evtData);
                        Console.WriteLine("inside flag : " + flag);
                    }
                }

                if (eventBatch != default && eventBatch.Count > 0)
                {

                    var st = DateTime.Now;
                    await producerClient.SendAsync(eventBatch).ConfigureAwait(false);
                    var et = DateTime.Now;
                    var diff = et - st;
                    Console.WriteLine("event entered: " + eventBatch.Count + "Time: " + diff + " Size bytes:" + eventBatch.SizeInBytes / 1000);
                }

                var check4BUF = Task.Run(async () => await CheckForBufferFiles());
            }
            catch (Exception exception)
            {
                //Write to Buffer file
                // check if the file size limit has been reached ---  1,000,000 bytes = 1 megabyte
                if (File.Exists(filePath) && new FileInfo(filePath).Length > 50000000)
                {
                    // create a new file with a timestamp in the file name
                    filePath = $@"\\QANOC-FP01.ALS.LOCAL\home-noc-1$\EXSAH101\Desktop\Data\Event{DateTime.Now:yyyyMMddHHmmss}.json";
                }
                string jsonString = System.Text.Json.JsonSerializer.Serialize(dobj);
                File.AppendAllText(filePath, jsonString);
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("{0} > Exception: {1}", DateTime.Now, exception.Message);
                Console.ResetColor();

                Logs.log.Error("ERROR |  " + exception.Message);
            }

            _ = producerClient.CloseAsync();
        }

        public EventData CreateEvent(string jsonStr)
        {
            DataCompression dc = new DataCompression();
            EventData eventData;
            if (compression.ToLower() == "gzip")
            {
                eventData = new EventData(dc.CompressMessage(jsonStr, true));
            }
            else
            {
                eventData = new EventData(dc.CompressMessage(jsonStr, false));
            }
            return eventData;
        }
        internal List<JsonWrap> getobj(IList<AFDataPipeEvent> pievents)  //added for unit test case - internal was public
        {
            List<JsonWrap> dataobjects = new List<JsonWrap>();

            foreach (var dataPipeEvent in pievents)
            {
                JsonWrap jobj = new JsonWrap();
                var pt = dataPipeEvent.Value.Attribute.PIPoint;
                var pv = dataPipeEvent.PreviousEventAction;
                pt.LoadAttributes();
                jobj.pointId = dataPipeEvent.Value.Attribute.PIPoint.ID;
                jobj.name = dataPipeEvent.Value.Attribute.PIPoint.Name;
                jobj.value = (dataPipeEvent.Value.Attribute.PIPoint.PointType.ToString().ToLower() == "digital") ? dataPipeEvent.Value.Value.ToString() : dataPipeEvent.Value.Value.ToString();
                jobj.uom = pt.GetAttribute(PICommonPointAttributes.EngineeringUnits).ToString();
                jobj.quality = dataPipeEvent.Value.Status.ToString();
                jobj.mode = (int)dataPipeEvent.Action;
                jobj.prevEvtact = ((int)dataPipeEvent.PreviousEventAction);
                jobj.timestamp = (tz.ToLower() == "utc") ? dataPipeEvent.Value.Timestamp.UtcTime.ToString() : dataPipeEvent.Value.Timestamp.LocalTime.ToString();
                dataobjects.Add(jobj);
            }
            return dataobjects;
        }

        public  async Task<Task> CheckForBufferFiles()
        {
            string[] files = Directory.GetFiles(folderPath.ToString());
            if (files.Length != 0)
            {
                //SendData sd = new SendData();
                // Process each file in the folder
                foreach (string file in files)
                {
                    // Read the data from the file
                    string data = File.ReadAllText(file);

                    //sd.SendEHData(data);
                    // Process the data here...

                    Console.WriteLine($"Processing file: {file}");
                   // EventHubIngestionAsync(data);
                   // Console.WriteLine($"Data: {data}");

                    // Delete the file after processing 
                    // File.Delete(file);
                }

            }

            return Task.CompletedTask;
        }
    }
}
