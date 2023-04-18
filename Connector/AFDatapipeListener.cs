using Azure.Core;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Newtonsoft.Json;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.PI;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace OSIsoftPI2AzureEventHub
{
    public class AFDatapipeListener  // added for unit test case - public 
    {
        // variable that tells if the task (thread) to read data pipes can continue
        // we need to set it to volatile to make sure the compiler generate proper code
        // so the _canContinue variable is always updated in all our threads
        private static volatile bool _canContinue = true;
        private readonly List<Task> _tasks = new List<Task>();

        AfContext af = new AfContext();

        string ehName = System.Configuration.ConfigurationManager.AppSettings["EventHubName"];
        static string clientId = System.Configuration.ConfigurationManager.AppSettings["ClientId"];
        static string clientSecret = System.Configuration.ConfigurationManager.AppSettings["ClientSecret"];
        static string tenantId = System.Configuration.ConfigurationManager.AppSettings["TenantId"];
        string ehNameSpace = System.Configuration.ConfigurationManager.AppSettings["EhNameSpace"];
        string afsrv = System.Configuration.ConfigurationManager.AppSettings["afserver"];
        string afdb = System.Configuration.ConfigurationManager.AppSettings["afdatabase"];
        string afTemplate = System.Configuration.ConfigurationManager.AppSettings["aftemplate"];
        string[] alist = System.Configuration.ConfigurationManager.AppSettings["attriblist"].Split(',');
        static int afScan = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["afScantime"]);
        string logpath = System.Configuration.ConfigurationManager.AppSettings["logpath"];
        string tz = System.Configuration.ConfigurationManager.AppSettings["timezone"];
       

        private int afScantime = afScan == 0 ? 60000 : afScan * 1000;

        // instance of the TokenCredential class using client ID, tenant ID, and client secret
        TokenCredential credential = new ClientSecretCredential(tenantId, clientId, clientSecret);

        /// <summary>
        /// Creates an AF data Pipe and starts listening it.
        /// </summary>
        public void CreateAFDataPipe(AFDataPipe afDataPipe, string aftemp, IList<AFAttribute> attributes, int dataUpdateInterval)
        {
            // var producerClient = new EventHubProducerClient(ehconnString);
           // var producerClient = new EventHubProducerClient(ehNameSpace, ehName, credential);
            var afDatapipeTask = Task.Run(async () => await AfDataPipeTask(afDataPipe, aftemp, attributes, dataUpdateInterval));
            _tasks.Add(afDatapipeTask);


            Logs.log.Info("INFO | Created AF DataPipe Task - thread ID " + afDatapipeTask.Id + ".");


            Console.WriteLine("Created AF DataPipe Task - thread ID {0}", afDatapipeTask.Id);

        }

        /// <summary>
        /// Stops all DataPipes currently running.
        /// </summary>
        public void StopListening(List<AFDataPipe> dplist)
        {
            Console.WriteLine("Terminating, waiting for processing threads to terminate");
            Logs.log.Info("INFO | Terminating, waiting for processing threads to terminate.");
            Logs.log.Info("INFO | Task count " + _tasks.Count);

            // we wait until all "listening" tasks are terminated
            Task.WhenAll(_tasks);
            _tasks.Clear();
            Logs.log.Info("INFO | Task clear ");
            foreach (var dpl in dplist)
            {
                dpl.Dispose();
            }


            Console.WriteLine("Monitoring Events Stopped");
            Logs.log.Info("INFO | Monitoring Events Stopped.");
        }


        /// <summary>
        /// Task to monitor AF for new added attributes.
        /// </summary>
        // added for unit test case - public was private
        public void AFMonitor(AFDataPipe afDataPipe, string afTemplate)
        {
            while (_canContinue) // this statement keepes the task (thread) alive
            {
                IList<AFAttribute> attrib = af.GetAttributes(afsrv, afdb, afTemplate, alist);
                var signedupAttributes = afDataPipe.GetSignups();

                var newAttributes = attrib.Except<AFAttribute>(signedupAttributes).ToList();  // need to check

                var deleteAttributes = signedupAttributes.Except<AFAttribute>(attrib).ToList();

                if (newAttributes.Count > 0)
                {
                    afDataPipe.AddSignups(newAttributes); // Subscribe the new attributes.
                    Console.WriteLine(newAttributes.Count + " New Attributes Added. Total attributes " + afDataPipe.GetSignups().Count);
                    Logs.log.Info("INFO | " + newAttributes.Count + " New Attributess Added. Total attributes signedup: " + afDataPipe.GetSignups().Count);
                }
                if (deleteAttributes.Count > 0)
                {
                    afDataPipe.RemoveSignups(deleteAttributes); // deleted attributes Subscribed list.
                    Console.WriteLine(deleteAttributes.Count + " Attributes Deleted. Total attributes " + afDataPipe.GetSignups().Count);
                    Logs.log.Info("INFO | " + deleteAttributes.Count + " Attributess Deleted.Total attributes signedup: " + afDataPipe.GetSignups().Count);
                }
                Thread.Sleep(afScantime);
            }

        }


        /// <summary>
        /// Task (Action) to monitor the AF Data Pipe.
        /// </summary>
        //added for unit test case - public was private
        public  async Task<Task> AfDataPipeTask(AFDataPipe afDataPipe, string aftemp, IList<AFAttribute> attributes, int dataUpdateInterval)
        {
            int maxConcurrency = 5;

            try
            {
                using (SemaphoreSlim concurrencySemaphore = new SemaphoreSlim(maxConcurrency))
                {
                    afDataPipe.AddSignups(attributes);


                    Logs.log.Info("INFO | Total attributes signedup: " + attributes.Count);
                   
                   // var afMonitorTask = Task.Run(() => AFMonitor(afDataPipe, aftemp));
                   // _tasks.Add(afMonitorTask);

                    //Console.WriteLine("Created AF Monitor Task - thread ID {0}", afMonitorTask.Id);

                    List<Task> evt_tasks = new List<Task>();



                    EventHub eh = new EventHub();
                    while (_canContinue) // this statement keepes the task (thread) alive
                    {
                        bool bMoreEvents = true;
                        while (bMoreEvents) // we need this loop to be certain that we empty the data pipe each time we come...
                        {
                            //// Check for buffer iles ////
                            

                            concurrencySemaphore.Wait();

                            List<JsonWrap> dataobjects = new List<JsonWrap>();
                            var results = afDataPipe.GetUpdateEvents(out bMoreEvents);
                            var robj = results.Results;
                            if (robj.Count > 0)
                            {
                                Logs.log.Info("Sending events - " + robj.Count);
                                Console.Write("Sending events - " + robj.Count);
                                var t = Task.Factory.StartNew(() =>
                               {
                                   try
                                   {
                                       //await
                                       eh.EventHubIngestionAsync(robj);
                                   }
                                   catch (Exception ex)
                                   {
                                       Logs.log.Error(ex.Message);
                                   }
                                   finally
                                   {
                                       concurrencySemaphore.Release();
                                   }
                               });
                                Console.WriteLine("New Ingestion: " + DateTime.Now + " Task count: " + evt_tasks.Count() + " Old Task" + _tasks.Count());
                                evt_tasks.Add(t);
                                _tasks.Add(t);
                            }
                        }
                        _tasks.RemoveAll(x => x.IsCompleted);
                        evt_tasks.RemoveAll(x => x.IsCompleted);
                        Thread.Sleep(dataUpdateInterval); // this helps reducing CPU consumption and network calls to PI to check the Pipe.
                    }
                }
            }
            catch (Exception ex)
            {
                Logs.log.Error(ex.ToString());
            }

            return Task.CompletedTask;
        }

    }
}
