using OSIsoft.AF.Data;
using OSIsoft.AF.Asset;
using System;
using System.Collections.Generic;
using System.IO;
using System.Timers;

namespace OSIsoftPI2AzureEventHub
{
    class InitiateTransfer
    {
        string afsrv = System.Configuration.ConfigurationManager.AppSettings["afserver"];
        string afdb = System.Configuration.ConfigurationManager.AppSettings["afdatabase"];
        string[] afTemplate = System.Configuration.ConfigurationManager.AppSettings["aftemplate"].Split(',');
        static int runInterval = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["sendschedule"]);
        string[] alist = System.Configuration.ConfigurationManager.AppSettings["attriblist"].Split(',');
        string logpath = System.Configuration.ConfigurationManager.AppSettings["afserver"];

        private int DataReadInterval = runInterval * 1000;

        private readonly System.Timers.Timer _timer;
        public static volatile bool _canContinue = true;
        private AFDatapipeListener afListener = new AFDatapipeListener();

        List<AFDataPipe> dplist = new List<AFDataPipe>();


        /// <summary>
        /// Constructor : InitiateTransfer()
        /// Details : This constructor is inilized when the service first runs which call the "Run" Event handler after 5 seconds.
        /// </summary>
        public InitiateTransfer()
        {
            _timer = new System.Timers.Timer(5000);
            _timer.Elapsed += Run;

        }

        /// <summary>
        /// Method : start().
        /// Details : This method is called when service starts and logs an entry to the log file.
        /// </summary>
        public void start()
        {
            _timer.Start();
            Logs.log.Info("INFO | Service Started.");

        }

        /// <summary>
        /// Event: Run()
        /// Details : This Event is triggered the elapsed timer is set to 15 mins.
        /// </summary>
        public void Run(object sender, ElapsedEventArgs e)
        {
            _timer.Stop();
            _timer.Dispose();

            //AFElementTemplate
            List<AFAttributeList> attribList = new List<AFAttributeList>();

            AfContext afContext = new AfContext();
            foreach (string aftemp in afTemplate)
            {
                Console.WriteLine(DateTime.Now+" "+"Reading Attribute for : " + aftemp);
                var attrList = afContext.GetAttributes(afsrv, afdb, aftemp, alist);
                attribList.Add(attrList);                
            }

            foreach(var alist in attribList)
            {
                AFDataPipe afDataPipe = new AFDataPipe();
                afListener.CreateAFDataPipe(afDataPipe, "aftemp", alist, DataReadInterval);
                dplist.Add(afDataPipe);
            }
        }

        /// <summary>
        /// Methode: stop()
        /// Details : This method is called when the service is stopped.
        /// </summary>
        public void stop()
        {
            try
            {
                _canContinue = false;
                afListener.StopListening(dplist);
                Logs.log.Info("INFO | Service Stopped.");
            }
            catch (Exception exp)
            {
                Logs.log.Error("ERROR | " + exp.Message);
            }
        }
    }
}
