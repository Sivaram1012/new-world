using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Topshelf;

namespace OSIsoftPI2AzureEventHub
{
    class Program
    {

        static void Main(string[] args)
        {
            var exitCode = HostFactory.Run(srvc =>
            {
                srvc.UseAssemblyInfoForServiceInfo();

                srvc.Service<InitiateTransfer>(s =>
                {
                    s.ConstructUsing(initProgram => new InitiateTransfer());
                    s.WhenStarted(initProgram => initProgram.start());
                    s.WhenStopped(initProgram => initProgram.stop());
                });

                srvc.RunAsLocalSystem();
            });

            int exitCodeValue = (int)Convert.ChangeType(exitCode, exitCode.GetTypeCode());
            Environment.ExitCode = exitCodeValue;
        }
    }

}
