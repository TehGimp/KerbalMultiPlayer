using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;

namespace KMPServerList
{
    // NOTE: You can use the "Rename" command on the "Refactor" menu to change the class name "Report" in code, svc and config file together.
    // NOTE: In order to launch WCF Test Client for testing this service, please select Report.svc or Report.svc.cs at the Solution Explorer and start debugging.
    public class Report : IReport
    {
        public void Ping(string PublicIP, string Name, string Description, int Port)
        {
            //Find server with the same IP + Port, if it exists - update its timestamp + name + desc. Otherwise, add a new record.
        }
    }
}
