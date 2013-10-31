using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;

namespace KMPServerList
{
    // NOTE: You can use the "Rename" command on the "Refactor" menu to change the interface name "IReport" in both code and config file together.
    [ServiceContract]
    public interface IReport
    {
        [OperationContract]
        void Ping(string PublicIP, string Name, string Description, int Port);
    }
}
