using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;

namespace KMPServer
{
    [DataContract]
    class PublicServerInfo
    {
        [DataMember]
        public string Information;

        [DataMember]
        public string Version;

        [DataMember]
        public int MaxPlayers;
        [DataMember]
        public int PlayerCount;

        [DataMember]
        public List<string> PlayerList = new List<string>();

        [DataMember]
        public bool Whitelisted;
        [DataMember]
        public bool Piracy;

        [DataMember]
        public string GameMode;

        [DataMember]
        public double BubbleRadius;

        [DataMember]
        public int GamePort;

        public PublicServerInfo(ServerSettings.ConfigStore settings)
        {
            Information = settings.serverInfo;
            Version = KMPCommon.PROGRAM_VERSION;
            MaxPlayers = settings.maxClients;
            PlayerCount = ServerMain.server.activeClientCount();

            foreach (var c in ServerMain.server.clients.ToList().Where(c => c.isReady))
            {
                PlayerList.Add(c.username);
            }

            switch (settings.gameMode)
            {
                case 0:
                    GameMode = "Sandbox";
                    break;
                case 1:
                    GameMode = "Career";
                    break;
            }

            BubbleRadius = settings.safetyBubbleRadius;

            GamePort = settings.port;

            Piracy = settings.allowPiracy;
            Whitelisted = settings.whitelisted;
        }

        public string GetJSON()
        {
            var serializer = new DataContractJsonSerializer(typeof(PublicServerInfo));

            var outStream = new MemoryStream();
            serializer.WriteObject(outStream,this);

            outStream.Position = 0;

            var sr = new StreamReader(outStream);

            return sr.ReadToEnd();
        }
    }
}
