using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

using System.Security.Cryptography;
using System.Runtime.Serialization.Formatters.Binary;

using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Diagnostics;

using System.Collections;

using KSP.IO;
using UnityEngine;
using System.Xml;

namespace KMP
{
    class KMPClientMain
    {

        public struct InTextMessage
        {
            public bool fromServer;
            public bool isMOTD;
            public String message;
        }

        public struct ServerMessage
        {
            public KMPCommon.ServerMessageID id;
            public byte[] data;
        }

        //Constants

        public const String USERNAME_LABEL = "username";
        public const String IP_LABEL = "hostname";
		public const String PORT_LABEL = "port";
        public const String AUTO_RECONNECT_LABEL = "reconnect";
        public const String FAVORITE_LABEL = "pos";
		public const String NAME_LABEL = "name";

        //public const String INTEROP_CLIENT_FILENAME = "interopclient.txt";
        //public const String INTEROP_PLUGIN_FILENAME = "interopplugin.txt";
        public const string PLUGIN_DATA_DIRECTORY = "KMP/Plugins/PluginData/KerbalMultiPlayer/";
        public const string CLIENT_CONFIG_FILENAME = "KMPClientConfig.xml";
        public const string CLIENT_TOKEN_FILENAME = "KMPPlayerToken.txt";
        public const string MOD_CONTROL_FILENAME = "KMPModControl.txt";
        public const string CRAFT_FILE_EXTENSION = ".craft";

        public const int MAX_USERNAME_LENGTH = 16;
        public const int MAX_TEXT_MESSAGE_QUEUE = 128;
        public const long KEEPALIVE_DELAY = 2000;
        public const long UDP_PROBE_DELAY = 1000;
        public const long UDP_TIMEOUT_DELAY = 8000;
        public const int SLEEP_TIME = 5;
        public const int CLIENT_DATA_FORCE_WRITE_INTERVAL = 10000;
        public const int RECONNECT_DELAY = 1000;
        public const int MAX_RECONNECT_ATTEMPTS = 0;
        public const long PING_TIMEOUT_DELAY = 10000;

        public const int MAX_QUEUED_CHAT_LINES = 8;
        public const int DEFAULT_PORT = 2076;

        public static UnicodeEncoding encoder = new UnicodeEncoding();

        //Settings

        private static String mUsername = "";
        private static Guid playerGuid;
        public static String username
        {
            set
            {
                if (value != null && value.Length > MAX_USERNAME_LENGTH)
                    mUsername = value.Substring(0, MAX_USERNAME_LENGTH);
                else
                    mUsername = value;
            }

            get
            {
                return mUsername;
            }
        }
        public static String hostname = "localhost:2076";
        public static int updateInterval = 100;
        public static int screenshotInterval = 1000;
        public static bool autoReconnect = true;
        public static byte inactiveShipsPerUpdate = 0;
        public static ScreenshotSettings screenshotSettings = new ScreenshotSettings();
		public static Dictionary<String, String[]> favorites = new Dictionary<String, String[]>();
		
		//ModChecking
		public static bool modFileChecked = false;
		public static List<string> partList = new List<string>();
        public static Dictionary<string, SHAMod> modFileList = new Dictionary<string, SHAMod>();
        public static List<string> resourceList = new List<string>();
        public static List<string> requiredModList = new List<string>();
		public static string resourceControlMode = "blacklist";
		public static string modMismatchError = "Mod Verification Failed - Reason Unknown";
        public static string GAMEDATAPATH = new System.IO.DirectoryInfo(getKMPDirectory()).Parent.Parent.FullName;
        public static byte[] kmpModControl_bytes;
		
        //Connection
        public static int clientID;
        public static bool endSession;
        public static bool intentionalConnectionEnd;
        public static bool handshakeCompleted;
        public static TcpClient tcpClient;
        public static long lastTCPMessageSendTime;
        public static bool quitHelperMessageShow;
        public static int reconnectAttempts;
        public static UdpClient udpClient;
        public static bool udpConnected;
        public static long lastUDPMessageSendTime;
        public static long lastUDPAckReceiveTime;
        public static long lastUDPProbeTime;

        public static bool receivedSettings;

        //Plugin Interop
        public static Queue<byte[]> interopInQueue;

        public static Queue<byte[]> pluginUpdateInQueue;
        public static Queue<InTextMessage> textMessageQueue;
        public static long lastScreenshotShareTime;

        public static byte[] queuedOutScreenshot;
        public static byte[] lastSharedScreenshot;
        public static List<String> screenshotsWaiting = new List<String>();

        public static String currentGameTitle;
        public static String watchPlayerName;

        public static long lastClientDataWriteTime;
        public static long lastClientDataChangeTime;

        public static String message = "Not connected";

        //Messages

        public static Queue<ServerMessage> receivedMessageQueue;

        public static byte[] currentMessageHeader = new byte[KMPCommon.MSG_HEADER_LENGTH];
        public static int currentMessageHeaderIndex;
        public static byte[] currentMessageData;
        public static int currentMessageDataIndex;
        public static KMPCommon.ServerMessageID currentMessageID;

        private static byte[] receiveBuffer = new byte[8192];
        private static int receiveIndex = 0;
        private static int receiveHandleIndex = 0;

		private static Queue<byte[]> queuedOutMessagesHighPriority;
		private static Queue<byte[]> queuedOutMessagesSplit;
		private static Queue<byte[]> queuedOutMessages;
		private static bool isClientSendingData;

		private static Queue<byte[]> queuedOutUDPMessages;
		private static bool isClientSendingUDPData;

        //Split message
        private static int splitMessageReceiveIndex = 0;
        private static byte[] splitMessageData;

        //Threading

		public static object sendOutgoingMessagesLock = new object();
		public static object sendOutgoingUDPMessagesLock = new object();
        public static object serverSettingsLock = new object();
        public static object screenshotOutLock = new object();
        public static object threadExceptionLock = new object();
        public static object clientDataLock = new object();
        public static object udpTimestampLock = new object();
        public static object receiveBufferLock = new object();
        public static object interopOutQueueLock = new object();

        public static String threadExceptionStackTrace;
        public static Exception threadException;

        public static Thread serverThread;
        public static Thread interopThread;
        public static Thread chatThread;
        public static Thread connectionThread;
        public static bool connectionThreadRunning = false;

        public static Stopwatch stopwatch;
        public static Stopwatch pingStopwatch = new Stopwatch();

        public static KMPManager gameManager;
        public static long lastPing;
        public static bool debugging = false;

        public static void InitMPClient(KMPManager manager)
        {
            if (Environment.GetCommandLineArgs().Contains("-kmpdebug"))
            {
                Log.MinLogLevel = Log.LogLevels.Debug;
            }
            else if (Environment.GetCommandLineArgs().Count(s => s.Contains("-kmpLogLevel:")) == 1)//if a -kmpLogLevel:[loglevel] is in the arguments
            {
                string logLevel = Environment.GetCommandLineArgs().First(s => s.Contains("-kmpLogLevel:"));
                Log.MinLogLevel = (Log.LogLevels)Enum.Parse(typeof(Log.LogLevels), logLevel.Split(':')[1],true);
            }
            else
            {
                Log.MinLogLevel = Log.LogLevels.Info;
            }


            gameManager = manager;



            Log.Debug("KMP Client version " + KMPCommon.PROGRAM_VERSION);
            Log.Debug("    Created by Shaun Esau and developed by the KMP team http://sesau.ca/ksp/KMP_contribs.html");
            Log.Debug("    Based on Kerbal LiveFeed created by Alfred Lam");


            Log.Info("KMP started in LogLevel {0}",Log.MinLogLevel);


            stopwatch = new Stopwatch();
            stopwatch.Start();

            favorites.Clear();
        }


        public static String GetUsername()
        {
            return username;
        }

        public static void SetUsername(String newUsername)
        {
            if (username != newUsername)
            {
                username = newUsername;
                if (username.Length > MAX_USERNAME_LENGTH)
                    username = username.Substring(0, MAX_USERNAME_LENGTH); //Trim username
                writeConfigFile();
            }
        }

        public static void SetServer(String newHostname)
        {
            if (hostname != newHostname)
            {
                hostname = newHostname;
                writeConfigFile();
            }
        }

        public static void SetAutoReconnect(bool newAutoReconnect)
        {
            if (autoReconnect != newAutoReconnect)
            {
                autoReconnect = newAutoReconnect;
                writeConfigFile();
            }
        }

        public static Dictionary<String, String[]> GetFavorites()
        {
            return favorites;
        }

        public static void SetFavorites(Dictionary<String, String[]> newFavorites)
        {
            // Verification of change is handled in KMPManager
            favorites = newFavorites;
            writeConfigFile();
        }
        
        private static void parseModFile(string ModFileContent)
        {
            System.IO.StringReader reader = new System.IO.StringReader(ModFileContent);
        	
	        string resourcemode = "whitelist";
	        List<string> allowedParts = new List<string>();
            Dictionary<string, SHAMod> hashes = new Dictionary<string, SHAMod>();
	        List<string> resources = new List<string>();
	        List<string> modList = new List<string>();
	        string line;
	        string[] splitline;
	        string readmode = "";
	        while (true)
	        {
	        	line = reader.ReadLine();
                if (line == null)
                {
                    break;
                }
	        	try
	        	{
				    if(!String.IsNullOrEmpty(line))
				    {
					    if(line[0] != '#')//allows commented lines
					    {
						    if(line[0] == '!')//changing readmode
						    {
							    if(line.Contains("partslist")){
								    readmode = "parts";
							    }
                                else if (line.Contains("required-files"))
                                {
                                    readmode = "required-files";
							    }
                                else if (line.Contains("optional-files"))
                                {
                                    readmode = "optional-files";
                                }
							    else if(line.Contains("resource-blacklist")){ //allow all resources EXCEPT these in file
								    readmode = "resource";
								    resourcemode = "blacklist";
							    }
							    else if(line.Contains("resource-whitelist")){ //allow NO resources EXCEPT these in file
								    readmode = "resource";
								    resourcemode = "whitelist";
							    }
							    else if(line.Contains("required")){
								    readmode = "required";
							    }
						    }
						    else if(readmode == "parts")
						    {
							    allowedParts.Add(line);
						    }
                            else if (readmode == "required-files")
						    {
                                splitline = line.Split('=');
                                string hash = "";
                                if (splitline.Length > 1)
                                {
                                    hash = splitline[1];
                                }
                                hashes.Add(splitline[0], new SHAMod { sha = hash, required = true });
						    }
                            else if (readmode == "optional-files")
                            {
                                splitline = line.Split('=');
                                string hash = "";
                                if (splitline.Length > 1)
                                {
                                    hash = splitline[1];
                                }
                                hashes.Add(splitline[0], new SHAMod { sha = hash, required = false });
                            }
						    else if(readmode == "resource"){
							    resources.Add(line);
						    }
						    else if(readmode == "required"){
							    modList.Add(line);
						    }
					    }
				    }
			    }
		        catch (Exception e)
		        {
		            Log.Info(e.ToString());
		        }
	        }
	        
	        reader.Close();
	        partList = allowedParts; //make all the vars global once we're done parsing
            modFileList = hashes;
	        resourceControlMode = resourcemode;
	        resourceList = resources;
	        requiredModList = modList;
        }
        
        private static bool FileCheck()
        {
            char[] replaceChars = {'\\', '/'};
            try
            {
                foreach (KeyValuePair<string, SHAMod> entry in modFileList)
                {
                    // if server settings don't say to check all mod files, and it's not a DLL or CFG file, then skip it
                    if (!gameManager.checkAllModFiles && !entry.Key.EndsWith(".cfg", System.StringComparison.InvariantCultureIgnoreCase) && !entry.Key.EndsWith(".dll", System.StringComparison.InvariantCultureIgnoreCase))
                    {
                        continue;
                    }
                    LoadedFileInfo FileInfo = null;
                    if (resourceControlMode == "whitelist")
                    {
                        if (resourceList.Contains(entry.Key)) // don't check anything that has been specifically whitelisted
                        {
                            Log.Debug("SHA hash checking skipped due to whitelisting: " + entry.Key);
                            continue;
                        }
                    }
                    else if (entry.Key.StartsWith(PLUGIN_DATA_DIRECTORY) && (entry.Key.EndsWith(".txt") || entry.Key.EndsWith(".xml"))) // if using a blacklist, ignore any files in the KMP PluginData folder that are supposed to be there
                    {
                        Log.Debug("SHA hash checking skipped due to being a KMP file that changes: " + entry.Key);
                        continue;
                    }
                    try
                    {
                        FileInfo = KMPManager.LoadedModfiles.SingleOrDefault(x => x.ModPath == entry.Key);
                    }
                    catch (InvalidOperationException)
                    {
                        modMismatchError = "Multiple of same file defined in different directories: " + entry.Key;
                        return false;
                    }
                    if (FileInfo == null)
                    {
                        if (entry.Value.required) // the file doesn't exist, so refuse connection if it's a required mod
                        {
                            modMismatchError = "Required File Missing: " + entry.Key;
                            return false;
                        }
                        else // if it's just an optional mod, continue because it's ok if it's missing
                        {
                            continue;
                        }
                    }
                    else if (String.Compare(FileInfo.SHA256, entry.Value.sha, true) != 0 && entry.Value.sha != "") // if the mod file exists, then it MUST match the SHA from the server (unless SHA section was blank, which means it is a required file but doesn't need to match)
                    {
                        modMismatchError = "SHA Checksum Mismatch: " + FileInfo.LoadedPath;
                        return false;
                    }
                }
            }
            catch (Exception e)
            {
                modMismatchError = e.Message;
                return false;
            }
	        return true;
	    }
        
        
        private static bool resourceCheck()
        {
        	try
        	{
	        	if(resourceControlMode == "blacklist")
	        	{
	        		foreach(string checkedResource in resourceList)
	        		{
                        foreach (LoadedFileInfo file in KMPManager.LoadedModfiles)
                        {
	        				if(file.ModPath.Contains(checkedResource))
	        				{
                                modMismatchError = "File blacklisted: " + file.LoadedPath;
	        					return false;
	        				}
	        			}
	        		}
	        	}
	        	else if(resourceControlMode == "whitelist")
	        	{
                    foreach (LoadedFileInfo file in KMPManager.LoadedModfiles)
                    {
                        if (file.LoadedPath.StartsWith("Plugins") || file.LoadedPath.StartsWith("Parts")) // do not allow mod files that are in the Plugins or Parts directories (they load differently, and often cause errors). All mods should be in the GameData directory.
                        {
                            modMismatchError = "You may not join a server if you have mods installed in the deprecated mod directories ('Plugins' or 'Parts' directories in the KSP root directory)";
                            return false;
                        }
                        // if server settings don't say to check all mod files, and it's not a DLL or CFG file, then skip it
                        if (gameManager.checkAllModFiles || file.ModPath.EndsWith(".dll", System.StringComparison.InvariantCultureIgnoreCase) || file.ModPath.EndsWith(".cfg", System.StringComparison.InvariantCultureIgnoreCase))
                        {
                            if (!resourceList.Contains(file.ModPath) && !modFileList.ContainsKey(file.ModPath)) // check if the resource is a) whitelisted, or b) listed in the optional or required SHA sections. If not, the file is not allowed to be loaded.
                            {
                                modMismatchError = "File not allowed on this server: " + file.LoadedPath;
                                return false;
                            }
                        }
	        		}
	        	}
	        	return true;
	        }
	        catch (Exception e)
	        {
                modMismatchError = e.Message;
	        	Log.Debug(e.ToString());
	        	return false;
	        }
        }
        
        private static bool modCheck(byte[] kmpModControl_bytes)
        {	
        	if (!modFileChecked)
        	{
        		modFileChecked = true;
				string modFilePath = System.IO.Path.Combine(GAMEDATAPATH, "KMP/Plugins/PluginData/KerbalMultiPlayer/" + MOD_CONTROL_FILENAME);
	        	System.IO.File.WriteAllBytes(modFilePath, kmpModControl_bytes);
	        	
				parseModFile(System.Text.Encoding.UTF8.GetString(kmpModControl_bytes));
	        	
	        	if (!resourceCheck() || !FileCheck())
	        	{
	        		return false;
	        	}
	        	else
	        	{
	        		return true;
	        	}

        	}
        	else
        	{
        		return true;
        	}
        }

        public static void Connect()
        {
            clearConnectionState();
            File.Delete<KMPClientMain>("debug");
            serverThread = new Thread(beginConnect);
            serverThread.Start();
        }

        private static void beginConnect()
        {
            SetMessage("Attempting to connect...");
            bool allow_reconnect = false;
            reconnectAttempts = MAX_RECONNECT_ATTEMPTS;

            do
            {

                allow_reconnect = false;

                try
                {
                    //Run the connection loop then determine if a reconnect attempt should be made
                    if (connectionLoop())
                        reconnectAttempts = 0;

                    allow_reconnect = autoReconnect && !intentionalConnectionEnd && reconnectAttempts < MAX_RECONNECT_ATTEMPTS;
                }
                catch (Exception e)
                {

                    //Write an error log
                    Log.Debug("Exception thrown in beginConnect(), catch 1, Exception: {0}", e.ToString());
                    KSP.IO.TextWriter writer = KSP.IO.File.AppendText<KMPClientMain>("KMPClientlog.txt");
                    writer.WriteLine(e.ToString());
                    if (threadExceptionStackTrace != null && threadExceptionStackTrace.Length > 0)
                    {
                        writer.WriteLine("KMP Stacktrace: ");
                        writer.WriteLine(threadExceptionStackTrace);
                    }
                    writer.Close();

                    Log.Error(e.ToString());
                    if (threadExceptionStackTrace != null && threadExceptionStackTrace.Length > 0)
                    {
                        Log.Debug(threadExceptionStackTrace);
                    }

                    Log.Error("Unexpected exception encountered! Crash report written to KMPClientlog.txt");
                }

                if (allow_reconnect)
                {
                    //Attempt a reconnect after a delay
                    SetMessage("Attempting to reconnect...");
                    Thread.Sleep(RECONNECT_DELAY);
                    reconnectAttempts++;
                }

            } while (allow_reconnect);
        }

        /// <summary>
        /// Connect to the server and run a session until the connection ends
        /// </summary>
        /// <returns>True iff a connection was successfully established with the server</returns>
        static bool connectionLoop()
        {
            //Look for a port-number in the hostname
            int port = DEFAULT_PORT;
            String trimmed_hostname = hostname;

            int port_start_index = hostname.LastIndexOf(':');
            if (port_start_index >= 0 && port_start_index < (hostname.Length - 1))
            {
                String port_substring = hostname.Substring(port_start_index + 1);
                if (!int.TryParse(port_substring, out port) || port < IPEndPoint.MinPort || port > IPEndPoint.MaxPort)
                    port = DEFAULT_PORT;

                trimmed_hostname = hostname.Substring(0, port_start_index);
            }

            //Look up the actual IP address
            bool ipv6_connected = false;
            IPAddress address = null;
            IPAddress.TryParse(trimmed_hostname, out address);
            if (address == null) {
                IPHostEntry host_entry = new IPHostEntry();
                try
                {
                    host_entry = Dns.GetHostEntry(trimmed_hostname);
                }
                catch (Exception e)
                {
                    Log.Debug("Exception thrown in connectionLoop(), catch 1, Exception: {0}", e.ToString());
                    host_entry = null;
                }
                if (host_entry != null)
                {
                    IPAddress ipv4_address = Array.Find(host_entry.AddressList, a => a.AddressFamily == AddressFamily.InterNetwork);
                    IPAddress ipv6_address = Array.Find(host_entry.AddressList, a => a.AddressFamily == AddressFamily.InterNetworkV6);
                    address = ipv4_address;
                    if ( ipv6_address != null ) {
                        try {
                            //Connects IPv6 Hostnames
                            TcpClient ipv6_tcpClient = new TcpClient(ipv6_address.AddressFamily);
                            ipv6_tcpClient.NoDelay = true;
                            IPEndPoint ipv6_endpoint = new IPEndPoint(ipv6_address, port);
                            SetMessage("Connecting to IPv6: [" + ipv6_address + "]:" + port);
                            ipv6_tcpClient.Connect(ipv6_endpoint);
                            if (ipv6_tcpClient.Client.Connected) {
                                ipv6_connected = true;
                                address = ipv6_address;
                                tcpClient = ipv6_tcpClient;
                            } else {
                                ipv6_tcpClient = null;
                                ipv6_endpoint = null;
                            }
                        }
                        catch (Exception e) {
                            Log.Debug("Exception thrown in connectionLoop(), catch 2, Exception: {0}", e.ToString());
                        }
                    }
                }
            }

            if (address == null)
            {
                SetMessage("Invalid server address.");
                return false;
            }


            

            try
            {
                //Connects IPv4 Hostnames, And IPv6/IPv4 IP's.
                if (ipv6_connected == false) {
                    tcpClient = new TcpClient(address.AddressFamily);
                    tcpClient.NoDelay = true;
                    IPEndPoint endpoint = new IPEndPoint(address, port);
                    if (address.AddressFamily == AddressFamily.InterNetworkV6) {
                        SetMessage("Connecting to IPv6: [" + address + "]:" + port);
                    } else {
                        SetMessage("Connecting to IPv4: " + address + ":" + port);
                    }
                    tcpClient.Connect(endpoint);
                }
                //tcpSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.KeepAlive, true);
                if (tcpClient.Connected)
                {
                    SetMessage("TCP connection established");
                    clientID = -1;
                    endSession = false;
                    intentionalConnectionEnd = false;
                    handshakeCompleted = false;
                    receivedSettings = false;

					splitMessageData = null;
					splitMessageReceiveIndex = 0;
					screenshotsWaiting.Clear();
					modFileChecked = false;

					isClientSendingData = false;
					isClientSendingUDPData = false;

                    pluginUpdateInQueue = new Queue<byte[]>();
                    textMessageQueue = new Queue<InTextMessage>();
                    interopInQueue = new Queue<byte[]>();

                    receivedMessageQueue = new Queue<ServerMessage>();

					queuedOutMessages = new Queue<byte[]>();
					queuedOutMessagesSplit = new Queue<byte[]>();
					queuedOutMessagesHighPriority = new Queue<byte[]>();
					queuedOutUDPMessages = new Queue<byte[]>();

                    threadException = null;

                    currentGameTitle = String.Empty;
                    watchPlayerName = String.Empty;
                    lastSharedScreenshot = null;
                    lastScreenshotShareTime = 0;
                    lastTCPMessageSendTime = 0;
                    lastClientDataWriteTime = 0;
                    lastClientDataChangeTime = stopwatch.ElapsedMilliseconds;

                    quitHelperMessageShow = true;

                    //Init udp socket
                    try
                    {
                        IPEndPoint endpoint = new IPEndPoint(address, port);
                        udpClient = new UdpClient(endpoint.AddressFamily);
                        udpClient.Connect(endpoint);
                    }
                    catch (Exception e)
                    {
                        Log.Debug("Exception thrown in connectionLoop(), catch 3, Exception: {0}", e.ToString());
                        if (udpClient != null)
                            udpClient.Close();

                        udpClient = null;
                    }

                    udpConnected = false;
                    lastUDPAckReceiveTime = 0;
                    lastUDPMessageSendTime = stopwatch.ElapsedMilliseconds;

                    //Create a thread to handle chat
                    chatThread = new Thread(new ThreadStart(handleChat));
                    chatThread.Start();

                    //Create a thread to handle client interop
                    interopThread = new Thread(new ThreadStart(handlePluginInterop));
                    interopThread.Start();

                    //Create a thread to handle disconnection
                    connectionThread = new Thread(new ThreadStart(handleConnection));
                    connectionThreadRunning = true;
                    connectionThread.Start();

                    beginAsyncRead();

                    SetMessage("Connected to server! Handshaking...");

                    while (!endSession && !intentionalConnectionEnd && tcpClient.Connected)
                    {
                        //Check for exceptions thrown by threads
                        lock (threadExceptionLock)
                        {
                            if (threadException != null)
                            {
                                Exception e = threadException;
                                threadExceptionStackTrace = e.StackTrace;
                                throw e;
                            }
                        }

                        Thread.Sleep(SLEEP_TIME);
                    }

                    //clearConnectionState();

                    if (intentionalConnectionEnd)
                        enqueuePluginChatMessage("Closed connection with server", true);
                    else
                        enqueuePluginChatMessage("Lost connection with server", true);
                        ScreenMessages.PostScreenMessage("Lost connection with server. Please return to the Main Menu to reconnect.",300f,ScreenMessageStyle.UPPER_CENTER);

                    return true;
                }

            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in connectionLoop(), catch 4, Exception: {0}", e.ToString());
                SetMessage("Disconnected");
                if (tcpClient != null)
                   tcpClient.Close();

                tcpClient = null;
            }

            return false;
        }

        static void handleMessage(KMPCommon.ServerMessageID id, byte[] data)
        {
            //LogAndShare("Message ID: " + id.ToString() + " data: " + (data == null ? "0" : System.Text.Encoding.ASCII.GetString(data)));
            switch (id)
            {
                case KMPCommon.ServerMessageID.HANDSHAKE:
                    Int32 protocol_version = KMPCommon.intFromBytes(data);

                    if (data.Length >= 8)
                    {
                        Int32 server_version_length = KMPCommon.intFromBytes(data, 4);

                        if (data.Length >= 12 + server_version_length)
                        {
                            String server_version = encoder.GetString(data, 8, server_version_length);
                            clientID = KMPCommon.intFromBytes(data, 8 + server_version_length);
							gameManager.gameMode = KMPCommon.intFromBytes(data, 12 + server_version_length);
                            int kmpModControl_length = KMPCommon.intFromBytes(data, 16 + server_version_length);
                            kmpModControl_bytes = new byte[kmpModControl_length];
                            Array.Copy(data, 20 + server_version_length, kmpModControl_bytes, 0, kmpModControl_length);
                            gameManager.checkAllModFiles = Convert.ToBoolean(data[20+server_version_length+kmpModControl_length]);
                            SetMessage("Handshake received. Server version: " + server_version);
                        }
                    }

                    //End the session if the protocol versions don't match
                    if (protocol_version != KMPCommon.NET_PROTOCOL_VERSION)
                    {
                        endSession = true;
                        intentionalConnectionEnd = true;
                        gameManager.disconnect("Your client is incompatible with this server");
                    }
                    else
                    {
                        if (!modCheck(kmpModControl_bytes))
                        {
                            endSession = true;
                            intentionalConnectionEnd = true;
                            gameManager.disconnect(modMismatchError);
                        }
                        else
                        {
                            sendHandshakeMessage(); //Reply to the handshake
                            lock (udpTimestampLock)
                            {
                                lastUDPMessageSendTime = stopwatch.ElapsedMilliseconds;
                            }
                            handshakeCompleted = true;
                        }
                    }

                    break;

                case KMPCommon.ServerMessageID.HANDSHAKE_REFUSAL:

                    String refusal_message = encoder.GetString(data, 0, data.Length);

                    endSession = true;
                    intentionalConnectionEnd = true;

                    enqueuePluginChatMessage("Server refused connection. Reason: " + refusal_message, true);

                    break;

                case KMPCommon.ServerMessageID.SERVER_MESSAGE:
                case KMPCommon.ServerMessageID.TEXT_MESSAGE:

                    if (data != null)
                    {

                        InTextMessage in_message = new InTextMessage();

                        in_message.fromServer = (id == KMPCommon.ServerMessageID.SERVER_MESSAGE);
                        in_message.isMOTD = (id == KMPCommon.ServerMessageID.MOTD_MESSAGE);
                        in_message.message = encoder.GetString(data, 0, data.Length);
                        if (in_message.message.Contains(" has shared a screenshot.")) {
                            int screenshotSharePlayerNameIndex = in_message.message.IndexOf(" has shared a screenshot.");
                            string screenshotSharePlayerName = in_message.message.Substring(0, screenshotSharePlayerNameIndex);
                            if (screenshotSharePlayerName != username) {
                                bool listPlayerNameInScreenshotsWaiting = false;
                                foreach (string listPlayer in screenshotsWaiting)
                                {
                                    if (listPlayer == screenshotSharePlayerName) {
                                        listPlayerNameInScreenshotsWaiting = true;
                                    }
                                }
                                if (listPlayerNameInScreenshotsWaiting == false)
                                {
                                    screenshotsWaiting.Add(screenshotSharePlayerName);
                                }
                            }
                        }

                        if (in_message.message.Contains(" has disconnected : ")) {
                            int quitPlayerNameIndex = in_message.message.IndexOf(" has disconnected : ");
                            string quitPlayerName = in_message.message.Substring(0, quitPlayerNameIndex);
                            if (quitPlayerName != username) {
                                bool listPlayerNameInScreenshotsWaiting = false;
                                foreach (string listPlayer in screenshotsWaiting)
                                {
                                    if (listPlayer == quitPlayerName) {
                                        listPlayerNameInScreenshotsWaiting = true;
                                    }
                                }
                                if (listPlayerNameInScreenshotsWaiting)
                                {
                                    screenshotsWaiting.Remove(quitPlayerName);
                                }
                            }
                        }

                        //Queue the message
                        enqueueTextMessage(in_message);
                    }

                    break;

                case KMPCommon.ServerMessageID.MOTD_MESSAGE:

                    if (data != null)
                    {
                        InTextMessage in_message = new InTextMessage();
                        in_message.fromServer = (id == KMPCommon.ServerMessageID.SERVER_MESSAGE);
                        in_message.isMOTD = (id == KMPCommon.ServerMessageID.MOTD_MESSAGE);
                        in_message.message = encoder.GetString(data, 0, data.Length);

                        enqueueTextMessage(in_message);
                    }

                    break;

                case KMPCommon.ServerMessageID.PLUGIN_UPDATE:

                    if (data != null)
                        enqueueClientInteropMessage(KMPCommon.ClientInteropMessageID.PLUGIN_UPDATE, data);

                    break;
				
				case KMPCommon.ServerMessageID.SCENARIO_UPDATE:

                    if (data != null)
                        enqueueClientInteropMessage(KMPCommon.ClientInteropMessageID.SCENARIO_UPDATE, data);

                    break;

                case KMPCommon.ServerMessageID.SERVER_SETTINGS:

                    lock (serverSettingsLock)
                    {
                        if (data != null && data.Length >= KMPCommon.SERVER_SETTINGS_LENGTH && handshakeCompleted)
                        {

                            updateInterval = KMPCommon.intFromBytes(data, 0);
                            screenshotInterval = KMPCommon.intFromBytes(data, 4);

                            lock (clientDataLock)
                            {
                                int new_screenshot_height = KMPCommon.intFromBytes(data, 8);
                                if (screenshotSettings.maxHeight != new_screenshot_height)
                                {
                                    screenshotSettings.maxHeight = new_screenshot_height;
                                    lastClientDataChangeTime = stopwatch.ElapsedMilliseconds;
                                    enqueueTextMessage("Screenshot Height has been set to " + screenshotSettings.maxHeight);
                                }

                                gameManager.safetyBubbleRadius = BitConverter.ToDouble(data, 12);

                                if (inactiveShipsPerUpdate != data[20])
                                {
                                    inactiveShipsPerUpdate = data[20];
                                    lastClientDataChangeTime = stopwatch.ElapsedMilliseconds;
                                }
                                gameManager.gameCheatsEnabled = Convert.ToBoolean(data[21]);
								gameManager.gameArrr = Convert.ToBoolean(data[22]);
                                //partList, requiredModList, shaList, resourceList and resourceControlMode 
                            }

                            receivedSettings = true;
                            /*
                            Log.Debug("Update interval: " + updateInterval);
                            Log.Debug("Screenshot interval: " + screenshotInterval);
                            Log.Debug("Inactive ships per update: " + inactiveShipsPerUpdate);
                             */
                        }
                    }

                    break;

                case KMPCommon.ServerMessageID.SCREENSHOT_SHARE:

                    if (data != null && data.Length > 0 && data.Length < screenshotSettings.maxNumBytes
                        && watchPlayerName.Length > 0 && watchPlayerName != username)
                    {
                        enqueueClientInteropMessage(KMPCommon.ClientInteropMessageID.SCREENSHOT_RECEIVE, data);
                    }
                    break;

                case KMPCommon.ServerMessageID.CONNECTION_END:
                    if (data != null)
                    {
                        String message = encoder.GetString(data, 0, data.Length);

                        gameManager.disconnect(message);
                        clearConnectionState();

                        //If the reason is not a timeout, connection end is intentional
                        intentionalConnectionEnd = message.ToLower() != "timeout";
                        enqueuePluginChatMessage("Server closed the connection: " + message, true);

                        SetMessage("Disconnected from server: " + message);
                    }
                    else
                    {
                        gameManager.disconnect();
                        clearConnectionState();
                        SetMessage("Disconnected from server");
                    }

                    break;

                case KMPCommon.ServerMessageID.UDP_ACKNOWLEDGE:
                    lock (udpTimestampLock)
                    {
                        lastUDPAckReceiveTime = stopwatch.ElapsedMilliseconds;
                    }
                    break;

                case KMPCommon.ServerMessageID.CRAFT_FILE:

                    if (data != null && data.Length > 8)
                    {
                        //Read craft name length
                        KMPCommon.CraftType craft_type = (KMPCommon.CraftType)KMPCommon.intFromBytes(data, 0);
                        int craft_name_length = KMPCommon.intFromBytes(data, 4);
                        if (craft_name_length < data.Length - 8)
                        {
                            //Read craft name
                            String craft_name = encoder.GetString(data, 8, craft_name_length);

                            //Read craft bytes
                            byte[] craft_bytes = new byte[data.Length - craft_name_length - 8];
                            Array.Copy(data, 8 + craft_name_length, craft_bytes, 0, craft_bytes.Length);

                            //Write the craft to a file
                            String filename = getCraftFilename(craft_name, craft_type);
                            if (filename != null)
                            {
                                try
                                {
                                    //KSP.IO.File.WriteAllBytes<KMPClientMain>(craft_bytes, filename);
                                    System.IO.File.WriteAllBytes(filename, craft_bytes);
                                    enqueueTextMessage("Received craft file: " + craft_name);
                                }
                                catch (Exception e)
                                {
                                    Log.Debug("Exception thrown in handleMessage(), catch 1, Exception: {0}", e.ToString());
                                    enqueueTextMessage("Error saving received craft file: " + craft_name);
                                }
                            }
                            else
                                enqueueTextMessage("Unable to save received craft file.");
                        }
                    }

                    break;

                case KMPCommon.ServerMessageID.PING_REPLY:
                    if (pingStopwatch.IsRunning)
                    {
                        enqueueTextMessage("Ping Reply: " + pingStopwatch.ElapsedMilliseconds + "ms");
                        lastPing = pingStopwatch.ElapsedMilliseconds;
                        pingStopwatch.Stop();
                        pingStopwatch.Reset();
                    }
                    break;

				case KMPCommon.ServerMessageID.SYNC:
					if (data != null) {
						gameManager.targetTick = BitConverter.ToDouble (data, 0) + Convert.ToDouble (lastPing);
						gameManager.skewTargetTick = BitConverter.ToDouble (data, 0);
						gameManager.skewServerTime = BitConverter.ToInt64 (data, 8);
						gameManager.skewSubspaceSpeed = BitConverter.ToSingle (data, 16);
						gameManager.lastSubspaceLockChange = UnityEngine.Time.realtimeSinceStartup;
						Log.Debug ("Client time locked to server:" + gameManager.skewTargetTick + " server time: " + gameManager.skewServerTime + " frequency " + gameManager.skewSubspaceSpeed + "x.");
					}
                    break;
                case KMPCommon.ServerMessageID.SYNC_COMPLETE:
                    gameManager.HandleSyncCompleted();
                    break;
                case KMPCommon.ServerMessageID.SPLIT_MESSAGE:
		    handleSplitMessage(data);
                    break;
                case KMPCommon.ServerMessageID.SYNC_TIME:
                    gameManager.HandleSyncTimeCompleted(data);
                    break;
            }
        }

		public static void handleSplitMessage(byte[] data) {
			if (splitMessageReceiveIndex == 0) {
				//New split message
				int split_message_length = KMPCommon.intFromBytes (data, 4);
				splitMessageData = new byte[8 + split_message_length];
				data.CopyTo (splitMessageData, 0);
				splitMessageReceiveIndex = data.Length;
			} else {
				//Continued split message
				data.CopyTo (splitMessageData, splitMessageReceiveIndex);
				splitMessageReceiveIndex = splitMessageReceiveIndex + data.Length;
			}
				//Check if we have filled the byte array, if so, handle the message.
			if (splitMessageReceiveIndex == splitMessageData.Length) {
				//Parse the message and feed it into handleMessage
				int joined_message_id = KMPCommon.intFromBytes (splitMessageData, 0);
				int joined_message_length = KMPCommon.intFromBytes (splitMessageData, 4);
				byte[] joined_message_data = new byte[joined_message_length];
				Array.Copy (splitMessageData, 8, joined_message_data, 0, joined_message_length);
				byte[] joined_message_data_decompressed = KMPCommon.Decompress(joined_message_data);
				handleMessage ((KMPCommon.ServerMessageID)joined_message_id, joined_message_data_decompressed);
				splitMessageReceiveIndex = 0;
			}
		}

        public static void clearConnectionState()
        {
            try
            {
                //Abort all threads
                Log.Debug("Aborting chat thread...");
                safeAbort(chatThread, true);
                Log.Debug("Aborting interop thread...");
                safeAbort(interopThread, true);
                Log.Debug("Aborting client thread...");
                safeAbort(serverThread, true);
                Log.Debug("Aborting connection thread...");
                connectionThreadRunning = false;

                Log.Debug("Closing connections...");
                //Close the socket if it's still open
                if (tcpClient != null)
                    tcpClient.Close();
                tcpClient = null;

                if (udpClient != null)
                    udpClient.Close();
                udpClient = null;
            }
            catch (ThreadAbortException e) {
                Log.Debug("Exception thrown in clearConnectionState(), catch 1, Exception: {0}", e.ToString());
            }
            Log.Debug("Disconnected");
        }

        static void handleChatInput(String line)
        {
            StringBuilder sb = new StringBuilder();
            if (line.Length > 0)
            {
                if (quitHelperMessageShow && (line == "q" || line == "Q"))
                {
                    enqueuePluginChatMessage("If you are trying to quit, use the !quit command.", true);
                    quitHelperMessageShow = false;
                }
                bool handled = false;
                if (line.ElementAt(0) == '!')
                {
                    String line_lower = line.ToLower();

                    // There's atleast one character (!), so we can be sure that line_part will have length 1 at minimum.
                    String[] line_part = line_lower.Split(' ');

                    if (line_lower == "!quit")
                    {
                        handled = true;
                        intentionalConnectionEnd = true;
                        endSession = true;
                        sendConnectionEndMessage("Quit");
                    }
                    else if (line_lower == "!ping")
                    {
                        handled = true;
                        if (!pingStopwatch.IsRunning)
                        {
                            queueOutgoingMessage(KMPCommon.ClientMessageID.PING, null);
                            pingStopwatch.Start();
                        }
                    }
                    else if (line_lower == "!debug")
                    {
                        handled = true;
                        debugging = !debugging;
						if (debugging) Log.MinLogLevel = Log.LogLevels.Debug;
						else Log.MinLogLevel = Log.LogLevels.Info;
                        enqueuePluginChatMessage("debug " + debugging);
                    }
					else if(line_lower == "!clear")
					{
						KMPChatDX.chatLineQueue.Clear();
						handled = true;
					}
                    else if (line_lower == "!whereami")
                    {
                        handled = true;

                        sb.Append("You are connected to: ");
                        sb.Append(hostname);

                        enqueuePluginChatMessage(sb.ToString());
                    }
                    else if (line_lower == "!bubble")
                    {
                        if (gameManager.horizontalDistanceToSafetyBubbleEdge() < 1 || gameManager.verticalDistanceToSafetyBubbleEdge() < 1)
                        {
                            sb.Append("The bubble radius is: ");
                            sb.Append(gameManager.safetyBubbleRadius.ToString("N1", CultureInfo.CreateSpecificCulture("en-US")));
                            sb.Append("m\n");
                            sb.Append("You are outside of the bubble!");
                        }
                        else
                        {
                            sb.Append("The bubble radius is: ");
                            sb.Append(gameManager.safetyBubbleRadius.ToString("N1", CultureInfo.CreateSpecificCulture("en-US")));
                            sb.Append("m\n");
                            sb.Append("You are ");
                            sb.Append(gameManager.verticalDistanceToSafetyBubbleEdge().ToString("N1", CultureInfo.CreateSpecificCulture("en-US")));
                            sb.Append("m away from the bubble top.\n");
                            sb.Append("You are ");
                            sb.Append(gameManager.horizontalDistanceToSafetyBubbleEdge().ToString("N1", CultureInfo.CreateSpecificCulture("en-US")));
                            sb.Append("m away from the nearest bubble side.");
                        }
                        enqueuePluginChatMessage(sb.ToString());
                        handled = true;
                    }
                    else if (line_part[0] == "!chat")
                    {
                        handled = true;
                        int length = line_part.Length;
                        if (length > 1)
                        {
                            string command = line_part[1];
                            if (command == "dragwindow")
                            {
                                bool state = false;
                                if (length >= 3)
                                {
                                    // Set they requested value
                                    state = line_part[2] == "true";
                                }
                                else
                                {
                                    // Or toggle.
                                    state = !KMPChatDX.draggable;
                                }


                                if (!state)
                                {
                                    KMPChatDX.chatboxX = KMPChatDX.windowPos.x;
                                    KMPChatDX.chatboxY = KMPChatDX.windowPos.y;
                                }

                                KMPChatDX.draggable = state;
                                enqueueTextMessage(String.Format("The chat window is now {0}", (KMPChatDX.draggable) ? "draggable" : "not draggable"));
                            }
                            else if (command == "offsetting")
                            {
                                bool state = true;

                                if (length >= 3)
                                {
                                    state = line_part[2] == "true";
                                }
                                else
                                {
                                    state = !KMPChatDX.offsettingEnabled;
                                }

                                KMPChatDX.offsettingEnabled = state;
                                enqueueTextMessage(String.Format("Chat window offsetting has been {0}", (KMPChatDX.offsettingEnabled) ? "enabled" : "disabled"));
                            }
                            else if (command == "offset")
                            {
                                if (length >= 5)
                                {
                                    try
                                    {
                                        // 0 = tracking station, 1 = editor/sph
                                        int target = (line_part[2] == "tracking") ? 0 : 1;
                                        float offsetX = Convert.ToSingle(line_part[3]);
                                        float offsetY = Convert.ToSingle(line_part[4]);

                                        if (target == 0)
                                        {
                                            KMPChatDX.trackerOffsetX = offsetX;
                                            KMPChatDX.trackerOffsetY = offsetY;
                                        }
                                        else if (target == 1)
                                        {
                                            KMPChatDX.editorOffsetX = offsetX;
                                            KMPChatDX.editorOffsetY = offsetY;
                                        }

                                        enqueueTextMessage(String.Format("The {0} offsets has been set to X: {1} Y: {2}", (target == 0) ? "tracking station" : "rocket/spaceplane editor", offsetX, offsetY));
                                    }
                                    catch (Exception e)
                                    {
                                        Log.Debug("Exception thrown in handleChatInput(), catch 1, Exception: {0}", e.ToString());
                                        enqueueTextMessage("Syntax error. Usage: !chat offset [tracking|editor] [offsetX] [offsetY]");
                                    }
                                }

                            }
                            else if (command == "width" || command == "height" || command == "top" || command == "left")
                            {
                                if (length >= 3)
                                {
                                    try
                                    {
                                        float size = Convert.ToSingle(line_part[2]);
                                        bool percent = true;

                                        if (length >= 4)
                                        {
                                            percent = line_part[3] == "percent";
                                        }

                                        switch (command)
                                        {
                                            case "width":
                                                KMPChatDX.chatboxWidth = (percent) ? Screen.width * (size / 100) : size;
                                                sb.Append(String.Format("Chatbox width has been set to {0} {1}", size, (percent) ? "percent" : "pixels"));
                                                break;
                                            case "height":
                                                KMPChatDX.chatboxHeight = (percent) ? Screen.height * (size / 100) : size;
                                                sb.Append(String.Format("Chatbox height has been set to {0} {1}", size, (percent) ? "percent" : "pixels"));
                                                break;
                                            case "top":
                                                KMPChatDX.chatboxY = (percent) ? Screen.height * (size / 100) : size;
                                                sb.Append(String.Format("Chatbox top offset has been set to {0} {1}", size, (percent) ? "percent" : "pixels"));
                                                break;
                                            case "left":
                                                KMPChatDX.chatboxX = (percent) ? Screen.width * (size / 100) : size;
                                                sb.Append(String.Format("Chatbox left offset has been set to {0} {1}", size, (percent) ? "percent" : "pixels"));
                                                break;
                                        }

                                        KMPChatDX.windowPos.x = KMPChatDX.chatboxX;
                                        KMPChatDX.windowPos.y = KMPChatDX.chatboxY;

                                        KMPChatDX.windowPos.height = KMPChatDX.chatboxHeight;
                                        KMPChatDX.windowPos.width = KMPChatDX.chatboxWidth;

                                        enqueueTextMessage(sb.ToString());
                                    }
                                    catch (Exception e)
                                    {
                                        Log.Debug("Exception thrown in handleChatInput(), catch 2, Exception: {0}", e.ToString());
                                        enqueueTextMessage("Syntax error. Usage: !chat [width|height|top|left] [value] <percent|pixels>\nWhere value is a number.");
                                    }
                                }
                                else
                                {
                                    enqueueTextMessage("Syntax error. Usage: !chat [width|height|top|left] [value] <percent|pixels>");
                                }
                            }
                        }
                    }
                    else if (line_lower.Length > (KMPCommon.SHARE_CRAFT_COMMAND.Length + 1)
                        && line_lower.Substring(0, KMPCommon.SHARE_CRAFT_COMMAND.Length) == KMPCommon.SHARE_CRAFT_COMMAND)
                    {
                        handled = true;
                        //Share a craft file
                        String craft_name = line.Substring(KMPCommon.SHARE_CRAFT_COMMAND.Length + 1);
                        KMPCommon.CraftType craft_type = KMPCommon.CraftType.VAB;
                        String filename = findCraftFilename(craft_name, ref craft_type);

                        if (filename != null && filename.Length > 0)
                        {
                            try
                            {
                                //byte[] craft_bytes = KSP.IO.File.ReadAllBytes<KMPClientMain>(filename);
                                byte[] craft_bytes = System.IO.File.ReadAllBytes(filename);
                                sendShareCraftMessage(craft_name, craft_bytes, craft_type);
                            }
                            catch (Exception e)
                            {
                                Log.Debug("Exception thrown in handleChatInput(), catch 3, Exception: {0}", e.ToString());
                                enqueueTextMessage("Error reading craft file: " + filename);
                            }
                        }
                        else
                            enqueueTextMessage("Craft file not found: " + craft_name);
                    }

                }
                if (!handled)
                {
                    sendTextMessage(line);
                }
            }
        }

        static void passExceptionToMain(Exception e)
        {
            lock (threadExceptionLock)
            {
                if (threadException == null)
                    threadException = e;
            }
        }

        //Threads

        static void handlePluginInterop()
        {
            try
            {
                while (true)
                {
                    writeClientData();

                    if (handshakeCompleted)
                        processPluginInterop();


                    //Throttle the rate at which you can share screenshots
                    if (stopwatch.ElapsedMilliseconds - lastScreenshotShareTime > screenshotInterval)
                    {
                        lock (screenshotOutLock)
                        {
                            if (queuedOutScreenshot != null)
                            {
                                Log.Debug("screenshot");
                                //Share the screenshot
                                sendShareScreenshotMessage(queuedOutScreenshot);
                                lastSharedScreenshot = queuedOutScreenshot;
                                queuedOutScreenshot = null;
                                lastScreenshotShareTime = stopwatch.ElapsedMilliseconds;

                                //Send the screenshot back to the plugin if the player is watching themselves
                                if (watchPlayerName == username)
                                    enqueueClientInteropMessage(KMPCommon.ClientInteropMessageID.SCREENSHOT_RECEIVE, lastSharedScreenshot);
                                Log.Debug("done screenshot");
                            }
                        }
                    }

                    Thread.Sleep(SLEEP_TIME);
                }

            }
            catch (ThreadAbortException e)
            {
                Log.Debug("Exception thrown in handlePluginInterop(), catch 1, Exception: {0}", e.ToString());
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in handlePluginInterop(), catch 2, Exception: {0}", e.ToString());
                Log.Debug("Error in handlePluginInterop: " + e.Message);
                passExceptionToMain(e);
            }
        }

        static void handlePluginUpdates()
        {
            try
            {

                while (true)
                {
                    writeClientData();

                    //readPluginUpdates();

                    //writeQueuedUpdates();

                    int sleep_time = 0;
                    lock (serverSettingsLock)
                    {
                        sleep_time = updateInterval;
                    }

                    Thread.Sleep(sleep_time);
                }

            }
            catch (ThreadAbortException e)
            {
                Log.Debug("Exception thrown in handlePluginUpdates(), catch 1, Exception: {0}", e.ToString());
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in handlePluginUpdates(), catch 2, Exception: {0}", e.ToString());
                passExceptionToMain(e);
            }
        }

        static void handleConnection()
        {
            try
            {
                while (connectionThreadRunning)
                {
                    if (pingStopwatch.IsRunning && pingStopwatch.ElapsedMilliseconds > PING_TIMEOUT_DELAY)
                    {
                        enqueueTextMessage("Ping timed out.", true);
                        pingStopwatch.Stop();
                        pingStopwatch.Reset();
                    }

                    //Send a keep-alive message to prevent timeout
                    if (stopwatch.ElapsedMilliseconds - lastTCPMessageSendTime >= KEEPALIVE_DELAY)
                        queueOutgoingMessage(KMPCommon.ClientMessageID.KEEPALIVE, null);

                    //Handle received messages
                    while (receivedMessageQueue.Count > 0)
                    {
                        ServerMessage message;
                        message = receivedMessageQueue.Dequeue();
                        handleMessage(message.id, message.data);
                    }

                    if (udpClient != null && handshakeCompleted)
                    {

                        //Update the status of the udp connection
                        long last_udp_ack = 0;
                        long last_udp_send = 0;
                        lock (udpTimestampLock)
                        {
                            last_udp_ack = lastUDPAckReceiveTime;
                            last_udp_send = lastUDPMessageSendTime;
                        }

                        bool udp_should_be_connected =
                            last_udp_ack > 0 && (stopwatch.ElapsedMilliseconds - last_udp_ack) < UDP_TIMEOUT_DELAY;

                        if (udpConnected != udp_should_be_connected)
                        {
                            if (udp_should_be_connected)
                                enqueueTextMessage("UDP connection established.", false, true);
                            else
                                enqueueTextMessage("UDP connection lost.", false, true);

                            udpConnected = udp_should_be_connected;
                            if ((stopwatch.ElapsedMilliseconds - last_udp_ack) > UDP_TIMEOUT_DELAY * 10)
                                throw new Exception("UDP connection lost and could not be reconnected.");
                        }

                        //Send a probe message to try to establish a udp connection
                        if ((stopwatch.ElapsedMilliseconds - lastUDPProbeTime) > UDP_PROBE_DELAY) {
                            if ((stopwatch.ElapsedMilliseconds - last_udp_send) > UDP_TIMEOUT_DELAY)
                            {
                                sendUDPProbeMessage(true);
                            }
                            else
                            {
                                sendUDPProbeMessage(false);
                            }
                        }
                    }
					sendOutgoingMessages();
					sendOutgoingUDPMessages();
                    Thread.Sleep(SLEEP_TIME);
                }

                Log.Debug("Aborted connection thread...");
            }
            catch (ThreadAbortException e)
            {
                Log.Debug("Exception thrown in handleConnection(), catch 1, Exception: {0}", e.ToString());
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in handleConnection(), catch 2, Exception: {0}", e.ToString());
                passExceptionToMain(e);
            }
        }

        static void handleChat()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                while (true)
                {
                    if (sb.Length == 0)
                    {
                        //Handle incoming messages
                        try
                        {
                            while (textMessageQueue.Count > 0)
                            {
                                InTextMessage message;
                                message = textMessageQueue.Dequeue();

                                Log.Debug(message.message);
                            }
                        }
                        catch (KSP.IO.IOException e)
                        {
                            Log.Debug("Exception thrown in handleChat(), catch 1, Exception: {0}", e.ToString());
                        }
                    }

                    Thread.Sleep(SLEEP_TIME);
                }

            }
            catch (ThreadAbortException e)
            {
                Log.Debug("Exception thrown in handleChat(), catch 2, Exception: {0}", e.ToString());
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in handleChat(), catch 3, Exception: {0}", e.ToString());
                passExceptionToMain(e);
            }
        }

        static void safeAbort(Thread thread, bool join = false)
        {
            try
            {
                if (thread != null)
                {

                    thread.Abort();
                    if (join)
                        thread.Join();
                }
            }
            catch (ThreadAbortException e) {
                Log.Debug("Exception thrown in safeAbort(), catch 1, Exception: {0}", e.ToString());
            }
            catch (ThreadStateException e) {
                Log.Debug("Exception thrown in safeAbort(), catch 2, Exception: {0}", e.ToString());
            }
            catch (ThreadInterruptedException e) {
                Log.Debug("Exception thrown in safeAbort(), catch 3, Exception: {0}", e.ToString());
            }
        }

        //Plugin Interop

        static void processPluginInterop()
        {
			if (interopInQueue.Count > 0 )
			{
				try
				{
				while (interopInQueue.Count > 0 && tcpClient.Connected)
					{
						byte[] bytes;
						bytes = interopInQueue.Dequeue();

						//Read the message id
						int id_int = KMPCommon.intFromBytes(bytes, 0);

						KMPCommon.PluginInteropMessageID id = KMPCommon.PluginInteropMessageID.NULL;
						if (id_int >= 0 && id_int < Enum.GetValues(typeof(KMPCommon.PluginInteropMessageID)).Length)
							id = (KMPCommon.PluginInteropMessageID)id_int;

						//Read the length of the message data
						int data_length = KMPCommon.intFromBytes(bytes, 4);

						if (data_length <= 0)
							handleInteropMessage(id, null);
						else
						{
							//Copy the message data
							byte[] data = new byte[data_length];
							Array.Copy(bytes, 8, data, 0, data.Length);
							handleInteropMessage(id, data);
						}
					}
				}
				catch (Exception e) { Log.Debug("Exception thrown in processPluginInterop(), catch 1, Exception: {0}", e.ToString()); }
			}
        }
        public static void acceptPluginInterop(byte[] bytes)
        {

            try
            {
                interopInQueue.Enqueue(bytes);
            }
            catch (Exception e) {
                Log.Debug("Exception thrown in acceptPluginInterop(), catch 1, Exception: {0}", e.ToString());
            }
        }

        static void handleInteropMessage(KMPCommon.PluginInteropMessageID id, byte[] data)
        {
            switch (id)
            {

                case KMPCommon.PluginInteropMessageID.CHAT_SEND:
                    if (data != null)
                    {
                        String line = encoder.GetString(data);

                        InTextMessage message = new InTextMessage();
                        message.fromServer = false;
                        message.isMOTD = false;
                        message.message = "[" + username + "] " + line;
                        enqueueTextMessage(message, false);

                        handleChatInput(line);
                    }

                    break;

                case KMPCommon.PluginInteropMessageID.PLUGIN_DATA:
                    String new_watch_player_name = String.Empty;

                    if (data != null && data.Length >= 9)
                    {
                        UnicodeEncoding encoder = new UnicodeEncoding();
                        int index = 0;

                        //Read current activity status
                        bool in_flight = data[index] != 0;
                        index++;

                        //Read current game title
                        int current_game_title_length = KMPCommon.intFromBytes(data, index);
                        index += 4;

                        currentGameTitle = encoder.GetString(data, index, current_game_title_length);
                        index += current_game_title_length;

                        //Read the watch player name
                        int watch_player_name_length = KMPCommon.intFromBytes(data, index);
                        index += 4;

                        new_watch_player_name = encoder.GetString(data, index, watch_player_name_length);
                        index += watch_player_name_length;

                        //Send the activity status to the server
                        if (in_flight)
                            queueOutgoingMessage(KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_FLIGHT, null);
                        else
                            queueOutgoingMessage(KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_GAME, null);
                    }

                    if (watchPlayerName != new_watch_player_name)
                    {
                        watchPlayerName = new_watch_player_name;

                        if (watchPlayerName == username && lastSharedScreenshot != null)
                            enqueueClientInteropMessage(KMPCommon.ClientInteropMessageID.SCREENSHOT_RECEIVE, lastSharedScreenshot);

                        sendScreenshotWatchPlayerMessage(watchPlayerName);
                    }
                    break;

                case KMPCommon.PluginInteropMessageID.PRIMARY_PLUGIN_UPDATE:
                    sendPluginUpdate(data, true);
                    break;

                case KMPCommon.PluginInteropMessageID.SECONDARY_PLUGIN_UPDATE:
                    sendPluginUpdate(data, false);
                    break;
				
				case KMPCommon.PluginInteropMessageID.SCENARIO_UPDATE:
                    sendScenarioUpdate(data);
                    break;

                case KMPCommon.PluginInteropMessageID.SCREENSHOT_SHARE:
                    if (data != null)
                    {
                        lock (screenshotOutLock)
                        {
                            queuedOutScreenshot = data;
                        }
                    }

                    break;
                case KMPCommon.PluginInteropMessageID.WARPING:
                    queueOutgoingMessage(KMPCommon.ClientMessageID.WARPING, data);
                    break;
                case KMPCommon.PluginInteropMessageID.SSYNC:
                    queueOutgoingMessage(KMPCommon.ClientMessageID.SSYNC, data);
                    break;
				case KMPCommon.PluginInteropMessageID.SYNC_TIME:
					queueOutgoingMessage(KMPCommon.ClientMessageID.SYNC_TIME, null);
					break;
            }
        }

        static void enqueueClientInteropMessage(KMPCommon.ClientInteropMessageID id, byte[] data)
        {
            int msg_data_length = 0;
            if (data != null)
                msg_data_length = data.Length;

            byte[] message_bytes = new byte[KMPCommon.INTEROP_MSG_HEADER_LENGTH + msg_data_length];

            KMPCommon.intToBytes((int)id).CopyTo(message_bytes, 0);
            KMPCommon.intToBytes(msg_data_length).CopyTo(message_bytes, 4);
            if (data != null)
                data.CopyTo(message_bytes, KMPCommon.INTEROP_MSG_HEADER_LENGTH);

            lock (interopOutQueueLock)
            {
				gameManager.acceptClientInterop (message_bytes);
            }
        }

        static void writeClientData()
        {

            lock (clientDataLock)
            {

                if (lastClientDataChangeTime > lastClientDataWriteTime
                    || (stopwatch.ElapsedMilliseconds - lastClientDataWriteTime) > CLIENT_DATA_FORCE_WRITE_INTERVAL)
                {
                    byte[] username_bytes = encoder.GetBytes(username);

                    //Build client data array
                    byte[] bytes = new byte[9 + username_bytes.Length];

                    bytes[0] = inactiveShipsPerUpdate;
                    KMPCommon.intToBytes(screenshotSettings.maxHeight).CopyTo(bytes, 1);
                    KMPCommon.intToBytes(updateInterval).CopyTo(bytes, 5);
                    username_bytes.CopyTo(bytes, 9);

                    enqueueClientInteropMessage(KMPCommon.ClientInteropMessageID.CLIENT_DATA, bytes);

                    lastClientDataWriteTime = stopwatch.ElapsedMilliseconds;
                }
            }

        }

        static void enqueueTextMessage(String message, bool from_server = false, bool to_plugin = true, bool isMOTD = false)
        {
            InTextMessage text_message = new InTextMessage();
            text_message.message = message;
            text_message.fromServer = from_server;
            enqueueTextMessage(text_message, to_plugin);
        }

        static void enqueueTextMessage(InTextMessage message, bool to_plugin = true)
        {
            //Dequeue an old text message if there are a lot of messages backed up
            if (textMessageQueue.Count >= MAX_TEXT_MESSAGE_QUEUE)
            {
                textMessageQueue.Dequeue();
            }

            textMessageQueue.Enqueue(message);

            if (to_plugin)
            {
                if (message.fromServer)
                    enqueuePluginChatMessage("[Server] " + message.message, false);
                else if (message.isMOTD)
                    enqueuePluginChatMessage("[MOTD] " + message.message, false);
                else
                    enqueuePluginChatMessage(message.message);
            }
        }

        static void enqueuePluginChatMessage(String message, bool print = false)
        {
            enqueueClientInteropMessage(
                KMPCommon.ClientInteropMessageID.CHAT_RECEIVE,
                encoder.GetBytes(message)
                );

            if (print)
                Log.Debug(message);
        }

        static void safeDelete(String filename)
        {
            if (KSP.IO.File.Exists<KMPClientMain>(filename))
            {
                try
                {
                    KSP.IO.File.Delete<KMPClientMain>(filename);
                }
                catch (KSP.IO.IOException e)
                {
                    Log.Debug("Exception thrown in writePluginInterop(), catch 1, Exception: {0}", e.ToString());
                }
            }
        }

        static String findCraftFilename(String craft_name, ref KMPCommon.CraftType craft_type)
        {
            String vab_filename = getCraftFilename(craft_name, KMPCommon.CraftType.VAB);
            if (vab_filename != null && System.IO.File.Exists(vab_filename))
            {
                craft_type = KMPCommon.CraftType.VAB;
                return vab_filename;
            }

            String sph_filename = getCraftFilename(craft_name, KMPCommon.CraftType.SPH);
            if (sph_filename != null && System.IO.File.Exists(sph_filename))
            {
                craft_type = KMPCommon.CraftType.SPH;
                return sph_filename;
            }
            
            String subassembly_filename = getCraftFilename(craft_name, KMPCommon.CraftType.SUBASSEMBLY);
            if (subassembly_filename != null && System.IO.File.Exists(subassembly_filename))
            {
                craft_type = KMPCommon.CraftType.SUBASSEMBLY;
                return subassembly_filename;
            }
            
            return null;

        }

        static String getCraftFilename(String craft_name, KMPCommon.CraftType craft_type)
        {
            //Filter the craft name for illegal characters
            String filtered_craft_name = KMPCommon.filteredFileName(craft_name.Replace('.', '_'));

            if (currentGameTitle.Length <= 0 || filtered_craft_name.Length <= 0)
                return null;

            switch (craft_type)
            {
                case KMPCommon.CraftType.VAB:
                    return "saves/" + currentGameTitle + "/Ships/VAB/" + filtered_craft_name + CRAFT_FILE_EXTENSION;

                case KMPCommon.CraftType.SPH:
                    return "saves/" + currentGameTitle + "/Ships/SPH/" + filtered_craft_name + CRAFT_FILE_EXTENSION;
                    
                case KMPCommon.CraftType.SUBASSEMBLY:
                    return "saves/" + currentGameTitle + "/Subassemblies/" + filtered_craft_name + CRAFT_FILE_EXTENSION;
                                        
            }

            return null;

        }


        // Reads the player's token (GUID)
        private static void readTokenFile()
        {
            try
            {
                //Get the player's token if available
                KSP.IO.TextReader reader = KSP.IO.File.OpenText<KMPClientMain>(CLIENT_TOKEN_FILENAME);
                String line = reader.ReadLine();
                reader.Close();
                playerGuid = new Guid(line);
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in readTokenFile(), catch 1, Exception: {0}", e.ToString());
                //Generate a new token for server authentication
                playerGuid = Guid.NewGuid();
                KSP.IO.TextWriter writer = KSP.IO.File.CreateText<KMPClientMain>(CLIENT_TOKEN_FILENAME);
                writer.WriteLine(playerGuid.ToString());
                writer.Close();
            }
        }

        private static string buildNewXML()
        {
            return String.Format("<?xml version=\"1.0\"?><settings><global {0}=\"\" {1}=\"\" {2}=\"\"/><favourites></favourites></settings>", USERNAME_LABEL, IP_LABEL, AUTO_RECONNECT_LABEL);
        }


        // Reads the client's KMPClientConfig.xml file
        public static void readConfigFile()
        {
            try
            {
                XmlDocument xmlDoc = new XmlDocument();
                String sPath = "";
                try
                {
                    sPath = KSP.IO.IOUtils.GetFilePathFor(typeof(KMPClientMain), CLIENT_CONFIG_FILENAME);  // Get the Client config file path

                    if (!System.IO.File.Exists(sPath))  // Build a default style
                    {
                        xmlDoc.LoadXml(buildNewXML());
                        xmlDoc.Save(sPath);
                    }
                    xmlDoc.Load(sPath);
                }
                catch (Exception e)
                {
                    Log.Debug("Exception thrown in readConfigFile(), catch 1, Exception: {0}", e.ToString());
                    try
                    {
                        xmlDoc.LoadXml(buildNewXML());
                        xmlDoc.Save(sPath);
                        xmlDoc.Load(sPath);
                    }
                    catch (Exception seconde)
                    {
                        Log.Debug("Exception thrown in readConfigFile(), catch 2, Exception: {0}", seconde.ToString());
                        username = "";
                        hostname = "";
                        autoReconnect = true;
                    }
                }

                username = xmlDoc.SelectSingleNode("/settings/global/@" + USERNAME_LABEL).Value;
                hostname = xmlDoc.SelectSingleNode("/settings/global/@" + IP_LABEL).Value;
                bool.TryParse(xmlDoc.SelectSingleNode("/settings/global/@" + AUTO_RECONNECT_LABEL).Value, out autoReconnect);

                XmlNodeList elemList = xmlDoc.GetElementsByTagName("favourite");
                foreach (XmlNode xmlNode in elemList)
                {
                    int nPos = -1;
                    int.TryParse(xmlNode.Attributes[FAVORITE_LABEL].Value, out nPos);
                    if (nPos >= 0)
                    {
						String[] sArr = {xmlNode.Attributes[IP_LABEL].Value,  xmlNode.Attributes[PORT_LABEL].Value, xmlNode.Attributes[USERNAME_LABEL].Value};
                        favorites.Add(xmlNode.Attributes[NAME_LABEL].Value, sArr);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in readConfigFile(), catch 3, Exception: {0}", e.ToString());
                username = "";
                hostname = "";
                autoReconnect = true;
            }

            readTokenFile();
            //readPartsList(); now server sided
        }

        static void writeConfigFile()
        {

            XmlDocument xmlDoc = new XmlDocument();
            String sPath = KSP.IO.IOUtils.GetFilePathFor(typeof(KMPClientMain), CLIENT_CONFIG_FILENAME); // Get the path to the config file

            if (!System.IO.File.Exists(sPath))  // Build a default style
            {
                xmlDoc.LoadXml(String.Format("<?xml version=\"1.0\"?><settings><global {0}=\"\" {1}=\"\" {2}=\"\"/><favourites></favourites></settings>", USERNAME_LABEL, IP_LABEL, AUTO_RECONNECT_LABEL));
                xmlDoc.Save(sPath);
            }

            xmlDoc.Load(sPath);

            xmlDoc.SelectSingleNode("/settings/global/@" + USERNAME_LABEL).Value = username; // Set the username attribute
            xmlDoc.SelectSingleNode("/settings/global/@" + IP_LABEL).Value = hostname; // Set the hostname attribute
            xmlDoc.SelectSingleNode("/settings/global/@" + AUTO_RECONNECT_LABEL).Value = autoReconnect.ToString(); // Set the reconnect attribute

            XmlNode xFav = xmlDoc.SelectSingleNode("/settings/favourites");
            xFav.RemoveAll(); // Delete all the favourites

            int count = 0;
            foreach (String sIP in favorites.Keys) // Rebuild the favourites from memory
            {
				String[] sArr = new String[favorites.Count];
				favorites.TryGetValue(sIP, out sArr);

                XmlElement xEl = xmlDoc.CreateElement("favourite");
                xEl.SetAttribute(FAVORITE_LABEL, "" + count);
				xEl.SetAttribute(NAME_LABEL, sIP);
                xEl.SetAttribute(IP_LABEL, sArr[0]);
				xEl.SetAttribute(PORT_LABEL, sArr[1]);
				xEl.SetAttribute(USERNAME_LABEL, sArr[2]);
                xFav.AppendChild(xEl);
                count++;
            }

            xmlDoc.Save(sPath); // Save :)
        }

        //Messages

        private static void beginAsyncRead()
        {
            try
            {
                if (tcpClient != null)
                {
                    currentMessageHeaderIndex = 0;
                    currentMessageDataIndex = 0;
                    receiveIndex = 0;
                    receiveHandleIndex = 0;

                    StateObject state = new StateObject();
                    state.workClient = tcpClient;
                    tcpClient.GetStream().BeginRead(state.buffer, 0, StateObject.BufferSize, new AsyncCallback(ReceiveCallback), state);
                }
            }
            catch (KSP.IO.IOException e)
            {
                Log.Debug("Exception thrown in beginAsyncRead(), catch 1, Exception: {0}", e.ToString());
            }
            catch (InvalidOperationException e)
            {
                Log.Debug("Exception thrown in beginAsyncRead(), catch 2, Exception: {0}", e.ToString());
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in beginAsyncRead(), catch 3, Exception: {0}", e.ToString());
                passExceptionToMain(e);
            }
        }

        private static void ReceiveCallback(IAsyncResult ar)
        {
            try
            {
                // Retrieve the state object and the client socket 
                // from the asynchronous state object.
                StateObject state = (StateObject)ar.AsyncState;
                TcpClient client = state.workClient;
                // Read data from the remote device.
                int bytesRead = client.GetStream().EndRead(ar);
                if (bytesRead > 0)
                {
                    // There might be more data, so store the data received so far.
                    lock (receiveBufferLock)
                    {
                        KSP.IO.MemoryStream ms = new KSP.IO.MemoryStream();
                        ms.Write(receiveBuffer, 0, receiveIndex);
                        ms.Write(state.buffer, 0, bytesRead);
                        receiveBuffer = ms.ToArray();
                        receiveIndex += bytesRead;
                        handleReceive();
                        //  Get the rest of the data.
                        client.GetStream().BeginRead(state.buffer, 0, StateObject.BufferSize, new AsyncCallback(ReceiveCallback), state);
                    }
                }
                else
                {
                    //		            // All the data has arrived
                    //		            if (receiveIndex > 1) {
                    //						//LogAndShare("Done:" + System.Text.Encoding.ASCII.GetString(receiveBuffer));
                    //		                
                    //		            }
                }
            }
            catch (InvalidOperationException e)
            {
                Log.Debug("Exception thrown in ReceiveCallback(), catch 1, Exception: {0}", e.ToString());
            }
            catch (ThreadAbortException e)
            {
                Log.Debug("Exception thrown in ReceiveCallback(), catch 2, Exception: {0}", e.ToString());
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in ReceiveCallback(), catch 3, Exception: {0}", e.ToString());
                passExceptionToMain(e);
            }
        }

        private static void handleReceive()
        {
            while (receiveHandleIndex < receiveIndex)
            {

                //Read header bytes
                if (currentMessageHeaderIndex < KMPCommon.MSG_HEADER_LENGTH)
                {
                    //Determine how many header bytes can be read
                    int bytes_to_read = Math.Min(receiveIndex - receiveHandleIndex, KMPCommon.MSG_HEADER_LENGTH - currentMessageHeaderIndex);

                    //Read header bytes
                    Array.Copy(receiveBuffer, receiveHandleIndex, currentMessageHeader, currentMessageHeaderIndex, bytes_to_read);

                    //Advance buffer indices
                    currentMessageHeaderIndex += bytes_to_read;
                    receiveHandleIndex += bytes_to_read;

                    //Handle header
                    if (currentMessageHeaderIndex >= KMPCommon.MSG_HEADER_LENGTH)
                    {
                        int id_int = KMPCommon.intFromBytes(currentMessageHeader, 0);

                        //Make sure the message id section of the header is a valid value
                        if (id_int >= 0 && id_int < Enum.GetValues(typeof(KMPCommon.ServerMessageID)).Length)
                            currentMessageID = (KMPCommon.ServerMessageID)id_int;
                        else
                            currentMessageID = KMPCommon.ServerMessageID.NULL;

                        int data_length = KMPCommon.intFromBytes(currentMessageHeader, 4);

                        if (data_length > 0)
                        {
                            //Init message data buffer
                            currentMessageData = new byte[data_length];
                            currentMessageDataIndex = 0;
                        }
                        else
                        {
                            currentMessageData = null;
                            //Handle received message
                            messageReceived(currentMessageID, null);

                            //Prepare for the next header read
                            currentMessageHeaderIndex = 0;
                        }
                    }
                }

                if (currentMessageData != null)
                {
                    //Read data bytes
                    if (currentMessageDataIndex < currentMessageData.Length)
                    {
                        //Determine how many data bytes can be read
                        int bytes_to_read = Math.Min(receiveIndex - receiveHandleIndex, currentMessageData.Length - currentMessageDataIndex);

                        //Read data bytes
                        Array.Copy(receiveBuffer, receiveHandleIndex, currentMessageData, currentMessageDataIndex, bytes_to_read);

                        //Advance buffer indices
                        currentMessageDataIndex += bytes_to_read;
                        receiveHandleIndex += bytes_to_read;

                        //Handle data
                        if (currentMessageDataIndex >= currentMessageData.Length)
                        {
                            //Handle received message
                            byte[] messageData = KMPCommon.Decompress(currentMessageData);
                            if (messageData != null) messageReceived(currentMessageID, messageData);
                            //Consider adding re-request here

                            currentMessageData = null;

                            //Prepare for the next header read
                            currentMessageHeaderIndex = 0;
                        }
                    }
                }

            }

            //Once all receive bytes have been handled, reset buffer indices to use the whole buffer again
            receiveHandleIndex = 0;
            receiveIndex = 0;
        }

        private static void messageReceived(KMPCommon.ServerMessageID id, byte[] data)
        {
            ServerMessage message;
            message.id = id;
            message.data = data;
            if (id != KMPCommon.ServerMessageID.NULL) receivedMessageQueue.Enqueue(message);
        }

        private static void sendHandshakeMessage()
        {
            //Encode username
            byte[] username_bytes = encoder.GetBytes(username);
            byte[] version_bytes = encoder.GetBytes(KMPCommon.PROGRAM_VERSION);
            byte[] guid_bytes = encoder.GetBytes(playerGuid.ToString());

            byte[] message_data = new byte[4 + username_bytes.Length + 4 + guid_bytes.Length + version_bytes.Length];

            KMPCommon.intToBytes(username_bytes.Length).CopyTo(message_data, 0);
            username_bytes.CopyTo(message_data, 4);
            KMPCommon.intToBytes(guid_bytes.Length).CopyTo(message_data, 4 + username_bytes.Length);
            guid_bytes.CopyTo(message_data, 4 + username_bytes.Length + 4);
            version_bytes.CopyTo(message_data, 4 + username_bytes.Length + 4 + guid_bytes.Length);
            queueOutgoingMessage(KMPCommon.ClientMessageID.HANDSHAKE, message_data);
        }

        internal static void sendTextMessage(String message)
        {
            //Encode message
            byte[] message_bytes = encoder.GetBytes(message);
            queueOutgoingMessage(KMPCommon.ClientMessageID.TEXT_MESSAGE, message_bytes);
        }

        private static void sendPluginUpdate(byte[] data, bool primary)
        {
            if (data != null && data.Length > 0)
            {
                KMPCommon.ClientMessageID id
                    = primary ? KMPCommon.ClientMessageID.PRIMARY_PLUGIN_UPDATE : KMPCommon.ClientMessageID.SECONDARY_PLUGIN_UPDATE;

                if (udpConnected && data.Length < 100)
					queueOutgoingUDPMessage(id, data);
                else
                    queueOutgoingMessage(id, data);
            }
        }
		
		private static void sendScenarioUpdate(byte[] data)
        {
            if (data != null && data.Length > 0)
            {
                if (udpConnected && data.Length < 100)
					queueOutgoingUDPMessage(KMPCommon.ClientMessageID.SCENARIO_UPDATE, data);
                else
                    queueOutgoingMessage(KMPCommon.ClientMessageID.SCENARIO_UPDATE, data);
            }
        }
		
        private static void sendShareScreenshotMessage(byte[] data)
        {
            if (data != null && data.Length > 0)
                queueOutgoingMessage(KMPCommon.ClientMessageID.SCREENSHOT_SHARE, data);
        }

        private static void sendScreenshotWatchPlayerMessage(String name)
        {
            //Encode name
            byte[] bytes = encoder.GetBytes(name);

            queueOutgoingMessage(KMPCommon.ClientMessageID.SCREEN_WATCH_PLAYER, bytes);
        }

        internal static void sendConnectionEndMessage(String message)
        {
            //Encode message
            byte[] message_bytes = encoder.GetBytes(message);

            queueOutgoingMessage(KMPCommon.ClientMessageID.CONNECTION_END, message_bytes);
        }

        private static void sendShareCraftMessage(String craft_name, byte[] data, KMPCommon.CraftType type)
        {
            //Encode message
            byte[] name_bytes = encoder.GetBytes(craft_name);

            byte[] bytes = new byte[8 + name_bytes.Length + data.Length];

            //Check size of data to make sure it's not too large
            if ((name_bytes.Length + data.Length) <= KMPCommon.MAX_CRAFT_FILE_BYTES)
            {
                //Copy data
                KMPCommon.intToBytes((int)type).CopyTo(bytes, 0);
                KMPCommon.intToBytes(name_bytes.Length).CopyTo(bytes, 4);
                name_bytes.CopyTo(bytes, 8);
                data.CopyTo(bytes, 8 + name_bytes.Length);

                queueOutgoingMessage(KMPCommon.ClientMessageID.SHARE_CRAFT_FILE, bytes);
            }
            else
                enqueueTextMessage("Craft file is too large to send.", false, true);


        }

		private static void splitOutgoingMessage(ref byte[] next_message)
		{
			//Only split messages bigger than SEND_BUFFER.
			if (next_message.Length > KMPCommon.SPLIT_MESSAGE_SIZE) {
				int split_index = 0;
				while (split_index < next_message.Length) {
					int bytes_to_read = Math.Min (next_message.Length - split_index, KMPCommon.SPLIT_MESSAGE_SIZE);
					byte[] split_message_bytes = new byte[bytes_to_read];
					Array.Copy (next_message, split_index, split_message_bytes, 0, bytes_to_read);
					byte[] split_message = buildMessageByteArray (KMPCommon.ClientMessageID.SPLIT_MESSAGE, split_message_bytes);
					queuedOutMessagesSplit.Enqueue(split_message);
					split_index = split_index + bytes_to_read;
				}
				//Return the first split message if we just split.
				Log.Debug("Split message into "  + queuedOutMessagesSplit.Count.ToString());
				next_message = queuedOutMessagesSplit.Dequeue ();
			}
		}

		private static void queueOutgoingMessage (KMPCommon.ClientMessageID id, byte[] data)
		{
			//Figure out if this is a high or low priority message
			byte[] message_bytes = buildMessageByteArray(id, data);
			switch (id) {
				case KMPCommon.ClientMessageID.HANDSHAKE:
				case KMPCommon.ClientMessageID.TEXT_MESSAGE:
				case KMPCommon.ClientMessageID.KEEPALIVE:
				case KMPCommon.ClientMessageID.UDP_PROBE:
				case KMPCommon.ClientMessageID.PING:
				case KMPCommon.ClientMessageID.SYNC_TIME:
					queuedOutMessagesHighPriority.Enqueue (message_bytes);
					break;
				default:
					queuedOutMessages.Enqueue (message_bytes);
					break;
			}
		}

		private static void queueOutgoingUDPMessage (KMPCommon.ClientMessageID id, byte[] data)
		{
			byte[] message_bytes = buildMessageByteArray(id, data, KMPCommon.intToBytes(clientID));
			queuedOutUDPMessages.Enqueue (message_bytes);

		}

		private static void sendOutgoingMessages() {
			try
			{
				lock (sendOutgoingMessagesLock) {
					if ((queuedOutMessagesHighPriority.Count > 0 || queuedOutMessagesSplit.Count > 0 || queuedOutMessages.Count > 0) && !isClientSendingData)
					{
						//Send high priority first, then any split messages (chopped up low priority messages), and then the normal queue.
						//Large low priorty messages get chopped up into a split message just before send.
						byte[] next_message = null;
						if (queuedOutMessagesHighPriority.Count > 0) {
							next_message = queuedOutMessagesHighPriority.Dequeue();
						} else {
							if (queuedOutMessagesSplit.Count > 0) {
								next_message = queuedOutMessagesSplit.Dequeue();
							} else {
								next_message = queuedOutMessages.Dequeue();
								splitOutgoingMessage(ref next_message);
							}
						}
						if (next_message != null) {
							isClientSendingData = true;
							syncTimeRewrite(ref next_message);
							tcpClient.GetStream().BeginWrite(next_message, 0, next_message.Length, new AsyncCallback(asyncTCPSend), next_message);
						}
					}
				}
			}
			// Socket closed or not connected.
			catch (System.InvalidOperationException e)
			{
				Log.Debug("Exception thrown in sendOutgoingMessages(), catch 1, Exception: {0}", e.ToString());
			}
			// Raised by BeginWrite, can mean socket is down.
			catch (System.IO.IOException e)
			{
				Log.Debug("Exception thrown in sendOutgoingMessages(), catch 2, Exception: {0}", e.ToString());
			}
			catch (System.NullReferenceException e)
			{
				Log.Debug("Exception thrown in sendOutgoingMessages(), catch 3, Exception: {0}", e.ToString());
			}
		
		}

		private static void syncTimeRewrite(ref byte[] next_message) {
			//SYNC_TIME Rewriting
			int next_message_id = BitConverter.ToInt32(next_message, 0);
			if (next_message_id == (int)KMPCommon.ClientMessageID.SYNC_TIME) {
				byte[] time_sync_rewrite = new byte[8];
				BitConverter.GetBytes(DateTime.UtcNow.Ticks).CopyTo(time_sync_rewrite, 0);
				next_message = buildMessageByteArray(KMPCommon.ClientMessageID.SYNC_TIME, time_sync_rewrite);
			}
		}

		private static void asyncTCPSend(IAsyncResult result)
		{
			try
			{
				if (tcpClient.Connected)
				{
					tcpClient.GetStream().EndWrite(result);
					isClientSendingData = false;
					if (queuedOutMessagesHighPriority.Count > 0 || queuedOutMessagesSplit.Count > 0 || queuedOutMessages.Count > 0) {
						sendOutgoingMessages();
					}
				}
			}
			catch (Exception e)
			{
				Log.Debug("Exception thrown in asyncTCPSend(), catch 1, Exception: {0}", e.ToString());
				gameManager.disconnect ("Disconnected: Send Error");
			}
		}

        private static void sendUDPProbeMessage(bool forceUDP)
        {
			byte[] timeData = new byte[12];
			if (gameManager.lastTick > 0) BitConverter.GetBytes(gameManager.lastTick).CopyTo(timeData, 0);
			if (gameManager.listClientTimeWarp.Count > 0) {
				BitConverter.GetBytes (gameManager.listClientTimeWarp.Average ()).CopyTo (timeData, 8);
			} else {
				BitConverter.GetBytes (1f).CopyTo (timeData, 8);
			}

            if (udpConnected || forceUDP)//Always try UDP periodically
            {
                queueOutgoingUDPMessage(KMPCommon.ClientMessageID.UDP_PROBE, timeData);
            }
            else
            {
                queueOutgoingMessage(KMPCommon.ClientMessageID.UDP_PROBE, timeData);
            }
            lastUDPProbeTime = stopwatch.ElapsedMilliseconds;
        }

		private static void sendOutgoingUDPMessages() {
			try
			{
				lock (sendOutgoingUDPMessagesLock) {
					if (queuedOutUDPMessages.Count > 0 && !isClientSendingUDPData)
					{
						byte[] next_message = null;
						next_message = queuedOutUDPMessages.Dequeue();
						isClientSendingUDPData = true;
						udpClient.BeginSend(next_message, next_message.Length, new AsyncCallback(asyncUDPSend), next_message);
					}
				}
			}
			// Socket closed or not connected.
			catch (System.InvalidOperationException e)
			{
				Log.Debug("Exception thrown in sendOutgoingUDPMessages(), catch 1, Exception: {0}", e.ToString());
			}
			// Raised by BeginWrite, can mean socket is down.
			catch (System.IO.IOException e)
			{
				Log.Debug("Exception thrown in sendOutgoingUDPMessages(), catch 2, Exception: {0}", e.ToString());
			}
			catch (System.NullReferenceException e)
			{
				Log.Debug("Exception thrown in sendOutgoingUDPMessages(), catch 3, Exception: {0}", e.ToString());
			}

		}

		private static void asyncUDPSend(IAsyncResult result)
		{
			try
			{
				udpClient.EndSend(result);
				lastUDPMessageSendTime = stopwatch.ElapsedMilliseconds;
				isClientSendingUDPData = false;
				if (queuedOutUDPMessages.Count > 0) {
					sendOutgoingUDPMessages();
				}
			}
			catch (Exception e)
			{
				Log.Debug("Exception thrown in asyncUDPSend(), catch 1, Exception: {0}", e.ToString());
				gameManager.disconnect ("Disconnected: Send Error");
			}
		}

        private static byte[] buildMessageByteArray(KMPCommon.ClientMessageID id, byte[] data, byte[] prefix = null)
        {
            byte[] compressed_data = null;
            int prefix_length = 0;
            if (prefix != null)
                prefix_length = prefix.Length;

            int msg_data_length = 0;
            if (data != null)
            {
                compressed_data = KMPCommon.Compress(data);
                if (compressed_data == null) compressed_data = KMPCommon.Compress(data, true);
                msg_data_length = compressed_data.Length;
            }

            byte[] message_bytes = new byte[KMPCommon.MSG_HEADER_LENGTH + msg_data_length + prefix_length];

            int index = 0;

            if (prefix != null)
            {
                prefix.CopyTo(message_bytes, index);
                index += 4;
            }

            KMPCommon.intToBytes((int)id).CopyTo(message_bytes, index);
            index += 4;

            KMPCommon.intToBytes(msg_data_length).CopyTo(message_bytes, index);
            index += 4;

            if (compressed_data != null)
            {
                compressed_data.CopyTo(message_bytes, index);
                index += compressed_data.Length;
            }
            return message_bytes;
        }

        public static void SetMessage(String newMessage)
        {
            message = newMessage;
            Log.Debug(newMessage);
        }

        // Returns the absolute path of the directory which contains KerbalMultiPlayer.dll as a String
        // E.g. "C:\Program Files (x86)\Steam\SteamApps\common\Kerbal Space Program\GameData\KMP\Plugins"
        // Note the lack of ending DirectorySeparator.
        internal static String getKMPDirectory()
        {
            return System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
        }

        // Verifies that the folder structure for /saves/KMP/Ships/VAB and /saves/KMP/Ships/SPH exists
        // Creates the tree if it does not exist.
        internal static void verifyShipsDirectory()
        {
            char cSep = '/';
            String sPath = getKMPDirectory();
            System.IO.DirectoryInfo dir = System.IO.Directory.GetParent(sPath);
            dir = dir.Parent.Parent;
            sPath = dir.FullName;
            sPath += "/saves/KMP/";
            if (!System.IO.Directory.Exists(sPath))
                System.IO.Directory.CreateDirectory(sPath);
            sPath += cSep + "Ships";
            //Log.Debug(sPath);
            if (!System.IO.Directory.Exists(sPath))
                System.IO.Directory.CreateDirectory(sPath);

            if (!System.IO.Directory.Exists(sPath + cSep + "VAB"))
                System.IO.Directory.CreateDirectory(sPath + cSep + "VAB");

            if (!System.IO.Directory.Exists(sPath + cSep + "SPH"))
                System.IO.Directory.CreateDirectory(sPath + cSep + "SPH");
        }

        // Checks to see if start.sfs exists in the /saves/KMP/ directory.
        internal static bool startSaveExists()
        {
            String sPath = getKMPDirectory();
            System.IO.DirectoryInfo dir = System.IO.Directory.GetParent(sPath);
            dir = dir.Parent.Parent;
            sPath = dir.FullName;
            sPath += "/saves/KMP/";
            if (!System.IO.File.Exists(sPath + "start.sfs"))
                return false;
            else
                return true;
        }
    }

    public class StateObject
    {
        // Client socket.
        public TcpClient workClient = null;
        // Size of receive buffer.
        public const int BufferSize = 8192;
        // Receive buffer.
        public byte[] buffer = new byte[BufferSize];
        // Received data string.
        public byte[] data = new byte[BufferSize];
    }

    public class SHAMod
    {
        public string sha { get; set; }
        public bool required { get; set; }
    }
}
