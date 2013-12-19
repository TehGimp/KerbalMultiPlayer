//#define DEBUG_OUT
//#define SEND_UPDATES_TO_SENDER

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Net.Sockets;
using System.Threading;
using System.Net;
using System.IO;
using System.Diagnostics;

using System.Collections;

using System.Data;
using System.Data.SQLite;
//using Mono.Data.Sqlite;

using KMP;
using System.Data.Common;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text.RegularExpressions;
using System.Globalization;
using System.Collections.Concurrent;

namespace KMPServer
{
    class Server
    {

        public struct ClientMessage
        {
            public Client client;
            public KMPCommon.ClientMessageID id;
            public byte[] data;
        }

        public const long CLIENT_TIMEOUT_DELAY = 16000;
        public const long CLIENT_HANDSHAKE_TIMEOUT_DELAY = 6000;
        public const int GHOST_CHECK_DELAY = 30000;
        public const int SLEEP_TIME = 10;
        public const int MAX_SCREENSHOT_COUNT = 10000;
        public const int UDP_ACK_THROTTLE = 1000;

        public const float NOT_IN_FLIGHT_UPDATE_WEIGHT = 1.0f / 4.0f;
        public const int ACTIVITY_RESET_DELAY = 10000;

        public const String SCREENSHOT_DIR = "KMPScreenshots";
        public const string DB_FILE_CONN = "Data Source=KMP_universe.db";
        public const string DB_FILE = "KMP_universe.db";
        public const string MOD_CONTROL_FILE = "KMPModControl.txt";
        
        public static byte[] kmpModControl;

        public const int UNIVERSE_VERSION = 4;

        public bool quit = false;
        public bool stop = false;

        public String threadExceptionStackTrace;
        public Exception threadException;

        public object threadExceptionLock = new object();
        public static object consoleWriteLock = new object();
		public static object databaseVacuumLock = new object();

        public Thread listenThread;
        public Thread commandThread;
        public Thread connectionThread;
        public Thread outgoingMessageThread;
        public Thread ghostCheckThread;

        public Timer autoDekesslerTimer;

        public TcpListener tcpListener;
        public UdpClient udpClient;

        public HttpListener httpListener;

        public SynchronizedCollection<Client> clients;
        public SynchronizedCollection<Client> flight_clients;
        public SynchronizedCollection<Client> cleanupClients;
        public ConcurrentQueue<ClientMessage> clientMessageQueue;

        public int clientIndex = 0;

        public ServerSettings.ConfigStore settings;

        public Stopwatch stopwatch = new Stopwatch();

        public static SQLiteConnection universeDB;

        private bool backedUpSinceEmpty = false;
        private Dictionary<Guid, long> recentlyDestroyed = new Dictionary<Guid, long>();

		private Boolean bHandleCommandsRunning = true;
		
		private int uncleanedBackups = 0;
		
        public long currentMillisecond
        {
            get
            {
                return stopwatch.ElapsedMilliseconds;
            }
        }

        public int updateInterval
        {
            get
            {
                float relevant_player_count = 0;

                //Create a weighted count of clients in-flight and not in-flight to estimate the amount of update traffic
                relevant_player_count = flight_clients.Count + (activeClientCount() - flight_clients.Count) * NOT_IN_FLIGHT_UPDATE_WEIGHT;

                if (relevant_player_count <= 0)
                    return ServerSettings.MIN_UPDATE_INTERVAL;

                //Calculate the value that satisfies updates per second
                int val = (int)Math.Round(1.0f / (settings.updatesPerSecond / relevant_player_count) * 1000);

                //Bound the values by the minimum and maximum
                if (val < ServerSettings.MIN_UPDATE_INTERVAL)
                    return ServerSettings.MIN_UPDATE_INTERVAL;

                if (val > ServerSettings.MAX_UPDATE_INTERVAL)
                    return ServerSettings.MAX_UPDATE_INTERVAL;

                return val;
            }
        }

        public byte inactiveShipsPerClient
        {
            get
            {
                int relevant_player_count = 0;
                relevant_player_count = flight_clients.Count;

                if (relevant_player_count <= 0)
                    return settings.totalInactiveShips;

                if (relevant_player_count > settings.totalInactiveShips)
                    return 0;

                return (byte)(settings.totalInactiveShips / relevant_player_count);

            }
        }

        //Methods

        public Server(ServerSettings.ConfigStore settings)
        {
            this.settings = settings;
        }

        public void clearState()
        {
            safeAbort(listenThread);
            safeAbort(commandThread);
            safeAbort(connectionThread);
            safeAbort(outgoingMessageThread);
            safeAbort(ghostCheckThread);

            if (clients != null)
            {
                foreach (Client client in clients.ToList())
                {
                    client.endReceivingMessages();
                    if (client.tcpClient != null)
                        client.tcpClient.Close();
                }
            }

            if (tcpListener != null)
            {
                try
                {
                    tcpListener.Stop();
                }
                catch (System.Net.Sockets.SocketException)
                {
                }
            }

            if (httpListener != null)
            {
                try
                {
                    httpListener.Stop();
                    httpListener.Close();
                }
                catch (ObjectDisposedException)
                {
                }
            }

            if (udpClient != null)
            {
                try
                {
                    udpClient.Close();
                }
                catch { }
            }

            udpClient = null;

            if (universeDB != null && universeDB.State != ConnectionState.Closed)
            {
                try
                {
                    backupDatabase();
                    universeDB.Close();
                    universeDB.Dispose();
                }
                catch { }
            }

            startDatabase();
        }
        
		private static void generatePartsList(TextWriter writer)
        {
                List<string> partList = new List<string>();

                //0.21 (& below) parts
                partList.Add("StandardCtrlSrf"); partList.Add("CanardController"); partList.Add("noseCone"); partList.Add("AdvancedCanard"); partList.Add("airplaneTail");
                partList.Add("deltaWing"); partList.Add("noseConeAdapter"); partList.Add("rocketNoseCone"); partList.Add("smallCtrlSrf"); partList.Add("standardNoseCone");
                partList.Add("sweptWing"); partList.Add("tailfin"); partList.Add("wingConnector"); partList.Add("winglet"); partList.Add("R8winglet");
                partList.Add("winglet3"); partList.Add("Mark1Cockpit"); partList.Add("Mark2Cockpit"); partList.Add("Mark1-2Pod"); partList.Add("advSasModule");
                partList.Add("asasmodule1-2"); partList.Add("avionicsNoseCone"); partList.Add("crewCabin"); partList.Add("cupola"); partList.Add("landerCabinSmall");

                partList.Add("mark3Cockpit"); partList.Add("mk1pod"); partList.Add("mk2LanderCabin"); partList.Add("probeCoreCube"); partList.Add("probeCoreHex");
                partList.Add("probeCoreOcto"); partList.Add("probeCoreOcto2"); partList.Add("probeCoreSphere"); partList.Add("probeStackLarge"); partList.Add("probeStackSmall");
                partList.Add("sasModule"); partList.Add("seatExternalCmd"); partList.Add("rtg"); partList.Add("batteryBank"); partList.Add("batteryBankLarge");
                partList.Add("batteryBankMini"); partList.Add("batteryPack"); partList.Add("ksp.r.largeBatteryPack"); partList.Add("largeSolarPanel"); partList.Add("solarPanels1");
                partList.Add("solarPanels2"); partList.Add("solarPanels3"); partList.Add("solarPanels4"); partList.Add("solarPanels5"); partList.Add("JetEngine");

                partList.Add("engineLargeSkipper"); partList.Add("ionEngine"); partList.Add("liquidEngine"); partList.Add("liquidEngine1-2"); partList.Add("liquidEngine2");
                partList.Add("liquidEngine2-2"); partList.Add("liquidEngine3"); partList.Add("liquidEngineMini"); partList.Add("microEngine"); partList.Add("nuclearEngine");
                partList.Add("radialEngineMini"); partList.Add("radialLiquidEngine1-2"); partList.Add("sepMotor1"); partList.Add("smallRadialEngine"); partList.Add("solidBooster");
                partList.Add("solidBooster1-1"); partList.Add("toroidalAerospike"); partList.Add("turboFanEngine"); partList.Add("MK1Fuselage"); partList.Add("Mk1FuselageStructural");
                partList.Add("RCSFuelTank"); partList.Add("RCSTank1-2"); partList.Add("rcsTankMini"); partList.Add("rcsTankRadialLong"); partList.Add("fuelTank");

                partList.Add("fuelTank1-2"); partList.Add("fuelTank2-2"); partList.Add("fuelTank3-2"); partList.Add("fuelTank4-2"); partList.Add("fuelTankSmall");
                partList.Add("fuelTankSmallFlat"); partList.Add("fuelTank.long"); partList.Add("miniFuelTank"); partList.Add("mk2Fuselage"); partList.Add("mk2SpacePlaneAdapter");
                partList.Add("mk3Fuselage"); partList.Add("mk3spacePlaneAdapter"); partList.Add("radialRCSTank"); partList.Add("toroidalFuelTank"); partList.Add("xenonTank");
                partList.Add("xenonTankRadial"); partList.Add("adapterLargeSmallBi"); partList.Add("adapterLargeSmallQuad"); partList.Add("adapterLargeSmallTri"); partList.Add("adapterSmallMiniShort");
                partList.Add("adapterSmallMiniTall"); partList.Add("nacelleBody"); partList.Add("radialEngineBody"); partList.Add("smallHardpoint"); partList.Add("stationHub");

                partList.Add("structuralIBeam1"); partList.Add("structuralIBeam2"); partList.Add("structuralIBeam3"); partList.Add("structuralMiniNode"); partList.Add("structuralPanel1");
                partList.Add("structuralPanel2"); partList.Add("structuralPylon"); partList.Add("structuralWing"); partList.Add("strutConnector"); partList.Add("strutCube");
                partList.Add("strutOcto"); partList.Add("trussAdapter"); partList.Add("trussPiece1x"); partList.Add("trussPiece3x"); partList.Add("CircularIntake");
                partList.Add("landingLeg1"); partList.Add("landingLeg1-2"); partList.Add("RCSBlock"); partList.Add("stackDecoupler"); partList.Add("airScoop");
                partList.Add("commDish"); partList.Add("decoupler1-2"); partList.Add("dockingPort1"); partList.Add("dockingPort2"); partList.Add("dockingPort3");

                partList.Add("dockingPortLarge"); partList.Add("dockingPortLateral"); partList.Add("fuelLine"); partList.Add("ladder1"); partList.Add("largeAdapter");
                partList.Add("largeAdapter2"); partList.Add("launchClamp1"); partList.Add("linearRcs"); partList.Add("longAntenna"); partList.Add("miniLandingLeg");
                partList.Add("parachuteDrogue"); partList.Add("parachuteLarge"); partList.Add("parachuteRadial"); partList.Add("parachuteSingle"); partList.Add("radialDecoupler");
                partList.Add("radialDecoupler1-2"); partList.Add("radialDecoupler2"); partList.Add("ramAirIntake"); partList.Add("roverBody"); partList.Add("sensorAccelerometer");
                partList.Add("sensorBarometer"); partList.Add("sensorGravimeter"); partList.Add("sensorThermometer"); partList.Add("spotLight1"); partList.Add("spotLight2");

                partList.Add("stackBiCoupler"); partList.Add("stackDecouplerMini"); partList.Add("stackPoint1"); partList.Add("stackQuadCoupler"); partList.Add("stackSeparator");
                partList.Add("stackSeparatorBig"); partList.Add("stackSeparatorMini"); partList.Add("stackTriCoupler"); partList.Add("telescopicLadder"); partList.Add("telescopicLadderBay");
                partList.Add("SmallGearBay"); partList.Add("roverWheel1"); partList.Add("roverWheel2"); partList.Add("roverWheel3"); partList.Add("wheelMed"); partList.Add("flag");
                partList.Add("kerbalEVA");

                //0.22 parts
                partList.Add("mediumDishAntenna"); partList.Add("GooExperiment"); partList.Add("science.module");

                //0.23 parts
                partList.Add("RAPIER"); partList.Add("Large.Crewed.Lab");

                foreach(string part in partList) writer.WriteLine(part);
        }
        
        private static void readModControl()
        {
            try
            {	Log.Info("Reading {0}", MOD_CONTROL_FILE);
            	kmpModControl = File.ReadAllBytes(MOD_CONTROL_FILE);
            	Log.Info("Mod control reloaded.");
            }
            catch
            {
            	Log.Info(MOD_CONTROL_FILE + " not found, generating...");
                TextWriter writer = File.CreateText(MOD_CONTROL_FILE);
                writer.WriteLine("#You can comment by starting a line with a #, these are ignored by the server.");
                writer.WriteLine("#Commenting will NOT work unless the line STARTS with a #.");
                writer.WriteLine("#Sections supported are md5, partslist, resource-blacklist and resource-whitelist.");
                writer.WriteLine("#You cannot use both resource-blacklist AND resource-whitelist in the same file.");
                writer.WriteLine("#resource-blacklist bans ONLY the files you specify whereas resource-whitelist bans ALL resources except those you specify.");
                writer.WriteLine("#Each section has its own type of formatting. Examples have been given.");
                writer.WriteLine("#Sections are defined as follows");
                writer.WriteLine();
                writer.WriteLine("!required");
                writer.WriteLine();
                writer.WriteLine("#Here you can define GameData (mod) folders that the client requires before joining the server");
                writer.WriteLine("#[Folder]");
                writer.WriteLine("#Example: MechJeb2");
                writer.WriteLine();
                writer.WriteLine("Squad");
                writer.WriteLine("#NOTE: This squad entry ensures that the client hasn't deleted the default parts. Disable this if undesired.");
                writer.WriteLine();
                writer.WriteLine("!md5");
                writer.WriteLine();
                writer.WriteLine("#To generate the md5 of a file you can use a utility such as this one: http://onlinemd5.com/");
                writer.WriteLine("#For the MD5 section, file paths are read from inside GameData. If a client's MD5 does not match, they will not be permitted entry.");
                writer.WriteLine("#You may not specify multiple MD5s for the same file. Do not put spaces around equals sign. Follow the example carefully.");
                writer.WriteLine("#[File Path]=[MD5]");
                writer.WriteLine("#Example: MechJeb2/Plugins/MechJeb2.dll=64E6E05C88F3466C63EDACD5CF8E5919");
                writer.WriteLine();
                writer.WriteLine("!resource-blacklist");
                writer.WriteLine();
                writer.WriteLine("#In this section you can specify the files to ban (or permit, if you change blacklist to whitelist).");
                writer.WriteLine("#You do not need to specify a path, just a resource name.");
                writer.WriteLine("#You can control any file from GameData here. It's prefered if you just specify the names of resources (as parts are controled by the partlist).");
                writer.WriteLine("#[File]");
                writer.WriteLine("#Example: MechJeb2.dll");
                writer.WriteLine();
                writer.WriteLine("!partslist");
                writer.WriteLine();
                writer.WriteLine("#This is a list of parts to allow users to put on their ships.");
                writer.WriteLine("#If a part the client has doesn't appear on this list, they can still join the server but not use the part.");
                writer.WriteLine("#The default stock parts have been added already for you.");
                writer.WriteLine("#To add a mod part, add the name from the part's .cfg file. The name is the name from the PART{} section, where underscores are replaced with periods.");
                writer.WriteLine("#[partname]");
                writer.WriteLine("#Example: mumech.MJ2.Pod (NOTE: In the part.cfg this MechJeb2 pod is named mumech_MJ2_Pod. The _ have been replaced with .)");
                writer.WriteLine("#You can use this application to generate partlists from a KSP installation if you want to add mod parts: http://forum.kerbalspaceprogram.com/threads/57284");
                writer.WriteLine();
                generatePartsList(writer);
                writer.Close();
                readModControl();
            }
        }
        
        public void saveScreenshot(byte[] bytes, String player)
        {
            if (!Directory.Exists(SCREENSHOT_DIR))
            {
                //Create the screenshot directory
                try
                {
                    if (!Directory.CreateDirectory(SCREENSHOT_DIR).Exists)
                        return;
                }
                catch (Exception)
                {
                    return;
                }
            }

            //Write the screenshot to file
            String filename = string.Format("{0}/{1} {2}.png", SCREENSHOT_DIR, KMPCommon.filteredFileName(player), System.DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss"));
            if (!File.Exists(filename))
            {
                try
                {
                    //Read description length
                    int description_length = KMPCommon.intFromBytes(bytes, 0);

                    //Trim the description bytes from the image
                    byte[] trimmed_bytes = new byte[bytes.Length - 4 - description_length];
                    Array.Copy(bytes, 4 + description_length, trimmed_bytes, 0, trimmed_bytes.Length);

                    File.WriteAllBytes(filename, trimmed_bytes);
                }
                catch (Exception)
                {
                }
            }
        }

        private void safeAbort(Thread thread, bool join = false)
        {
            if (thread != null)
            {
                try
                {
                    thread.Abort();
                    if (join)
                        thread.Join();
                }
                catch (ThreadStateException) { }
                catch (ThreadInterruptedException) { }
            }
        }

        public void passExceptionToMain(Exception e)
        {
            lock (threadExceptionLock)
            {
                if (threadException == null)
                    threadException = e; //Pass exception to main thread
            }
        }

        //Threads

        public void hostingLoop()
        {
            clearState();

            //Start hosting server
            stopwatch.Start();
			
			//read info for server sided mod support
			readModControl();
			
            Log.Info("Hosting server on port {0} ...", settings.port);

            clients = new SynchronizedCollection<Client>(settings.maxClients);
            flight_clients = new SynchronizedCollection<Client>(settings.maxClients);
            cleanupClients = new SynchronizedCollection<Client>(settings.maxClients);
            clientMessageQueue = new ConcurrentQueue<ClientMessage>();

            listenThread = new Thread(new ThreadStart(listenForClients));
            commandThread = new Thread(new ThreadStart(handleCommands));
            connectionThread = new Thread(new ThreadStart(handleConnections));
            outgoingMessageThread = new Thread(new ThreadStart(sendOutgoingMessages));
            ghostCheckThread = new Thread(new ThreadStart(checkGhosts));

            threadException = null;
            if (settings.ipBinding == "0.0.0.0" && settings.hostIPv6 == true) {
                settings.ipBinding = "::";
            }
            tcpListener = new TcpListener(IPAddress.Parse(settings.ipBinding), settings.port);
            
            listenThread.Start();

            try
            {
                if (settings.hostIPv6 == true) {
                    udpClient = new UdpClient(settings.port, AddressFamily.InterNetworkV6);
                    udpClient.BeginReceive(asyncUDPReceive, null);
                    //udpClient.Client.AllowNatTraversal(1);
                } else {
                    udpClient = new UdpClient(settings.port, AddressFamily.InterNetwork);
                    udpClient.BeginReceive(asyncUDPReceive, null);
                }
                
            }
            catch
            {
                udpClient = null;
            }

            displayCommands();

            commandThread.Start();
            connectionThread.Start();
            outgoingMessageThread.Start();
            ghostCheckThread.Start();


            if (settings.autoDekessler) 
			{
				autoDekesslerTimer = new Timer(_ => dekesslerServerCommand(new string[0]), null, settings.autoDekesslerTime * 60000, settings.autoDekesslerTime * 60000);
				Log.Debug("Starting AutoDekessler: Timer Set to " + settings.autoDekesslerTime + " Minutes");
			}

            if (settings.httpBroadcast)
                startHttpServer();

            long last_backup_time = 0;

            while (!stop)
            {
                //Check for exceptions that occur in threads
                lock (threadExceptionLock)
                {
                    if (threadException != null)
                    {
                        Exception e = threadException;
                        threadExceptionStackTrace = e.StackTrace;
                        throw e;
                    }
                }

                if (currentMillisecond - last_backup_time > (settings.backupInterval * 60000) && (activeClientCount() > 0 || !backedUpSinceEmpty))
                {
                    if (activeClientCount() <= 0)
                    {
                        backedUpSinceEmpty = true;
                        cleanDatabase();
                    }

                    last_backup_time = currentMillisecond;
                    backupDatabase();
                }

                Thread.Sleep(SLEEP_TIME);
            }

            clearState();
            stopwatch.Stop();

            Log.Info("Server session ended.");
            if (quit) { Log.Info("Quitting"); Thread.Sleep(1000); Environment.Exit(0); }

        }

        private void startHttpServer()
        {
            //Begin listening for HTTP requests
            httpListener = new HttpListener(); //Might need a replacement as HttpListener needs admin rights
            try
            {
                httpListener.Prefixes.Add("http://*:" + settings.httpPort + '/');
                httpListener.Start();
                httpListener.BeginGetContext(asyncHTTPCallback, httpListener);
            }
            catch (Exception e)
            {
                Log.Error("Error starting http server: {0}", e);
                Log.Error("Please try running the server as an administrator");
            }
        }

        private void killHttpServer()
        {
            httpListener.Stop();
        }

		private void processCommand (String input)
		{
			try
			{
				String cleanInput = input.ToLower().Trim();
                var rawParts = input.Split(new char[] { ' ' }, 2);
				var parts = cleanInput.Split(new char[] { ' ' }, 2);
                //if (!parts[0].StartsWith("/")) { return; } //Allow server to send chat messages
                switch (parts[0])
                {
                    case "/ban": banServerCommand(parts); break;
                    case "/clearclients": clearClientsServerCommand(); break;
                    case "/countclients": countServerCommand(); break;
                    case "/help": displayCommands(); break;
                    case "/kick": kickServerCommand(parts); break;
                    case "/listclients": listServerCommand(); break;
                    case "/quit":
                    case "/stop": quitServerCommand(parts); bHandleCommandsRunning = false; break;
                    case "/save": saveServerCommand(); break;
                    case "/register": registerServerCommand(parts); break;
                    case "/update": updateServerCommand(parts); break;
                    case "/unregister": unregisterServerCommand(parts); break;
                    case "/dekessler": dekesslerServerCommand(parts); break;
                    case "/countships": countShipsServerCommand(); break;
                    case "/listships": listShipsServerCommand(); break;
					case "/deleteship": deleteShipServerCommand(parts); break;
					case "/reloadmodfile": reloadModFileServerCommand(); break;
					case "/say": sayServerCommand(rawParts); break;
					case "/motd": motdServerCommand(rawParts); break;
					case "/rules": rulesServerCommand(rawParts); break;
					case "/setinfo": serverInfoServerCommand(rawParts);break;
                    default: Log.Info("Unknown Command: "+cleanInput); break;
            	}
			}
			catch (FormatException e)
			{
				Log.Error("Error handling server command. Maybe a typo? {0} {1}", e.Message,e.StackTrace);
			}
			catch (IndexOutOfRangeException)
			{
				Log.Error("Command found but missing elements.");
			}
		}

        private void handleCommands()
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("en-GB");
            try
            {
                while (bHandleCommandsRunning)
                {
					String input = Console.ReadLine();
					processCommand (input);
                }
            }
			catch (ArgumentOutOfRangeException)
			{
			}
            catch (ThreadAbortException)
            {
            }
            catch (Exception e)
            {
                passExceptionToMain(e);
            }
        }

		//Sends messages from Server
		private void sayServerCommand(string[] parts)
		{
			if(parts.Length > 1)
			{
				if(parts[1].IndexOf("-u") == 0)
				{
					parts = parts[1].Split(new char[] { ' ' }, 3);
					if (parts.Length > 2)
					{
						String sName = parts[1];
						var clientToMessage = clients.Where(cl => cl.username.ToLower() == sName && cl.isReady).FirstOrDefault();

						if (clientToMessage != null)
						{
							string message = parts[2];
							sendServerMessage(clientToMessage, message);
						}
						else
							Log.Info("Username " + sName + " not found.");
					}
					else
						Log.Info("Error: -u flag found but missing message.");
				}
				else if (parts[1].IndexOf("-u") != -1)
				{
					Log.Info("Error: -u flag found but in wrong location.");
				}
				else
					sendServerMessageToAll(parts[1]);
			}
			else
				Log.Info("Error: /say command improperly formatted.  Missing message.  /say <-u username> [message]");
		}

        private void countShipsServerCommand(bool bList = false)
        {
            DbCommand cmd = universeDB.CreateCommand();
            String sql = "SELECT  vu.UpdateMessage, v.ProtoVessel, v.Guid" +
                        " FROM kmpVesselUpdate vu" +
                        " INNER JOIN kmpVessel v ON v.Guid = vu.Guid AND v.Destroyed != 1" +
                        " INNER JOIN kmpSubspace s ON s.ID = vu.Subspace" +
                        " INNER JOIN" +
                        "  (SELECT vu.Guid, MAX(s.LastTick) AS LastTick" +
                        "  FROM kmpVesselUpdate vu" +
                        "  INNER JOIN kmpSubspace s ON s.ID = vu.Subspace" +
                        "  GROUP BY vu.Guid) t ON t.Guid = vu.Guid AND t.LastTick = s.LastTick;";
            cmd.CommandText = sql;
            DbDataReader reader = cmd.ExecuteReader();
            int count = 0;
            while (reader.Read())
            {
                KMPVesselUpdate vessel_update = (KMPVesselUpdate)ByteArrayToObject(GetDataReaderBytes(reader, 0));
                if (bList)
                    Log.Info("Name {0}\tID: {1}", vessel_update.name, vessel_update.kmpID);
                count++;
            }

            if (count == 0)
                Log.Info("No ships.");
            else if (!bList)
                Log.Info("Number of ships: {0}", count);

        }

        private void listShipsServerCommand()
        {
            countShipsServerCommand(true);
        }
        
        private void deleteShipServerCommand(string[] parts)
        {
            try
            {
				DbCommand cmd = universeDB.CreateCommand();
            	String sql = "UPDATE kmpVessel SET Destroyed = 1 WHERE Guid = @guid;";
            	Guid tokill = new Guid(parts[1]);
            	cmd.Parameters.AddWithValue("guid", tokill.ToByteArray());
           		cmd.CommandText = sql;
				int rows = -1;
	            rows = cmd.ExecuteNonQuery();
	            if(rows != -1 && rows != 0)
	            {
	            	Log.Info("Vessel {0} marked for deletion.", parts[1]);
	            }
	            
	            else
	            {
	            	Log.Info("Vessel {0} not found.", parts[1]);
	            }
			}
            
            catch (FormatException)
            {
				Log.Info("Vessel ID invalid.");
            }

        }
        
        private void reloadModFileServerCommand()
        {
        	readModControl();
        }
        
		private void motdServerCommand(string[] parts)
		{
			if(parts.Length > 1)
			{
				settings.serverMotd = (String) parts[1];
			}
			else
			{
				settings.serverMotd = "";
			}
			ServerSettings.writeToFile(settings);
			Log.Info("MOTD Updated");
		}
		
		private void rulesServerCommand(string[] parts)
		{
			if(parts.Length > 1)
			{
				settings.serverRules = (String) parts[1];
			}
			else
			{
				settings.serverRules = "";
			}
			ServerSettings.writeToFile(settings);	
			Log.Info("Rules Updated");
		}
		
		private void serverInfoServerCommand(string[] parts)
		{
			if(parts.Length > 1)
			{
				settings.serverInfo = (String) parts[1];
			}
			else
			{
				settings.serverInfo = "";
			}
			ServerSettings.writeToFile(settings);
			Log.Info("Server Info Updated");
		}
		
        //Ban specified user, by name, from the server
        private void banServerCommand(string[] parts)
        {
            int days = 365;

            if (parts.Length > 1)
            {
				String[] args = parts[1].Split(' ');
                String ban_name = args[0];
                Guid guid = Guid.Empty;
                if (args.Length == 2)
                {
                    days = Convert.ToInt32(args[1]);
                }

                var userToBan = clients.Where(c => c.username.ToLower() == ban_name && c.isReady).FirstOrDefault();

                if (userToBan != null)
                {
                    markClientForDisconnect(userToBan, "You were banned from the server!");
                    guid = userToBan.guid;

	                var rec = new ServerSettings.BanRecord()
	                {
	                    BannedGUID = guid,
	                    BannedIP = userToBan.IPAddress,
	                    BannedName = ban_name,
	                    Expires = DateTime.Now.AddDays(days),
	                    Why = "Ban by console",
	                    WhoBy = "Console",
	                    When = DateTime.Now,
	                };
	
	                settings.bans.Add(rec);
	                ServerSettings.saveBans(settings);
	
                    DbCommand cmd = universeDB.CreateCommand();
                    string sql = "UPDATE kmpPlayer SET Guid = @newGuid WHERE Guid = @guid;";
                    cmd.Parameters.AddWithValue("newGuid", Guid.NewGuid());
                    cmd.Parameters.AddWithValue("guid", guid);
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    Log.Info("Player '{0}' and all known aliases banned from server for {1} days. Edit KMPBans.txt or /unregister to allow this user to reconnect.", ban_name, days);
				}
                else
                {
                    Log.Info("Failed to locate player {0}.", ban_name);
                }
            }
        }

        //Disconnects all valid clients
        private void clearClientsServerCommand()
        {
            foreach (Client client in clients.ToList().Where(c => !c.isReady))
            {
                markClientForDisconnect(client, "Disconnected via /clearclients command");

                /*
                   Let's be a bit more aggresive, immediately close the socket, but leave the object intact.
                   That should get the handleConnections thread the break it needs to have a chance to disconnect the ghost.
                */
                try
                {
                    if (client.tcpClient != null)
                    {
                        client.tcpClient.Close();

                    }
                }
                catch (Exception) { };

                Log.Info("Force-disconnected client: {0}", client.playerID);
            }
        }

        //Reports the number of clients connected to the server
        private void countServerCommand()
        {
            Log.Info("In-Game Clients: {0}", activeClientCount());
            Log.Info("In-Flight Clients: {0}", flight_clients.Count);
        }

        //Kicks the specified user from the server
        private void kickServerCommand(String[] parts)
        {
			if (parts.Length == 2)
			{
				try
				{
					String kick_name = parts[1].ToLower();
					var clientToDisconnect = clients.Where(cl => cl.username.ToLower() == kick_name && cl.isReady).FirstOrDefault();
					if (clientToDisconnect != null)
					{
						markClientForDisconnect(clientToDisconnect, "You were kicked from the server.");
						Log.Info("{0} was kicked from the server.", clientToDisconnect.username);
					}
					else
					{
						Log.Info("Username {0} not found.", kick_name);
					}
				} catch (Exception e)
				{
					Log.Error("Could not kick user.");
					Log.Debug(e.Message);
				}
			}
			else
				Log.Info("Could not parse /kick command.  Format is \"/kick <username>\"");
        }

        //Lists the users currently connected
        private void listServerCommand()
        {
            //Display player list
            StringBuilder sb = new StringBuilder();
            if (activeClientCount() > 0)
            {
                foreach (var client in clients.ToList().Where(c => c.isReady))
                {
                    sb.Append(client.username);
                    sb.Append(" - ");
                    sb.Append(client.activityLevel.ToString());
                    sb.Append('\n');
                }
            }
            else
                sb.Append("No clients connected.");
            Log.Info(sb.ToString());
        }

        //Quits or Stops the server, based on input
        private void quitServerCommand(String[] parts)
        {
            stop = true;
            if (parts[0] == "/quit")
                quit = true;
            //Disconnect all clients, no need to clean them all up since we're shutting down anyway
            foreach (var c in clients.ToList())
            {
                disconnectClient(c, "Server is shutting down");
            }
            autoDekesslerTimer.Dispose();
        }

        //Registers the specified username to the server
        private void registerServerCommand(String[] parts)
        {
            String[] args = parts[1].Split(' ');
            if (args.Length == 2)
            {
                try
                {
                    Guid guid = new Guid(args[1]);
                    String username_lower = args[0].ToLower();

                    DbCommand cmd = universeDB.CreateCommand();
                    string sql = "DELETE FROM kmpPlayer WHERE Name LIKE @username;" +
                        " INSERT INTO kmpPlayer (Name, Guid) VALUES (@username,@guid);";
                    cmd.Parameters.AddWithValue("username", username_lower);
                    cmd.Parameters.AddWithValue("guid", guid);
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    Log.Info("Player {0} added to player roster with token {1}.", args[0], args[1]);
                }
                catch (FormatException)
                {
                    Log.Error("Supplied token is invalid.");
                }
                catch (Exception)
                {
                    Log.Error("Registration failed, possibly due to a malformed /register command.");
                }
            }
            else
            {
                Log.Info("Could not parse register command. Format is \"/register <username> <token>\"");
            }
        }

        //Saves the database on request
        private void saveServerCommand()
        {
            //Save the universe!
            Log.Info("Saving the universe! ;-)");
            backupDatabase();
        }

        //Updates the specified username GUID
        private void updateServerCommand(String[] parts)
        {
            String[] args = parts[1].Split(' ');
            if (args.Length == 2)
            {
                try
                {
                    Guid guid = new Guid(args[1]);
                    String username_lower = args[0].ToLower();
                    DbCommand cmd = universeDB.CreateCommand();
                    string sql = "UPDATE kmpPlayer SET Name=@username, Guid=@guid WHERE Name LIKE @username OR Guid = @guid;";
                    cmd.CommandText = sql;
                    cmd.Parameters.AddWithValue("username", username_lower);
                    cmd.Parameters.AddWithValue("guid", guid);
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    Log.Info("Updated roster with player {0} and token {1}.", args[0], args[1]);
                }
                catch (FormatException)
                {
                    Log.Error("Supplied token is invalid.");
                }
                catch (Exception)
                {
                    Log.Error("Update failed, possibly due to a malformed /update command.");
                }
            }
            else
            {
                Log.Info("Could not parse update command. Format is \"/update <username> <token>\"");
            }
        }

        //Unregisters the specified username from the server
        private void unregisterServerCommand(String[] parts)
        {
			if (parts.Length == 2)
			{
				try
				{
					String dereg = parts[1];
					DbCommand cmd = universeDB.CreateCommand();
					string sql = "DELETE FROM kmpPlayer WHERE Guid = @dereg OR Name LIKE @dereg;";
					cmd.CommandText = sql;
					cmd.Parameters.AddWithValue("dereg", dereg);
					cmd.ExecuteNonQuery();
					cmd.Dispose();
					Log.Info("Players with name/token {0} removed from player roster.", dereg);
				}
				catch (Exception e)
				{
					Log.Error("Unregister failed.");
					Log.Debug(e.Message);
				}
			}
			else
				Log.Info("Could not parse unregister command.  Format is \"/unregister <username OR GUID>\"");
        }

        //Clears old debris
        private void dekesslerServerCommand(String[] parts)
        {
            int minsToKeep = 30;
            if (parts.Length == 2)
            {
                String[] args = parts[1].Split(' ');
                if (args.Length == 1)
                    minsToKeep = Convert.ToInt32(args[0]);
                else
                    Log.Info("Could not parse dekessler command. Format is \"/dekessler <mins>\"");
            }

            try
            {
                //Get latest tick & calculate cut-off
                DbCommand cmd = universeDB.CreateCommand();
                string sql = "SELECT MAX(LastTick) FROM kmpSubspace";
                cmd.CommandText = sql;
                double cutOffTick = Convert.ToDouble(cmd.ExecuteScalar()) - Convert.ToDouble(minsToKeep * 60);
                //Get all vessels, remove Debris that is too old
                cmd = universeDB.CreateCommand();
                sql = "SELECT  vu.UpdateMessage, v.ProtoVessel, v.Guid" +
                    " FROM kmpVesselUpdate vu" +
                    " INNER JOIN kmpVessel v ON v.Guid = vu.Guid AND v.Destroyed != 1" +
                    " INNER JOIN kmpSubspace s ON s.ID = vu.Subspace" +
                    " INNER JOIN" +
                    "  (SELECT vu.Guid, MAX(s.LastTick) AS LastTick" +
                    "  FROM kmpVesselUpdate vu" +
                    "  INNER JOIN kmpSubspace s ON s.ID = vu.Subspace" +
                    "  GROUP BY vu.Guid) t ON t.Guid = vu.Guid AND t.LastTick = s.LastTick;";
                cmd.CommandText = sql;
                DbDataReader reader = cmd.ExecuteReader();

                int clearedCount = 0;
                try
                {
                    while (reader.Read())
                    {
                        KMPVesselUpdate vessel_update = (KMPVesselUpdate)ByteArrayToObject(GetDataReaderBytes(reader, 0));
                        if (vessel_update.tick < cutOffTick)
                        {
                            byte[] configNodeBytes = GetDataReaderBytes(reader, 1);
                            string s = Encoding.UTF8.GetString(configNodeBytes, 0, configNodeBytes.Length);
                            if (s.IndexOf("type") > 0 && s.Length > s.IndexOf("type") + 20)
                            {
                                if (s.Substring(s.IndexOf("type"), 20).Contains("Debris"))
                                {
                                    DbCommand cmd2 = universeDB.CreateCommand();
                                    string sql2 = "UPDATE kmpVessel SET Destroyed = 1 WHERE Guid = @guid";
                                    cmd2.CommandText = sql2;
                                    cmd2.Parameters.AddWithValue("guid", reader.GetGuid(2));
                                    cmd2.ExecuteNonQuery();
                                    clearedCount++;
                                }
                            }
                        }
                    }
                }
                finally
                {
                    reader.Close();
                }

                Log.Info("Debris older than {0} minutes cleared from universe database, {1} vessels affected.", minsToKeep, clearedCount);
            }
            catch (Exception e)
            {
                Log.Info("Universe cleanup failed! {0} {1}", e.Message, e.StackTrace);
            }
        }



        private void listenForClients()
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("en-GB");
            try
            {
                Log.Info("Listening for clients...");
                tcpListener.Start(4);
                
                while (true)
                {

                    TcpClient client = null;
                    String error_message = String.Empty;

                    try
                    {
                        if (tcpListener.Pending())
                        {
                            client = tcpListener.AcceptTcpClient(); //Accept a TCP client
                            client.NoDelay = true;
                            Log.Info("New client...");
                        }
                    }
                    catch (System.Net.Sockets.SocketException e)
                    {
                        if (client != null)
                            client.Close();
                        client = null;
                        error_message = e.ToString();
                    }

                    if (client != null && client.Connected)
                    {
                        Log.Info("Client TCP connection established...");
                        //Try to add the client
                        Client cl = addClient(client);
                        if (cl != null)
                        {
                            if (cl.isValid)
                            {
                                //Send a handshake to the client
                                Log.Info("Accepted client from {0}. Handshaking...", client.Client.RemoteEndPoint.ToString());
                                sendHandshakeMessage(cl);

                                sendMessageDirect(client, KMPCommon.ServerMessageID.NULL, null);

                                //Send the join message to the client
                                if (settings.joinMessage.Length > 0)
                                    sendServerMessage(cl, settings.joinMessage);
                            }
                            else
                            {
                                Log.Info("Client attempted to connect, but connection was lost.");
                            }

                            //Send a server setting update to all clients
                            sendServerSettingsToAll();
                        }
                        else
                        {
                            //Client array is full
                            Log.Info("Client attempted to connect, but server is full.");
                            sendHandshakeRefusalMessageDirect(client, "Server is currently full");
                            client.Close();
                        }
                    }
                    else
                    {
                        if (client != null)
                            client.Close();
                        client = null;
                    }

                    if (client == null && error_message.Length > 0)
                    {
                        //There was an error accepting the client
                        Log.Error("Error accepting client: ");
                        Log.Error(error_message);
                    }

                    Thread.Sleep(SLEEP_TIME);

                }
            }
            catch (ThreadAbortException)
            {
            }
            catch (Exception e)
            {
                passExceptionToMain(e);
            }
        }

        private void handleConnections()
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("en-GB");
            try
            {
                Log.Debug("Starting disconnect thread");

                while (true)
                {
                    //Handle received messages
                    while (clientMessageQueue.Count > 0)
                    {
                        ClientMessage message;

                        if (clientMessageQueue.TryDequeue(out message))
                            handleMessage(message.client, message.id, message.data);
                        else
                            break;
                    }

                    List<Client> disconnectedClients = new List<Client>();
                    List<Client> markedClients = cleanupClients.ToList();
                    cleanupClients.Clear();
                    foreach (var client in clients.ToList().Where(c => !c.isValid || markedClients.Exists(mc => mc.clientIndex == c.clientIndex)))
                    {
                        //Client should be disconnected
                        disconnectClient(client, (String.IsNullOrEmpty(client.disconnectMessage)) ? "Connection lost" : client.disconnectMessage);
                        disconnectedClients.Add(client);
                    }
                    foreach (var client in disconnectedClients.ToList())
                    {
                        //Perform final cleanup
                        postDisconnectCleanup(client);
                    }

                    foreach (var client in clients.ToList().Where(c => c.isValid))
                    {
                        long last_receive_time = 0;
                        long connection_start_time = 0;
                        bool handshook = false;

                        lock (client.timestampLock)
                        {
                            last_receive_time = client.lastReceiveTime;
                            connection_start_time = client.connectionStartTime;
                            handshook = client.receivedHandshake;
                        }

                        if (currentMillisecond - last_receive_time > CLIENT_TIMEOUT_DELAY
                            || (!handshook && (currentMillisecond - connection_start_time) > CLIENT_HANDSHAKE_TIMEOUT_DELAY))
                        {
                            //Disconnect the client
                            markClientForDisconnect(client);
                        }
                        else
                        {
                            bool changed = false;

                            //Reset the client's activity level if the time since last update was too long
                            lock (client.activityLevelLock)
                            {
                                if (client.activityLevel == Client.ActivityLevel.IN_FLIGHT
                                    && (currentMillisecond - client.lastInFlightActivityTime) > ACTIVITY_RESET_DELAY)
                                {
                                    client.activityLevel = Client.ActivityLevel.IN_GAME;
                                    changed = true;
                                }

                                if (client.activityLevel == Client.ActivityLevel.IN_GAME
                                    && (currentMillisecond - client.lastInGameActivityTime) > ACTIVITY_RESET_DELAY)
                                {
                                    client.activityLevel = Client.ActivityLevel.INACTIVE;
                                    changed = true;
                                }
                            }

                            if (changed)
                                clientActivityLevelChanged(client);
                        }
                    }

                    Thread.Sleep(SLEEP_TIME);
                }
            }
            catch (ThreadAbortException)
            {
                Log.Debug("ThreadAbortException caught in handleConnections");
            }
            catch (Exception e)
            {
                passExceptionToMain(e);
            }

            Log.Debug("Ending disconnect thread.");
        }

        void sendOutgoingMessages()
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("en-GB");
            try
            {
                while (true)
                {
                    foreach (var client in clients.ToList().Where(c => c.isValid))
                    {
                        client.sendOutgoingMessages();
                    }

                    Thread.Sleep(SLEEP_TIME);
                }

            }
            catch (ThreadAbortException)
            {
            }
            catch (Exception e)
            {
                passExceptionToMain(e);
            }
        }

        //Clients

        private Client addClient(TcpClient tcp_client)
        {

            if (tcp_client == null || !tcp_client.Connected || activeClientCount() >= settings.maxClients)
                return null;
            Client newClient = new Client(this);
            newClient.tcpClient = tcp_client;
            newClient.resetProperties();
            newClient.startReceivingMessages();
            clients.Add(newClient);
            newClient.clientIndex = this.clientIndex++; //Assign unique clientIndex to each client
            return newClient;
        }

        public void disconnectClient(Client cl, String message)
        {
            try
            {
                //Send a message to client informing them why they were disconnected
                if (cl.tcpClient != null)
                {
                    if (cl.tcpClient.Connected)
                        sendConnectionEndMessageDirect(cl.tcpClient, message);

                    //Close the socket
                    lock (cl.tcpClientLock)
                    {
                        cl.endReceivingMessages();
                        cl.tcpClient.Close();
                    }
                }
				
				//Update the database
                if (cl.currentVessel != Guid.Empty)
                {
                    try
                    {
                        DbCommand cmd = universeDB.CreateCommand();
                        string sql = "UPDATE kmpVessel SET Active = 0 WHERE Guid = @guid";
                        cmd.CommandText = sql;
                        cmd.Parameters.AddWithValue("guid", cl.currentVessel);
                        cmd.ExecuteNonQuery();
                        cmd.Dispose();
                    }
                    catch { }
                    sendVesselStatusUpdateToAll(cl, cl.currentVessel);
                }

                bool emptySubspace = true;

                foreach (Client client in clients.ToList())
                {
                    if (cl.currentSubspaceID == client.currentSubspaceID && client.tcpClient.Connected && cl.playerID != client.playerID)
                    {
                        emptySubspace = false;
                        break;
                    }
                }

                if (emptySubspace)
                {
                    DbCommand cmd = universeDB.CreateCommand();
                    string sql = "DELETE FROM kmpSubspace WHERE ID = @id AND LastTick < (SELECT MIN(s.LastTick) FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID);";
                    cmd.CommandText = sql;
                    cmd.Parameters.AddWithValue("id", cl.currentSubspaceID.ToString("D"));
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                }
				
                //Only send the disconnect message if the client performed handshake successfully
                if (cl.receivedHandshake)
                {
                    Log.Info("Player #{0} {1} has disconnected: {2}", cl.playerID, cl.username, message);

                    //Send the disconnect message to all other clients
                    sendServerMessageToAll(string.Format("User {0} has disconnected : {1}", cl.username, message));

                    //backupDatabase();
                }
                else
                    Log.Info("Client failed to handshake successfully: {0}", message);
            }
            catch (NullReferenceException e)
            {
                //Almost certainly need to be smarter about this.
                cl.tcpClient = null;

                Log.Info("Internal error during disconnect: {0}", e.StackTrace);
            }
            catch (InvalidOperationException)
            {
                cl.tcpClient = null;
            }

            cl.receivedHandshake = false;

            if (cl.activityLevel != Client.ActivityLevel.INACTIVE)
                clientActivityLevelChanged(cl);
            else
                sendServerSettingsToAll();

            cl.disconnected();
        }

        public void markClientForDisconnect(Client client, string message = "Connection Lost")
        {
            if (clients.Contains(client))
            {
                Log.Debug("Client " + client.username + " added to disconnect list: " + message);
                client.disconnectMessage = message;
                cleanupClients.Add(client);
            }
        }

        public void postDisconnectCleanup(Client client)
        {
            if (clients.Contains(client)) clients.Remove(client);
            if (flight_clients.Contains(client)) flight_clients.Remove(client);
            client = null;
            if (activeClientCount() > 0) backedUpSinceEmpty = false;
        }

        public void clientActivityLevelChanged(Client cl)
        {
            Log.Activity(cl.username + " activity level is now " + cl.activityLevel);

            switch (cl.activityLevel)
            {
                case Client.ActivityLevel.IN_GAME:
					HandleActivityUpdateInGame(cl);
                    break;

                case Client.ActivityLevel.IN_FLIGHT:
                    HandleActivityUpdateInFlight(cl);
                    break;
            }

            sendServerSettingsToAll();
        }

        private void asyncUDPReceive(IAsyncResult result)
        {
            try
            {
                if (settings.ipBinding == "0.0.0.0" && settings.hostIPv6 == true) {
                    settings.ipBinding = "::";
                }
                IPEndPoint endpoint = new IPEndPoint(IPAddress.Parse(settings.ipBinding), settings.port);
                if (udpClient == null) { return; }
                byte[] received = udpClient.EndReceive(result, ref endpoint);
                if (received.Length >= KMPCommon.MSG_HEADER_LENGTH + 4)
                {
                    int index = 0;

                    //Get the sender index
                    int sender_index = KMPCommon.intFromBytes(received, index);
                    index += 4;

                    //Get the message header data
                    KMPCommon.ClientMessageID id = (KMPCommon.ClientMessageID)KMPCommon.intFromBytes(received, index);
                    index += 4;

                    int data_length = KMPCommon.intFromBytes(received, index);
                    index += 4;

                    //Get the data
                    byte[] data = null;

                    if (data_length > 0 && data_length <= received.Length - index)
                    {
                        data = new byte[data_length];
                        Array.Copy(received, index, data, 0, data.Length);
                    }

                    Client client = clients.Where(c => c.isReady && c.clientIndex == sender_index).FirstOrDefault();
                    if (client != null)
                    {
                        if ((currentMillisecond - client.lastUDPACKTime) > UDP_ACK_THROTTLE)
                        {
                            //Acknowledge the client's message with a TCP message
                            client.queueOutgoingMessage(KMPCommon.ServerMessageID.UDP_ACKNOWLEDGE, null);
                            client.lastUDPACKTime = currentMillisecond;
                        }

                        //Handle the message
						if (data == null)
						{
							handleMessage(client, id, data);
						}
						else
						{
                    		byte[] messageData = KMPCommon.Decompress(data);
                    		if (messageData != null) handleMessage(client, id, messageData);
							//Consider adding re-request here
						}
                    }

                }

                udpClient.BeginReceive(asyncUDPReceive, null); //Begin receiving the next message

            }
            catch (ThreadAbortException)
            {
            }
            catch (Exception e)
            {
                passExceptionToMain(e);
            }
        }

        private Client getClientByName(String name)
        {
            return clients.Where(c => c.isReady && c.username.Equals(name, StringComparison.InvariantCultureIgnoreCase)).FirstOrDefault();
        }

        //HTTP

        private void asyncHTTPCallback(IAsyncResult result)
        {
            try
            {
                HttpListener listener = (HttpListener)result.AsyncState;

                HttpListenerContext context = listener.EndGetContext(result);
                //HttpListenerRequest request = context.Request;

                HttpListenerResponse response = context.Response;

                //Build response string
                StringBuilder response_builder = new StringBuilder();

                response_builder.Append("Version: ");
                response_builder.Append(KMPCommon.PROGRAM_VERSION);
                response_builder.Append('\n');

                response_builder.Append("Port: ");
                response_builder.Append(settings.port);
                response_builder.Append('\n');

                response_builder.Append("Num Players: ");
                response_builder.Append(activeClientCount());
                response_builder.Append('/');
                response_builder.Append(settings.maxClients);
                response_builder.Append('\n');

                response_builder.Append("Players: ");

                bool first = true;

                foreach (var client in clients.ToList().Where(c => c.isReady))
                {
                    if (first)
                        first = false;
                    else
                        response_builder.Append(", ");

                    response_builder.Append(client.username);
                }

                response_builder.Append('\n');

                response_builder.Append("Information: ");
                response_builder.Append(settings.serverInfo);
                response_builder.Append('\n');

                response_builder.Append("Updates per Second: ");
                response_builder.Append(settings.updatesPerSecond);
                response_builder.Append('\n');

                response_builder.Append("Inactive Ship Limit: ");
                response_builder.Append(settings.totalInactiveShips);
                response_builder.Append('\n');

                response_builder.Append("Screenshot Height: ");
                response_builder.Append(settings.screenshotSettings.maxHeight);
                response_builder.Append('\n');

                response_builder.Append("Screenshot Save: ");
                response_builder.Append(settings.saveScreenshots);
                response_builder.Append('\n');

                response_builder.Append("Whitelisted: ");
                response_builder.Append(settings.whitelisted);
                response_builder.Append('\n');



                //Send response
                byte[] buffer = System.Text.Encoding.UTF8.GetBytes(response_builder.ToString());
                response.ContentLength64 = buffer.LongLength;
                response.OutputStream.Write(buffer, 0, buffer.Length);
                response.OutputStream.Close();

                //Begin listening for the next http request
                listener.BeginGetContext(asyncHTTPCallback, listener);

            }
            catch (ThreadAbortException)
            {
            }
            catch (Exception e)
            {
                passExceptionToMain(e);
            }
        }

        //Messages

        public void queueClientMessage(Client cl, KMPCommon.ClientMessageID id, byte[] data)
        {
            ClientMessage message = new ClientMessage();
            message.client = cl;
            message.id = id;
            message.data = data;

            clientMessageQueue.Enqueue(message);
        }

        private KMPCommon.ClientMessageID[] AllowNullDataMessages = { KMPCommon.ClientMessageID.SCREEN_WATCH_PLAYER, KMPCommon.ClientMessageID.CONNECTION_END, KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_FLIGHT, KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_GAME, KMPCommon.ClientMessageID.PING };
        private KMPCommon.ClientMessageID[] AllowClientNotReadyMessages = { KMPCommon.ClientMessageID.HANDSHAKE, KMPCommon.ClientMessageID.TEXT_MESSAGE, KMPCommon.ClientMessageID.SCREENSHOT_SHARE, KMPCommon.ClientMessageID.CONNECTION_END, KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_FLIGHT, KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_GAME, KMPCommon.ClientMessageID.PING, KMPCommon.ClientMessageID.UDP_PROBE, KMPCommon.ClientMessageID.WARPING, KMPCommon.ClientMessageID.SSYNC };

        public void handleMessage(Client cl, KMPCommon.ClientMessageID id, byte[] data)
        {
			lock (databaseVacuumLock)
			{
	            if (!cl.isValid)
	            { return; }
	
	            if (!AllowNullDataMessages.Contains(id) && data == null) { return; }
	            if (!AllowClientNotReadyMessages.Contains(id) && !cl.isReady) { return; }
	
	            try
	            {
	                //Log.Info("Message id: " + id.ToString() + " from client: " + cl + " data: " + (data != null ? data.Length.ToString() : "0"));
	
	                UnicodeEncoding encoder = new UnicodeEncoding();
	
	                switch (id)
	                {
	                    case KMPCommon.ClientMessageID.HANDSHAKE:
	                        HandleHandshake(cl, data, encoder);
	                        break;
	                    case KMPCommon.ClientMessageID.PRIMARY_PLUGIN_UPDATE:
	                    case KMPCommon.ClientMessageID.SECONDARY_PLUGIN_UPDATE:
	                        HandlePluginUpdate(cl, id, data);
	                        break;
						case KMPCommon.ClientMessageID.SCENARIO_UPDATE:
	                        HandleScenarioUpdate(cl, data);
	                        break;
	                    case KMPCommon.ClientMessageID.TEXT_MESSAGE:
	                        handleClientTextMessage(cl, encoder.GetString(data, 0, data.Length));
	                        break;
	                    case KMPCommon.ClientMessageID.SCREEN_WATCH_PLAYER:
	                        HandleScreenWatchPlayer(cl, data, encoder);
	                        break;
	                    case KMPCommon.ClientMessageID.SCREENSHOT_SHARE:
	                        HandleScreenshotShare(cl, data);
	                        break;
	                    case KMPCommon.ClientMessageID.CONNECTION_END:
	                        HandleConnectionEnd(cl, data, encoder);
	                        break;
	                    case KMPCommon.ClientMessageID.SHARE_CRAFT_FILE:
	                        HandleShareCraftFile(cl, data, encoder);
	                        break;
	                    case KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_FLIGHT:
	                        HandleActivityUpdateInFlight(cl);
	                        break;
	                    case KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_GAME:
	                        HandleActivityUpdateInGame(cl);
	                        break;
	                    case KMPCommon.ClientMessageID.PING:
	                        cl.queueOutgoingMessage(KMPCommon.ServerMessageID.PING_REPLY, null);
	                        break;
	                    case KMPCommon.ClientMessageID.UDP_PROBE:
	                        HandleUDPProbe(cl, data);
	                        break;
	                    case KMPCommon.ClientMessageID.WARPING:
	                        HandleWarping(cl, data);
	                        break;
	                    case KMPCommon.ClientMessageID.SSYNC:
	                        HandleSSync(cl, data);
	                        break;
	                }
	            }
	            catch (NullReferenceException)
	            {
	
	            }
			}
        }

        private void HandleSSync(Client cl, byte[] data)
        {
            int subspaceID = KMPCommon.intFromBytes(data, 0);
            if (subspaceID == -1)
            {
                //Latest available subspace sync request    
                DbCommand cmd = universeDB.CreateCommand();
                string sql = "SELECT ss1.ID FROM kmpSubspace ss1 LEFT JOIN kmpSubspace ss2 ON ss1.LastTick < ss2.LastTick WHERE ss2.ID IS NULL;";
                cmd.CommandText = sql;
                DbDataReader reader = cmd.ExecuteReader();
                try
                {
                    while (reader.Read())
                    {
                        subspaceID = reader.GetInt32(0);
                    }
                }
                finally
                {
                    reader.Close();
                }
            }
            cl.currentSubspaceID = subspaceID;
            Log.Info("{0} sync request to subspace {1}", cl.username, subspaceID);
            sendSubspace(cl, true);
        }

        private void HandleWarping(Client cl, byte[] data)
        {
            float rate = BitConverter.ToSingle(data, 0);
            if (cl.warping)
            {
                if (rate < 1.1f)
                {
                    //stopped warping-create subspace & add player to it
                    DbCommand cmd = universeDB.CreateCommand();
                    string sql = "INSERT INTO kmpSubspace (LastTick) VALUES (@tick);";
                    cmd.CommandText = sql;
                    cmd.Parameters.AddWithValue("tick", 0d);
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    cmd = universeDB.CreateCommand();
                    sql = "SELECT last_insert_rowid();";
                    cmd.CommandText = sql;
                    DbDataReader reader = cmd.ExecuteReader();
                    int newSubspace = -1;
                    try
                    {
                        while (reader.Read())
                        {
                            newSubspace = reader.GetInt32(0);
                        }
                    }
                    finally
                    {
                        reader.Close();
                        cmd.Dispose();
                    }
					
                    cl.currentSubspaceID = newSubspace;
					cl.warping = false;
                    sendSubspace(cl, false, false);
					cl.lastTick = -1d;
                    Log.Activity("{0} set to new subspace {1}", cl.username, newSubspace);
                }
            }
            else
            {
                if (rate > 1.1f)
                {
                    cl.warping = true;
                    cl.currentSubspaceID = -1;
                    Log.Activity("{0} is warping", cl.username);
                }
            }
        }

        private void HandleUDPProbe(Client cl, byte[] data)
        {
            double incomingTick = BitConverter.ToDouble(data, 0);
            double lastSubspaceTick = incomingTick;

            cl.lastTick = incomingTick;
            if (!cl.warping)
            {
                DbCommand cmd = universeDB.CreateCommand();
                string sql = "SELECT LastTick FROM kmpSubspace WHERE ID = @id;";
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("id", cl.currentSubspaceID.ToString("D"));
                DbDataReader reader = cmd.ExecuteReader();

                try
                {
                    while (reader.Read())
                    {
                        lastSubspaceTick = reader.GetDouble(0);
                    }
                }
                finally
                {
                    reader.Close();
                    cmd.Dispose();
                }

                if (lastSubspaceTick - incomingTick > 0.2d)
                {
                    cl.syncOffset += 0.001d;
                    if (cl.syncOffset > 0.5d) cl.syncOffset = 0.5;
                    if (cl.receivedHandshake && cl.lastSyncTime < (currentMillisecond - 2500L))
                    {
                        Log.Debug("Sending time-sync to {0} current offset {1}", cl.username, cl.syncOffset);
                        if (cl.lagWarning > 24)
                        {
                            cl.lastSyncTime = currentMillisecond;
                            markClientForDisconnect(cl, "Your game was running too slowly compared to other players. Please try reconnecting in a moment.");
                        }
                        else
                        {
                            sendSyncMessage(cl, lastSubspaceTick + cl.syncOffset);
                            cl.lastSyncTime = currentMillisecond;
                            cl.lagWarning++;
                        }
                    }
                }
                else
                {
                    cl.lagWarning = 0;
                    if (cl.syncOffset > 0.01d) cl.syncOffset -= 0.001d;
                    cmd = universeDB.CreateCommand();
                    sql = "UPDATE kmpSubspace SET LastTick = @tick WHERE ID = @subspaceID AND LastTick < @tick;";
                    cmd.Parameters.AddWithValue("tick", incomingTick.ToString("0.0").Replace(",", "."));
                    cmd.Parameters.AddWithValue("subspaceID", cl.currentSubspaceID.ToString("D"));
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    sendHistoricalVesselUpdates(cl.currentSubspaceID, incomingTick, lastSubspaceTick);
                }
            }
        }

        private void HandleActivityUpdateInGame(Client cl)
        {
			if (flight_clients.Contains(cl)) flight_clients.Remove(cl);
            if (cl.activityLevel == Client.ActivityLevel.INACTIVE) sendServerSync(cl);
            if (cl.activityLevel == Client.ActivityLevel.IN_FLIGHT && cl.currentVessel != Guid.Empty)
            {
                try
                {
                    DbCommand cmd = universeDB.CreateCommand();
                    string sql = "UPDATE kmpVessel SET Active = 0 WHERE Guid = @id";
                    cmd.CommandText = sql;
                    cmd.Parameters.AddWithValue("id", cl.currentVessel);
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                }
                catch { }
                sendVesselStatusUpdateToAll(cl, cl.currentVessel);
            }
            cl.updateActivityLevel(Client.ActivityLevel.IN_GAME);
        }

        private void HandleActivityUpdateInFlight(Client cl)
        {
			if (!flight_clients.Contains(cl)) flight_clients.Add(cl);
            cl.updateActivityLevel(Client.ActivityLevel.IN_FLIGHT);
        }

        private void HandleShareCraftFile(Client cl, byte[] data, UnicodeEncoding encoder)
        {
            if (!(data.Length > 8 && (data.Length - 8) <= KMPCommon.MAX_CRAFT_FILE_BYTES)) { return; }

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

                lock (cl.sharedCraftLock)
                {
                    cl.sharedCraftName = craft_name;
                    cl.sharedCraftFile = craft_bytes;
                    cl.sharedCraftType = craft_type;
                }

                //Send a message to players informing them that a craft has been shared
                StringBuilder sb = new StringBuilder();
                sb.Append(cl.username);
                sb.Append(" shared ");
                sb.Append(craft_name);

                switch (craft_type)
                {
                    case KMPCommon.CraftType.VAB:
                        sb.Append(" (VAB)");
                        break;

                    case KMPCommon.CraftType.SPH:
                        sb.Append(" (SPH)");
                        break;
                    case KMPCommon.CraftType.SUBASSEMBLY:
                        sb.Append(" (Subassembly)");
                        break;
                }

                Log.Info(sb.ToString());

                sb.Append(" . Enter !getcraft ");
                sb.Append(cl.username);
                sb.Append(" to get it.");
                sendTextMessageToAll(sb.ToString());
            }
        }

        private void HandleConnectionEnd(Client cl, byte[] data, UnicodeEncoding encoder)
        {
            String message = String.Empty;
            if (data != null)
                message = encoder.GetString(data, 0, data.Length); //Decode the message

            markClientForDisconnect(cl, message); //Disconnect the client
        }

        private void HandleScreenshotShare(Client cl, byte[] data)
        {
            if (data.Length > settings.screenshotSettings.maxNumBytes) { return; }

            //Set the screenshot for the player
            lock (cl.screenshotLock)
            {
                cl.screenshot = data;
            }

            StringBuilder sb = new StringBuilder();
            sb.Append(cl.username);
            sb.Append(" has shared a screenshot.");

            sendTextMessageToAll(sb.ToString());
            Log.Info(sb.ToString());

            //Send the screenshot to every client watching the player
            sendScreenshotToWatchers(cl, data);

            if (settings.saveScreenshots)
                saveScreenshot(data, cl.username);
        }

        private void HandleScreenWatchPlayer(Client cl, byte[] data, UnicodeEncoding encoder)
        {
            String watch_name = String.Empty;

            if (data != null)
                watch_name = encoder.GetString(data);

            bool watch_name_changed = false;

            lock (cl.watchPlayerNameLock)
            {
                if (watch_name != cl.watchPlayerName)
                {
                    //Set the watch player name
                    cl.watchPlayerName = watch_name;
                    watch_name_changed = true;
                }
            }

            if (watch_name_changed && watch_name.Length > 0
                && watch_name != cl.username)
            {
                //Try to find the player the client is watching and send that player's current screenshot
                Client watch_client = getClientByName(watch_name);
                if (watch_client.isReady)
                {
                    byte[] screenshot = null;
                    lock (watch_client.screenshotLock)
                    {
                        screenshot = watch_client.screenshot;
                    }

                    if (screenshot != null)
                        sendScreenshot(cl, watch_client.screenshot);
                }
            }
        }

        private void HandlePluginUpdate(Client cl, KMPCommon.ClientMessageID id, byte[] data)
        {
            if (cl.isReady)
            {
                sendPluginUpdateToAll(data, id == KMPCommon.ClientMessageID.SECONDARY_PLUGIN_UPDATE, cl);
            }
        }
		
		private void HandleScenarioUpdate(Client cl, byte[] data)
        {
            if (cl.isReady)
            {
				var scenario_update = ByteArrayToObject<KMPScenarioUpdate>(data);

                if (scenario_update != null)
                {
					Log.Activity("Received scenario update from {0}", cl.username);
                    DbCommand cmd = universeDB.CreateCommand();
                    string sql = "SELECT ID FROM kmpScenarios WHERE PlayerID = @playerID AND Name = @name;";
                    cmd.CommandText = sql;
                    cmd.Parameters.AddWithValue("playerID", cl.playerID);
					cmd.Parameters.AddWithValue("name", scenario_update.name);
                    object result = cmd.ExecuteScalar();
                    cmd.Dispose();
                    if (result == null)
                    {
                        cmd = universeDB.CreateCommand();
                        sql = "INSERT INTO kmpScenarios (PlayerID, Name, Tick, UpdateMessage)" +
                            " VALUES (@playerID, @name, @tick, @updateMessage);";
                        cmd.Parameters.AddWithValue("playerID", cl.playerID);
                        cmd.Parameters.AddWithValue("name", scenario_update.name);
                        cmd.Parameters.AddWithValue("tick", scenario_update.tick.ToString("0.0").Replace(",", "."));
                        cmd.Parameters.AddWithValue("updateMessage", data);
                        cmd.CommandText = sql;
                        cmd.ExecuteNonQuery();
                        cmd.Dispose();
                    }
					else
					{
						cmd = universeDB.CreateCommand();
                        sql = "UPDATE kmpScenarios SET Tick = @tick, UpdateMessage = @updateMessage WHERE ID = @id";
                        cmd.Parameters.AddWithValue("id", Convert.ToInt32(result));
                        cmd.Parameters.AddWithValue("tick", scenario_update.tick.ToString("0.0").Replace(",", "."));
                        cmd.Parameters.AddWithValue("updateMessage", data);
                        cmd.CommandText = sql;
                        cmd.ExecuteNonQuery();
                        cmd.Dispose();
					}
				}
            }
        }

        private void HandleHandshake(Client cl, byte[] data, UnicodeEncoding encoder)
        {
            StringBuilder sb = new StringBuilder();

            //Read username
            Int32 username_length = KMPCommon.intFromBytes(data, 0);
            String username = encoder.GetString(data, 4, username_length);

			Guid guid = Guid.Empty;
            Int32 guid_length = KMPCommon.intFromBytes(data, 4 + username_length);
            int offset = 4 + username_length + 4;
			try
			{
            	guid = new Guid(encoder.GetString(data, offset, guid_length));
			}
			catch
			{
				markClientForDisconnect(cl, "You're authentication token is not valid.");
				Log.Info("Rejected client due to invalid guid: {0}", encoder.GetString(data, offset, guid_length));
			}
            offset = 4 + username_length + 4 + guid_length;
            String version = encoder.GetString(data, offset, data.Length - offset);

            String username_lower = username.ToLower();

            bool accepted = true;

            //Ensure no other players have the same username.
            if (clients.Any(c => c.isReady && c.username.ToLower() == username_lower))
            {
                markClientForDisconnect(cl, "Your username is already in use.");
                Log.Info("Rejected client due to duplicate username: {0}", username);
                accepted = false;
            }

            //If whitelisting is enabled and the user is *not* on the list:
            if (settings.whitelisted && settings.whitelist.Contains(username, StringComparer.InvariantCultureIgnoreCase) == false)
            {
                markClientForDisconnect(cl, "You are not on this servers whitelist.");
                Log.Info("Rejected client due to not being on the whitelist: {0}", username);
                accepted = false;
            }

            //Check if banned
            if (settings.bans.Any(b => b.BannedGUID == guid || b.BannedName == username || b.BannedIP == cl.IPAddress))
            {
                markClientForDisconnect(cl, "You are banned from this server.");
                Log.Info("Rejected client due to being banned: {0}", username);
                accepted = false;
            }
			
            if (!accepted)
            return;

            //Check if this player is new to universe
            DbCommand cmd = universeDB.CreateCommand();
            string sql = "SELECT COUNT(*) FROM kmpPlayer WHERE Name = @username AND Guid != @guid;";
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("username", username_lower);
            cmd.Parameters.AddWithValue("guid", guid);
            Int32 name_taken = Convert.ToInt32(cmd.ExecuteScalar());
            cmd.Dispose();
            if (name_taken > 0)
            {
                //Disconnect the player
                markClientForDisconnect(cl, "Your username is already claimed by an existing user.");
                Log.Info("Rejected client due to duplicate username w/o matching guid: {0}", username);
                //return;
            }
            cmd = universeDB.CreateCommand();
            sql = "SELECT COUNT(*) FROM kmpPlayer WHERE Guid = @guid AND Name LIKE @username";
            cmd.CommandText = sql;
			cmd.Parameters.AddWithValue("username", username_lower);
            cmd.Parameters.AddWithValue("guid", guid);
            Int32 player_exists = Convert.ToInt32(cmd.ExecuteScalar());
            cmd.Dispose();
            if (player_exists == 0) //New user
            {
				Log.Info("New user");
                cmd = universeDB.CreateCommand();
                sql = "INSERT INTO kmpPlayer (Name, Guid) VALUES (@username,@guid);";
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("username", username_lower);
                cmd.Parameters.AddWithValue("guid", guid);
                cmd.ExecuteNonQuery();
                cmd.Dispose();
            }
            cmd = universeDB.CreateCommand();
            sql = "SELECT ID FROM kmpPlayer WHERE Guid = @guid AND Name LIKE @username;";
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("username", username_lower);
            cmd.Parameters.AddWithValue("guid", guid);
            Int32 playerID = Convert.ToInt32(cmd.ExecuteScalar());
            cmd.Dispose();

            //Send the active user count to the client
            if (activeClientCount() == 1)
            {
                //Get the username of the other user on the server
                sb.Append("There is currently 1 other user on this server: ");

                foreach (var client in clients.ToList().Where(c => c.isReady && c != cl))
                {
                    sb.Append(client.username);
                    //return;
                }
            }
            else
            {
                sb.Append("There are currently ");
                sb.Append(activeClientCount());
                sb.Append(" other users on this server.");
                if (activeClientCount() > 1)
                {
                    sb.Append(" Enter !list to see them.");
                }
            }
			
			//Check if server has filled up while waiting for handshake
			if (activeClientCount() >= settings.maxClients)
			{
				markClientForDisconnect(cl, "The server is full.");
				Log.Info("Rejected client, server is full.");
			}
			else
			{
				//Server isn't full, accept client
	            cl.username = username;
	            cl.receivedHandshake = true;
	            cl.guid = guid;
	            cl.playerID = playerID;
	
	            sendServerMessage(cl, sb.ToString());
	            sendServerSettings(cl);
	
				//Send the MOTD
				sb.Remove(0, sb.Length);
				sb.Append(settings.serverMotd);
				sendMotdMessage(cl, sb.ToString());
	
	            Log.Info("{0} (#{2}) has joined the server using client version {1}", username, version, playerID);
	
	            //Build join message
	            //sb.Clear();
	            sb.Remove(0, sb.Length);
	            sb.Append("User ");
	            sb.Append(username);
	            sb.Append(" has joined the server.");
	
	            //Send the join message to all other clients
	            sendServerMessageToAll(sb.ToString(), cl);
			}

			
        }

        private void sendHistoricalVesselUpdates(int toSubspace, double atTick, double lastTick)
        {
            DbCommand cmd = universeDB.CreateCommand();
            string sql = "SELECT  vu.UpdateMessage, v.Private" +
                " FROM kmpVesselUpdateHistory vu" +
                " INNER JOIN kmpVessel v ON v.Guid = vu.Guid" +
                " INNER JOIN (SELECT Guid, MAX(Tick) Tick" +
                "   FROM kmpVesselUpdateHistory" +
                "   WHERE Tick > @lastTick AND Tick < @atTick" +
                "   GROUP BY Guid) t ON t.Guid = vu.Guid AND t.Tick = vu.Tick" +
                " WHERE vu.Subspace != @toSubspace;";
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("lastTick", lastTick.ToString("0.0").Replace(",", "."));
            cmd.Parameters.AddWithValue("atTick", atTick.ToString("0.0").Replace(",", "."));
            cmd.Parameters.AddWithValue("toSubspace", toSubspace);
            DbDataReader reader = cmd.ExecuteReader();
            try
            {
                while (reader.Read())
                {
                    KMPVesselUpdate vessel_update = (KMPVesselUpdate)ByteArrayToObject(GetDataReaderBytes(reader, 0));
                    vessel_update.state = State.ACTIVE;
                    vessel_update.isPrivate = reader.GetBoolean(1);
                    vessel_update.isMine = false;
                    vessel_update.relTime = RelativeTime.FUTURE;
                    byte[] update = ObjectToByteArray(vessel_update);

                    foreach (var client in clients.ToList().Where(c => c.currentSubspaceID == toSubspace && !c.warping && c.currentVessel != vessel_update.kmpID))
                    {
                        sendVesselMessage(client, update);
                    }
                }
            }
            finally
            {
                reader.Close();
            }
            cmd.Dispose();
            cmd = universeDB.CreateCommand();
            sql = "DELETE FROM kmpVesselUpdateHistory WHERE Tick < (SELECT MIN(LastTick) FROM kmpSubspace);";
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
            cmd.Dispose();
        }

        private void sendSubspace(Client cl, bool excludeOwnActive = false, bool sendTimeSync = true)
        {
            if (!cl.warping)
            {
                if (sendTimeSync) sendSubspaceSync(cl);
                Log.Activity("Sending all vessels in current subspace for " + cl.username);
                DbCommand cmd = universeDB.CreateCommand();
                string sql = "SELECT  vu.UpdateMessage, v.ProtoVessel, v.Private, v.OwnerID" +
                    " FROM kmpVesselUpdate vu" +
                    " INNER JOIN kmpVessel v ON v.Guid = vu.Guid AND v.Destroyed != 1" +
                    " INNER JOIN kmpSubspace s ON s.ID = vu.Subspace" +
                    " INNER JOIN" +
                    "  (SELECT vu.Guid, MAX(s.LastTick) AS LastTick" +
                    "  FROM kmpVesselUpdate vu" +
                    "  INNER JOIN kmpSubspace s ON s.ID = vu.Subspace AND s.LastTick <= (SELECT LastTick FROM kmpSubspace WHERE ID = @curSubspaceID)" +
                    "  GROUP BY vu.Guid) t ON t.Guid = vu.Guid AND t.LastTick = s.LastTick";
                if (excludeOwnActive) sql += " AND NOT v.Guid = @curVessel";
                sql += ";";
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("curSubspaceID", cl.currentSubspaceID.ToString("D"));
                if (excludeOwnActive) cmd.Parameters.AddWithValue("curVessel", cl.currentVessel);
                DbDataReader reader = cmd.ExecuteReader();
                try
                {
                    while (reader.Read())
                    {
                        KMPVesselUpdate vessel_update = (KMPVesselUpdate)ByteArrayToObject(GetDataReaderBytes(reader, 0));
                        ConfigNode protoVessel = (ConfigNode)ByteArrayToObject(GetDataReaderBytes(reader, 1));
                        vessel_update.state = State.INACTIVE;
                        vessel_update.isPrivate = reader.GetBoolean(2);
                        vessel_update.isMine = reader.GetInt32(3) == cl.playerID;
                        vessel_update.setProtoVessel(protoVessel);
                        vessel_update.isSyncOnlyUpdate = true;
                        vessel_update.distance = 0;
                        byte[] update = ObjectToByteArray(vessel_update);
                        sendVesselMessage(cl, update);
                    }
                }
                finally
                {
                    reader.Close();
                }
                sendSyncCompleteMessage(cl);
            }
        }

        private void sendSubspaceSync(Client cl, bool sendSync = true)
        {
            SQLiteCommand cmd = universeDB.CreateCommand();
            string sql = "SELECT LastTick FROM kmpSubspace WHERE ID = @curSubspaceID;";
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("curSubspaceID", cl.currentSubspaceID.ToString("D"));
            DbDataReader reader = cmd.ExecuteReader();
            double tick = 0d;
            try
            {
                while (reader.Read())
                {
                    tick = reader.GetDouble(0);
                }
            }
            finally
            {
                reader.Close();
            }
            if (sendSync)
			{
				sendSyncMessage(cl, tick);
				cl.lastTick = tick;
				sendScenarios(cl);
			}
        }

        private void sendServerSync(Client cl)
        {
            if (!cl.warping)
            {
                DbCommand cmd = universeDB.CreateCommand();
                string sql = "SELECT ss1.ID, ss1.LastTick FROM kmpSubspace ss1 LEFT JOIN kmpSubspace ss2 ON ss1.LastTick < ss2.LastTick WHERE ss2.ID IS NULL;";
                cmd.CommandText = sql;
                DbDataReader reader = cmd.ExecuteReader();
                double tick = 0d; int subspace = 0;
                try
                {
                    while (reader.Read())
                    {
                        subspace = reader.GetInt32(0);
                        tick = reader.GetDouble(1);
                    }
                }
                finally
                {
                    reader.Close();
                }
                cl.currentSubspaceID = subspace;
                Log.Activity(cl.username + " set to lead subspace " + subspace);
                sendSyncMessage(cl, tick);
            }
        }

        public void handleClientTextMessage(Client cl, String message_text)
        {
            try
            {
                StringBuilder sb = new StringBuilder();

                if (message_text.Length > 0 && message_text.First() == '!')
                {
                    string message_lower = message_text.ToLower();

                    if (message_lower == "!list")
                    {
                        //Compile list of usernames
                        sb.Append("Connected users:\n");

                        foreach (var client in clients.ToList().Where(c => c.isReady))
                        {
                            sb.Append(client.username);
                            sb.Append('\n');
                        }

                        sendTextMessage(cl, sb.ToString());
                        return;
                    }
                    else if (message_lower == "!help")
                    {
                        sb.Append("Available Chat Commands:\n");
                        sb.Append("!help - Displays this message\n");
                        sb.Append("!list - View all connected players\n");
                        sb.Append("!quit - Leaves the server\n");
                        sb.Append(KMPCommon.SHARE_CRAFT_COMMAND + " <craftname> - Shares the craft of name <craftname> with all other players\n");
                        sb.Append(KMPCommon.GET_CRAFT_COMMAND + " <playername> - Gets the most recent craft shared by the specified player\n");
                        sb.Append("!motd - Displays Server MOTD\n");
                        sb.Append("!rules - Displays Server Rules\n");
						sb.Append("!bubble - Displays server bubble size, and how far you are from its borders\n");
                        sb.Append("!chat - Various commands to manipulate the chat window\n");
                        sb.Append("!chat dragwindow <true|false> - Makes the chat draggable\n");
                        sb.Append("!chat offsetting <true|false> - Turn on/off the tracking center and editor offsets\n");
                        sb.Append("!chat offset [tracking|editor] [offsetX] [offsetY] - Set the offset values (pixels)\n");
                        sb.Append("!chat [width|height|top|left] [value] <percent|pixels>\n");
                        sb.Append("!whereami - Displays server connection information\n");
						if(isAdmin(cl.username)) {
							sb.Append(KMPCommon.RCON_COMMAND + " <cmd> - Execute command /<cmd> as if typed from server console\n");
						}
						sb.Append("!clear - Clears the chat window");
                        sb.Append(Environment.NewLine);

                        sendTextMessage(cl, sb.ToString());

                        return;
                    }
                    else if (message_lower == "!motd")
                    {
                        sb.Append(settings.serverMotd);
                        sendMotdMessage(cl, sb.ToString());
                        return;
                    }
                    else if (message_lower == "!rules")
                    {
                        sb.Append(settings.serverRules);
                        sendTextMessage(cl, sb.ToString());
                        return;
                    }
                    else if (message_lower.Length > (KMPCommon.GET_CRAFT_COMMAND.Length + 1)
                        && message_lower.Substring(0, KMPCommon.GET_CRAFT_COMMAND.Length) == KMPCommon.GET_CRAFT_COMMAND)
                    {
                        String player_name = message_lower.Substring(KMPCommon.GET_CRAFT_COMMAND.Length + 1);

                        //Find the player with the given name
                        Client target_client = getClientByName(player_name);

                        if (target_client.isReady)
                        {
                            //Send the client the craft data
                            lock (target_client.sharedCraftLock)
                            {
                                if (target_client.sharedCraftName.Length > 0
                                    && target_client.sharedCraftFile != null && target_client.sharedCraftFile.Length > 0)
                                {
                                    sendCraftFile(cl,
                                        target_client.sharedCraftName,
                                        target_client.sharedCraftFile,
                                        target_client.sharedCraftType);

                                    Log.Info("Sent craft {0} to client {1}", target_client.sharedCraftName, cl.username);
                                }
                            }
                        }

                        return;
                    }
                    else if (message_lower.Length > (KMPCommon.RCON_COMMAND.Length + 1)
                        && message_lower.Substring(0, KMPCommon.RCON_COMMAND.Length) == KMPCommon.RCON_COMMAND)
                    {
						if(isAdmin(cl.username)) {
							String command = message_lower.Substring(KMPCommon.RCON_COMMAND.Length + 1);
							Log.Info("RCON from client {0} (#{1}): {2}", cl.username, cl.clientIndex, command);
							processCommand("/"+command);
						} else {
							sendTextMessage(cl, "You are not an admin!");
						}

						return;
					}
                }

                if (settings.profanityFilter)
                    message_text = WashMouthWithSoap(message_text);

				string full_message = string.Format("{2}<{0}> {1}", cl.username, message_text, (isAdmin(cl.username) ? "["+KMPCommon.ADMIN_MARKER+"] " : ""));

                //Console.SetCursorPosition(0, Console.CursorTop);
                Log.Chat(cl.username, message_text);

                //Send the update to all other clients
                sendTextMessageToAll(full_message);
            }
            catch (NullReferenceException) { }
        }

        private string WashMouthWithSoap(string message_text)
        {
            var msg = message_text;

            foreach (var kvp in settings.Profanity)
            {
                msg = msg.Replace(kvp.Key, kvp.Value);
            }

            return msg;
        }

        public static byte[] buildMessageArray(KMPCommon.ServerMessageID id, byte[] data)
        {
            //Construct the byte array for the message
            byte[] compressed_data = null;
            int msg_data_length = 0;
            if (data != null)
            {
                compressed_data = KMPCommon.Compress(data);
                if (compressed_data == null)
                    compressed_data = KMPCommon.Compress(data, true);
                msg_data_length = compressed_data.Length;
            }

            byte[] message_bytes = new byte[KMPCommon.MSG_HEADER_LENGTH + msg_data_length];

            KMPCommon.intToBytes((int)id).CopyTo(message_bytes, 0);
            KMPCommon.intToBytes(msg_data_length).CopyTo(message_bytes, 4);
            if (compressed_data != null)
                compressed_data.CopyTo(message_bytes, KMPCommon.MSG_HEADER_LENGTH);

            return message_bytes;
        }

        private void sendMessageDirect(TcpClient client, KMPCommon.ServerMessageID id, byte[] data)
        {
            try
            {
                byte[] message_bytes = buildMessageArray(id, data);
                client.GetStream().Write(message_bytes, 0, message_bytes.Length);

                Log.Debug("Sending message: " + id.ToString());
            }
            catch { }
        }

        private void sendHandshakeRefusalMessageDirect(TcpClient client, String message)
        {
            try
            {
                //Encode message
                UnicodeEncoding encoder = new UnicodeEncoding();
                byte[] message_bytes = encoder.GetBytes(message);

                sendMessageDirect(client, KMPCommon.ServerMessageID.HANDSHAKE_REFUSAL, message_bytes);
            }
            catch (System.IO.IOException)
            {
            }
            catch (System.ObjectDisposedException)
            {
            }
            catch (System.InvalidOperationException)
            {
            }
        }

        private void sendConnectionEndMessageDirect(TcpClient client, String message)
        {
            try
            {
                //Encode message
                UnicodeEncoding encoder = new UnicodeEncoding();
                byte[] message_bytes = encoder.GetBytes(message);

                sendMessageDirect(client, KMPCommon.ServerMessageID.CONNECTION_END, message_bytes);
            }
            catch (System.IO.IOException)
            {
            }
            catch (System.ObjectDisposedException)
            {
            }
            catch (System.InvalidOperationException)
            {
            }
        }

        private void sendHandshakeMessage(Client cl)
        {
            //Encode version string
            UnicodeEncoding encoder = new UnicodeEncoding();
            


            byte[] version_bytes = encoder.GetBytes(KMPCommon.PROGRAM_VERSION);

            byte[] data_bytes = new byte[version_bytes.Length + 20 + kmpModControl.Length];

            //Write net protocol version
            KMPCommon.intToBytes(KMPCommon.NET_PROTOCOL_VERSION).CopyTo(data_bytes, 0);

            //Write version string length
            KMPCommon.intToBytes(version_bytes.Length).CopyTo(data_bytes, 4);

            //Write version string
            version_bytes.CopyTo(data_bytes, 8);

            //Write client ID
            KMPCommon.intToBytes(cl.clientIndex).CopyTo(data_bytes, 8 + version_bytes.Length);
            
			//Write gameMode
            KMPCommon.intToBytes(settings.gameMode).CopyTo(data_bytes, 12 + version_bytes.Length);
			
            KMPCommon.intToBytes(kmpModControl.Length).CopyTo(data_bytes, 16 + version_bytes.Length);
            kmpModControl.CopyTo(data_bytes, 20 + version_bytes.Length);
			
			byte[] piracySetting = new byte[1];
			piracySetting[0] = settings.allowPiracy ? (byte) 1 : (byte) 0;
			piracySetting.CopyTo(data_bytes, 20 + version_bytes.Length + kmpModControl.Length);

            cl.queueOutgoingMessage(KMPCommon.ServerMessageID.HANDSHAKE, data_bytes);
        }

        private void sendServerMessageToAll(String message, Client exclude = null)
        {
            UnicodeEncoding encoder = new UnicodeEncoding();
            byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.SERVER_MESSAGE, encoder.GetBytes(message));

            foreach (var client in clients.ToList().Where(cl => cl.isReady && cl != exclude))
            {
                client.queueOutgoingMessage(message_bytes);
            }
            Log.Debug("[Server] message sent to all.");
        }

        private void sendServerMessage(Client cl, String message)
        {
            UnicodeEncoding encoder = new UnicodeEncoding();
            cl.queueOutgoingMessage(KMPCommon.ServerMessageID.SERVER_MESSAGE, encoder.GetBytes(message));
        }

        private void sendTextMessageToAll(String message, Client exclude = null)
        {
            UnicodeEncoding encoder = new UnicodeEncoding();
            byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.TEXT_MESSAGE, encoder.GetBytes(message));

            foreach (var client in clients.ToList().Where(cl => cl.isReady && cl != exclude))
            {
                client.queueOutgoingMessage(message_bytes);
            }
        }

        public void sendTextMessageToAdmins(String message, Client exclude = null)
        {
            UnicodeEncoding encoder = new UnicodeEncoding();
            byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.TEXT_MESSAGE, encoder.GetBytes(message));

            foreach (var client in clients.ToList().Where(cl => cl.isReady && cl != exclude && isAdmin(cl.username)))
            {
                client.queueOutgoingMessage(message_bytes);
            }
        }

        private void sendTextMessage(Client cl, String message)
        {
            UnicodeEncoding encoder = new UnicodeEncoding();
            cl.queueOutgoingMessage(KMPCommon.ServerMessageID.SERVER_MESSAGE, encoder.GetBytes(message));
        }

		private void sendMotdMessage(Client cl, String message)
		{
			UnicodeEncoding encoder = new UnicodeEncoding();

            foreach (var line in message.Split(new string[] { @"\n" }, StringSplitOptions.None))
            {
                cl.queueOutgoingMessage(KMPCommon.ServerMessageID.MOTD_MESSAGE, encoder.GetBytes(line));
            }
		}

		private void sendMotdMessageToAll(String message, Client exclude = null)
		{
			foreach (var client in clients.ToList().Where(cl => cl.isReady && cl != exclude))
			{
                sendMotdMessage(client, message);
			}
			Log.Debug("[MOTD] sent to all.");
		}

        private void sendPluginUpdateToAll(byte[] data, bool secondaryUpdate, Client cl = null)
        {
            //Extract the KMPVesselUpdate & ProtoVessel, if present, for universe DB
            byte[] infoOnly_data = new byte[data.Length];
            byte[] owned_data = new byte[data.Length];
            byte[] past_data = new byte[data.Length];
            data.CopyTo(infoOnly_data, 0);
            data.CopyTo(owned_data, 0);
            data.CopyTo(past_data, 0);
            String[] vessel_info = null;
            int OwnerID = -1;
            try
            {
                DbCommand cmd;
                string sql;
                if (!secondaryUpdate && cl != null)
                {
                    var vessel_update = ByteArrayToObject<KMPVesselUpdate>(data);

                    if (vessel_update != null)
                    {
                        OwnerID = cl.playerID;
                        vessel_info = new String[5];
                        vessel_info[0] = vessel_update.player;
                        vessel_info[2] = "Using vessel: " + vessel_update.name;
                        vessel_info[3] = "";
                        vessel_info[4] = vessel_update.id.ToString();

                        //Log.Info("Unpacked update from tick=" + vessel_update.tick + " @ client tick=" + cl.lastTick);
                        ConfigNode node = vessel_update.getProtoVesselNode();
                        if (node != null)
                        {
                            byte[] protoVesselBlob = ObjectToByteArray(node);
                            cmd = universeDB.CreateCommand();
                            sql = "SELECT kmpVessel.Subspace FROM kmpVessel LEFT JOIN kmpSubspace ON kmpSubspace.ID = kmpVessel.Subspace" +
                                " WHERE Guid = @kmpID ORDER BY kmpSubspace.LastTick DESC LIMIT 1;";
                            cmd.CommandText = sql;
                            cmd.Parameters.AddWithValue("kmpID", vessel_update.kmpID);
                            object result = cmd.ExecuteScalar();
                            cmd.Dispose();
                            if (result == null)
                            {
                                Log.Info("New vessel {0} from {1} added to universe", vessel_update.kmpID, cl.username);
                                cmd = universeDB.CreateCommand();
                                sql = "INSERT INTO kmpVessel (Guid, GameGuid, OwnerID, Private, Active, ProtoVessel, Subspace)" +
                                    " VALUES (@kmpID,@ves_up_ID, @playerID, @ves_up_isPrivate, @ves_up_state, @protoVessel, @curSubspaceID);";
                                cmd.Parameters.AddWithValue("protoVessel", protoVesselBlob);
                                cmd.Parameters.AddWithValue("kmpID", vessel_update.kmpID);
                                cmd.Parameters.AddWithValue("ves_up_ID", vessel_update.id);
                                cmd.Parameters.AddWithValue("playerID", cl.playerID);
                                cmd.Parameters.AddWithValue("ves_up_isPrivate", Convert.ToInt32(vessel_update.isPrivate));
                                cmd.Parameters.AddWithValue("ves_up_state", Convert.ToInt32(vessel_update.state == State.ACTIVE));
                                cmd.Parameters.AddWithValue("curSubspaceID", cl.currentSubspaceID.ToString("D"));
                                cmd.CommandText = sql;
                                cmd.ExecuteNonQuery();
                                cmd.Dispose();
                            }
                            else
                            {
                                int current_subspace = Convert.ToInt32(result);
                                if (current_subspace == cl.currentSubspaceID)
                                {
                                    cmd = universeDB.CreateCommand();
                                    sql = "UPDATE kmpVessel SET Private = @ves_up_isPrivate, Active = @ves_up_state, OwnerID = @playerID," +
                                        " ProtoVessel = @protoVessel WHERE Guid = @kmpID;";
                                    cmd.Parameters.AddWithValue("protoVessel", protoVesselBlob);
                                    cmd.Parameters.AddWithValue("ves_up_isPrivate", Convert.ToInt32(vessel_update.isPrivate));
                                    cmd.Parameters.AddWithValue("ves_up_state", Convert.ToInt32(vessel_update.state == State.ACTIVE));
                                    cmd.Parameters.AddWithValue("playerID", cl.playerID);
                                    cmd.Parameters.AddWithValue("kmpID", vessel_update.kmpID);
                                    cmd.CommandText = sql;
                                    cmd.ExecuteNonQuery();
                                    cmd.Dispose();
                                }
                                else
                                {

                                    cmd = universeDB.CreateCommand();
                                    sql = "UPDATE kmpVessel SET Private = @ves_up_isPrivate, Active = @ves_up_state, OwnerID = @playerID," +
                                        " ProtoVessel = @protoVessel, Subspace = @curSubspace WHERE Guid = @kmpID;";
                                    cmd.Parameters.AddWithValue("protoVessel", protoVesselBlob);
                                    cmd.Parameters.AddWithValue("ves_up_isPrivate", Convert.ToInt32(vessel_update.isPrivate));
                                    cmd.Parameters.AddWithValue("ves_up_state", Convert.ToInt32(vessel_update.state == State.ACTIVE));
                                    cmd.Parameters.AddWithValue("playerID", cl.playerID);
                                    cmd.Parameters.AddWithValue("curSubspace", cl.currentSubspaceID.ToString("D"));
                                    cmd.Parameters.AddWithValue("kmpID", vessel_update.kmpID);
                                    cmd.CommandText = sql;
                                    cmd.ExecuteNonQuery();
                                    cmd.Dispose();
                                    bool emptySubspace = true;
                                    foreach (Client client in clients.ToList())
                                    {
                                        if (client != null && current_subspace == client.currentSubspaceID && client.tcpClient.Connected)
                                        {
                                            emptySubspace = false;
                                            break;
                                        }
                                    }
                                    if (emptySubspace)
                                    {
                                        cmd = universeDB.CreateCommand();
                                        //Clean up database entries
                                        sql = "DELETE FROM kmpSubspace WHERE ID = @curSubspace AND LastTick < (SELECT MIN(s.LastTick)" +
                                            " FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID);";
                                        cmd.CommandText = sql;
                                        cmd.Parameters.AddWithValue("curSubspace", cl.currentSubspaceID.ToString("D"));
                                        cmd.ExecuteNonQuery();
                                        cmd.Dispose();
                                    }
                                }
                            }

                            if (cl != null && cl.currentVessel != vessel_update.kmpID && cl.currentVessel != Guid.Empty)
                            {

                                try
                                {
                                    cmd = universeDB.CreateCommand();
                                    sql = "UPDATE kmpVessel SET Active = 0 WHERE Guid = @curVessel;";
                                    cmd.CommandText = sql;
                                    cmd.Parameters.AddWithValue("curVessel", cl.currentVessel);
                                    cmd.ExecuteNonQuery();
                                    cmd.Dispose();
                                }
                                catch { }

                                sendVesselStatusUpdateToAll(cl, cl.currentVessel);
                            }

                            cl.currentVessel = vessel_update.kmpID;

                        }
                        else
                        {
                            //No protovessel
                            cmd = universeDB.CreateCommand();
                            sql = "SELECT kmpVessel.Subspace FROM kmpVessel LEFT JOIN kmpSubspace ON kmpSubspace.ID = kmpVessel.Subspace" +
                                " WHERE Guid = @kmpID ORDER BY kmpSubspace.LastTick DESC LIMIT 1;";
                            cmd.CommandText = sql;
                            cmd.Parameters.AddWithValue("kmpID", vessel_update.kmpID);
                            object result = cmd.ExecuteScalar();
                            cmd.Dispose();
                            if (result != null)
                            {
                                int current_subspace = Convert.ToInt32(result);
                                if (current_subspace == cl.currentSubspaceID)
                                {
                                    cmd = universeDB.CreateCommand();
                                    sql = "UPDATE kmpVessel SET Private = @ves_up_isPrivate, Active = @ves_up_state, OwnerID = @playerID" +
                                        " WHERE Guid = @kmpID;";
                                    cmd.CommandText = sql;
                                    cmd.Parameters.AddWithValue("ves_up_isPrivate", Convert.ToInt32(vessel_update.isPrivate));
                                    cmd.Parameters.AddWithValue("ves_up_state", Convert.ToInt32(vessel_update.state == State.ACTIVE));
                                    cmd.Parameters.AddWithValue("playerID", cl.playerID);
                                    cmd.Parameters.AddWithValue("kmpID", vessel_update.kmpID);
                                    cmd.ExecuteNonQuery();
                                    cmd.Dispose();
                                }
                                else
                                {

                                    cmd = universeDB.CreateCommand();
                                    sql = "UPDATE kmpVessel SET Private = @ves_up_isPrivate, Active = @ves_up_state, OwnerID = @playerID," +
                                        " Subspace = @curSubspace WHERE Guid = @kmpID;";
                                    cmd.CommandText = sql;
                                    cmd.Parameters.AddWithValue("ves_up_isPrivate", Convert.ToInt32(vessel_update.isPrivate));
                                    cmd.Parameters.AddWithValue("ves_up_state", Convert.ToInt32(vessel_update.state == State.ACTIVE));
                                    cmd.Parameters.AddWithValue("playerID", cl.playerID);
                                    cmd.Parameters.AddWithValue("curSubspace", cl.currentSubspaceID.ToString("D"));
                                    cmd.Parameters.AddWithValue("kmpID", vessel_update.kmpID);
                                    cmd.ExecuteNonQuery();
                                    cmd.Dispose();
                                    bool emptySubspace = true;
                                    foreach (Client client in clients.ToList())
                                    {
                                        if (client != null && current_subspace == client.currentSubspaceID && client.tcpClient.Connected)
                                        {
                                            emptySubspace = false;
                                            break;
                                        }
                                    }
                                    if (emptySubspace)
                                    {
                                        cmd = universeDB.CreateCommand();
                                        //Clean up database entries
                                        sql = "DELETE FROM kmpSubspace WHERE ID = @curSubspace AND LastTick < (SELECT MIN(s.LastTick)" +
                                            " FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID);";
                                        cmd.CommandText = sql;
                                        cmd.Parameters.AddWithValue("curSubspace", cl.currentSubspaceID.ToString("D"));
                                        cmd.ExecuteNonQuery();
                                        cmd.Dispose();
                                    }
                                }
                            }

                            if (cl != null && cl.currentVessel != vessel_update.kmpID && cl.currentVessel != Guid.Empty)
                            {

                                try
                                {
                                    cmd = universeDB.CreateCommand();
                                    sql = "UPDATE kmpVessel SET Active = 0 WHERE Guid = @curVessel";
                                    cmd.CommandText = sql;
                                    cmd.Parameters.AddWithValue("curVessel", cl.currentVessel);
                                    cmd.ExecuteNonQuery();
                                    cmd.Dispose();
                                }
                                catch { }

                                sendVesselStatusUpdateToAll(cl, cl.currentVessel);
                            }

                            cl.currentVessel = vessel_update.kmpID;
                        }

                        //Store update
                        storeVesselUpdate(vessel_update, cl);

                        //Update vessel destroyed status
                        if (checkVesselDestruction(vessel_update, cl))
                            vessel_update.situation = Situation.DESTROYED;

                        //Repackage the update for distribution
                        vessel_update.isMine = true;
                        owned_data = ObjectToByteArray(vessel_update);
                        vessel_update.isMine = false;
                        data = ObjectToByteArray(vessel_update);
                        vessel_update.relTime = RelativeTime.PAST;
                        vessel_update.name = vessel_update.name + " [Past]";
                        past_data = ObjectToByteArray(vessel_update);
                    }
                }
                else if (cl != null)
                {
                    //Secondary update
                    var vessel_update = ByteArrayToObject<KMPVesselUpdate>(data);

                    if (vessel_update != null)
                    {
                        try
                        {
                            bool active = false;
                            cmd = universeDB.CreateCommand();
                            sql = "SELECT kmpVessel.OwnerID, kmpVessel.Active FROM kmpVessel LEFT JOIN kmpSubspace" +
                                " ON kmpSubspace.ID = kmpVessel.Subspace WHERE Guid = @kmpID" +
                                " ORDER BY kmpSubspace.LastTick DESC LIMIT 1;";
                            cmd.CommandText = sql;
                            cmd.Parameters.AddWithValue("kmpID", vessel_update.kmpID);
                            DbDataReader reader = cmd.ExecuteReader();
                            try
                            {
                                while (reader.Read())
                                {
                                    OwnerID = reader.GetInt32(0);
                                    active = reader.GetBoolean(1);
                                }
                            }
                            catch { }
                            cmd.Dispose();

                            if (!active || OwnerID == cl.playerID) //Inactive vessel or this player was last in control of it
                            {
                                if (vessel_update.getProtoVesselNode() != null)
                                {
                                    //Store included protovessel, update subspace
                                    byte[] protoVesselBlob = ObjectToByteArray(vessel_update.getProtoVesselNode());
                                    cmd = universeDB.CreateCommand();
                                    sql = "UPDATE kmpVessel SET ProtoVessel = @protoVessel, Subspace = @curSubspace WHERE Guid = @kmpID;";
                                    cmd.Parameters.AddWithValue("protoVessel", protoVesselBlob);
                                    cmd.Parameters.AddWithValue("curSubspace", cl.currentSubspaceID.ToString("D"));
                                    cmd.Parameters.AddWithValue("kmpID", vessel_update.kmpID);
                                    cmd.CommandText = sql;
                                    cmd.ExecuteNonQuery();
                                    cmd.Dispose();
                                }
                                if (OwnerID == cl.playerID)
                                {
                                    //Update Active status
                                    cmd = universeDB.CreateCommand();
                                    sql = "UPDATE kmpVessel SET Active = 0 WHERE Guid = @kmpID;";
                                    cmd.CommandText = sql;
                                    cmd.Parameters.AddWithValue("kmpID", vessel_update.kmpID);
                                    cmd.ExecuteNonQuery();
                                    cmd.Dispose();
									sendVesselStatusUpdateToAll(cl, vessel_update.kmpID);
                                }
                                //No one else is controlling it, so store the update
                                storeVesselUpdate(vessel_update, cl, true);
                                //Update vessel destroyed status
                                if (checkVesselDestruction(vessel_update, cl))
                                    vessel_update.situation = Situation.DESTROYED;
                            }
                        }
                        catch { }

                        //Repackage the update for distribution (secondary updates are not delivered to players in the future)
                        vessel_update.isMine = true;
                        owned_data = ObjectToByteArray(vessel_update);
                        vessel_update.isMine = false;
                        data = ObjectToByteArray(vessel_update);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Info("Vessel update error: {0} {1} ", e.Message, e.StackTrace);
            }

            //Build the message array
            byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.PLUGIN_UPDATE, data);
            byte[] owned_message_bytes = buildMessageArray(KMPCommon.ServerMessageID.PLUGIN_UPDATE, owned_data);
            byte[] past_message_bytes = buildMessageArray(KMPCommon.ServerMessageID.PLUGIN_UPDATE, past_data);

            foreach (var client in clients.ToList().Where(c => c != cl && c.isReady && c.activityLevel != Client.ActivityLevel.INACTIVE))
            {
                if ((client.currentSubspaceID == cl.currentSubspaceID)
                    && !client.warping && !cl.warping
                    && (cl.activityLevel == Client.ActivityLevel.IN_GAME || cl.lastTick > 0d))
                {
                    if (OwnerID == client.playerID)
                        client.queueOutgoingMessage(owned_message_bytes);
                    else
                        client.queueOutgoingMessage(message_bytes);
                }
                else if (!secondaryUpdate
						&& !client.warping && !cl.warping
				     	&& (cl.activityLevel == Client.ActivityLevel.IN_GAME || cl.lastTick > 0d)
                     	&& firstSubspaceIsPresentOrFutureOfSecondSubspace(client.currentSubspaceID, cl.currentSubspaceID))
                {
                    client.queueOutgoingMessage(past_message_bytes);
                }
                else if (!secondaryUpdate && (cl.activityLevel == Client.ActivityLevel.IN_GAME || cl.lastTick > 0d))
                {
                    if (vessel_info != null)
                    {
                        if (client.warping || cl.warping) vessel_info[1] = "Unknown due to warp";
                        else
                        {
                            vessel_info[1] = "In the future";
                            vessel_info[2] = vessel_info[2] + " [Future]";
                            vessel_info[3] = cl.currentSubspaceID.ToString();
                        }
                        infoOnly_data = ObjectToByteArray(vessel_info);
                    }
                    byte[] infoOnly_message_bytes = buildMessageArray(KMPCommon.ServerMessageID.PLUGIN_UPDATE, infoOnly_data);
                    client.queueOutgoingMessage(infoOnly_message_bytes);
                }
            }
        }

        private void storeVesselUpdate(KMPVesselUpdate vessel_update, Client cl, bool isSecondary = false)
        {
            byte[] updateBlob = ObjectToByteArray(vessel_update);
            DbCommand cmd = universeDB.CreateCommand();
            string sql = "DELETE FROM kmpVesselUpdate WHERE Guid = @kmpID AND Subspace = @curSubspace;" +
                " INSERT INTO kmpVesselUpdate (Guid, Subspace, UpdateMessage)" +
                " VALUES (@kmpID, @curSubspace ,@update);";
            if (!isSecondary) sql += " INSERT INTO kmpVesselUpdateHistory (Guid, Subspace, Tick, UpdateMessage)" +
                " VALUES (@kmpID, @curSubspace, @ves_tick, @update);";
            cmd.Parameters.AddWithValue("update", updateBlob);
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("kmpID", vessel_update.kmpID);
            cmd.Parameters.AddWithValue("curSubspace", cl.currentSubspaceID.ToString("D"));
            if (!isSecondary) cmd.Parameters.AddWithValue("ves_tick", vessel_update.tick);
            cmd.ExecuteNonQuery();
            cmd.Dispose();
        }

        private bool checkVesselDestruction(KMPVesselUpdate vessel_update, Client cl)
        {
            try
            {
                if (!recentlyDestroyed.ContainsKey(vessel_update.kmpID) || (recentlyDestroyed[vessel_update.kmpID] + 1500L) < currentMillisecond)
                {
                    DbCommand cmd = universeDB.CreateCommand();
                    string sql = "UPDATE kmpVessel SET Destroyed = @ves_up_destroyed WHERE Guid = @kmpID;";
                    cmd.Parameters.AddWithValue("ves_up_destroyed", Convert.ToInt32(vessel_update.situation == Situation.DESTROYED));
                    cmd.Parameters.AddWithValue("kmpID", vessel_update.kmpID);
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    if (!recentlyDestroyed.ContainsKey(vessel_update.kmpID) && vessel_update.situation == Situation.DESTROYED) //Only report first destruction event
                    {
                        Log.Activity("Vessel " + vessel_update.kmpID + " reported as destroyed");
                        recentlyDestroyed[vessel_update.kmpID] = currentMillisecond;
                    }
                    else if (recentlyDestroyed.ContainsKey(vessel_update.kmpID)) recentlyDestroyed.Remove(vessel_update.kmpID); //Vessel was restored for whatever reason
                    return vessel_update.situation == Situation.DESTROYED;
                }
                else return true;
            }
            catch { }
            return false;
        }

        private void sendVesselStatusUpdateToAll(Client cl, Guid vessel)
        {
            foreach (var client in clients.ToList().Where(c => c.isReady && c != cl && c.activityLevel != Client.ActivityLevel.INACTIVE))
            {
                sendVesselStatusUpdate(client, vessel);
            }
        }

        private void sendVesselStatusUpdate(Client cl, Guid vessel)
        {
            DbCommand cmd = universeDB.CreateCommand();
            string sql = "SELECT vu.UpdateMessage, v.ProtoVessel, v.Private, v.OwnerID, v.Active" +
                " FROM kmpVesselUpdate vu" +
                " INNER JOIN kmpVessel v ON v.Guid = vu.Guid" +
                " WHERE vu.Subspace = @curSubspace AND v.Guid = @vessel;";
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("curSubspace", cl.currentSubspaceID.ToString("D"));
            cmd.Parameters.AddWithValue("vessel", vessel);
            DbDataReader reader = cmd.ExecuteReader();
            try
            {
                while (reader.Read())
                {
                    KMPVesselUpdate vessel_update = (KMPVesselUpdate)ByteArrayToObject(GetDataReaderBytes(reader, 0));
                    ConfigNode protoVessel = (ConfigNode)ByteArrayToObject(GetDataReaderBytes(reader, 1));
                    vessel_update.isPrivate = reader.GetBoolean(2);
                    vessel_update.isMine = reader.GetInt32(3) == cl.playerID;
                    if (reader.GetBoolean(4))
                        vessel_update.state = State.ACTIVE;
                    else
                        vessel_update.state = State.INACTIVE;
                    vessel_update.setProtoVessel(protoVessel);
                    byte[] update = ObjectToByteArray(vessel_update);
                    sendVesselMessage(cl, update);
                }
            }
            finally
            {
                reader.Close();
            }
        }

        private void sendScreenshot(Client cl, byte[] bytes)
        {
            Log.Info("Sending screenshot to player {0}", cl.username);
            cl.queueOutgoingMessage(KMPCommon.ServerMessageID.SCREENSHOT_SHARE, bytes);
        }

        private void sendScreenshotToWatchers(Client cl, byte[] bytes)
        {
            //Build the message and send it to all watchers
            byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.SCREENSHOT_SHARE, bytes);
            foreach (var client in clients.ToList().Where(c => c != cl && c.isReady && c.activityLevel != Client.ActivityLevel.INACTIVE))
            {
                bool match = false;

                lock (client.watchPlayerNameLock)
                {
                    match = client.watchPlayerName == cl.username;
                }

                if (match)
                    client.queueOutgoingMessage(message_bytes);
            }
        }

        private void sendCraftFile(Client cl, String craft_name, byte[] data, KMPCommon.CraftType type)
        {

            UnicodeEncoding encoder = new UnicodeEncoding();
            byte[] name_bytes = encoder.GetBytes(craft_name);

            byte[] bytes = new byte[8 + name_bytes.Length + data.Length];

            //Copy data
            KMPCommon.intToBytes((int)type).CopyTo(bytes, 0);
            KMPCommon.intToBytes(name_bytes.Length).CopyTo(bytes, 4);
            name_bytes.CopyTo(bytes, 8);
            data.CopyTo(bytes, 8 + name_bytes.Length);

            cl.queueOutgoingMessage(KMPCommon.ServerMessageID.CRAFT_FILE, bytes);
        }

        private void sendServerSettingsToAll()
        {
            //Build the message array
            byte[] setting_bytes = serverSettingBytes();
            byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.SERVER_SETTINGS, setting_bytes);

            foreach (var client in clients.ToList().Where(c => c.isValid))
            {
                client.queueOutgoingMessage(message_bytes);
            }
        }

        private void sendServerSettings(Client cl)
        {
            cl.queueOutgoingMessage(KMPCommon.ServerMessageID.SERVER_SETTINGS, serverSettingBytes());
        }
		
		private void sendScenarios(Client cl)
		{
			if (settings.gameMode == 1) //Career mode
			{
				DbCommand cmd = universeDB.CreateCommand();
                string sql = "SELECT UpdateMessage FROM kmpScenarios WHERE PlayerID = @playerID;";
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("playerID", cl.playerID);
                DbDataReader reader = cmd.ExecuteReader();
	            try
	            {
	                while (reader.Read())
	                {
	                    byte[] data = GetDataReaderBytes(reader, 0);
	                    sendScenarioMessage(cl, data);
	                }
	            }
	            finally
	            {
	                reader.Close();
	            }
			}
		}
		
        private void sendSyncMessage(Client cl, double tick)
        {
            //Log.Info("Time sync for: " + cl.username);
            byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.SYNC, BitConverter.GetBytes(tick));
            cl.queueOutgoingMessage(message_bytes);
        }
		
        private void sendSyncCompleteMessage(Client cl)
        {
            byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.SYNC_COMPLETE, null);
            cl.queueOutgoingMessage(message_bytes);
        }

        private void sendVesselMessage(Client cl, byte[] data)
        {
            byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.PLUGIN_UPDATE, data);
            cl.queueOutgoingMessage(message_bytes);
        }
		
		private void sendScenarioMessage(Client cl,  byte[] data)
        {
            byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.SCENARIO_UPDATE, data);
            cl.queueOutgoingMessage(message_bytes);
        }

        private byte[] serverSettingBytes()
        {
            byte[] bytes = new byte[KMPCommon.SERVER_SETTINGS_LENGTH];

            KMPCommon.intToBytes(updateInterval).CopyTo(bytes, 0); //Update interval
            KMPCommon.intToBytes(settings.screenshotInterval).CopyTo(bytes, 4); //Screenshot interval
            KMPCommon.intToBytes(settings.screenshotSettings.maxHeight).CopyTo(bytes, 8); //Screenshot height
			BitConverter.GetBytes(settings.safetyBubbleRadius).CopyTo(bytes,12); //Safety bubble radius
            bytes[20] = inactiveShipsPerClient; //Inactive ships per client
            bytes[21] = Convert.ToByte(settings.cheatsEnabled);

            return bytes;
        }

        //Universe

        public void startDatabase()
        {
            universeDB = new SQLiteConnection("Data Source=:memory:");
            universeDB.Open();

            SQLiteConnection diskDB = new SQLiteConnection(DB_FILE_CONN);
            diskDB.Open();

            DbCommand init_cmd = universeDB.CreateCommand();
            string sql = "PRAGMA auto_vacuum = 1;"; //"FULL" auto_vacuum
            init_cmd.CommandText = sql;
            init_cmd.ExecuteNonQuery();

            Int32 version = 0;
            try
            {
                DbCommand cmd = diskDB.CreateCommand();
                sql = "SELECT version FROM kmpInfo";
                cmd.CommandText = sql;
                version = Convert.ToInt32(cmd.ExecuteScalar());
            }
            catch { Log.Info("Missing (or bad) universe database file."); }
            finally
            {
                if (version > 0 && version < UNIVERSE_VERSION)
                {
					DbCommand cmd;
					if (version == 1)
					{
	                    //Upgrade old universe to version 2
	                    Log.Info("Upgrading universe database...");
	                    cmd = diskDB.CreateCommand();
	                    sql = "CREATE INDEX IF NOT EXISTS kmpVesselIdxGuid on kmpVessel(Guid);" +
	                        "CREATE INDEX IF NOT EXISTS kmpVesselUpdateIdxGuid on kmpVesselUpdate(guid);" +
	                        "CREATE INDEX IF NOT EXISTS kmpVesselUpdateHistoryIdxTick on kmpVesselUpdateHistory(Tick);";
	                    cmd.CommandText = sql;
	                    cmd.ExecuteNonQuery();
					}
					
					if (version == 2)
					{
						//Upgrade old universe to version 3
	                    Log.Info("Upgrading universe database...");
						diskDB.BackupDatabase(universeDB, "main", "main", -1, null, 0);
						
						cmd = universeDB.CreateCommand();
	                    sql = "SELECT Guid FROM kmpPlayer;";
	                    cmd.CommandText = sql;
	                    DbDataReader reader = cmd.ExecuteReader();
						while (reader.Read())
			            {
			                string old_guid = reader.GetString(0);
							Guid guid = Guid.Empty;
							try {
								guid = new Guid(old_guid);
							}
							catch
							{
								//Already converted?
								try 
								{
									guid = new Guid(System.Text.Encoding.ASCII.GetBytes(old_guid.Substring(0,16)));
								}
								catch
								{
									guid = Guid.Empty;	
								}
							}
							DbCommand cmd2 = universeDB.CreateCommand();
		                    string sql2 = "UPDATE kmpPlayer SET Guid = @guid WHERE Guid = @old_guid;";
		                    cmd2.CommandText = sql2;
		                    cmd2.Parameters.AddWithValue("guid", guid);
							cmd2.Parameters.AddWithValue("old_guid", old_guid);
		                    cmd2.ExecuteNonQuery();
			            }
						
						cmd = universeDB.CreateCommand();
	                    sql = "SELECT Guid, GameGuid FROM kmpVessel;";
	                    cmd.CommandText = sql;
	                    reader = cmd.ExecuteReader();
						while (reader.Read())
			            {
			                string old_guid = reader.GetString(0);
							string old_guid2 = reader.GetString(1);
							Guid guid = Guid.Empty;
							Guid guid2 = Guid.Empty;
							try {
								guid = new Guid(old_guid);
							}
							catch
							{
								//Already converted?
								try 
								{
									guid = new Guid(System.Text.Encoding.ASCII.GetBytes(old_guid.Substring(0,16)));
								}
								catch
								{
									guid = Guid.Empty;	
								}
							}
							try {
								guid2 = new Guid(old_guid2);
							}
							catch
							{
								//Already converted?
								try 
								{
									guid = new Guid(System.Text.Encoding.ASCII.GetBytes(old_guid2.Substring(0,16)));
								}
								catch
								{
									guid = Guid.Empty;	
								}
							}
							DbCommand cmd2 = universeDB.CreateCommand();
		                    string sql2 = "UPDATE kmpVessel SET Guid = @guid, GameGuid = @guid2 WHERE Guid = @old_guid;";
		                    cmd2.CommandText = sql2;
		                    cmd2.Parameters.AddWithValue("guid", guid);
							cmd2.Parameters.AddWithValue("guid2", guid2);
							cmd2.Parameters.AddWithValue("old_guid", old_guid);
		                    cmd2.ExecuteNonQuery();
			            }
						
						cmd = universeDB.CreateCommand();
	                    sql = "SELECT Guid FROM kmpVesselUpdate;";
	                    cmd.CommandText = sql;
	                    reader = cmd.ExecuteReader();
						while (reader.Read())
			            {
			                string old_guid = reader.GetString(0);
							Guid guid = Guid.Empty;
							try {
								guid = new Guid(old_guid);
							}
							catch
							{
								//Already converted?
								try 
								{
									guid = new Guid(System.Text.Encoding.ASCII.GetBytes(old_guid.Substring(0,16)));
								}
								catch
								{
									guid = Guid.Empty;	
								}
							}
							DbCommand cmd2 = universeDB.CreateCommand();
		                    string sql2 = "UPDATE kmpVesselUpdate SET Guid = @guid WHERE Guid = @old_guid;";
		                    cmd2.CommandText = sql2;
		                    cmd2.Parameters.AddWithValue("guid", guid);
							cmd2.Parameters.AddWithValue("old_guid", old_guid);
		                    cmd2.ExecuteNonQuery();
			            }
						
						universeDB.BackupDatabase(diskDB, "main", "main", -1, null, 0);
					}
					
					//Upgrade old universe to version 4
					Log.Info("Upgrading universe database to current version...");
                    cmd = diskDB.CreateCommand();
                    sql = "CREATE TABLE kmpScenarios (ID INTEGER PRIMARY KEY AUTOINCREMENT, PlayerID INTEGER, Name NVARCHAR(100), Tick DOUBLE, UpdateMessage BLOB);" +
						"CREATE INDEX kmpScenariosIdxPlayerID on kmpScenarios(PlayerID);";
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
					
					diskDB.BackupDatabase(universeDB, "main", "main", -1, null, 0);
					
                    cmd = universeDB.CreateCommand();
                    sql = "UPDATE kmpInfo SET Version = @uni_version;";
                    cmd.CommandText = sql;
                    cmd.Parameters.AddWithValue("uni_version", UNIVERSE_VERSION);
                    cmd.ExecuteNonQuery();
                    Log.Info("Loading universe...");
                }
                else if (version != UNIVERSE_VERSION)
                {
                    Log.Info("Creating new universe...");
                    try
                    {
                        File.Delete("KMP_universe.db");
                    }
                    catch { }
                    DbCommand cmd = universeDB.CreateCommand();
                    sql = "CREATE TABLE kmpInfo (Version INTEGER);" +
                        "INSERT INTO kmpInfo (Version) VALUES (@uni_version);" +
                        "CREATE TABLE kmpSubspace (ID INTEGER PRIMARY KEY AUTOINCREMENT, LastTick DOUBLE);" +
                        "INSERT INTO kmpSubspace (LastTick) VALUES (100);" +
                        "CREATE TABLE kmpPlayer (ID INTEGER PRIMARY KEY AUTOINCREMENT, Name NVARCHAR(100), Guid CHAR(16));" +
                        "CREATE TABLE kmpVessel (Guid CHAR(16), GameGuid CHAR(16), OwnerID INTEGER, Private BIT, Active BIT, ProtoVessel BLOB, Subspace INTEGER, Destroyed BIT);" +
                        "CREATE TABLE kmpVesselUpdate (ID INTEGER PRIMARY KEY AUTOINCREMENT, Guid CHAR(16), Subspace INTEGER, UpdateMessage BLOB);" +
                        "CREATE TABLE kmpVesselUpdateHistory (Guid CHAR(16), Subspace INTEGER, Tick DOUBLE, UpdateMessage BLOB);" +
						"CREATE TABLE kmpScenarios (ID INTEGER PRIMARY KEY AUTOINCREMENT, PlayerID INTEGER, Name NVARCHAR(100), Tick DOUBLE, UpdateMessage BLOB);" +
                        "CREATE INDEX kmpVesselIdxGuid on kmpVessel(Guid);" +
                        "CREATE INDEX kmpVesselUpdateIdxGuid on kmpVesselUpdate(guid);" +
                        "CREATE INDEX kmpVesselUpdateHistoryIdxTick on kmpVesselUpdateHistory(Tick);" +
						"CREATE INDEX kmpScenariosIdxPlayerID on kmpScenarios(PlayerID);";
                    cmd.CommandText = sql;
                    cmd.Parameters.AddWithValue("uni_version", UNIVERSE_VERSION);
                    cmd.ExecuteNonQuery();
                }
                else
                {
                    Log.Info("Loading universe...");
                    diskDB.BackupDatabase(universeDB, "main", "main", -1, null, 0);
                }
                diskDB.Close();
            }

            DbCommand cmd3 = universeDB.CreateCommand();
            sql = "VACUUM; UPDATE kmpVessel SET Active = 0;";
            cmd3.CommandText = sql;
            cmd3.ExecuteNonQuery();
            Log.Info("Universe OK.");
        }

        public void backupDatabase()
        {
            Log.Info("Backing up old disk DB...");
            try
            {
				if (!File.Exists(DB_FILE))
					throw new IOException();

                File.Copy(DB_FILE, DB_FILE + ".bak", true);
                Log.Debug("Successfully backup up database.");
            }
			catch (IOException)
			{
				Log.Error("Database does not exist.  Recreating.");
			}
            catch (Exception e)
            {
                Log.Error("Failed to backup DB:");
                Log.Error(e.Message);
            }

            try
            {
				if (uncleanedBackups > settings.maxDirtyBackups) cleanDatabase();
				else uncleanedBackups++;
                saveDatabaseToDisk();
                Log.Info("Universe saved to disk.");
            }
            catch(Exception e)
            {
                Log.Error("Failed to save database:");
                Log.Error(e.Message);
                Log.Error(e.ToString());
                Log.Error(e.StackTrace);

                Log.Info("Saving secondary copy of last backup.");
                File.Copy(DB_FILE + ".bak", DB_FILE + ".before_failure.bak", true);

                Log.Info("Press any key to quit - ensure database is valid or reset database before restarting server.");
                Console.ReadKey();
                Environment.Exit(0);
            }
        }

        public void saveDatabaseToDisk()
        {
            var asSqlite = universeDB as SQLiteConnection;

            if (asSqlite == null) { return; }

            SQLiteConnection diskDB = new SQLiteConnection(DB_FILE_CONN);
            diskDB.Open();
            asSqlite.BackupDatabase(diskDB, "main", "main", -1, null, 0);
            DbCommand cmd = diskDB.CreateCommand();
            string sql = "DELETE FROM kmpSubspace WHERE LastTick < (SELECT MIN(s.LastTick) FROM kmpSubspace s" +
                " INNER JOIN kmpVessel v ON v.Subspace = s.ID);" +
                " DELETE FROM kmpVesselUpdateHistory;" +
                " DELETE FROM kmpVesselUpdate WHERE ID IN (SELECT ID FROM kmpVesselUpdate vu" +
                " WHERE Subspace != (SELECT ID FROM kmpSubspace WHERE LastTick = (SELECT MAX(LastTick)" +
                " FROM kmpSubspace WHERE ID IN (SELECT Subspace FROM kmpVesselUpdate WHERE Guid = vu.Guid))));";
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
            diskDB.Close();
        }

        public void cleanDatabase()
        {
            try
            {
                Log.Info("Attempting to optimize database...");
				
				uncleanedBackups = 0;
				
				if (activeClientCount() > 0)
				{
					//Get the oldest tick for an active player
					double earliestClearTick = -1d;
					
					foreach (Client client in clients)
					{
						DbCommand cmd = universeDB.CreateCommand();
						string sql = "SELECT LastTick FROM kmpSubspace WHERE ID = @subspace;";
						cmd.Parameters.AddWithValue("subspace", client.currentSubspaceID);
		                cmd.CommandText = sql;
		                double clientTick = Convert.ToDouble(cmd.ExecuteScalar());
						if (earliestClearTick < 0d || clientTick < earliestClearTick) earliestClearTick = clientTick;
					}
					
					//Clear anything before that
					DbCommand cmd2 = universeDB.CreateCommand();
	                string sql2 = "DELETE FROM kmpSubspace WHERE LastTick < @minTick AND LastTick < (SELECT MIN(s.LastTick) FROM kmpSubspace s" +
	                    " INNER JOIN kmpVessel v ON v.Subspace = s.ID);" +
	                    " DELETE FROM kmpVesselUpdateHistory WHERE Tick < @minTick;";
					cmd2.Parameters.AddWithValue("minTick", earliestClearTick);
	                cmd2.CommandText = sql2;
	                cmd2.ExecuteNonQuery();
				}
				else
				{
					//Clear all but the latest subspace
	                DbCommand cmd = universeDB.CreateCommand();
	                string sql = "DELETE FROM kmpSubspace WHERE LastTick < (SELECT MIN(s.LastTick) FROM kmpSubspace s" +
	                    " INNER JOIN kmpVessel v ON v.Subspace = s.ID);" +
	                    " DELETE FROM kmpVesselUpdateHistory;" +
	                    " DELETE FROM kmpVesselUpdate WHERE ID IN (SELECT ID FROM kmpVesselUpdate vu" +
	                    " WHERE Subspace != (SELECT ID FROM kmpSubspace WHERE LastTick = (SELECT MAX(LastTick) FROM kmpSubspace" +
	                    " WHERE ID IN (SELECT Subspace FROM kmpVesselUpdate WHERE Guid = vu.Guid))));";
	                cmd.CommandText = sql;
	                cmd.ExecuteNonQuery();
				}
				
				lock (databaseVacuumLock)
				{
	                DbCommand cmd = universeDB.CreateCommand();
	                string sql = "VACUUM;";
	                cmd.CommandText = sql;
	                cmd.ExecuteNonQuery();
				}

                Log.Info("Optimized in-memory universe database.");
            }
            catch (System.Data.SQLite.SQLiteException ex)
            {
                Log.Error("Couldn't optimize database: {0}", ex.Message);
            }
        }

        public bool firstSubspaceIsPresentOrFutureOfSecondSubspace(int comparisonSubspace, int referenceSubspace)
        {
            if (comparisonSubspace == -1 || referenceSubspace == -1) return false;
            if (comparisonSubspace == referenceSubspace) return true;
            double refTime = 0d, compTime = 0d;
            DbCommand cmd = universeDB.CreateCommand();
            string sql = "SELECT LastTick FROM kmpSubspace WHERE ID = @refSubspace;";
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("refSubspace", referenceSubspace);
            refTime = Convert.ToDouble(cmd.ExecuteScalar());
            cmd.Dispose();

            cmd = universeDB.CreateCommand();
            sql = "SELECT LastTick FROM kmpSubspace WHERE ID = @compSubspace;";
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("compSubspace", comparisonSubspace);
            compTime = Convert.ToDouble(cmd.ExecuteScalar());
            cmd.Dispose();
			
			if (compTime < 1d || refTime < 1d) return true;
			
            return (compTime >= refTime);
        }

        static byte[] GetDataReaderBytes(DbDataReader reader, int column)
        {
            const int CHUNK_SIZE = 2 * 1024;
            byte[] buffer = new byte[CHUNK_SIZE];
            long bytesRead;
            long fieldOffset = 0;
            using (MemoryStream stream = new MemoryStream())
            {
                while ((bytesRead = reader.GetBytes(column, fieldOffset, buffer, 0, buffer.Length)) > 0)
                {
                    stream.Write(buffer, 0, (int)bytesRead);
                    fieldOffset += bytesRead;
                }
                return stream.ToArray();
            }
        }

        private byte[] ObjectToByteArray(Object obj)
        {
            if (obj == null)
                return null;
            BinaryFormatter bf = new BinaryFormatter();
            MemoryStream ms = new MemoryStream();
            bf.Serialize(ms, obj);
            return ms.ToArray();
        }

        private object ByteArrayToObject(byte[] data)
        {
            if (data == null)
                return null;
            BinaryFormatter bf = new BinaryFormatter();
            MemoryStream ms = new MemoryStream(data);
            return bf.Deserialize(ms);
        }

        private T ByteArrayToObject<T>(byte[] data)
        {
            if (data.Length == 0)
                return default(T);
            BinaryFormatter bf = new BinaryFormatter();
            MemoryStream ms = new MemoryStream(data);
            try
            {
                Object o = bf.Deserialize(ms);
                if (o is T)
                {
                    return (T)o;
                }
                else
                {
                    return default(T);
                }
            }
            catch
            {
                return default(T);
            }
        }
		
        private string CleanInput(string strIn)
        {
            // Replace invalid characters with empty strings. 
            try
            {
                return Regex.Replace(strIn, @"[\r\n\x00\x1a\\'""]", "");
            }
            catch { return String.Empty; }
        }

        private void displayCommands()
        {
            Log.Info("Commands:");
            Log.Info("/quit or /stop - Quit server cleanly");
            Log.Info("/listclients - List players");
            Log.Info("/countclients - Display player counts");
            Log.Info("/kick [username] - Kick player <username>");
            Log.Info("/ban [username] - Permanently ban player <username> and any known aliases");
            Log.Info("/register [username] [token] - Add new roster entry for player <username> with authentication token <token> (BEWARE: will delete any matching roster entries)");
            Log.Info("/update [username] [token] - Update existing roster entry for player <username>/token <token> (one param must match existing roster entry, other will be updated)");
            Log.Info("/unregister [username/token] - Remove any player that has a matching username or token from the roster");
            Log.Info("/clearclients - Attempt to clear 'ghosted' clients");
			Log.Info("/countships - Lists number of ships in universe.");
			Log.Info("/listships - List all ships in universe along with their ID");
			Log.Info("/deleteship [ID] - Removes ship from universe."); 
            Log.Info("/dekessler <mins> - Remove debris that has not been updated for at least <mins> minutes (in-game time) (If no <mins> value is specified, debris that is older than 30 minutes will be cleared)");
            Log.Info("/save - Backup universe");
            Log.Info("/reloadmodfile - Reloads the {0} file. Note that this will not recheck any currently logged in clients, only those joining", MOD_CONTROL_FILE);
			Log.Info("/setinfo [info] - Updates the server info seen on master server list");
			Log.Info("/motd [message] - Sets message of the day, leave blank for none");
			Log.Info("/rules [rules] - Sets server rules, leave blank for none");
            Log.Info("/say <-u username> [message] - Send a Server message <to specified user>");
			Log.Info("/help - Displays all commands in the server\n");

            // to add a new command to the command list just copy the Log.Info method and add how to use that command.
        }

        private void checkGhosts()
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("en-GB");
            Log.Debug("Starting ghost-check thread");
            while (true)
            {
                int foundGhost = 0;
                foreach (Client client in clients.ToList().Where(c => !c.isReady && currentMillisecond - c.connectionStartTime > CLIENT_HANDSHAKE_TIMEOUT_DELAY + CLIENT_TIMEOUT_DELAY))
                {
                    markClientForDisconnect(client, "Disconnected via ghost-check command. Not a ghost? Sorry!");
                    Log.Debug("Force-disconnected client: {0}", client.playerID);

                    try
                    {
                        client.tcpClient.Close();
                    }
                    catch (Exception) { }
                    finally { foundGhost++; }

                }
                if (foundGhost > 0)
                {
                    Log.Debug("Ghost check complete. Removed {0} ghost(s).", foundGhost);
                }

                Thread.Sleep(GHOST_CHECK_DELAY);
            }
        }

        private bool isAdmin(String username)
        {
            return settings.admins.Contains(username);
        }
		
		private int activeClientCount()
		{
			return clients.Where(cl => cl.isReady).Count();
		}
    }
}
