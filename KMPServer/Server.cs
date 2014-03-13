//#define DEBUG_OUT
//#define SEND_UPDATES_TO_SENDER

using KMP;
using MySql.Data.MySqlClient;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace KMPServer
{
    internal class Server
    {
        public struct ClientMessage
        {
            public Client client;
            public KMPCommon.ClientMessageID id;
            public byte[] data;
        }

        #region Singletons

        private DatabaseHelper _helperSingleton = null;

        #endregion

        public const long CLIENT_TIMEOUT_DELAY = 16000;
        public const long CLIENT_HANDSHAKE_TIMEOUT_DELAY = 6000;
        public const int GHOST_CHECK_DELAY = 30000;
        public const int SLEEP_TIME = 10;
        public const int MAX_SCREENSHOT_COUNT = 10000;
        public const int UDP_ACK_THROTTLE = 1000;
        public const int MESSAGE_HANDLE_TIMEOUT = 500; //Allow a maximum of 0.5 seconds of server lag, then lets try to handle server lag.

        public const float NOT_IN_FLIGHT_UPDATE_WEIGHT = 1.0f / 4.0f;
        public const int ACTIVITY_RESET_DELAY = 10000;

        public const String SCREENSHOT_DIR = "KMPScreenshots";
        // No longer valid as Database Helper takes care of this -NC
        // public const string DB_FILE_CONN = "Data Source=KMP_universe.db";
        public const string DB_FILE = "KMP_universe.db";
        public const string MOD_CONTROL_FILE = "KMPModControl.txt";
        public const string MODS_PATH = "Mods";

        public const string PLUGIN_DATA_DIRECTORY = "KMP/Plugins/PluginData/KerbalMultiPlayer/";
        public const string CLIENT_CONFIG_FILENAME = "KMPClientConfig.xml";
        public const string CLIENT_TOKEN_FILENAME = "KMPPlayerToken.txt";
        public const string MOD_CONTROL_FILENAME = "KMPModControl.txt";
        public const String GLOBAL_SETTINGS_FILENAME = "globalsettings.txt";

        public static byte[] kmpModControl;

        public const int UNIVERSE_VERSION = 6;

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

        // BOOM! TODO: Remove all this commented code. Just leaving here for first commit to show when it was removed
        // public static DatabaseHelper universeDB;

        private bool backedUpSinceEmpty = false;
        private Dictionary<Guid, long> recentlyDestroyed = new Dictionary<Guid, long>();
        private Dictionary<int, double> subSpaceMasterTick = new Dictionary<int, double>();
        private Dictionary<int, long> subSpaceMasterTime = new Dictionary<int, long>();
        private Dictionary<int, float> subSpaceMasterSpeed = new Dictionary<int, float>();
        private Dictionary<int, long> subSpaceLastRateCheck = new Dictionary<int, long>();

        private Boolean bHandleCommandsRunning = true;

        private int uncleanedBackups = 0;

        /// <summary>
        /// Database Helper Instance
        /// </summary>
        private DatabaseHelper Database
        {
            get
            {
                return _helperSingleton = _helperSingleton ?? (settings.useMySQL ? 
                    DatabaseHelper.CreateForMySQL(settings.mySQLConnString) : 
                    DatabaseHelper.CreateForSQLite(DB_FILE));
            }
        }

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

            BackupDatabase();
        }

        private static List<string> generatePartsList()
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

            return partList;
            //foreach(string part in partList) writer.WriteLine(part);
        }

        private static void readModControl()
        {
            try
            {
                Log.Info("Reading {0}", MOD_CONTROL_FILE);
                kmpModControl = File.ReadAllBytes(MOD_CONTROL_FILE);
                Log.Info("Mod control reloaded.");
            }
            catch
            {
                Log.Info(MOD_CONTROL_FILE + " not found, generating...");
                //Generate a default blacklist no-sha file.
                writeModControl(true, false);
            }
        }

        public static string ModFilesToListing(string mode, bool sha)
        {
            string result = "";
            if (mode == "required" || mode == "optional")
            {
                string[] lsDirectory = Directory.GetDirectories(MODS_PATH);
                foreach (string modDirectory in lsDirectory)
                {
                    string modType = "optional";
                    string[] lsFiles = Directory.GetFiles(modDirectory, "*", SearchOption.AllDirectories);
                    List<string> dllFiles = new List<string>();
                    foreach (string modFile in lsFiles)
                    {
                        //Remove the Mods/ part, Change path seperators to the unix ones.
                        string trimmedModFile = modFile.Remove(0, MODS_PATH.Length + 1);
                        if (!trimmedModFile.ToLowerInvariant().StartsWith("squad") && !trimmedModFile.ToLowerInvariant().StartsWith("kmp") && !trimmedModFile.ToLowerInvariant().StartsWith("000_toolbar"))
                            if (trimmedModFile.ToLowerInvariant().EndsWith(".cfg"))
                            {
                                using (StreamReader sr = new StreamReader(modFile))
                                {
                                    string line;
                                    while ((line = sr.ReadLine()) != null)
                                    {
                                        if (line.Contains("PART"))
                                        {
                                            modType = "required";
                                        }
                                    }
                                }
                            }
                        if (trimmedModFile.ToLowerInvariant().EndsWith(".dll"))
                        {
                            dllFiles.Add(modFile);
                        }
                    }

                    if (modType == mode)
                    {
                        foreach (string dllFile in dllFiles)
                        {
                            //Remove the Mods/ part, Change path seperators to the unix ones.
                            string trimmedDllFile = dllFile.Remove(0, MODS_PATH.Length + 1).Replace("\\", "/");
                            Log.Info("Adding " + trimmedDllFile + " into the " + modType + " section.");
                            result += trimmedDllFile;
                            //We shouldn't ever care what version of non-part-adding mods the client has.
                            if (sha && modType == "required")
                            {
                                using (SHA256Managed shaManager = new SHA256Managed())
                                {
                                    using (FileStream stream = File.OpenRead(dllFile))
                                    {
                                        byte[] hash = shaManager.ComputeHash(stream);
                                        result += "=" + BitConverter.ToString(hash).Replace("-", String.Empty);
                                    }
                                }
                            }
                            result += "\n";
                        }
                    }
                }
            }

            if (mode == "resource-whitelist")
            {
                string[] lsFiles = Directory.GetFiles(MODS_PATH, "*", SearchOption.AllDirectories);
                foreach (string modFile in lsFiles)
                {
                    string trimmedModFile = modFile.Remove(0, MODS_PATH.Length + 1).Replace("\\", "/");
                    if (!trimmedModFile.ToLowerInvariant().StartsWith("squad") && !trimmedModFile.ToLowerInvariant().StartsWith("kmp") && !trimmedModFile.ToLowerInvariant().StartsWith("000_toolbar") && trimmedModFile.ToLowerInvariant().EndsWith(".dll"))
                    {
                        result += modFile.Remove(0, MODS_PATH.Length + 1).Replace("\\", "/") + "\n"; //Remove the starting parth and add it to the list.
                    }
                }
            }
            return result;
        }

        public static void writeModControlCommand(string[] input)
        {
            string[] commandParts = new string[0];
            if (input.Length == 2)
            {
                commandParts = input[1].Split(new char[] { ' ' });
            }
            bool blacklist = true;
            bool sha = false;
            //This allows a user to type the modgen parameters in any order.
            foreach (string part in commandParts)
            {
                if (part.ToLowerInvariant() == "whitelist")
                {
                    blacklist = false;
                }
                if (part.ToLowerInvariant() == "sha")
                {
                    sha = true;
                }
            }
            writeModControl(blacklist, sha);
        }

        public static void writeModControl(bool blacklist, bool sha)
        {
            bool autoAdd = Directory.GetDirectories(MODS_PATH).Count() > 0;
            if (!autoAdd)
            {
                Log.Info("To generate an automatic KMPModControl.txt file, Copy mods from the GameData directory to the '" + MODS_PATH + "' folder.");
            }
            string filestring = "\n" +
                "#You can comment by starting a line with a #, these are ignored by the server.\n" +
                "#Commenting will NOT work unless the line STARTS with a '#'.\n" +
                "#You can also indent the file with tabs or spaces.\n" +
                "#Sections supported are required-files, optional-files, partslist, resource-blacklist and resource-whitelist.\n" +
                "#The client will be required to have the files found in required-files, and they must match the SHA hash if specified (this is where part mod files and play-altering files should go, like KWRocketry or Ferram Aerospace Research" +
                "#The client may have the files found in optional-files, but IF they do then they must match the SHA hash (this is where mods that do not affect other players should go, like EditorExtensions or part catalogue managers\n" +
                "#You cannot use both resource-blacklist AND resource-whitelist in the same file.\n" +
                "#resource-blacklist bans ONLY the files you specify\n" +
                "#resource-whitelist bans ALL resources except those specified in the resource-whitelist section OR in the SHA sections. A file listed in resource-whitelist will NOT be checked for SHA hash. This is useful if you want a mod that modifies files in its own directory as you play.\n" +
                "#Each section has its own type of formatting. Examples have been given.\n" +
                "#Sections are defined as follows:\n" +
                "\n" +
                "\n" +
                "!required-files" +
                "\n" +
                "#To generate the SHA256 of a file you can use a utility such as this one: http://hash.online-convert.com/sha256-generator (use the 'hex' string), or use sha256sum on linux.\n" +
                "#File paths are read from inside GameData.\n" +
                "#If there is no SHA256 hash listed here (i.e. blank after the equals sign or no equals sign), SHA matching will not be enforced.\n" +
                "#You may not specify multiple SHAs for the same file. Do not put spaces around equals sign. Follow the example carefully.\n" +
                "#Syntax:\n" +
                "#[File Path]=[SHA] or [File Path]\n" +
                "#Example: MechJeb2/Plugins/MechJeb2.dll=B84BB63AE740F0A25DA047E5EDA35B26F6FD5DF019696AC9D6AF8FC3E031F0B9\n" +
                "#Example: MechJeb2/Plugins/MechJeb2.dll\n\n";
            if (autoAdd)
            {
                Log.Info("Beginning SHA256 hash of required mod files...");
                // add mods that user must have, but don't have to match SHA hash
                filestring += ModFilesToListing("required", sha);
            }
            filestring += "\n\n!optional-files\n" +
                "#Formatting for this section is the same as the 'required-files' section\n\n";
            if (autoAdd)
            {
                Log.Info("Beginning SHA256 hash of optional mod files...");
                filestring += ModFilesToListing("optional", sha);
            }
            if (blacklist)
            {
                filestring += "\n\n!resource-blacklist\n#!resource-whitelist\n\n";
                filestring += "#Alternatively, change 'blacklist' to 'whitelist' and clients will only be allowed to use dll's listed here or in the 'required-files' and 'optional-files' sections.\n";
            }
            else
            {
                filestring += "\n\n!resource-whitelist\n#!resource-blacklist\n\n";
                filestring += "#Alternatively, change 'whitelist' to 'blacklist' and clients will not be allowed to use dll's listed here.\n";
            }
            filestring += "#You can ban specific files in resource-blacklist mode, or only allow specific files in resource-whitelist mode.\n" +
                    "#Syntax:\n" +
                    "#[File Path]\n" +
                    "#Example: MechJeb2/Plugins/MechJeb2.dll\n\n";

            //We don't need to write any files here in blacklist mode.
            if (autoAdd && !blacklist)
            {
                filestring += ModFilesToListing("resource-whitelist", sha);
            }

            filestring += "\n\n" +
                "!partslist\n" +
                "#This is a list of parts to allow users to put on their ships.\n" +
                "#If a part the client has doesn't appear on this list, they can still join the server but not use the part.\n" +
                "#The default stock parts have been added already for you.\n" +
                "#To add a mod part, add the name from the part's .cfg file. The name is the name from the PART{} section, where underscores are replaced with periods.\n" +
                "#[partname]\n" +
                "#Example: mumech.MJ2.Pod (NOTE: In the part.cfg this MechJeb2 pod is named mumech_MJ2_Pod. The _ have been replaced with .)\n" +
                "#You can use this application to generate partlists from a KSP installation if you want to add mod parts: http://forum.kerbalspaceprogram.com/threads/57284 \n" +
                "\n";

            List<string> parts = new List<string>();
            parts = generatePartsList();
            if (autoAdd) // add a part list for all part in the Required and Optional folders
            {
                string[] ls = Directory.GetFiles(MODS_PATH, "*", SearchOption.AllDirectories);
                ls = ls.Distinct().ToArray();
                char[] toperiod = { '_' };
                foreach (string file in ls)
                {
                    //We add Squad files manually in generatePartsList above.
                    if (!file.ToLowerInvariant().StartsWith("squad") && file.Substring(file.Length - 4).Equals(".cfg", StringComparison.InvariantCultureIgnoreCase)) // check if config file (only place where parts are located)
                    {
                        using (StreamReader sr = new StreamReader(file))
                        {
                            bool newPart = false;
                            while (!sr.EndOfStream)
                            {
                                string word = str_nextword(sr);
                                if (word == "PART")
                                {
                                    newPart = true;
                                }
                                else if (word.Equals("name", StringComparison.InvariantCultureIgnoreCase) && str_nextword(sr).Equals("=", StringComparison.InvariantCultureIgnoreCase) && newPart)
                                {
                                    string line = sr.ReadLine();
                                    line.Trim(); // remove whitespace from part name
                                    string[] temp = line.Split(toperiod, StringSplitOptions.RemoveEmptyEntries); // convert spaces and underscores to periods
                                    line = String.Join(".", temp);
                                    parts.Add(line);
                                    newPart = false;
                                }
                            }
                        }
                    }
                }
                parts = parts.Distinct().ToList();
            }
            foreach (string part in parts)
            {
                filestring += part + "\n";
            }
            File.WriteAllText(MOD_CONTROL_FILE, filestring);
            Log.Info("New {0} file written.", MOD_CONTROL_FILE);
            readModControl();
        }

        private static string str_nextword(StreamReader file)
        {
            char[] wordBoundaries = { '\n', ' ', '\t', '\r' };
            string output = "";
            bool started = false;
            while (!file.EndOfStream)
            {
                char c = (char)file.Read();
                if (wordBoundaries.Contains(c))
                {
                    if (!started)
                    {
                        continue;
                    }
                    else
                    {
                        break;
                    }
                }
                else
                {
                    started = true;
                    output += c;
                }
            }
            return output;
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

            try
            {
                startDatabase();

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
                if (settings.ipBinding == "0.0.0.0" && settings.hostIPv6 == true)
                {
                    settings.ipBinding = "::";
                }
                tcpListener = new TcpListener(IPAddress.Parse(settings.ipBinding), settings.port);
                if (settings.hostIPv6 == true)
                {
                    try
                    {
                        //Windows defaults to v6 only, but this option does not exist in mono so it has to be in a try/catch block along with the casted int.
                        tcpListener.Server.SetSocketOption(SocketOptionLevel.IPv6, (SocketOptionName)27, 0);
                    }
                    catch
                    {
                        Log.Debug("Failed to unset IPv6Only. Linux and Mac have this option off by default.");
                    }
                }

                listenThread.Start();

                try
                {
                    udpClient = new UdpClient((IPEndPoint)tcpListener.LocalEndpoint);
                    udpClient.BeginReceive(asyncUDPReceive, null);
                    //udpClient.Client.AllowNatTraversal(1);
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
                            CleanDatabase();
                        }

                        last_backup_time = currentMillisecond;
                        BackupDatabase();
                    }

                    Thread.Sleep(SLEEP_TIME);
                }

                clearState();
                stopwatch.Stop();

                Log.Info("Server session ended.");
                if (quit) { Log.Info("Quitting"); Thread.Sleep(1000); Environment.Exit(0); }
            }
            catch (MySqlException e)
            {
                Log.Error("Fatal error accessing MySQL database, server session ended!");
                Log.Error(e.Message);
            }
            catch (Exception e)
            {
                Log.Error("Fatal error, server session ended! Exception details:\n{0}\n{1}", e.Message, e.StackTrace);
            }
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

        private void processCommand(String input)
        {
            Log.Info("Command Input: {0}", input);
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
                    case "/lockship": lockShipServerCommand(parts); break;
                    case "/deleteship": deleteShipServerCommand(parts); break;
                    case "/reloadmodfile": reloadModFileServerCommand(); break;
                    case "/say": sayServerCommand(rawParts); break;
                    case "/motd": motdServerCommand(rawParts); break;
                    case "/rules": rulesServerCommand(rawParts); break;
                    case "/setinfo": serverInfoServerCommand(rawParts); break;
                    case "/modgen": writeModControlCommand(parts); break;
                    case "/dbdiag": Log.Info("[DBDIAG] {0}", Database); break;
                    default: Log.Info("Unknown Command: " + cleanInput); break;
                }
            }
            catch (FormatException e)
            {
                Log.Error("Error handling server command. Maybe a typo? {0} {1}", e.Message, e.StackTrace);
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
                    processCommand(input);
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
            if (parts.Length > 1)
            {
                if (parts[1].IndexOf("-u") == 0)
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

            int count = 0;
            Database.ExecuteReader("SELECT  vu.UpdateMessage, v.ProtoVessel, v.Guid" +
                        " FROM kmpVesselUpdate vu" +
                        " INNER JOIN kmpVessel v ON v.Guid = vu.Guid AND v.Destroyed IS NULL" +
                        " INNER JOIN kmpSubspace s ON s.ID = vu.Subspace" +
                        " INNER JOIN" +
                        "  (SELECT vu.Guid, MAX(s.LastTick) AS LastTick" +
                        "  FROM kmpVesselUpdate vu" +
                        "  INNER JOIN kmpSubspace s ON s.ID = vu.Subspace" +
                        "  GROUP BY vu.Guid) t ON t.Guid = vu.Guid AND t.LastTick = s.LastTick;",
                record =>
                {
                    KMPVesselUpdate vessel_update = (KMPVesselUpdate)ByteArrayToObject(GetDataReaderBytes(record, 0));
                    if (bList)
                        Log.Info("Name: {0}\tID: {1}", vessel_update.name, vessel_update.kmpID);
                    count++;
                });

            if (count == 0)
                Log.Info("No ships.");
            else if (!bList)
                Log.Info("Number of ships: {0}", count);
        }

        private int countShipsInDatabase()
        {
            int count = Convert.ToInt32(Database.ExecuteScalar("SELECT COUNT(*) FROM kmpVessel WHERE Destroyed IS NULL;"));
			Log.Debug("Vessel count: {0}", count);
            return count; // TODO: @NeverCast, Give ExecuteScalar a generic overload
        }

        private void listShipsServerCommand()
        {
            countShipsServerCommand(true);
        }

        private void lockShipServerCommand(string[] parts)
        {
            String[] args = parts[1].Split(' ');
            if (args.Length == 2)
            {
                try
                {
                    Guid vesselGuid = new Guid(args[0]);
                    bool lockShip = Boolean.Parse(args[1].ToLower());



                    int rows = Database.ExecuteNonQuery("UPDATE kmpVessel" +
                                " SET Private = @private" +
                                " WHERE Guid = @guid",
                                "private", lockShip,
                                "guid", vesselGuid.ToByteArray());

                    if (rows != -1 && rows <= 1)
                    {
                        if (lockShip)
                            Log.Info("Vessel {0} is now private.", args[0]);
                        else
                            Log.Info("Vessel {0} is now public.", args[0]);
                    }
                    else
                        Log.Info("Vessel {0} not found.", args[0]);
                }
                catch (FormatException)
                {
                    Log.Error("Supplied tokens are invalid. Use /listships to double check your ID.");
                }
            }
            else
            {
                Log.Info("Could not parse lock ship command. Format is \"/lockship <vesselID> <true/false>\"");
            }
        }

        private void deleteShipServerCommand(string[] parts)
        {
            try
            {
                Guid tokill = new Guid(parts[1]);
                int rows = Database.ExecuteNonQuery("UPDATE kmpVessel SET Destroyed = 1 WHERE Guid = @guid;",
                    "guid", tokill.ToByteArray());

                if (rows != -1 && rows != 0)
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
            if (parts.Length > 1)
            {
                settings.serverMotd = (String)parts[1];
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
            if (parts.Length > 1)
            {
                settings.serverRules = (String)parts[1];
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
            if (parts.Length > 1)
            {
                settings.serverInfo = (String)parts[1];
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
                    Database.ExecuteNonQuery("UPDATE kmpPlayer SET Guid = @newGuid WHERE Guid = @guid;", "newGuid", Guid.NewGuid(), "guid", guid);
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
                }
                catch (Exception e)
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
                    Database.ExecuteNonQuery(@"DELETE FROM kmpPlayer WHERE Name LIKE @username;" +
                        " INSERT INTO kmpPlayer (Name, Guid) VALUES (@username,@guid);", 
                        "username", username_lower, 
                        "guid", guid);
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
            BackupDatabase();
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
                    Database.ExecuteNonQuery("UPDATE kmpPlayer SET Name=@username, Guid=@guid WHERE Name LIKE @username OR Guid = @guid;",
                        "username", username_lower,
                        "guid", guid);
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
                    Database.ExecuteNonQuery("DELETE FROM kmpPlayer WHERE Guid = @dereg OR Name LIKE @dereg;",
                        "dereg", dereg);
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
                double cutOffTick = Convert.ToDouble(Database.ExecuteScalar("SELECT MAX(LastTick) FROM kmpSubspace")) - Convert.ToDouble(minsToKeep * 60);
                //Get all vessels, remove Debris that is too old
                int clearedCount = 0;
                List<Tuple<byte[], byte[], Guid>> results = new List<Tuple<byte[], byte[], Guid>>();

                Database.ExecuteReader("SELECT  vu.UpdateMessage, v.ProtoVessel, v.Guid" +
                    " FROM kmpVesselUpdate vu" +
                    " INNER JOIN kmpVessel v ON v.Guid = vu.Guid AND v.Destroyed IS NULL" +
                    " INNER JOIN kmpSubspace s ON s.ID = vu.Subspace" +
                    " INNER JOIN" +
                    "  (SELECT vu.Guid, MAX(s.LastTick) AS LastTick" +
                    "  FROM kmpVesselUpdate vu" +
                    "  INNER JOIN kmpSubspace s ON s.ID = vu.Subspace" +
                    "  GROUP BY vu.Guid) t ON t.Guid = vu.Guid AND t.LastTick = s.LastTick;",
                    record =>
                    {
                        results.Add(new Tuple<byte[], byte[], Guid>(GetDataReaderBytes(record, 0), GetDataReaderBytes(record, 1), record.GetGuid(2)));
                    });

                foreach (Tuple<byte[], byte[], Guid> result in results)
                {
                    KMPVesselUpdate vessel_update = (KMPVesselUpdate)ByteArrayToObject(result.Item1);
                    if (vessel_update.tick < cutOffTick)
                    {
                        byte[] configNodeBytes = result.Item2;
                        string s = Encoding.UTF8.GetString(configNodeBytes, 0, configNodeBytes.Length);
                        if (s.IndexOf("type") > 0 && s.Length > s.IndexOf("type") + 20)
                        {
                            if (s.Substring(s.IndexOf("type"), 20).Contains("Debris"))
                            {
                                Database.ExecuteNonQuery("UPDATE kmpVessel SET Destroyed = 1 WHERE Guid = @guid",
                                    "guid", result.Item3);
                                clearedCount++;
                            }
                        }
                    }
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
                long lastMessageBreak;
                bool shouldOptimizeQueue = false;
                Log.Debug("Starting disconnect thread");

                while (true)
                {
                    lastMessageBreak = stopwatch.ElapsedMilliseconds;
                    //Handle received messages
                    while (clientMessageQueue.Count > 0)
                    {
                        ClientMessage message;

                        if (clientMessageQueue.TryDequeue(out message))
                            handleMessage(message.client, message.id, message.data);
                        else
                            break;

                        if (stopwatch.ElapsedMilliseconds > lastMessageBreak + MESSAGE_HANDLE_TIMEOUT)
                        {
                            Log.Debug("Warning: Server lag detected. Optimizing queue.");
                            shouldOptimizeQueue = true;
                            break;
                        }
                    }

                    if (shouldOptimizeQueue)
                    {
                        optimizeIncomingMessageQueue();
                        shouldOptimizeQueue = false;
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

        private void optimizeIncomingMessageQueue()
        {
            if (clientMessageQueue == null)
            {
                Log.Debug("Client message queue is null");
                return;
            }
            long optimizeTime = stopwatch.ElapsedMilliseconds;
            Queue<ClientMessage> tempQueue = new Queue<ClientMessage>(clientMessageQueue);
            ConcurrentQueue<ClientMessage> newQueue = new ConcurrentQueue<ClientMessage>();
            List<Guid> vesselsInQueue = new List<Guid>();
            //Process the queue in reverse, We want to keep the newest updates.
            tempQueue.Reverse();
            foreach (ClientMessage message in tempQueue)
            {
                if (message.id == KMPCommon.ClientMessageID.PRIMARY_PLUGIN_UPDATE || message.id == KMPCommon.ClientMessageID.SECONDARY_PLUGIN_UPDATE)
                {
                    if (message.data == null)
                    {
                        //Skip empty messages (this shouldn't happen so let's log them)
                        Log.Debug("Empty vessel update detected!");
                        continue;
                    }
                    KMPVesselUpdate vessel_update = ByteArrayToObject<KMPVesselUpdate>(message.data);
                    if (vessel_update == null)
                    {
                        //Status only updates
                        newQueue.Enqueue(message);
                        continue;
                    }
                    if (vessel_update.protoVesselNode != null)
                    {
                        //Keep protovessel messages
                        newQueue.Enqueue(message);
                    }
                    else
                    {
                        //Keep the latest non-protovessel update.
                        if (!vesselsInQueue.Contains(vessel_update.kmpID))
                        {
                            vesselsInQueue.Add(vessel_update.kmpID);
                            newQueue.Enqueue(message);
                        }
                    }
                }
                else
                {
                    //Keep all non-vessel messages.
                    newQueue.Enqueue(message);
                }
            }
            //Flip it back to the original order
            newQueue.Reverse();
            Log.Debug("Optimize took " + (stopwatch.ElapsedMilliseconds - optimizeTime) + "ms, old length: " + clientMessageQueue.Count + ", new length: " + newQueue.Count);
            clientMessageQueue = newQueue;
        }

        private void sendOutgoingMessages()
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("en-GB");
            try
            {
                while (true)
                {
                    try
                    {
                        foreach (var client in clients.ToList().Where(c => c.isValid).ToList())
                        {
                            client.sendOutgoingMessages();
                        }
                    }
                    catch (NullReferenceException e)
                    {
                        Log.Debug("Caught NRE in sendOutgoingMessages: {0}" + e.StackTrace);
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
                        Database.ExecuteNonQuery("UPDATE kmpVessel SET Active = 0 WHERE Guid = @guid",
                            "guid", cl.currentVessel);
                    }
                    catch { }
                    sendVesselStatusUpdateToAll(cl, cl.currentVessel);
                }

                clearEmptySubspace(cl.currentSubspaceID);

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
                if (settings.ipBinding == "0.0.0.0" && settings.hostIPv6 == true)
                {
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

                    Client client = clients.ToList().Where(c => c.isReady && c.clientIndex == sender_index).FirstOrDefault();
                    if (client != null)
                    {
                        if ((currentMillisecond - client.lastUDPACKTime) > UDP_ACK_THROTTLE)
                        {
                            //Acknowledge the client's message with a TCP message
                            client.queueOutgoingMessage(KMPCommon.ServerMessageID.UDP_ACKNOWLEDGE, null);
                            client.lastUDPACKTime = currentMillisecond;
                            client.updateReceiveTimestamp();
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
            //Write the receive time for NTP sync messages.
            if (message.id == KMPCommon.ClientMessageID.SYNC_TIME)
            {
                byte[] rewriteMessage = new byte[16]; //Holds the client send time and the server receive time.
                message.data.CopyTo(rewriteMessage, 0);
                BitConverter.GetBytes(DateTime.UtcNow.Ticks).CopyTo(rewriteMessage, 8);
                message.data = rewriteMessage;
            }
            clientMessageQueue.Enqueue(message);
        }

        private KMPCommon.ClientMessageID[] AllowNullDataMessages = { KMPCommon.ClientMessageID.SCREEN_WATCH_PLAYER, KMPCommon.ClientMessageID.CONNECTION_END, KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_FLIGHT, KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_GAME, KMPCommon.ClientMessageID.SYNC_TIME };
        private KMPCommon.ClientMessageID[] AllowClientNotReadyMessages = { KMPCommon.ClientMessageID.HANDSHAKE, KMPCommon.ClientMessageID.TEXT_MESSAGE, KMPCommon.ClientMessageID.SCREENSHOT_SHARE, KMPCommon.ClientMessageID.CONNECTION_END, KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_FLIGHT, KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_GAME, KMPCommon.ClientMessageID.PING, KMPCommon.ClientMessageID.UDP_PROBE, KMPCommon.ClientMessageID.WARPING, KMPCommon.ClientMessageID.SSYNC, KMPCommon.ClientMessageID.SYNC_TIME };

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
                            cl.queueOutgoingMessage(KMPCommon.ServerMessageID.PING_REPLY, data);
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

                        case KMPCommon.ClientMessageID.SYNC_TIME:
                            HandleTimeSync(cl, data);
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
                Database.ExecuteReader("SELECT ss1.ID FROM kmpSubspace ss1 LEFT JOIN kmpSubspace ss2 ON ss1.LastTick < ss2.LastTick WHERE ss2.ID IS NULL;",
                    record => subspaceID = record.GetInt32(0));
            }
            cl.currentSubspaceID = subspaceID;
            Log.Info("{0} sync request to subspace {1}", cl.username, subspaceID);
            sendSubspace(cl, true);
        }

        private void HandleTimeSync(Client cl, byte[] data)
        {
            //Message format: clientsendtick(8), serverreceivetick(8), serversendtick(8). The server send tick gets added during actual sending.
            byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.SYNC_TIME, data); //This has already been rewritten in the queueClientMessage.
            cl.queueOutgoingMessage(message_bytes); //This is still re-written during the actual send.
            Log.Debug("{0} time sync request", cl.username);
        }

        private void HandleWarping(Client cl, byte[] data)
        {
            float rate = BitConverter.ToSingle(data, 0);
            double newsubspacetick = BitConverter.ToDouble(data, 4);
            if (cl.warping)
            {
                if (rate < 1.1f)
                {
                    //stopped warping-create subspace & add player to it

                    Database.ExecuteNonQuery("INSERT INTO kmpSubspace (LastTick) VALUES (@tick);",
                        "tick", 0d.ToString("0.0").Replace(",", "."));

                    int newSubspace = -1;

                    Database.ExecuteReader(settings.useMySQL ?
                        "SELECT LAST_INSERT_ID();" :
                        "SELECT last_insert_rowid();",
                        record => newSubspace = record.GetInt32(0));
                    
                    cl.currentSubspaceID = newSubspace;
                    Log.Debug("Adding new time sync data for subspace {0}", newSubspace);
                    subSpaceMasterTick.Add(cl.currentSubspaceID, newsubspacetick);
                    subSpaceMasterTime.Add(cl.currentSubspaceID, DateTime.UtcNow.Ticks);
                    subSpaceMasterSpeed.Add(cl.currentSubspaceID, 1f);
                    cl.warping = false;
                    sendSubspace(cl, true, true);
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
                Database.ExecuteReader("SELECT LastTick FROM kmpSubspace WHERE ID = @id;",
                    record => lastSubspaceTick = record.GetDouble(0),
                    "id", cl.currentSubspaceID.ToString("D"));

                Database.ExecuteNonQuery("UPDATE kmpSubspace SET LastTick = @tick WHERE ID = @subspaceID AND LastTick < @tick;",
                    "tick", incomingTick.ToString("0.0").Replace(",", "."),
                    "subspaceID", cl.currentSubspaceID.ToString("D"));

                if (lastSubspaceTick > 100d) sendHistoricalVesselUpdates(cl.currentSubspaceID, incomingTick, lastSubspaceTick);
                cl.averageWarpRate = BitConverter.ToSingle(data, 8);
                processClientAverageWarpRates(cl.currentSubspaceID);
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
                    Database.ExecuteNonQuery("UPDATE kmpVessel SET Active = 0 WHERE Guid = @id",
                        "id", cl.currentVessel);
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
                    object result = Database.ExecuteScalar("SELECT ID FROM kmpScenarios WHERE PlayerID = @playerID AND Name = @name;",
                        "playerID", cl.playerID,
                        "name", scenario_update.name);
                    if (result == null)
                    {
                        Database.ExecuteNonQuery("INSERT INTO kmpScenarios (PlayerID, Name, Tick, UpdateMessage)" +
                            " VALUES (@playerID, @name, @tick, @updateMessage);",
                        "playerID", cl.playerID,
                        "name", scenario_update.name,
                        "tick", scenario_update.tick.ToString("0.0").Replace(",", "."),
                        "updateMessage", data);
                    }
                    else
                    {
                        Database.ExecuteNonQuery("UPDATE kmpScenarios SET Tick = @tick, UpdateMessage = @updateMessage WHERE ID = @id",
                            "id", Convert.ToInt32(result),
                            "tick", scenario_update.tick.ToString("0.0").Replace(",", "."),
                            "updateMessage", data);
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
            int name_taken = Convert.ToInt32(
                Database.ExecuteScalar("SELECT COUNT(*) FROM kmpPlayer WHERE Name = @username AND Guid != @guid;",
                "username", username_lower,
                "guid", guid));
            if (name_taken > 0)
            {
                //Disconnect the player
                markClientForDisconnect(cl, "Your username is already claimed by an existing user.");
                Log.Info("Rejected client due to duplicate username w/o matching guid: {0}", username);
                return;
            }
            int player_exists = Convert.ToInt32(Database.ExecuteScalar("SELECT COUNT(*) FROM kmpPlayer WHERE Guid = @guid AND Name LIKE @username",
                "username", username_lower,
                "guid", guid));

            if (player_exists == 0) //New user
            {
                Log.Info("New user");
                Database.ExecuteNonQuery("INSERT INTO kmpPlayer (Name, Guid) VALUES (@username,@guid);",
                    "username", username_lower,
                    "guid", guid);
            }
            int playerID = Convert.ToInt32(Database.ExecuteScalar("SELECT ID FROM kmpPlayer WHERE Guid = @guid AND Name LIKE @username;",
                "username", username_lower,
                "guid", guid));

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
            Database.ExecuteReader("SELECT  vu.UpdateMessage, v.Private" +
                " FROM kmpVesselUpdateHistory vu" +
                " INNER JOIN kmpVessel v ON v.Guid = vu.Guid" +
                " INNER JOIN (SELECT Guid, MAX(Tick) Tick" +
                "   FROM kmpVesselUpdateHistory" +
                "   WHERE Tick > @lastTick AND Tick < @atTick" +
                "   GROUP BY Guid) t ON t.Guid = vu.Guid AND t.Tick = vu.Tick" +
                " WHERE vu.Subspace != @toSubspace;",
                record => {
                    KMPVesselUpdate vessel_update = (KMPVesselUpdate)ByteArrayToObject(GetDataReaderBytes(record, 0));
                    vessel_update.state = State.ACTIVE;
                    vessel_update.isPrivate = record.GetBoolean(1);
                    vessel_update.isMine = false;
                    vessel_update.relTime = RelativeTime.FUTURE;
                    byte[] update = ObjectToByteArray(vessel_update);

                    foreach (var client in clients.ToList().Where(c => c.currentSubspaceID == toSubspace && !c.warping && c.currentVessel != vessel_update.kmpID))
                    {
                        sendVesselMessage(client, update);
                    }
                },
                "lastTick", lastTick.ToString("0.0").Replace(",", "."),
                "atTick", atTick.ToString("0.0").Replace(",", "."),
                "toSubspace", toSubspace);

            Database.ExecuteNonQuery("DELETE FROM kmpVesselUpdateHistory WHERE Tick < (SELECT MIN(LastTick) FROM kmpSubspace);");
        }

        private void sendSubspace(Client cl, bool excludeOwnActive = false, bool sendTimeSync = true)
        {
            if (!cl.warping)
            {
                if (sendTimeSync) sendSubspaceSync(cl);
                Log.Activity("Sending all vessels in current subspace for " + cl.username);

                double subTick = Convert.ToDouble(Database.ExecuteScalar("SELECT LastTick FROM kmpSubspace WHERE ID = @curSubspaceID;",
                    "curSubspaceID", cl.currentSubspaceID.ToString("D")));

                Database.ExecuteReader("SELECT vu.UpdateMessage, v.ProtoVessel, v.Private, v.OwnerID" +
                    " FROM kmpVesselUpdate vu" +
                    " INNER JOIN kmpVessel v ON v.Guid = vu.Guid AND (v.Destroyed IS NULL OR v.Destroyed > @subTick)" +
                    " INNER JOIN kmpSubspace s ON s.ID = vu.Subspace" +
                    " INNER JOIN" +
                    "  (SELECT vu.Guid, MAX(s.LastTick) AS LastTick" +
                    "  FROM kmpVesselUpdate vu" +
                    "  INNER JOIN kmpSubspace s ON s.ID = vu.Subspace AND s.LastTick <= @subTick" +
                    "  GROUP BY vu.Guid) t ON t.Guid = vu.Guid AND t.LastTick = s.LastTick" +
                    (excludeOwnActive ? " AND NOT v.Guid = @curVessel;" : ";"),
                    record => {
                        KMPVesselUpdate vessel_update = (KMPVesselUpdate)ByteArrayToObject(GetDataReaderBytes(record, 0));
                        ConfigNode protoVessel = (ConfigNode)ByteArrayToObject(GetDataReaderBytes(record, 1));
                        vessel_update.state = State.INACTIVE;
                        vessel_update.isPrivate = record.GetBoolean(2);
                        vessel_update.isMine = record.GetInt32(3) == cl.playerID;
                        vessel_update.setProtoVessel(protoVessel);
                        vessel_update.isSyncOnlyUpdate = true;
                        vessel_update.distance = 0;
                        byte[] update = ObjectToByteArray(vessel_update);
                        sendVesselMessage(cl, update);
                    },
                    "subTick", subTick.ToString("0.0").Replace(",", "."),
                    "curVessel", cl.currentVessel); // NOTE: Extra parameters shouldn't break functionality, but it might.
                
                if (sendTimeSync) sendScenarios(cl);
                sendSyncCompleteMessage(cl);
            }
        }

        private void sendSubspaceSync(Client cl, bool sendSync = true)
        {
            double tick = 0d;

            Database.ExecuteReader("SELECT LastTick FROM kmpSubspace WHERE ID = @curSubspaceID;",
                record => tick = record.GetDouble(0),
                "curSubspaceID", cl.currentSubspaceID.ToString("D"));
            
            if (sendSync)
            {
                sendSyncMessage(cl, tick);
                cl.lastTick = tick;
            }
        }

        private void sendServerSync(Client cl)
        {
            if (!cl.warping)
            {
                double tick = 0d; int subspace = 0;
                Database.ExecuteReader("SELECT ss1.ID, ss1.LastTick FROM kmpSubspace ss1 LEFT JOIN kmpSubspace ss2 ON ss1.LastTick < ss2.LastTick WHERE ss2.ID IS NULL;",
                    record =>
                    {
                        subspace = record.GetInt32(0);
                        tick = record.GetDouble(1);
                    });

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
                        sb.Append("!ping - Shows current server latency\n");
                        sb.Append("!ntp - Displays NTP-sync status\n");
                        sb.Append("!whereami - Displays server connection information\n");
                        if (isAdmin(cl.username))
                        {
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
                        if (isAdmin(cl.username))
                        {
                            String command = message_lower.Substring(KMPCommon.RCON_COMMAND.Length + 1);
                            Log.Info("RCON from client {0} (#{1}): {2}", cl.username, cl.clientIndex, command);
                            processCommand("/" + command);
                        }
                        else
                        {
                            sendTextMessage(cl, "You are not an admin!");
                        }

                        return;
                    }
                }

                if (settings.profanityFilter)
                    message_text = WashMouthWithSoap(message_text);

                string full_message = string.Format("{2}<{0}> {1}", cl.username, message_text, (isAdmin(cl.username) ? "[" + KMPCommon.ADMIN_MARKER + "] " : ""));

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

            byte[] data_bytes = new byte[version_bytes.Length + 24 + kmpModControl.Length + 1];

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

            //Write number of ships in initial sync
            KMPCommon.intToBytes(countShipsInDatabase()).CopyTo(data_bytes, 16 + version_bytes.Length);

            KMPCommon.intToBytes(kmpModControl.Length).CopyTo(data_bytes, 20 + version_bytes.Length);
            kmpModControl.CopyTo(data_bytes, 24 + version_bytes.Length);

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
                            object result = Database.ExecuteScalar("SELECT kmpVessel.Subspace FROM kmpVessel LEFT JOIN kmpSubspace ON kmpSubspace.ID = kmpVessel.Subspace" +
                                " WHERE Guid = @kmpID ORDER BY kmpSubspace.LastTick DESC LIMIT 1;",
                                "kmpID", vessel_update.kmpID);

                            if (result == null)
                            {
                                Log.Info("New vessel {0} from {1} added to universe", vessel_update.kmpID, cl.username);

                                Database.ExecuteNonQuery("INSERT INTO kmpVessel (Guid, GameGuid, OwnerID, Private, Active, ProtoVessel, Subspace)" +
                                    " VALUES (@kmpID,@ves_up_ID, @playerID, @ves_up_isPrivate, @ves_up_state, @protoVessel, @curSubspaceID);",
                                    "protoVessel", protoVesselBlob,
                                    "kmpID", vessel_update.kmpID,
                                    "ves_up_ID", vessel_update.id,
                                    "playerID", cl.playerID,
                                    "ves_up_isPrivate", Convert.ToInt32(vessel_update.isPrivate),
                                    "ves_up_state", Convert.ToInt32(vessel_update.state == State.ACTIVE),
                                    "curSubspaceID", cl.currentSubspaceID.ToString("D"));
                            }
                            else
                            {
                                int current_subspace = Convert.ToInt32(result);
                                if (current_subspace == cl.currentSubspaceID)
                                {
                                    Database.ExecuteNonQuery("UPDATE kmpVessel SET Private = @ves_up_isPrivate, Active = @ves_up_state, OwnerID = @playerID," +
                                        " ProtoVessel = @protoVessel WHERE Guid = @kmpID;",
                                        "protoVessel", protoVesselBlob,
                                        "ves_up_isPrivate", Convert.ToInt32(vessel_update.isPrivate),
                                        "ves_up_state", Convert.ToInt32(vessel_update.state == State.ACTIVE),
                                        "playerID", cl.playerID,
                                        "kmpID", vessel_update.kmpID);
                                }
                                else
                                {
                                    Database.ExecuteNonQuery("UPDATE kmpVessel SET Private = @ves_up_isPrivate, Active = @ves_up_state, OwnerID = @playerID," +
                                        " ProtoVessel = @protoVessel, Subspace = @curSubspace WHERE Guid = @kmpID;",
                                        "protoVessel", protoVesselBlob,
                                        "ves_up_isPrivate", Convert.ToInt32(vessel_update.isPrivate),
                                        "ves_up_state", Convert.ToInt32(vessel_update.state == State.ACTIVE),
                                        "playerID", cl.playerID,
                                        "curSubspace", cl.currentSubspaceID.ToString("D"),
                                        "kmpID", vessel_update.kmpID);

                                    clearEmptySubspace(cl.currentSubspaceID);
                                }
                            }

                            if (cl != null && cl.currentVessel != vessel_update.kmpID && cl.currentVessel != Guid.Empty)
                            {
                                try
                                {
                                    Database.ExecuteNonQuery("UPDATE kmpVessel SET Active = 0 WHERE Guid = @curVessel;",
                                        "curVessel", cl.currentVessel);
                                }
                                catch { }

                                sendVesselStatusUpdateToAll(cl, cl.currentVessel);
                            }

                            cl.currentVessel = vessel_update.kmpID;
                        }
                        else
                        {
                            //No protovessel
                            object result = Database.ExecuteScalar("SELECT kmpVessel.Subspace FROM kmpVessel LEFT JOIN kmpSubspace ON kmpSubspace.ID = kmpVessel.Subspace" +
                                " WHERE Guid = @kmpID ORDER BY kmpSubspace.LastTick DESC LIMIT 1;",
                                "kmpID", vessel_update.kmpID);
                            if (result != null)
                            {
                                int current_subspace = Convert.ToInt32(result);
                                if (current_subspace == cl.currentSubspaceID)
                                {
                                    Database.ExecuteNonQuery("UPDATE kmpVessel SET Private = @ves_up_isPrivate, Active = @ves_up_state, OwnerID = @playerID" +
                                        " WHERE Guid = @kmpID;",
                                        "ves_up_isPrivate", Convert.ToInt32(vessel_update.isPrivate),
                                        "ves_up_state", Convert.ToInt32(vessel_update.state == State.ACTIVE),
                                        "playerID", cl.playerID,
                                        "kmpID", vessel_update.kmpID);
                                }
                                else
                                {
                                    Database.ExecuteNonQuery("UPDATE kmpVessel SET Private = @ves_up_isPrivate, Active = @ves_up_state, OwnerID = @playerID," +
                                        " Subspace = @curSubspace WHERE Guid = @kmpID;",
                                        "ves_up_isPrivate", Convert.ToInt32(vessel_update.isPrivate),
                                        "ves_up_state", Convert.ToInt32(vessel_update.state == State.ACTIVE),
                                        "playerID", cl.playerID,
                                        "curSubspace", cl.currentSubspaceID.ToString("D"),
                                        "kmpID", vessel_update.kmpID);

                                    clearEmptySubspace(cl.currentSubspaceID);
                                }
                            }

                            if (cl != null && cl.currentVessel != vessel_update.kmpID && cl.currentVessel != Guid.Empty)
                            {
                                try
                                {
                                    Database.ExecuteNonQuery("UPDATE kmpVessel SET Active = 0 WHERE Guid = @curVessel",
                                        "curVessel", cl.currentVessel);
                                }
                                catch { }

                                sendVesselStatusUpdateToAll(cl, cl.currentVessel);
                            }

                            cl.currentVessel = vessel_update.kmpID;
                        }

                        //Store update
                        storeVesselUpdate(data, cl, vessel_update.kmpID, vessel_update.tick);

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
                            Database.ExecuteReader("SELECT kmpVessel.OwnerID, kmpVessel.Active FROM kmpVessel LEFT JOIN kmpSubspace" +
                                " ON kmpSubspace.ID = kmpVessel.Subspace WHERE Guid = @kmpID" +
                                " ORDER BY kmpSubspace.LastTick DESC LIMIT 1;",
                                record => {
                                    OwnerID = record.GetInt32(0);
                                    active = record.GetBoolean(1);
                                },
                                "kmpID", vessel_update.kmpID);                           

                            if (!active || OwnerID == cl.playerID) //Inactive vessel or this player was last in control of it
                            {
                                if (vessel_update.getProtoVesselNode() != null)
                                {
                                    //Store included protovessel, update subspace
                                    byte[] protoVesselBlob = ObjectToByteArray(vessel_update.getProtoVesselNode());
                                    Database.ExecuteNonQuery("UPDATE kmpVessel SET ProtoVessel = @protoVessel, Subspace = @curSubspace WHERE Guid = @kmpID;",
                                        "protoVessel", protoVesselBlob,
                                        "curSubspace", cl.currentSubspaceID.ToString("D"),
                                        "kmpID", vessel_update.kmpID);
                                }
                                if (OwnerID == cl.playerID)
                                {
                                    //Update Active status
                                    Database.ExecuteNonQuery("UPDATE kmpVessel SET Active = 0 WHERE Guid = @kmpID;",
                                        "kmpID", vessel_update.kmpID);

                                    sendVesselStatusUpdateToAll(cl, vessel_update.kmpID);
                                }
                                //No one else is controlling it, so store the update
                                storeVesselUpdate(data, cl, vessel_update.kmpID, vessel_update.tick, true);
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

        private void storeVesselUpdate(byte[] updateBlob, Client cl, Guid kmpID, double tick, bool isSecondary = false)
        {
            Database.ExecuteNonQuery("DELETE FROM kmpVesselUpdate WHERE Guid = @kmpID AND Subspace = @curSubspace;" +
                " INSERT INTO kmpVesselUpdate (Guid, Subspace, UpdateMessage)" +
                " VALUES (@kmpID, @curSubspace ,@update);" +
                (isSecondary ? "" :  ("INSERT INTO kmpVesselUpdateHistory (Guid, Subspace, Tick, UpdateMessage)" +
                " VALUES (@kmpID, @curSubspace, @ves_tick, @update);")),
                // Params
                "update", updateBlob,
                "kmpID", kmpID,
                "curSubspace", cl.currentSubspaceID.ToString("D"),
                "ves_tick", tick.ToString("0.0").Replace(",", "."));
        }

        private bool checkVesselDestruction(KMPVesselUpdate vessel_update, Client cl)
        {
            try
            {
                if (!recentlyDestroyed.ContainsKey(vessel_update.kmpID) || (recentlyDestroyed[vessel_update.kmpID] + 1500L) < currentMillisecond)
                {
                    Database.ExecuteNonQuery("UPDATE kmpVessel SET Destroyed = @ves_up_destroyed WHERE Guid = @kmpID AND (@ves_up_destroyed IS NULL OR Destroyed IS NULL OR Destroyed > @ves_up_destroyed);",
                        "ves_up_destroyed", vessel_update.situation == Situation.DESTROYED
                        ? vessel_update.tick.ToString("0.0").Replace(",", ".") : null,
                        "kmpID", vessel_update.kmpID);

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
          
            Database.ExecuteReader("SELECT vu.UpdateMessage, v.ProtoVessel, v.Private, v.OwnerID, v.Active" +
                " FROM kmpVesselUpdate vu" +
                " INNER JOIN kmpVessel v ON v.Guid = vu.Guid" +
                " WHERE vu.Subspace = @curSubspace AND v.Guid = @vessel;",
                record => {
                    KMPVesselUpdate vessel_update = (KMPVesselUpdate)ByteArrayToObject(GetDataReaderBytes(record, 0));
                    ConfigNode protoVessel = (ConfigNode)ByteArrayToObject(GetDataReaderBytes(record, 1));
                    vessel_update.isPrivate = record.GetBoolean(2);
                    vessel_update.isMine = record.GetInt32(3) == cl.playerID;
                    if (record.GetBoolean(4))
                        vessel_update.state = State.ACTIVE;
                    else
                        vessel_update.state = State.INACTIVE;
                    vessel_update.setProtoVessel(protoVessel);
                    byte[] update = ObjectToByteArray(vessel_update);
                    sendVesselMessage(cl, update);
                },
                "curSubspace", cl.currentSubspaceID.ToString("D"),
                "vessel", vessel);
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
            if (cl.hasReceivedScenarioModules) return;
            cl.hasReceivedScenarioModules = true;
            Database.ExecuteReader("SELECT UpdateMessage FROM kmpScenarios WHERE PlayerID = @playerID" +
                //Only include career ScenarioModules if game server is set to career mode
                (settings.gameMode != 1 ? " AND Name NOT IN ('ResearchAndDevelopment','ProgressTracking')" : ""),
                record => {
                    byte[] data = GetDataReaderBytes(record, 0);
                    Log.Activity("Sending scenario update to player {0}", cl.username);
                    sendScenarioMessage(cl, data);
                },
                "playerID", cl.playerID);

        }

        private void sendSyncMessage(Client cl, double tick)
        {
            double subspaceTick = tick;
            float subspaceSpeed = 1f;
            long subspaceTime = DateTime.UtcNow.Ticks;
            if (subSpaceMasterTick.ContainsKey(cl.currentSubspaceID))
            {
                double tickOffset = (double)(subspaceTime - subSpaceMasterTime[cl.currentSubspaceID]) / 10000000; //The magic number that converts 100ns to seconds.
                subspaceTick = subSpaceMasterTick[cl.currentSubspaceID] + tickOffset;
                subspaceSpeed = subSpaceMasterSpeed[cl.currentSubspaceID];
                Log.Debug("Found entry: " + tickOffset + " offset for subspace " + cl.currentSubspaceID);
            }
            else
            {
                subSpaceMasterTick.Add(cl.currentSubspaceID, subspaceTick);
                subSpaceMasterTime.Add(cl.currentSubspaceID, subspaceTime);
                subSpaceMasterSpeed.Add(cl.currentSubspaceID, 1f);
                Log.Debug("Added entry for subspace " + cl.currentSubspaceID);
            }
            //Log.Info("Time sync for: " + cl.username);
            byte[] timesyncdata = new byte[20]; //double (8) subspace Tick, long (8) server time, float (4) subspace speed.
            BitConverter.GetBytes(subspaceTick).CopyTo(timesyncdata, 0);
            BitConverter.GetBytes(subspaceTime).CopyTo(timesyncdata, 8);
            BitConverter.GetBytes(subspaceSpeed).CopyTo(timesyncdata, 16);
            byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.SYNC, timesyncdata);
            cl.queueOutgoingMessage(message_bytes);
        }

        private void sendSyncMessageToSubspace(int subspaceID)
        {
            foreach (Client cl in clients)
            {
                if (cl.currentSubspaceID == subspaceID)
                {
                    sendSyncMessage(cl, subSpaceMasterTick[subspaceID]); //The tick is skewed correctly in sendSyncMessage.
                }
            }
        }

        private void clearEmptySubspace(int subspaceID)
        {
            bool emptySubspace = true;
            foreach (Client client in clients.ToList())
            {
                if (client != null && subspaceID == client.currentSubspaceID && client.tcpClient.Connected)
                {
                    emptySubspace = false;
                    break;
                }
            }
            if (emptySubspace)
            {
                double minTick = 2d;
                try
                {
                    minTick = Convert.ToDouble(Database.ExecuteScalar("SELECT MIN(s.LastTick) Tick FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID AND v.Destroyed IS NULL;"));

                }
                catch { }
                
                Database.ExecuteNonQuery("DELETE FROM kmpSubspace WHERE ID = @id AND LastTick < @minTick;",
                    "id", subspaceID.ToString("D"),
                    "minTick", minTick.ToString("0.0"));
            }
        }

        private void processClientAverageWarpRates(int subspaceID)
        {
            if (subSpaceLastRateCheck.ContainsKey(subspaceID))
            {
                if (currentMillisecond < subSpaceLastRateCheck[subspaceID] + 30000) return; //Only check once every 30 seconds per subspace.
            }
            subSpaceLastRateCheck[subspaceID] = currentMillisecond;

            if (!subSpaceMasterSpeed.ContainsKey(subspaceID) || !subSpaceMasterTick.ContainsKey(subspaceID) || !subSpaceMasterSpeed.ContainsKey(subspaceID)) return; //Only works for locked subspaces

            int numberOfClientsInSubspace = 0;
            float subspaceWarpRateTotal = 0f;
            float subspaceMinWarpRate = 1f;
            foreach (Client cl in clients)
            {
                if (cl.currentSubspaceID == subspaceID)
                {
                    numberOfClientsInSubspace++;
                    subspaceWarpRateTotal += cl.averageWarpRate;
                    if (cl.averageWarpRate < subspaceMinWarpRate) subspaceMinWarpRate = cl.averageWarpRate;
                }
            }
            float subspaceAverageWarpRate = subspaceWarpRateTotal / numberOfClientsInSubspace; //Aka: The average warp rate of the subspace.
            float subspaceTargetRate = (subspaceAverageWarpRate + subspaceMinWarpRate) / 2; //Lets slow down to halfway between the average and slowest player.
            if (subspaceTargetRate > 1f) subspaceTargetRate = 1f; //Let's just not worry about rates above 0.95 times normal.
            if (subspaceTargetRate < 0.75f) subspaceTargetRate = 0.75f; //Let's set a lower bound to something still reasonable like 0.75f.
            float subspaceDiffRate = Math.Abs(subSpaceMasterSpeed[subspaceID] - subspaceTargetRate);
            if (subspaceDiffRate > 0.03f)
            { //Allow 3% tolerance
                Log.Debug("Subspace " + subspaceID + " relocked to " + subspaceTargetRate + "x speed.");
                long currenttime = DateTime.UtcNow.Ticks;
                double tickOffset = (double)(currenttime - subSpaceMasterTime[subspaceID]) / 10000000; //The magic number that converts 100ns to seconds.
                subSpaceMasterTick[subspaceID] = subSpaceMasterTick[subspaceID] + (tickOffset * subSpaceMasterSpeed[subspaceID]);
                subSpaceMasterTime[subspaceID] = currenttime;
                subSpaceMasterSpeed[subspaceID] = subspaceTargetRate;
                sendSyncMessageToSubspace(subspaceID);
            }
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

        private void sendScenarioMessage(Client cl, byte[] data)
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
            BitConverter.GetBytes(settings.safetyBubbleRadius).CopyTo(bytes, 12); //Safety bubble radius
            bytes[20] = inactiveShipsPerClient; //Inactive ships per client
            bytes[21] = Convert.ToByte(settings.cheatsEnabled);
            bytes[22] = Convert.ToByte(settings.allowPiracy);

            return bytes;
        }

        //Universe

        public void startDatabase()
        {
            
            Int32 version = 0;
            try
            {
                version = Convert.ToInt32(Database.ExecuteScalar("SELECT version FROM kmpInfo"));
            }
            catch { Log.Info("Missing (or bad) universe database file."); }
            finally
            {
                if (version > 0 && version < UNIVERSE_VERSION)
                {
                    Log.Info("Database version {0}, current version is {1}.", version, UNIVERSE_VERSION);
                    if (version == 1)
                    {
                        //Upgrade old universe to version 2
                        Log.Info("Upgrading universe database...");
                        Database.ExecuteNonQuery(
                        "CREATE INDEX IF NOT EXISTS kmpVesselIdxGuid on kmpVessel(Guid);" +
                            "CREATE INDEX IF NOT EXISTS kmpVesselUpdateIdxGuid on kmpVesselUpdate(guid);" +
                            "CREATE INDEX IF NOT EXISTS kmpVesselUpdateHistoryIdxTick on kmpVesselUpdateHistory(Tick);");
                        version = 2;
                    }

                    if (version == 2)
                    {
                        //Upgrade old universe to version 3
                        Log.Info("Upgrading universe database...");

                        Database.ExecuteReader("SELECT Guid FROM kmpPlayer;",
                            record =>
                            {
                                string old_guid = record.GetString(0);
                                Guid guid = Guid.Empty;
                                try
                                {
                                    guid = new Guid(old_guid);
                                }
                                catch
                                {
                                    //Already converted?
                                    try
                                    {
                                        guid = new Guid(System.Text.Encoding.ASCII.GetBytes(old_guid.Substring(0, 16)));
                                    }
                                    catch
                                    {
                                        guid = Guid.Empty;
                                    }
                                }
                                Database.ExecuteNonQuery("UPDATE kmpPlayer SET Guid = @guid WHERE Guid = @old_guid;",
                                    "guid", guid,
                                    "old_guid", old_guid);
                            });


                        Database.ExecuteReader("SELECT Guid, GameGuid FROM kmpVessel;",
                            record =>
                            {
                                string old_guid = record.GetString(0);
                                string old_guid2 = record.GetString(1);
                                Guid guid = Guid.Empty;
                                Guid guid2 = Guid.Empty;
                                try
                                {
                                    guid = new Guid(old_guid);
                                }
                                catch
                                {
                                    //Already converted?
                                    try
                                    {
                                        guid = new Guid(System.Text.Encoding.ASCII.GetBytes(old_guid.Substring(0, 16)));
                                    }
                                    catch
                                    {
                                        guid = Guid.Empty;
                                    }
                                }
                                try
                                {
                                    guid2 = new Guid(old_guid2);
                                }
                                catch
                                {
                                    //Already converted?
                                    try
                                    {
                                        guid = new Guid(System.Text.Encoding.ASCII.GetBytes(old_guid2.Substring(0, 16)));
                                    }
                                    catch
                                    {
                                        guid = Guid.Empty;
                                    }
                                }
                                Database.ExecuteNonQuery("UPDATE kmpVessel SET Guid = @guid, GameGuid = @guid2 WHERE Guid = @old_guid;",
                                    "guid", guid,
                                    "guid2", guid2,
                                    "old_guid", old_guid);
                            });
                        Database.ExecuteReader("SELECT Guid FROM kmpVesselUpdate;",
                            record =>
                            {
                                string old_guid = record.GetString(0);
                                Guid guid = Guid.Empty;
                                try
                                {
                                    guid = new Guid(old_guid);
                                }
                                catch
                                {
                                    //Already converted?
                                    try
                                    {
                                        guid = new Guid(System.Text.Encoding.ASCII.GetBytes(old_guid.Substring(0, 16)));
                                    }
                                    catch
                                    {
                                        guid = Guid.Empty;
                                    }
                                }
                                Database.ExecuteNonQuery("UPDATE kmpVesselUpdate SET Guid = @guid WHERE Guid = @old_guid;",
                                    "guid", guid,
                                    "old_guid", old_guid);
                            });
                        version = 3;
                    }

                    if (version == 3)
                    {
                        //Upgrade old universe to version 4
                        Log.Info("Upgrading universe database...");
                        Database.ExecuteNonQuery(String.Format("CREATE TABLE kmpScenarios (ID INTEGER PRIMARY KEY {0}, PlayerID INTEGER, Name NVARCHAR(100), Tick DOUBLE, UpdateMessage BLOB);" +
                            "CREATE INDEX kmpScenariosIdxPlayerID on kmpScenarios(PlayerID);", settings.useMySQL ? "AUTO_INCREMENT" : "AUTOINCREMENT"));
                        version = 4;
                    }

                    //NOTE: MySQL supported only as of UNIVERSE_VERSION 4+

                    if (version == 4)
                    {
                        //Upgrade old universe to version 5
                        Log.Info("Upgrading universe database...");
                        if (settings.useMySQL)
                        {
                            //v5 updates target MySQL databases only
                            Database.ExecuteNonQuery("ALTER TABLE kmpInfo ENGINE=MyISAM;" +
                                "ALTER TABLE kmpSubspace ENGINE=MyISAM;" +
                                "ALTER TABLE kmpPlayer ENGINE=MyISAM;" +
                                "ALTER TABLE kmpVessel ENGINE=MyISAM;" +
                                "ALTER TABLE kmpVesselUpdate ENGINE=MyISAM;" +
                                "ALTER TABLE kmpVesselUpdateHistory ENGINE=MyISAM;" +
                                "ALTER TABLE kmpScenarios ENGINE=MyISAM;");
                        }
                    }

                    Log.Info("Upgrading universe database to current version...");
                    if (settings.useMySQL)
                    {
                        //MySQL databases need the type changed to match v6 definition, SQLite doesn't allow us to alter tables and will happily store DOUBLEs in a BIT field anyway
                        
                        Database.ExecuteNonQuery("ALTER TABLE kmpVessel MODIFY Destroyed DOUBLE");
                    }
                    //Ensure old vessels get cleaned out
                    Database.ExecuteNonQuery("UPDATE kmpVessel SET Destroyed = NULL WHERE Destroyed != 1");
                    CleanDatabase();

                    Database.ExecuteNonQuery("UPDATE kmpInfo SET Version = @uni_version;",
                        "uni_version", UNIVERSE_VERSION);

                    Log.Info("Loading universe...");
                }
                else if (version != UNIVERSE_VERSION)
                {
                    Log.Info("Creating new universe...");
                    try
                    {
                        if (!settings.useMySQL)
                        {
                            if (File.Exists(DB_FILE))
                            {
                                File.Delete(DB_FILE);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Log.Debug("Error removing old database: " + e.Message);
                    }
                    Database.ExecuteNonQuery(String.Format("CREATE TABLE kmpInfo (Version INTEGER){3};" +
                        "CREATE TABLE kmpSubspace (ID INTEGER PRIMARY KEY {0}, LastTick DOUBLE){3};" +
                        "CREATE TABLE kmpPlayer (ID INTEGER PRIMARY KEY {0}, Name NVARCHAR(100), Guid CHAR({1})){3};" +
                        "CREATE TABLE kmpVessel (Guid CHAR({1}), GameGuid CHAR({1}), OwnerID INTEGER, Private BIT, Active BIT, ProtoVessel {2}, Subspace INTEGER, Destroyed DOUBLE){3};" +
                        "CREATE TABLE kmpVesselUpdate (ID INTEGER PRIMARY KEY {0}, Guid CHAR({1}), Subspace INTEGER, UpdateMessage {2}){3};" +
                        "CREATE TABLE kmpVesselUpdateHistory (Guid CHAR({1}), Subspace INTEGER, Tick DOUBLE, UpdateMessage {2}){3};" +
                        "CREATE TABLE kmpScenarios (ID INTEGER PRIMARY KEY {0}, PlayerID INTEGER, Name NVARCHAR(100), Tick DOUBLE, UpdateMessage {2}){3};" +
                        "CREATE INDEX kmpVesselIdxGuid on kmpVessel(Guid);" +
                        "CREATE INDEX kmpVesselUpdateIdxGuid on kmpVesselUpdate(guid);" +
                        "CREATE INDEX kmpVesselUpdateHistoryIdxTick on kmpVesselUpdateHistory(Tick);" +
                        "CREATE INDEX kmpScenariosIdxPlayerID on kmpScenarios(PlayerID);",
                                        settings.useMySQL ? "AUTO_INCREMENT" : "AUTOINCREMENT",
                                        settings.useMySQL ? 36 : 16,
                                        settings.useMySQL ? "LONGBLOB" : "BLOB",
                                        settings.useMySQL ? " ENGINE=MyISAM" : ""
                    ));

                    Database.ExecuteNonQuery("INSERT INTO kmpInfo (Version) VALUES (@uni_version);" +
                        "INSERT INTO kmpSubspace (LastTick) VALUES (100);",
                        "uni_version", UNIVERSE_VERSION);
                }
                else
                {
                    Log.Info("Loading universe...");
                }
            }

            if (!settings.useMySQL)
            {
                // Prep SQLite for zero-atmospheric presure
                Database.ExecuteNonQuery("VACUUM;");
            }

            Database.ExecuteNonQuery("UPDATE kmpVessel SET Active = 0;");
            Log.Info("Universe OK.");
        }

        /// <summary>
        /// Backs up the SQLlite database.
        /// If the current connection is MySQL, no backup is made.
        /// </summary>
        public void BackupDatabase()
        {
            if (settings.useMySQL) return;
            Log.Info("Backing up universe DB...");
            try
            {
                if (!File.Exists(DB_FILE))
                    throw new IOException();

                File.Copy(DB_FILE, DB_FILE + ".bak", true);
                Log.Debug("Successfully backed up database.");
            }
            catch (IOException)
            {
                Log.Error("Database does not exist.  Recreating.");
            }
            catch (Exception e)
            {
                Log.Error("Failed to backup DB: {0}", e.Message);
            }

            try
            {
                if (uncleanedBackups > settings.maxDirtyBackups) CleanDatabase();
                else uncleanedBackups++;
            }
            catch (Exception e)
            {
                Log.Error("Failed to backup database: {0} {1}", e.Message, e.StackTrace);

                Log.Info("Saving secondary copy of last backup.");
                File.Copy(DB_FILE + ".bak", DB_FILE + ".before_failure.bak", true);

                Log.Info("Press any key to quit - ensure database is valid or reset database before restarting server.");
                Console.ReadKey();
                Environment.Exit(0);
            }

        }

        public void CleanDatabase()
        {
            try
            {
                Log.Info("Attempting to optimize database...");

                uncleanedBackups = 0;

                if (clients != null && activeClientCount() > 0)
                {
                    //Get the oldest tick from any active player
                    double earliestClearTick = 2d;

                    string subspaceIDs = "";
                    foreach (Client client in clients)
                    {
                        subspaceIDs += (String.IsNullOrEmpty(subspaceIDs) ? "" : ",") + client.currentSubspaceID.ToString("D");
                    }

                    if (!String.IsNullOrEmpty(subspaceIDs))
                    {
                        earliestClearTick = Convert.ToDouble(Database.ExecuteScalar("SELECT MIN(LastTick) FROM kmpSubspace WHERE ID IN (@subspaceids);",
                            "subspaceids", subspaceIDs));
                    }

                    //Clear anything before that
                    double earliestClearSubspaceTick = Convert.ToDouble(Database.ExecuteScalar("SELECT MIN(s.LastTick) FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID AND v.Destroyed > @minTick;",
                        "minTick", earliestClearTick.ToString("0.0").Replace(",", ".")));

                    Database.ExecuteNonQuery("DELETE FROM kmpSubspace WHERE LastTick < @minSubTick;" +
                        " DELETE FROM kmpVesselUpdateHistory WHERE Tick < @minTick;" +
                        " DELETE FROM kmpVessel WHERE Destroyed < @minTick",
                    "minTick", earliestClearTick.ToString("0.0").Replace(",", "."),
                    "minSubTick", earliestClearSubspaceTick.ToString("0.0").Replace(",", "."));
                }
                else
                {
                    //Clear all but the latest subspace
                    double earliestClearSubspaceTick = Convert.ToDouble(
                        Database.ExecuteScalar("SELECT MIN(s.LastTick) FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID AND v.Destroyed IS NULL;"));
                    
                    Database.ExecuteNonQuery("DELETE FROM kmpSubspace WHERE LastTick < @minSubTick;" +
                        " DELETE FROM kmpVesselUpdateHistory;" +
                        " DELETE FROM kmpVessel WHERE Destroyed IS NOT NULL;" +
                        " DELETE FROM kmpVesselUpdate WHERE Guid NOT IN (SELECT Guid FROM kmpVessel);" +
                        " DELETE FROM kmpVesselUpdate WHERE ID IN (SELECT ID FROM (SELECT ID FROM kmpVesselUpdate vu" +
                        "  WHERE Subspace != (SELECT ID FROM kmpSubspace WHERE LastTick = (SELECT MAX(LastTick) FROM kmpSubspace" +
                        "  WHERE ID IN (SELECT Subspace FROM kmpVesselUpdate vu2 WHERE vu2.Guid = vu.Guid)))) a);",
                        "minSubTick", earliestClearSubspaceTick.ToString("0.0").Replace(",", "."));
                }

                if (!settings.useMySQL)
                {
                    lock (databaseVacuumLock)
                    {
                        Database.ExecuteNonQuery("VACUUM;");
                    }
                }

                Log.Info("Optimized in-memory universe database.");
            }
            catch (Exception ex)
            {
                Log.Error("Couldn't optimize database: {0}", ex.Message);
            }
        }

        public bool firstSubspaceIsPresentOrFutureOfSecondSubspace(int comparisonSubspace, int referenceSubspace)
        {
            if (comparisonSubspace == -1 || referenceSubspace == -1) return false;
            if (comparisonSubspace == referenceSubspace) return true;

            double refTime = 0d, compTime = 0d;
            refTime = Convert.ToDouble(
            Database.ExecuteScalar("SELECT LastTick FROM kmpSubspace WHERE ID = @refSubspace;",
                "refSubspace", referenceSubspace));

            compTime = Convert.ToDouble(
            Database.ExecuteScalar("SELECT LastTick FROM kmpSubspace WHERE ID = @compSubspace;",
                "compSubspace", comparisonSubspace));

            if (compTime < 1d || refTime < 1d) return true;

            return (compTime >= refTime);
        }

        private static byte[] GetDataReaderBytes(IDataRecord reader, int column)
        {
            int length = (int)reader.GetBytes(column, 0, null, 0, 0); //MySQL is apparently fussy about requesting too many bytes, so use exact length
            byte[] buffer = new byte[length];
            int fieldOffset = 0;
            using (MemoryStream stream = new MemoryStream())
            {
                while (fieldOffset < length)
                {
                    int bytesRead = (int)reader.GetBytes(column, (long)fieldOffset, buffer, 0, length - fieldOffset);
                    fieldOffset += bytesRead;
                    stream.Write(buffer, 0, bytesRead);
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
            Log.Info("/lockship [ID] [true/false] - Set ship as private or public.");
            Log.Info("/deleteship [ID] - Removes ship from universe.");
            Log.Info("/dekessler <mins> - Remove debris that has not been updated for at least <mins> minutes (in-game time) (If no <mins> value is specified, debris that is older than 30 minutes will be cleared)");
            Log.Info("/save - Backup universe");
            Log.Info("/reloadmodfile - Reloads the {0} file. Note that this will not recheck any currently logged in clients, only those joining", MOD_CONTROL_FILE);
            Log.Info("/setinfo [info] - Updates the server info seen on master server list");
            Log.Info("/motd [message] - Sets message of the day, leave blank for none");
            Log.Info("/rules [rules] - Sets server rules, leave blank for none");
            Log.Info("/say <-u username> [message] - Send a Server message <to specified user>");
            Log.Info("/help - Displays all commands in the server\n");
            Log.Info("/modgen - Auto-generate a KMPModControl.txt file using what you have placed in the server's 'Mods' directory.\n");

            // to add a new command to the command list just copy the Log.Info method and add how to use that command.
        }

        private void checkGhosts()
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("en-GB");
            Log.Debug("Starting ghost-check thread");
            while (true)
            {
                //Send periodic messages to the client so we can detect disconnected clients
                try
                {
                    foreach (Client cl in clients)
                    {
                        if (cl != null)
                        {
                            cl.queueOutgoingMessage(KMPCommon.ServerMessageID.KEEPALIVE, null);
                        }
                    }
                }
                catch (Exception e)
                {
                    Log.Debug("Exception in ghost thread: " + e.Message);
                }

                //Detect timed out connections
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