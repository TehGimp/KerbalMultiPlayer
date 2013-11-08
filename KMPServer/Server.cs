﻿//#define DEBUG_OUT
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
        public const long CLIENT_HANDSHAKE_TIMEOUT_DELAY = 18000;
        public const int GHOST_CHECK_DELAY = 30000;
        public const int SLEEP_TIME = 10;
        public const int MAX_SCREENSHOT_COUNT = 10000;
        public const int UDP_ACK_THROTTLE = 1000;
        public const int DATABASE_BACKUP_INTERVAL = 300000;

        public const float NOT_IN_FLIGHT_UPDATE_WEIGHT = 1.0f / 4.0f;
        public const int ACTIVITY_RESET_DELAY = 10000;

        public const String SCREENSHOT_DIR = "KMPScreenshots";
        public const string DB_FILE_CONN = "Data Source=KMP_universe.db";
        public const string DB_FILE = "KMP_universe.db";

        public const int UNIVERSE_VERSION = 2;

        public bool quit = false;
        public bool stop = false;

        public String threadExceptionStackTrace;
        public Exception threadException;

        public object threadExceptionLock = new object();
        public static object consoleWriteLock = new object();

        public Thread listenThread;
        public Thread commandThread;
        public Thread connectionThread;
        public Thread outgoingMessageThread;
        public Thread ghostCheckThread;

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
                relevant_player_count = flight_clients.Count + (clients.Count - flight_clients.Count) * NOT_IN_FLIGHT_UPDATE_WEIGHT;

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

            //Build the filename
            StringBuilder sb = new StringBuilder();
            sb.Append(SCREENSHOT_DIR);
            sb.Append('/');
            sb.Append(KMPCommon.filteredFileName(player));
            sb.Append(' ');
            sb.Append(System.DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss"));
            sb.Append(".png");

            //Write the screenshot to file
            String filename = sb.ToString();
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

            tcpListener = new TcpListener(IPAddress.Parse(settings.ipBinding), settings.port);
            listenThread.Start();

            try
            {
                udpClient = new UdpClient(settings.port);
                udpClient.BeginReceive(asyncUDPReceive, null);
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
                Log.Error("Error starting http server: " + e);
                Log.Error("Please try running the server as an administrator");
            }

            long last_backup_time = 0;

            while (!quit)
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

                if (currentMillisecond - last_backup_time > DATABASE_BACKUP_INTERVAL && (clients.Count > 0 || !backedUpSinceEmpty))
                {
                    if (clients.Count <= 0)
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

        }

        private void handleCommands()
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("en-GB");
            try
            {
                Boolean bRunning = true;
                while (bRunning)
                {
                    String input = Console.ReadLine().ToLower();
                    var parts = input.Split(new char[] { ' ' }, 2);
                    if (!parts[0].StartsWith("/")) { return; }
                    switch (parts[0])
                    {
                        case "/ban": banServerCommand(parts); break;
                        case "/clearclients": clearClientsServerCommand(); break;
                        case "/countclients": countServerCommand(); break;
                        case "/help": displayCommands(); break;
                        case "/kick": kickServerCommand(parts); break;
                        case "/listclients": listServerCommand(); break;
                        case "/quit":
                        case "/stop": quitServerCommand(parts); bRunning = false; break;
                        case "/save": saveServerCommand(); break;
                        case "/register": registerServerCommand(parts); break;
                        case "/update": updateServerCommand(input); break;
                        case "/unregister": unregisterServerCommand(parts); break;
                        case "/dekessler": dekesslerServerCommand(parts); break;
                        case "/countships": countShipsServerCommand(); break;
                        case "/listships": listShipsServerCommand(); break;
                        default: sendServerMessageToAll(input); break;
                    }
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

        private void countShipsServerCommand(bool bList = false)
        {
            SQLiteCommand cmd = universeDB.CreateCommand();
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
            SQLiteDataReader reader = cmd.ExecuteReader();
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

        //Ban specified user, by name, from the server
        private void banServerCommand(string[] parts)
        {
            int days = 365;

            if (parts.Length > 1)
            {
                String ban_name = parts[1];
                Guid guid = Guid.Empty;
                if (parts.Length == 3)
                {
                    days = Convert.ToInt32(parts[2]);
                }

                var userToBan = clients.Where(c => c.username.ToLower() == ban_name && c.isReady).FirstOrDefault();

                if (userToBan != null)
                {
                    markClientForDisconnect(userToBan, "You were banned from the server!");
                    guid = userToBan.guid;
                }

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

                if (!Guid.Empty.Equals(guid))
                {
                    SQLiteCommand cmd = universeDB.CreateCommand();
                    string sql = "UPDATE kmpPlayer SET Guid = @newGuid WHERE Guid = @guid;";
                    cmd.Parameters.AddWithValue("newGuid", Guid.NewGuid().ToString());
                    cmd.Parameters.AddWithValue("guid", guid);
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    Log.Info("Player '{0}' and all known aliases banned from server permanently. Use /unregister to allow this user to reconnect.", ban_name);
                }
                else
                {
                    Log.Info("Failed to locate player '{0}'.", ban_name);
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
            Log.Info("In-Game Clients: {0}", clients.Count);
            Log.Info("In-Flight Clients: {0}", flight_clients.Count);
        }

        //Kicks the specified user from the server
        private void kickServerCommand(String[] parts)
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
                Log.Info("Username '{0}' not found.", kick_name);
            }
        }

        //Lists the users currently connected
        private void listServerCommand()
        {
            //Display player list
            StringBuilder sb = new StringBuilder();
            if (clients.Count > 0)
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
            quit = true;
            if (parts[0] == "/stop")
                stop = true;

            //Disconnect all clients
            foreach (var c in clients.ToList())
            {
                disconnectClient(c, "Server is shutting down");
            }
            //No need to clean them all up, we're shutting down anyway
        }

        //Registers the specified username to the server
        private void registerServerCommand(String[] parts)
        {
            String[] args = parts.Skip(1).ToArray();
            if (args.Length == 2)
            {
                try
                {
                    Guid parser = new Guid(args[1]);
                    String guid = parser.ToString();
                    String username_lower = args[0].ToLower();

                    SQLiteCommand cmd = universeDB.CreateCommand();
                    string sql = "DELETE FROM kmpPlayer WHERE Name LIKE @username;" +
                        " INSERT INTO kmpPlayer (Name, Guid) VALUES (@username,@guid);";
                    cmd.Parameters.AddWithValue("username", username_lower);
                    cmd.Parameters.AddWithValue("guid", guid);
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    Log.Info("Player '{0}' added to player roster with token '{1}'.", args[0], args[1]);
                }
                catch (FormatException)
                {
                    Log.Info("Supplied token is invalid.");
                }
                catch (Exception)
                {
                    Log.Info("Registration failed, possibly due to a malformed /register command.");
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
        private void updateServerCommand(String input)
        {
            if (input.Length > 8 && input.Substring(0, 8) == "/update ")
            {
                String[] args = input.Substring(8, input.Length - 8).Split(' ');
                if (args.Length == 2)
                {
                    try
                    {
                        Guid parser = new Guid(args[1]);
                        String guid = parser.ToString();
                        String username_lower = args[0].ToLower();
                        SQLiteCommand cmd = universeDB.CreateCommand();
                        string sql = "UPDATE kmpPlayer SET Name=@username, Guid=@guid WHERE Name LIKE @username OR Guid = @guid;";
                        cmd.CommandText = sql;
                        cmd.Parameters.AddWithValue("username", username_lower);
                        cmd.Parameters.AddWithValue("guid", guid);
                        cmd.ExecuteNonQuery();
                        cmd.Dispose();
                        Log.Info("Updated roster with player '{0}' and token '{1}'.", args[0], args[1]);
                    }
                    catch (FormatException)
                    {
                        Log.Info("Supplied token is invalid.");
                    }
                    catch (Exception)
                    {
                        Log.Info("Update failed, possibly due to a malformed /update command.");
                    }
                }
                else
                {
                    Log.Info("Could not parse update command. Format is \"/update <username> <token>\"");
                }
            }
        }

        //Unregisters the specified username from the server
        private void unregisterServerCommand(String[] parts)
        {
            String dereg = parts[1];
            SQLiteCommand cmd = universeDB.CreateCommand();
            string sql = "DELETE FROM kmpPlayer WHERE Guid = @dereg OR Name LIKE @dereg;";
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("dereg", dereg);
            cmd.ExecuteNonQuery();
            cmd.Dispose();
            Log.Info("Players with name/token '{0}' removed from player roster.", dereg);
        }

        //Clears old debris
        private void dekesslerServerCommand(String[] parts)
        {
            int minsToKeep = 30;
            if (parts.Length >= 2)
            {
                String[] args = parts.Skip(1).ToArray();
                if (args.Length == 1)
                    minsToKeep = Convert.ToInt32(args[0]);
                else
                    Log.Info("Could not parse dekessler command. Format is \"/dekessler <mins>\"");
            }

            try
            {
                //Get latest tick & calculate cut-off
                SQLiteCommand cmd = universeDB.CreateCommand();
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
                SQLiteDataReader reader = cmd.ExecuteReader();

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
                                    SQLiteCommand cmd2 = universeDB.CreateCommand();
                                    string sql2 = "UPDATE kmpVessel SET Destroyed = 1 WHERE Guid = @guid";
                                    cmd2.CommandText = sql2;
                                    cmd2.Parameters.AddWithValue("guid", reader.GetGuid(2).ToString());
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
                                    client.universeSent = false;
                                }

                                if (client.activityLevel == Client.ActivityLevel.IN_GAME
                                    && (currentMillisecond - client.lastInGameActivityTime) > ACTIVITY_RESET_DELAY)
                                {
                                    client.activityLevel = Client.ActivityLevel.INACTIVE;
                                    changed = true;
                                    client.universeSent = false;
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

            if (tcp_client == null || !tcp_client.Connected || clients.Count >= settings.maxClients)
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

                //Only send the disconnect message if the client performed handshake successfully
                if (cl.receivedHandshake)
                {
                    Log.Info("Player #{0} {1} has disconnected: {2}", cl.playerID, cl.username, message);

                    StringBuilder sb = new StringBuilder();

                    //Build disconnect message
                    sb.Append("User ");
                    sb.Append(cl.username);
                    sb.Append(" has disconnected : " + message);

                    //Send the disconnect message to all other clients
                    sendServerMessageToAll(sb.ToString());

                    //Update the database
                    if (cl.currentVessel != Guid.Empty)
                    {
                        try
                        {
                            SQLiteCommand cmd = universeDB.CreateCommand();
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
                        SQLiteCommand cmd = universeDB.CreateCommand();
                        string sql = "DELETE FROM kmpSubspace WHERE ID = @id AND LastTick < (SELECT MIN(s.LastTick) FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID);";
                        cmd.CommandText = sql;
                        cmd.Parameters.AddWithValue("id", cl.currentSubspaceID.ToString("D"));
                        cmd.ExecuteNonQuery();
                        cmd.Dispose();
                    }

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
            cl.universeSent = false;

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
            if (clients.Count > 0) backedUpSinceEmpty = false;
        }

        public void clientActivityLevelChanged(Client cl)
        {
            Log.Activity(cl.username + " activity level is now " + cl.activityLevel);

            switch (cl.activityLevel)
            {
                case Client.ActivityLevel.IN_GAME:
                    if (flight_clients.Contains(cl)) flight_clients.Remove(cl);
                    break;

                case Client.ActivityLevel.IN_FLIGHT:
                    if (!flight_clients.Contains(cl)) flight_clients.Add(cl);
                    break;
            }

            sendServerSettingsToAll();
        }

        private void asyncUDPReceive(IAsyncResult result)
        {
            try
            {

                IPEndPoint endpoint = new IPEndPoint(IPAddress.Parse(settings.ipBinding), settings.port);
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
                        byte[] messageData = KMPCommon.Decompress(data);
                        if (messageData != null) handleMessage(client, id, messageData);
                        //Consider adding re-request here
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
                response_builder.Append(clients.Count);
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

        public void handleMessage(Client cl, KMPCommon.ClientMessageID id, byte[] data)
        {
            if (!cl.isValid || data == null)
            { return; }

            try
            {
                //Log.Info("Message id: " + id.ToString() + " from client: " + cl + " data: " + (data != null ? data.Length.ToString() : "0"));
                //Console.WriteLine("Message id: " + id.ToString() + " data: " + (data != null ? System.Text.Encoding.ASCII.GetString(data) : ""));

                UnicodeEncoding encoder = new UnicodeEncoding();

                switch (id)
                {
                    case KMPCommon.ClientMessageID.HANDSHAKE:
                        HandleHandshake(cl, data, encoder);
                        break;
                    case KMPCommon.ClientMessageID.PRIMARY_PLUGIN_UPDATE:
                    case KMPCommon.ClientMessageID.SECONDARY_PLUGIN_UPDATE:
                        HandePluginUpdate(cl, id, data);
                        break;
                    case KMPCommon.ClientMessageID.TEXT_MESSAGE:
                        if (!cl.isReady) { break; }
                        handleClientTextMessage(cl, encoder.GetString(data, 0, data.Length));
                        break;
                    case KMPCommon.ClientMessageID.SCREEN_WATCH_PLAYER:
                        if (!cl.isReady) { break; }
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

        private void HandleSSync(Client cl, byte[] data)
        {
            int subspaceID = KMPCommon.intFromBytes(data, 0);
            if (subspaceID == -1)
            {
                //Latest available subspace sync request	
                SQLiteCommand cmd = universeDB.CreateCommand();
                string sql = "SELECT ss1.ID FROM kmpSubspace ss1 LEFT JOIN kmpSubspace ss2 ON ss1.LastTick < ss2.LastTick WHERE ss2.ID IS NULL;";
                cmd.CommandText = sql;
                SQLiteDataReader reader = cmd.ExecuteReader();
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
                    SQLiteCommand cmd = universeDB.CreateCommand();
                    string sql = "INSERT INTO kmpSubspace (LastTick) VALUES (@tick);";
                    cmd.CommandText = sql;
                    cmd.Parameters.AddWithValue("tick", cl.lastTick.ToString("0.0").Replace(",", "."));
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    cmd = universeDB.CreateCommand();
                    sql = "SELECT last_insert_rowid();";
                    cmd.CommandText = sql;
                    SQLiteDataReader reader = cmd.ExecuteReader();
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
                    cl.lastTick = -1d;
                    sendSubspace(cl, false);
                    cl.warping = false;
                    Log.Activity(cl.username + " set to new subspace " + newSubspace);
                }
            }
            else
            {
                if (rate > 1.1f)
                {
                    cl.warping = true;
                    cl.currentSubspaceID = -1;
                    Log.Activity(cl.username + " is warping");
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
                SQLiteCommand cmd = universeDB.CreateCommand();
                string sql = "SELECT LastTick FROM kmpSubspace WHERE ID = @id;";
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("id", cl.currentSubspaceID.ToString("D"));
                SQLiteDataReader reader = cmd.ExecuteReader();

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
                        Log.Debug("Sending time-sync to " + cl.username + " current offset " + cl.syncOffset);
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
                    sql = "UPDATE kmpSubspace SET LastTick = " + incomingTick.ToString("0.0").Replace(",", ".") + " WHERE ID = " + cl.currentSubspaceID.ToString("D") + " AND LastTick < " + incomingTick.ToString("0.0").Replace(",", ".");
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    sendHistoricalVesselUpdates(cl.currentSubspaceID, incomingTick, lastSubspaceTick);
                }
            }
        }

        private void HandleActivityUpdateInGame(Client cl)
        {
            if (cl.activityLevel == Client.ActivityLevel.INACTIVE) sendServerSync(cl);
            if (cl.activityLevel == Client.ActivityLevel.IN_FLIGHT && cl.currentVessel != Guid.Empty)
            {
                try
                {
                    SQLiteCommand cmd = universeDB.CreateCommand();
                    string sql = "UPDATE kmpVessel SET Active = 0 WHERE Guid = @id";
                    cmd.CommandText = sql;
                    cmd.Parameters.AddWithValue("id", cl.currentVessel);
                    cmd.ExecuteNonQuery();
                    cmd.Dispose();
                }
                catch { }
                sendVesselStatusUpdateToAll(cl, cl.currentVessel);
                cl.universeSent = false;
            }
            cl.updateActivityLevel(Client.ActivityLevel.IN_GAME);
        }

        private void HandleActivityUpdateInFlight(Client cl)
        {
            if (cl.activityLevel == Client.ActivityLevel.IN_GAME && cl.isReady && !cl.universeSent)
            {
                cl.universeSent = true;
                sendSubspace(cl);
            }
            cl.updateActivityLevel(Client.ActivityLevel.IN_FLIGHT);
        }

        private void HandleShareCraftFile(Client cl, byte[] data, UnicodeEncoding encoder)
        {
            if (!(data.Length > 5 && (data.Length - 5) <= KMPCommon.MAX_CRAFT_FILE_BYTES)) { return; }

            //Read craft name length
            byte craft_type = data[0];
            int craft_name_length = KMPCommon.intFromBytes(data, 1);
            if (craft_name_length < data.Length - 5)
            {
                //Read craft name
                String craft_name = encoder.GetString(data, 5, craft_name_length);

                //Read craft bytes
                byte[] craft_bytes = new byte[data.Length - craft_name_length - 5];
                Array.Copy(data, 5 + craft_name_length, craft_bytes, 0, craft_bytes.Length);

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
                    case KMPCommon.CRAFT_TYPE_VAB:
                        sb.Append(" (VAB)");
                        break;

                    case KMPCommon.CRAFT_TYPE_SPH:
                        sb.Append(" (SPH)");
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

        private void HandePluginUpdate(Client cl, KMPCommon.ClientMessageID id, byte[] data)
        {
            if (cl.isReady)
            {
#if SEND_UPDATES_TO_SENDER
							sendPluginUpdateToAll(data, id == KMPCommon.ClientMessageID.SECONDARY_PLUGIN_UPDATE);
#else
                sendPluginUpdateToAll(data, id == KMPCommon.ClientMessageID.SECONDARY_PLUGIN_UPDATE, cl);
#endif
            }
        }

        private void HandleHandshake(Client cl, byte[] data, UnicodeEncoding encoder)
        {
            StringBuilder sb = new StringBuilder();

            //Read username
            Int32 username_length = KMPCommon.intFromBytes(data, 0);
            String username = encoder.GetString(data, 4, username_length);


            Int32 guid_length = KMPCommon.intFromBytes(data, 4 + username_length);
            int offset = 4 + username_length + 4;
            Guid guid = new Guid(encoder.GetString(data, offset, guid_length));
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

            //if (!accepted)
            //return;

            //Check if this player is new to universe
            SQLiteCommand cmd = universeDB.CreateCommand();
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
            sql = "SELECT COUNT(*) FROM kmpPlayer WHERE Guid = @guid";
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("guid", guid);
            Int32 player_exists = Convert.ToInt32(cmd.ExecuteScalar());
            cmd.Dispose();
            if (player_exists == 0) //New user
            {
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
            if (clients.Count == 2)
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
                sb.Append(clients.Count - 1);
                sb.Append(" other users on this server.");
                if (clients.Count > 1)
                {
                    sb.Append(" Enter !list to see them.");
                }
            }

            cl.username = username;
            cl.receivedHandshake = true;
            cl.guid = guid;
            cl.playerID = playerID;

            sendServerMessage(cl, sb.ToString());
            sendServerSettings(cl);

            Log.Info("{0} has joined the server using client version {1}", username, version);

            //Build join message
            //sb.Clear();
            sb.Remove(0, sb.Length);
            sb.Append("User ");
            sb.Append(username);
            sb.Append(" has joined the server.");

            //Send the join message to all other clients
            sendServerMessageToAll(sb.ToString(), cl);
        }

        private void sendHistoricalVesselUpdates(int toSubspace, double atTick, double lastTick)
        {
            SQLiteCommand cmd = universeDB.CreateCommand();
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
            SQLiteDataReader reader = cmd.ExecuteReader();
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

        private void sendSubspace(Client cl, bool excludeOwnActive = false)
        {
            if (!cl.warping)
            {
                sendSubspaceSync(cl);
                Log.Activity("Sending all vessels in current subspace for " + cl.username);
                SQLiteCommand cmd = universeDB.CreateCommand();
                string sql = "SELECT  vu.UpdateMessage, v.ProtoVessel, v.Private, v.OwnerID" +
                    " FROM kmpVesselUpdate vu" +
                    " INNER JOIN kmpVessel v ON v.Guid = vu.Guid AND v.Destroyed != 1" +
                    " INNER JOIN kmpSubspace s ON s.ID = vu.Subspace" +
                    " INNER JOIN" +
                    "  (SELECT vu.Guid, MAX(s.LastTick) AS LastTick" +
                    "  FROM kmpVesselUpdate vu" +
                    "  INNER JOIN kmpSubspace s ON s.ID = vu.Subspace AND s.LastTick <= (SELECT LastTick FROM kmpSubspace WHERE ID = " + cl.currentSubspaceID.ToString("D") + ")" +
                    "  GROUP BY vu.Guid) t ON t.Guid = vu.Guid AND t.LastTick = s.LastTick";
                if (excludeOwnActive) sql += " AND NOT v.Guid = '" + cl.currentVessel + "'";
                sql += ";";
                cmd.CommandText = sql;
                SQLiteDataReader reader = cmd.ExecuteReader();
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
            string sql = "SELECT LastTick FROM kmpSubspace WHERE ID = " + cl.currentSubspaceID.ToString("D") + ";";
            cmd.CommandText = sql;
            SQLiteDataReader reader = cmd.ExecuteReader();
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
            if (sendSync) sendSyncMessage(cl, tick);
        }

        private void sendServerSync(Client cl)
        {
            if (!cl.warping)
            {
                SQLiteCommand cmd = universeDB.CreateCommand();
                string sql = "SELECT ss1.ID, ss1.LastTick FROM kmpSubspace ss1 LEFT JOIN kmpSubspace ss2 ON ss1.LastTick < ss2.LastTick WHERE ss2.ID IS NULL;";
                cmd.CommandText = sql;
                SQLiteDataReader reader = cmd.ExecuteReader();
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
                    else if (message_lower == "!quit")
                    {
                        markClientForDisconnect(cl, "Requested quit");
                        return;
                    }
                    else if (message_lower == "!help")
                    {
                        sb.Append("Available Server Commands:\n");
                        sb.Append("!help - Displays this message\n");
                        sb.Append("!list - View all connected players\n");
                        sb.Append("!quit - Leaves the server\n");
                        sb.Append("!getcraft <playername> - Gets the most recent craft shared by the specified player\n");
                        sb.Append("!motd - Displays Server MOTD\n");
                        sb.Append("!rules - Displays Server Rules\n");
                        sb.Append(Environment.NewLine);

                        sendTextMessage(cl, sb.ToString());

                        return;
                    }
                    else if (message_lower == "!motd")
                    {
                        sb.Append(settings.serverMotd);
                        sendTextMessage(cl, sb.ToString());
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
                }

                if (settings.profanityFilter)
                    message_text = WashMouthWithSoap(message_text);

                string full_message = string.Format("<{0}> {1}", cl.username, message_text);

                //Console.SetCursorPosition(0, Console.CursorTop);
                Log.Chat(cl.username, message_text);

                //Send the update to all other clients
                sendTextMessageToAll(full_message, cl);
            }
            catch (NullReferenceException) { }
        }

        private string[] profanity = { "fucker", "faggot", "shit", "fuck", "cunt", "piss", "fag", "dick", "cock", "asshole" };
        private string[] replacements = { "kerper", "kerpot", "kerp", "guck", "kump", "heph", "olp", "derp", "beet", "hepderm" };

        private string WashMouthWithSoap(string message_text)
        {
            var msg = message_text;

            for (var i = 0; i < profanity.Length; i++)
            {
                string word = profanity[i];
                int profIndex = msg.IndexOf(word, StringComparison.InvariantCultureIgnoreCase);

                if (profIndex > -1)
                {
                    msg = msg.Remove(profIndex, word.Length);
                    msg = msg.Insert(profIndex, replacements[i]);
                }
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

            byte[] data_bytes = new byte[version_bytes.Length + 12];

            //Write net protocol version
            KMPCommon.intToBytes(KMPCommon.NET_PROTOCOL_VERSION).CopyTo(data_bytes, 0);

            //Write version string length
            KMPCommon.intToBytes(version_bytes.Length).CopyTo(data_bytes, 4);

            //Write version string
            version_bytes.CopyTo(data_bytes, 8);

            //Write client ID
            KMPCommon.intToBytes(cl.playerID).CopyTo(data_bytes, 8 + version_bytes.Length);

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
            Log.Debug("[Server] message sent.");
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

        private void sendTextMessage(Client cl, String message)
        {
            UnicodeEncoding encoder = new UnicodeEncoding();
            cl.queueOutgoingMessage(KMPCommon.ServerMessageID.SERVER_MESSAGE, encoder.GetBytes(message));
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
                SQLiteCommand cmd;
                string sql;
                if (!secondaryUpdate && cl != null)
                {
                    var vessel_update = ByteArrayToObject<KMPVesselUpdate>(data);

                    if (vessel_update != null)
                    {
                        OwnerID = cl.playerID;
                        vessel_info = new String[4];
                        vessel_info[0] = vessel_update.player;
                        vessel_info[2] = "Using vessel: " + vessel_update.name;
                        vessel_info[3] = "";

                        //Log.Info("Unpacked update from tick=" + vessel_update.tick + " @ client tick=" + cl.lastTick);
                        ConfigNode node = vessel_update.getProtoVesselNode();
                        if (node != null)
                        {
                            byte[] protoVesselBlob = ObjectToByteArray(node);
                            cmd = universeDB.CreateCommand();
                            sql = "SELECT kmpVessel.Subspace FROM kmpVessel LEFT JOIN kmpSubspace ON kmpSubspace.ID = kmpVessel.Subspace WHERE Guid = '" + vessel_update.kmpID + "' ORDER BY kmpSubspace.LastTick DESC LIMIT 1";
                            cmd.CommandText = sql;
                            object result = cmd.ExecuteScalar();
                            cmd.Dispose();
                            if (result == null)
                            {
                                Log.Info("New vessel {0} from {1} added to universe", vessel_update.kmpID, cl.username);
                                cmd = universeDB.CreateCommand();
                                sql = "INSERT INTO kmpVessel (Guid, GameGuid, OwnerID, Private, Active, ProtoVessel, Subspace)" +
                                    "VALUES ('" + vessel_update.kmpID + "','" + vessel_update.id + "'," + cl.playerID + "," + Convert.ToInt32(vessel_update.isPrivate) + "," + Convert.ToInt32(vessel_update.state == State.ACTIVE) + ",@protoVessel," + cl.currentSubspaceID.ToString("D") + ")";
                                cmd.Parameters.Add("@protoVessel", DbType.Binary, protoVesselBlob.Length).Value = protoVesselBlob;
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
                                    sql = "UPDATE kmpVessel SET Private = " + Convert.ToInt32(vessel_update.isPrivate) + ", Active = " + Convert.ToInt32(vessel_update.state == State.ACTIVE) + ", OwnerID=" + cl.playerID + ", ProtoVessel = @protoVessel WHERE Guid = '" + vessel_update.kmpID + "';";
                                    cmd.Parameters.Add("@protoVessel", DbType.Binary, protoVesselBlob.Length).Value = protoVesselBlob;
                                    cmd.CommandText = sql;
                                    cmd.ExecuteNonQuery();
                                    cmd.Dispose();
                                }
                                else
                                {

                                    cmd = universeDB.CreateCommand();
                                    sql = "UPDATE kmpVessel SET Private = " + Convert.ToInt32(vessel_update.isPrivate) + ", Active = " + Convert.ToInt32(vessel_update.state == State.ACTIVE) + ", OwnerID=" + cl.playerID + ", ProtoVessel = @protoVessel, Subspace = " + cl.currentSubspaceID.ToString("D") + " WHERE Guid = '" + vessel_update.kmpID + "';";
                                    cmd.Parameters.Add("@protoVessel", DbType.Binary, protoVesselBlob.Length).Value = protoVesselBlob;
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
                                        sql = "DELETE FROM kmpSubspace WHERE ID = " + cl.currentSubspaceID.ToString("D") + " AND LastTick < (SELECT MIN(s.LastTick) FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID);";
                                        cmd.CommandText = sql;
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
                                    sql = "UPDATE kmpVessel SET Active = 0 WHERE Guid = '" + cl.currentVessel + "'";
                                    cmd.CommandText = sql;
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
                            sql = "SELECT kmpVessel.Subspace FROM kmpVessel LEFT JOIN kmpSubspace ON kmpSubspace.ID = kmpVessel.Subspace WHERE Guid = '" + vessel_update.kmpID + "' ORDER BY kmpSubspace.LastTick DESC LIMIT 1";
                            cmd.CommandText = sql;
                            object result = cmd.ExecuteScalar();
                            cmd.Dispose();
                            if (result != null)
                            {
                                int current_subspace = Convert.ToInt32(result);
                                if (current_subspace == cl.currentSubspaceID)
                                {
                                    cmd = universeDB.CreateCommand();
                                    sql = "UPDATE kmpVessel SET Private = " + Convert.ToInt32(vessel_update.isPrivate) + ", Active = " + Convert.ToInt32(vessel_update.state == State.ACTIVE) + ", OwnerID=" + cl.playerID + " WHERE Guid = '" + vessel_update.kmpID + "';";
                                    cmd.CommandText = sql;
                                    cmd.ExecuteNonQuery();
                                    cmd.Dispose();
                                }
                                else
                                {

                                    cmd = universeDB.CreateCommand();
                                    sql = "UPDATE kmpVessel SET Private = " + Convert.ToInt32(vessel_update.isPrivate) + ", Active = " + Convert.ToInt32(vessel_update.state == State.ACTIVE) + ", OwnerID=" + cl.playerID + ", Subspace = " + cl.currentSubspaceID.ToString("D") + " WHERE Guid = '" + vessel_update.kmpID + "';";
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
                                        sql = "DELETE FROM kmpSubspace WHERE ID = " + cl.currentSubspaceID.ToString("D") + " AND LastTick < (SELECT MIN(s.LastTick) FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID);";
                                        cmd.CommandText = sql;
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
                                    sql = "UPDATE kmpVessel SET Active = 0 WHERE Guid = '" + cl.currentVessel + "'";
                                    cmd.CommandText = sql;
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
                else if (secondaryUpdate)
                {
                    //Secondary update
                    var vessel_update = ByteArrayToObject<KMPVesselUpdate>(data);

                    if (vessel_update != null)
                    {
                        try
                        {
                            bool active = false;
                            cmd = universeDB.CreateCommand();
                            sql = "SELECT kmpVessel.OwnerID, kmpVessel.Active FROM kmpVessel LEFT JOIN kmpSubspace ON kmpSubspace.ID = kmpVessel.Subspace WHERE Guid = '" + vessel_update.kmpID + "' ORDER BY kmpSubspace.LastTick DESC LIMIT 1";
                            cmd.CommandText = sql;
                            SQLiteDataReader reader = cmd.ExecuteReader();
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
                                    sql = "UPDATE kmpVessel SET ProtoVessel = @protoVessel, Subspace = " + cl.currentSubspaceID.ToString("D") + " WHERE Guid = '" + vessel_update.kmpID + "';";
                                    cmd.Parameters.Add("@protoVessel", DbType.Binary, protoVesselBlob.Length).Value = protoVesselBlob;
                                    cmd.CommandText = sql;
                                    cmd.ExecuteNonQuery();
                                    cmd.Dispose();
                                }
                                if (OwnerID == cl.playerID)
                                {
                                    //Update Active status
                                    cmd = universeDB.CreateCommand();
                                    sql = "UPDATE kmpVessel SET Active = 0 WHERE Guid = '" + vessel_update.kmpID + "';";
                                    cmd.CommandText = sql;
                                    cmd.ExecuteNonQuery();
                                    cmd.Dispose();
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

            foreach (var client in clients.ToList().Where(c => c != cl && c.isReady && c.activityLevel != Client.ActivityLevel.INACTIVE && (c.activityLevel == Client.ActivityLevel.IN_GAME || !secondaryUpdate)))
            {
                if ((client.currentSubspaceID == cl.currentSubspaceID)
                    && !client.warping && !cl.warping
                    && client.lastTick != -1d)
                {
                    if (OwnerID == client.playerID)
                        client.queueOutgoingMessage(owned_message_bytes);
                    else
                        client.queueOutgoingMessage(message_bytes);
                }
                else if (!secondaryUpdate
                     && firstSubspaceIsPresentOrFutureOfSecondSubspace(client.currentSubspaceID, cl.currentSubspaceID)
                     && !client.warping && !cl.warping && client.lastTick != -1d)
                {
                    client.queueOutgoingMessage(past_message_bytes);
                }
                else if (!secondaryUpdate && client.lastTick != -1d)
                {
                    if (vessel_info != null)
                    {
                        if (client.warping) vessel_info[1] = "Unknown due to warp";
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
            SQLiteCommand cmd = universeDB.CreateCommand();
            string sql = "DELETE FROM kmpVesselUpdate WHERE Guid = '" + vessel_update.kmpID + "' AND Subspace = " + cl.currentSubspaceID.ToString("D") + ";" +
                " INSERT INTO kmpVesselUpdate (Guid, Subspace, UpdateMessage)" +
                " VALUES ('" + vessel_update.kmpID + "'," + cl.currentSubspaceID.ToString("D") + ",@update);";
            if (!isSecondary) sql += " INSERT INTO kmpVesselUpdateHistory (Guid, Subspace, Tick, UpdateMessage)" +
                " VALUES ('" + vessel_update.kmpID + "'," + cl.currentSubspaceID.ToString("D") + "," + vessel_update.tick + ",@update);";
            cmd.Parameters.Add("@update", DbType.Binary, updateBlob.Length).Value = updateBlob;
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
            cmd.Dispose();
        }

        private bool checkVesselDestruction(KMPVesselUpdate vessel_update, Client cl)
        {
            try
            {
                if (!recentlyDestroyed.ContainsKey(vessel_update.kmpID) || (recentlyDestroyed[vessel_update.kmpID] + 1500L) < currentMillisecond)
                {
                    SQLiteCommand cmd = universeDB.CreateCommand();
                    string sql = "UPDATE kmpVessel SET Destroyed = " + Convert.ToInt32(vessel_update.situation == Situation.DESTROYED) + " WHERE Guid = '" + vessel_update.kmpID + "'";
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
            SQLiteCommand cmd = universeDB.CreateCommand();
            string sql = "SELECT vu.UpdateMessage, v.ProtoVessel, v.Private, v.OwnerID, v.Active" +
                " FROM kmpVesselUpdate vu" +
                " INNER JOIN kmpVessel v ON v.Guid = vu.Guid" +
                " WHERE vu.Subspace = " + cl.currentSubspaceID.ToString("D") + " AND v.Guid = '" + vessel.ToString() + "';";
            cmd.CommandText = sql;
            SQLiteDataReader reader = cmd.ExecuteReader();
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

        private void sendCraftFile(Client cl, String craft_name, byte[] data, byte type)
        {

            UnicodeEncoding encoder = new UnicodeEncoding();
            byte[] name_bytes = encoder.GetBytes(craft_name);

            byte[] bytes = new byte[5 + name_bytes.Length + data.Length];

            //Copy data
            bytes[0] = type;
            KMPCommon.intToBytes(name_bytes.Length).CopyTo(bytes, 1);
            name_bytes.CopyTo(bytes, 5);
            data.CopyTo(bytes, 5 + name_bytes.Length);

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

        private byte[] serverSettingBytes()
        {

            byte[] bytes = new byte[KMPCommon.SERVER_SETTINGS_LENGTH];

            KMPCommon.intToBytes(updateInterval).CopyTo(bytes, 0); //Update interval
            KMPCommon.intToBytes(settings.screenshotInterval).CopyTo(bytes, 4); //Screenshot interval
            KMPCommon.intToBytes(settings.screenshotSettings.maxHeight).CopyTo(bytes, 8); //Screenshot height
            bytes[12] = inactiveShipsPerClient; //Inactive ships per client

            return bytes;
        }

        //Universe

        public void startDatabase()
        {
            SQLiteConnection diskDB = new SQLiteConnection(DB_FILE_CONN);
            diskDB.Open();
            universeDB = new SQLiteConnection("Data Source=:memory:");
            universeDB.Open();

            SQLiteCommand init_cmd = universeDB.CreateCommand();
            string sql = "PRAGMA auto_vacuum = 1;"; //"FULL" auto_vacuum
            init_cmd.CommandText = sql;
            init_cmd.ExecuteNonQuery();

            Int32 version = 0;
            try
            {
                SQLiteCommand cmd = diskDB.CreateCommand();
                sql = "SELECT version FROM kmpInfo";
                cmd.CommandText = sql;
                version = Convert.ToInt32(cmd.ExecuteScalar());
            }
            catch { Log.Info("Missing (or bad) universe database file."); }
            finally
            {
                if (version == 1)
                {
                    //Upgrade old universe to version 2
                    Log.Info("Upgrading universe database...");
                    SQLiteCommand cmd = diskDB.CreateCommand();
                    sql = "CREATE INDEX IF NOT EXISTS kmpVesselIdxGuid on kmpVessel(Guid);" +
                        "CREATE INDEX IF NOT EXISTS kmpVesselUpdateIdxGuid on kmpVesselUpdate(guid);" +
                        "CREATE INDEX IF NOT EXISTS kmpVesselUpdateHistoryIdxTick on kmpVesselUpdateHistory(Tick);" +
                        "UPDATE kmpInfo SET Version = '" + UNIVERSE_VERSION + "';";
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                    Log.Info("Loading universe...");
                    diskDB.BackupDatabase(universeDB, "main", "main", -1, null, 0);
                }
                else if (version != UNIVERSE_VERSION)
                {
                    Log.Info("Creating new universe...");
                    try
                    {
                        File.Delete("KMP_universe.db");
                    }
                    catch { }
                    SQLiteCommand cmd = universeDB.CreateCommand();
                    sql = "CREATE TABLE kmpInfo (Version INTEGER);" +
                        "INSERT INTO kmpInfo (Version) VALUES (" + UNIVERSE_VERSION + ");" +
                        "CREATE TABLE kmpSubspace (ID INTEGER PRIMARY KEY AUTOINCREMENT, LastTick DOUBLE);" +
                        "INSERT INTO kmpSubspace (LastTick) VALUES (100);" +
                        "CREATE TABLE kmpPlayer (ID INTEGER PRIMARY KEY AUTOINCREMENT, Name NVARCHAR(100), Guid CHAR(40));" +
                        "CREATE TABLE kmpVessel (Guid CHAR(40), GameGuid CHAR(40), OwnerID INTEGER, Private BIT, Active BIT, ProtoVessel BLOB, Subspace INTEGER, Destroyed BIT);" +
                        "CREATE TABLE kmpVesselUpdate (ID INTEGER PRIMARY KEY AUTOINCREMENT, Guid CHAR(40), Subspace INTEGER, UpdateMessage BLOB);" +
                        "CREATE TABLE kmpVesselUpdateHistory (Guid CHAR(40), Subspace INTEGER, Tick DOUBLE, UpdateMessage BLOB);" +
                        "CREATE INDEX kmpVesselIdxGuid on kmpVessel(Guid);" +
                        "CREATE INDEX kmpVesselUpdateIdxGuid on kmpVesselUpdate(guid);" +
                        "CREATE INDEX kmpVesselUpdateHistoryIdxTick on kmpVesselUpdateHistory(Tick);";
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }
                else
                {
                    Log.Info("Loading universe...");
                    diskDB.BackupDatabase(universeDB, "main", "main", -1, null, 0);
                }
                diskDB.Close();
            }

            SQLiteCommand cmd2 = universeDB.CreateCommand();
            sql = "VACUUM; UPDATE kmpVessel SET Active = 0;";
            cmd2.CommandText = sql;
            cmd2.ExecuteNonQuery();
            Log.Info("Universe OK.");
        }

        public void backupDatabase()
        {
            try
            {
                Log.Info("Backing up old disk DB...");
                try
                {
                    File.Copy(DB_FILE, DB_FILE + ".bak", true);
                    File.Delete(DB_FILE);
                }
                catch { }
                SQLiteConnection diskDB = new SQLiteConnection(DB_FILE_CONN);
                diskDB.Open();
                universeDB.BackupDatabase(diskDB, "main", "main", -1, null, 0);
                SQLiteCommand cmd = diskDB.CreateCommand();
                string sql = "DELETE FROM kmpSubspace WHERE LastTick < (SELECT MIN(s.LastTick) FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID);" +
                " DELETE FROM kmpVesselUpdateHistory;" +
                " DELETE FROM kmpVesselUpdate WHERE ID IN (SELECT ID FROM kmpVesselUpdate vu WHERE Subspace != (SELECT ID FROM kmpSubspace WHERE LastTick = (SELECT MAX(LastTick) FROM kmpSubspace WHERE ID IN (SELECT Subspace FROM kmpVesselUpdate WHERE Guid = vu.Guid))));";
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
                diskDB.Close();
                Log.Info("Universe saved to disk.");
            }
            catch (IOException)
            {
                Log.Error("Backing up of database failed. Try again in a few seconds.");
            }
        }

        public void cleanDatabase()
        {
            try
            {
                Log.Info("Attempting to optimize database...");

                SQLiteCommand cmd = universeDB.CreateCommand();
                string sql = "DELETE FROM kmpSubspace WHERE LastTick < (SELECT MIN(s.LastTick) FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID);" +
                    " DELETE FROM kmpVesselUpdateHistory;" +
                    " DELETE FROM kmpVesselUpdate WHERE ID IN (SELECT ID FROM kmpVesselUpdate vu WHERE Subspace != (SELECT ID FROM kmpSubspace WHERE LastTick = (SELECT MAX(LastTick) FROM kmpSubspace WHERE ID IN (SELECT Subspace FROM kmpVesselUpdate WHERE Guid = vu.Guid))));";
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();

                cmd = universeDB.CreateCommand();
                sql = "VACUUM;";
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();

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
            SQLiteCommand cmd = universeDB.CreateCommand();
            string sql = "SELECT LastTick FROM kmpSubspace WHERE ID = " + referenceSubspace;
            cmd.CommandText = sql;
            refTime = Convert.ToDouble(cmd.ExecuteScalar());
            cmd.Dispose();

            cmd = universeDB.CreateCommand();
            sql = "SELECT LastTick FROM kmpSubspace WHERE ID = " + comparisonSubspace;
            cmd.CommandText = sql;
            compTime = Convert.ToDouble(cmd.ExecuteScalar());
            cmd.Dispose();

            return (compTime >= refTime);
        }

        static byte[] GetDataReaderBytes(SQLiteDataReader reader, int column)
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
            Log.Info("/quit - Quit server cleanly");
            Log.Info("/stop - Stop hosting server");
            Log.Info("/listclients - List players");
            Log.Info("/countclients - Display player counts");
            Log.Info("/kick <username> - Kick player <username>");
            Log.Info("/ban <username> - Permanently ban player <username> and any known aliases");
            Log.Info("/register <username> <token> - Add new roster entry for player <username> with authentication token <token> (BEWARE: will delete any matching roster entries)");
            Log.Info("/update <username> <token> - Update existing roster entry for player <username>/token <token> (one param must match existing roster entry, other will be updated)");
            Log.Info("/unregister <username/token> - Remove any player that has a matching username or token from the roster");
            Log.Info("/clearclients - Attempt to clear 'ghosted' clients");
            Log.Info("/dekessler <mins> - Remove debris that has not been updated for at least <mins> minutes (in-game time) (If no <mins> value is specified, debris that is older than 30 minutes will be cleared)");
            Log.Info("/save - Backup universe");
            Log.Info("/help - Displays all commands in the server");
            Log.Info("/set [key] [value] to modify a setting");
            Log.Info("/whitelist [add|del] [user] to update whitelist");
            Log.Info("Non-commands will be sent to players as a chat message");

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
    }

}
