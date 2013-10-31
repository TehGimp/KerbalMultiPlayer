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

namespace KMPServer
{
	class Server
	{

		public struct ClientMessage
		{
			public int clientIndex;
			public KMPCommon.ClientMessageID id;
			public byte[] data;
		}
		
		public const long CLIENT_TIMEOUT_DELAY = 16000;
		public const long CLIENT_HANDSHAKE_TIMEOUT_DELAY = 18000;
		public const int SLEEP_TIME = 3;
		public const int MAX_SCREENSHOT_COUNT = 10000;
		public const int UDP_ACK_THROTTLE = 1000;
		public const int DATABASE_BACKUP_INTERVAL = 300000;
		
		public const float NOT_IN_FLIGHT_UPDATE_WEIGHT = 1.0f/4.0f;
		public const int ACTIVITY_RESET_DELAY = 10000;

		public const String SCREENSHOT_DIR = "KMPScreenshots";
		public const string DB_FILE_CONN = "Data Source=KMP_universe.db";
		public const string DB_FILE = "KMP_universe.db";
		
		public const int UNIVERSE_VERSION = 1;

		public int numClients
		{
			private set;
			get;
		}

		public int numInGameClients
		{
			private set;
			get;
		}

		public int numInFlightClients
		{
			private set;
			get;
		}
		
		public bool quit = false;
		public bool stop = false;

		public String threadExceptionStackTrace;
		public Exception threadException;

		public object threadExceptionLock = new object();
		public object clientActivityCountLock = new object();
		public static object consoleWriteLock = new object();

		public Thread listenThread;
		public Thread commandThread;
		public Thread connectionThread;
		public Thread outgoingMessageThread;

		public TcpListener tcpListener;
		public UdpClient udpClient;

		public HttpListener httpListener;

		public ServerClient[] clients;
		public Queue<ClientMessage> clientMessageQueue;

		public ServerSettings.ConfigStore settings;

		public Stopwatch stopwatch = new Stopwatch();
		
		public static SQLiteConnection universeDB;
		
		private bool backedUpSinceEmpty = false;
		private Dictionary<Guid,long> recentlyDestroyed = new Dictionary<Guid,long>();
	
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

				lock (clientActivityCountLock)
				{
					//Create a weighted count of clients in-flight and not in-flight to estimate the amount of update traffic
					relevant_player_count = numInFlightClients + (numInGameClients - numInFlightClients) * NOT_IN_FLIGHT_UPDATE_WEIGHT;
				}

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

				lock (clientActivityCountLock)
				{
					relevant_player_count = numInFlightClients;
				}

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

			if (clients != null)
			{
				for (int i = 0; i < clients.Length; i++)
				{
					clients[i].endReceivingMessages();
					if (clients[i].tcpClient != null)
						clients[i].tcpClient.Close();
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
				try {
					backupDatabase();
					universeDB.Close ();
					universeDB.Dispose();
				} catch {}
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

			Log.Info("Hosting server on port " + settings.port + "...");

			clients = new ServerClient[settings.maxClients];
			for (int i = 0; i < clients.Length; i++)
			{
				clients[i] = new ServerClient(this, i);
			}

			clientMessageQueue = new Queue<ClientMessage>();

			numClients = 0;
			numInGameClients = 0;
			numInFlightClients = 0;

			listenThread = new Thread(new ThreadStart(listenForClients));
			commandThread = new Thread(new ThreadStart(handleCommands));
			connectionThread = new Thread(new ThreadStart(handleConnections));
			outgoingMessageThread = new Thread(new ThreadStart(sendOutgoingMessages));

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

			Log.Info("Commands:");
            Log.Info("/quit - Quit server cleanly");
            Log.Info("/stop - Stop hosting server");
            Log.Info("/list - List players");
            Log.Info("/count - Display player counts");
            Log.Info("/kick <username> - Kick player <username>");
            Log.Info("/ban <username> - Permanently ban player <username> and any known aliases");
            Log.Info("/register <username> <token> - Add new roster entry for player <username> with authentication token <token> (BEWARE: will delete any matching roster entries)");
            Log.Info("/update <username> <token> - Update existing roster entry for player <username>/token <token> (one param must match existing roster entry, other will be updated)");
            Log.Info("/unregister <username/token> - Temove any player that has a matching username or token from the roster");
            Log.Info("/save - Backup universe");
            Log.Info("Non-commands will be sent to players as a chat message");

			commandThread.Start();
			connectionThread.Start();
			outgoingMessageThread.Start();
			
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
				
				if (currentMillisecond - last_backup_time > DATABASE_BACKUP_INTERVAL && (numInGameClients > 0 || !backedUpSinceEmpty))
				{
					if (numInGameClients <= 0)
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
			try
			{
				while (true)
				{
					String input = Console.ReadLine().ToLower();

					if (input != null && input.Length > 0)
					{

						if (input.ElementAt(0) == '/')
						{
							if (input == "/quit" || input == "/stop")
							{
								quit = true;
								if (input == "/stop")
									stop = true;

								//Disconnect all clients
                                foreach (var c in clients)
                                {
                                    disconnectClient(c, "Server is shutting down");
                                }
								
								break;
							}
							else if (input.Length > 6 && input.Substring(0, 6) == "/kick ")
							{
								String kick_name = input.Substring(6, input.Length - 6).ToLower();

                                var clientToDisconnect = clients.Where(cl => cl.username.ToLower() == kick_name && cl.isReady).FirstOrDefault();

                                if (clientToDisconnect != null)
                                {
                                    disconnectClient(clientToDisconnect, "You were kicked from the server.");
                                }
							}
							else if (input == "/list")
							{
								//Display player list
								StringBuilder sb = new StringBuilder();

                                foreach (var client in clients.Where(c => c.isReady))
                                {
                                    sb.Append(client.username);
                                    sb.Append(" - ");
                                    sb.Append(client.activityLevel.ToString());
                                    sb.Append('\n');
                                }

                                Log.Info(sb.ToString());
							}
							else if (input == "/count")
							{
                                Log.Info("Total clients: " + numClients);

								lock (clientActivityCountLock)
								{
                                    Log.Info("In-Game Clients: " + numInGameClients);
                                    Log.Info("In-Flight Clients: " + numInFlightClients);
								}
							}
							else if (input == "/save")
							{
								//Save the universe!
                                Log.Info("Saving the universe! ;-)");
								backupDatabase();
							}
							else if (input.Length > 5 && input.Substring(0, 5) == "/ban ")
							{
								String ban_name = input.Substring(5, input.Length - 5).ToLower();
								string guid = Guid.Empty.ToString();

                                var userToBan = clients.Where(c => c.username.ToLower() == ban_name && c.isReady).FirstOrDefault();

                                if (userToBan != null)
                                {
                                    disconnectClient(userToBan, "You were banned from the server!");
                                    guid = userToBan.guid;
                                }

								if (guid != Guid.Empty.ToString())
								{
									SQLiteCommand cmd = universeDB.CreateCommand();
									string sql = "UPDATE kmpPlayer SET Guid = '@newGuid' WHERE Guid = '@guid';";
                                    cmd.Parameters.AddWithValue("newGuid", Guid.NewGuid().ToString());
                                    cmd.Parameters.AddWithValue("guid", guid);
									cmd.CommandText = sql;
									cmd.ExecuteNonQuery();
									cmd.Dispose();
                                    Log.Info("Player '" + ban_name + "' and all known aliases banned from server permanently. Use /unregister to allow this user to reconnect.");
								}
								else
								{
                                    Log.Info("Failed to locate player '" + ban_name + "'.");
								}
							}
							else if (input.Length > 10 && input.Substring(0, 10) == "/register ")
							{
								String[] args = input.Substring(10, input.Length - 10).Split(' ');
								if (args.Length == 2)
								{
									try
									{
										Guid parser = new Guid(args[1]);
										String guid = parser.ToString();
										String username_lower = args[0].ToLower();
										for (int i = 0; i < clients.Length; i++)
										{
											SQLiteCommand cmd = universeDB.CreateCommand();
											string sql = "DELETE FROM kmpPlayer WHERE Name LIKE '@username';" + 
												" INSERT INTO kmpPlayer (Name, Guid) VALUES ('@username','@guid');";
                                            cmd.Parameters.AddWithValue("username", username_lower);
                                            cmd.Parameters.AddWithValue("guid", guid);
											cmd.CommandText = sql;
											cmd.ExecuteNonQuery();
											cmd.Dispose();
										}
                                        Log.Info("Player '" + args[0] + "' added to player roster with token '" + args[1] + "'.");
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
							else if (input.Length > 12 && input.Substring(0, 12) == "/unregister ")
							{
								String dereg = input.Substring(12, input.Length - 12);
								SQLiteCommand cmd = universeDB.CreateCommand();
								string sql = "DELETE FROM kmpPlayer WHERE Guid = '@dereg' OR Name LIKE '@dereg';";
								cmd.CommandText = sql;
                                cmd.Parameters.AddWithValue("dereg", dereg);
								cmd.ExecuteNonQuery();
								cmd.Dispose();
                                Log.Info("Players with name/token '" + dereg + "' removed from player roster.");
							}
							else if (input.Length > 8 && input.Substring(0, 8) == "/update ")
							{
								String[] args = input.Substring(8, input.Length - 8).Split(' ');
								if (args.Length == 2)
								{
									try
									{
										Guid parser = new Guid(args[1]);
										String guid = parser.ToString();
										String username_lower = args[0].ToLower();
										for (int i = 0; i < clients.Length; i++)
										{
											SQLiteCommand cmd = universeDB.CreateCommand();
											string sql = "UPDATE kmpPlayer SET Name='@username', Guid='@guid' WHERE Name LIKE '@username' OR Guid = '@guid';";
											cmd.CommandText = sql;
                                            cmd.Parameters.AddWithValue("username", username_lower);
                                            cmd.Parameters.AddWithValue("guid", guid);
											cmd.ExecuteNonQuery();
											cmd.Dispose();
										}
                                        Log.Info("Updated roster with player '" + args[0] + "' and token '" + args[1] + "'.");
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
						else
						{
							//Send a message to all clients
							sendServerMessageToAll(input);
						}

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

		private void listenForClients()
		{

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
						//Try to add the client
						ServerClient cl = addClient(client);
						if (cl != null)
						{
							if (cl.isValid)
							{
								//Send a handshake to the client
								Log.Info("Accepted client. Handshaking...");
								sendHandshakeMessage(cl);

								sendMessageHeaderDirect(client, KMPCommon.ServerMessageID.NULL, 0);

								//Send the join message to the client
								if (settings.joinMessage.Length > 0)
									sendServerMessage(cl, settings.joinMessage);
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
			try
			{
				Log.Debug("Starting disconnect thread");

				while (true)
				{
					//Handle received messages
					while (clientMessageQueue.Count > 0)
					{
						ClientMessage message;
						
						message = clientMessageQueue.Dequeue();
						
						//if (clientMessageQueue.TryDequeue(out message))
						handleMessage(clients[message.clientIndex], message.id, message.data);
//						else
//							break;
					}

                    foreach (var client in clients.Where(c => c.isValid))
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
                            disconnectClient(client, "Timeout");
                        }
                        else
                        {
                            bool changed = false;

                            //Reset the client's activity level if the time since last update was too long
                            lock (client.activityLevelLock)
                            {
                                if (client.activityLevel == ServerClient.ActivityLevel.IN_FLIGHT
                                    && (currentMillisecond - client.lastInFlightActivityTime) > ACTIVITY_RESET_DELAY)
                                {
                                    client.activityLevel = ServerClient.ActivityLevel.IN_GAME;
                                    changed = true;
                                    client.universeSent = false;
                                }

                                if (client.activityLevel == ServerClient.ActivityLevel.IN_GAME
                                    && (currentMillisecond - client.lastInGameActivityTime) > ACTIVITY_RESET_DELAY)
                                {
                                    client.activityLevel = ServerClient.ActivityLevel.INACTIVE;
                                    changed = true;
                                    client.universeSent = false;
                                }
                            }

                            if (changed)
                                clientActivityLevelChanged(client);
                        } 
                    }
					
					foreach (var client in clients.Where(c => !c.isValid))
                    {
                        //Client is disconnected but slot has not been cleaned up
                        disconnectClient(client, "Connection lost");
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

			Log.Debug("Ending disconnect thread.");
		}

		void sendOutgoingMessages()
		{
			try
			{

				while (true)
				{
                    foreach (var client in clients.Where(c => c.isValid))
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

		private ServerClient addClient(TcpClient tcp_client)
		{

			if (tcp_client == null || !tcp_client.Connected)
				return null;

			//Find an open client slot
            var replaceSlot = clients.Where(c => c.canBeReplaced && !c.isValid).FirstOrDefault();

            if (replaceSlot != null)
            {
                replaceSlot.tcpClient = tcp_client;

                //Reset client properties
                replaceSlot.resetProperties();

                replaceSlot.startReceivingMessages();
                numClients++;

                return replaceSlot;
            }

            return null;
		}

		public void disconnectClient(ServerClient cl, String message)
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
	
				if (cl.canBeReplaced)
					return;
	
				numClients--;
	
				//Only send the disconnect message if the client performed handshake successfully
				if (cl.receivedHandshake)
				{
					Log.Info("Client #" + cl.playerID + " " + cl.username + " has disconnected: " + message);
	
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
						try {
							SQLiteCommand cmd = universeDB.CreateCommand();
							string sql = "UPDATE kmpVessel SET Active = 0 WHERE Guid = '@guid'";
							cmd.CommandText = sql;
	                        cmd.Parameters.AddWithValue("guid", cl.currentVessel);
							cmd.ExecuteNonQuery();
							cmd.Dispose();
						} catch { }
						sendVesselStatusUpdateToAll(cl, cl.currentVessel);
					}
					
					bool emptySubspace = true;
					
					foreach (ServerClient client in clients)
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
	                    cmd.Parameters.AddWithValue("id", cl.currentSubspaceID);
						cmd.ExecuteNonQuery();
						cmd.Dispose();
					}
					
					//backupDatabase();
				}
				else
					Log.Info("Client failed to handshake successfully: " + message);
			}
			catch (NullReferenceException e)
			{
				//Almost certainly need to be smarter about this.
				Log.Info("Internal error during disconnect: " + e.StackTrace);
			}
			
			cl.receivedHandshake = false;
			cl.universeSent = false;
			
			if (cl.activityLevel != ServerClient.ActivityLevel.INACTIVE)
				clientActivityLevelChanged(cl);
			else
				sendServerSettingsToAll();
			
			cl.disconnected();
		}

		public void clientActivityLevelChanged(ServerClient cl)
		{
			Log.Activity(cl.username + " activity level is now " + cl.activityLevel);
			
			//Count the number of in-game/in-flight clients
			int num_in_game = 0;
			int num_in_flight = 0;

            foreach (var client in clients.Where(c => c.isValid))
            {
                switch (client.activityLevel)
                {
                    case ServerClient.ActivityLevel.IN_GAME:
                        num_in_game++;
                        break;

                    case ServerClient.ActivityLevel.IN_FLIGHT:
                        num_in_game++;
                        num_in_flight++;
                        break;
                }
            }

			lock (clientActivityCountLock)
			{
				numInGameClients = num_in_game;
				numInFlightClients = num_in_flight;
			}
			if (numInGameClients > 0) backedUpSinceEmpty = false;
			
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

					if (clients[sender_index].isReady)
					{
						if ((currentMillisecond - clients[sender_index].lastUDPACKTime) > UDP_ACK_THROTTLE)
						{
							//Acknowledge the client's message with a TCP message
							clients[sender_index].queueOutgoingMessage(KMPCommon.ServerMessageID.UDP_ACKNOWLEDGE, null);
							clients[sender_index].lastUDPACKTime = currentMillisecond;
						}

						//Handle the message
						handleMessage(clients[sender_index], id, data);
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

		private ServerClient getClientByName(String name)
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
				response_builder.Append(numClients);
				response_builder.Append('/');
				response_builder.Append(settings.maxClients);
				response_builder.Append('\n');

				response_builder.Append("Players: ");

				bool first = true;

                foreach (var client in clients.Where(c => c.isReady))
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

		public void queueClientMessage(ServerClient cl, KMPCommon.ClientMessageID id, byte[] data)
		{
			ClientMessage message = new ClientMessage();
			message.clientIndex = cl.clientIndex;
			message.id = id;
			message.data = data;

			clientMessageQueue.Enqueue(message);
		}

		public void handleMessage(ServerClient cl, KMPCommon.ClientMessageID id, byte[] data)
		{
			if (!cl.isValid)
				return;

			//Log.Info("Message id: " + id.ToString() + " from client: " + cl + " data: " + (data != null ? data.Length.ToString() : "0"));
			//Console.WriteLine("Message id: " + id.ToString() + " data: " + (data != null ? System.Text.Encoding.ASCII.GetString(data) : ""));

			UnicodeEncoding encoder = new UnicodeEncoding();

			switch (id)
			{
				case KMPCommon.ClientMessageID.HANDSHAKE:
					if (data != null)
					{
						StringBuilder sb = new StringBuilder();

						//Read username
						Int32 username_length = KMPCommon.intFromBytes(data, 0);
						String username = encoder.GetString(data, 4, username_length);

						
						Int32 guid_length = KMPCommon.intFromBytes(data, 4 + username_length);
						int offset = 4 + username_length + 4;
						String guid = new Guid(encoder.GetString(data, offset, guid_length)).ToString();					
						offset = 4 + username_length + 4 + guid_length;
						String version = encoder.GetString(data, offset, data.Length - offset);

						String username_lower = username.ToLower();

						bool accepted = true;

                        //Ensure no other players have the same username.
                        if (clients.Any(c => c.isReady && c.username.ToLower() == username_lower))
                        {
                            disconnectClient(cl, "Your username is already in use.");
								Log.Info("Rejected client due to duplicate username: " + username);
								accepted = false;
                        }

                        //If whitelisting is enabled and the user is *not* on the list:
                        if (settings.whitelisted && settings.whitelist.Contains(username, StringComparer.InvariantCultureIgnoreCase) == false)
                        {
                            disconnectClient(cl, "You are not on this servers whitelist.");
                            Log.Info("Rejected client due to not being on the whitelist: " + username);
                            accepted = false;
                        }

						if (!accepted)
							break;
					
						//Check if this player is new to universe
						SQLiteCommand cmd = universeDB.CreateCommand();
                        string sql = "SELECT COUNT(*) FROM kmpPlayer WHERE Name = '@username' AND Guid != '@guid';";
						cmd.CommandText = sql;
                        cmd.Parameters.AddWithValue("username", username_lower);
                        cmd.Parameters.AddWithValue("guid", guid);
						Int32 name_taken = Convert.ToInt32(cmd.ExecuteScalar());
						cmd.Dispose();
						if (name_taken > 0)
						{
							//Disconnect the player
							disconnectClient(cl, "Your username is already claimed by an existing user.");
							Log.Info("Rejected client due to duplicate username w/o matching guid: " + username);
							break;
						}
						cmd = universeDB.CreateCommand();
						sql = "SELECT COUNT(*) FROM kmpPlayer WHERE Guid = '@guid'";
						cmd.CommandText = sql;
                        cmd.Parameters.AddWithValue("guid", guid);
						Int32 player_exists = Convert.ToInt32(cmd.ExecuteScalar());
						cmd.Dispose();
						if (player_exists == 0) //New user
						{
							cmd = universeDB.CreateCommand();
							sql = "INSERT INTO kmpPlayer (Name, Guid) VALUES ('@username','@guid');";
							cmd.CommandText = sql;
                            cmd.Parameters.AddWithValue("username", username_lower);
                            cmd.Parameters.AddWithValue("guid", guid);
							cmd.ExecuteNonQuery();
							cmd.Dispose();
						}
						cmd = universeDB.CreateCommand();
						sql = "SELECT ID FROM kmpPlayer WHERE Guid = '@guid' AND Name LIKE '@username';";
						cmd.CommandText = sql;
                        cmd.Parameters.AddWithValue("username", username_lower);
                        cmd.Parameters.AddWithValue("guid", guid);
						Int32 playerID = Convert.ToInt32(cmd.ExecuteScalar());
						cmd.Dispose();
					    
						//Send the active user count to the client
						if (numClients == 2)
						{
							//Get the username of the other user on the server
							sb.Append("There is currently 1 other user on this server: ");

                            foreach (var client in clients.Where(c => c.isReady && c != cl))
                            {
                                sb.Append(client.username);
									break;
                            }
						}
						else
						{
							sb.Append("There are currently ");
							sb.Append(numClients - 1);
							sb.Append(" other users on this server.");
							if (numClients > 1)
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

						Log.Info(username + " has joined the server using client version " + version);

						//Build join message
						//sb.Clear();
						sb.Remove(0,sb.Length);
						sb.Append("User ");
						sb.Append(username);
						sb.Append(" has joined the server.");

						//Send the join message to all other clients
						sendServerMessageToAll(sb.ToString(), cl);

					}

					break;

				case KMPCommon.ClientMessageID.PRIMARY_PLUGIN_UPDATE:
				case KMPCommon.ClientMessageID.SECONDARY_PLUGIN_UPDATE:

					if (data != null && cl.isReady)
					{
#if SEND_UPDATES_TO_SENDER
						sendPluginUpdateToAll(data, id == KMPCommon.ClientMessageID.SECONDARY_PLUGIN_UPDATE);
#else
						sendPluginUpdateToAll(data, id == KMPCommon.ClientMessageID.SECONDARY_PLUGIN_UPDATE, cl);
#endif
					}

					break;

				case KMPCommon.ClientMessageID.TEXT_MESSAGE:

					if (data != null && cl.isReady)
						handleClientTextMessage(cl, encoder.GetString(data, 0, data.Length));

					break;

				case KMPCommon.ClientMessageID.SCREEN_WATCH_PLAYER:

					if (!cl.isReady)
						break;

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
						ServerClient watch_client = getClientByName(watch_name);
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
					

					break;

				case KMPCommon.ClientMessageID.SCREENSHOT_SHARE:

					if (data != null && data.Length <= settings.screenshotSettings.maxNumBytes && cl.isReady)
					{
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

					break;

				case KMPCommon.ClientMessageID.CONNECTION_END:

					String message = String.Empty;
					if (data != null)
						message = encoder.GetString(data, 0, data.Length); //Decode the message

					disconnectClient(cl, message); //Disconnect the client
					break;

				case KMPCommon.ClientMessageID.SHARE_CRAFT_FILE:

					if (cl.isReady && data != null
						&& data.Length > 5 && (data.Length - 5) <= KMPCommon.MAX_CRAFT_FILE_BYTES)
					{
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
					break;

				case KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_FLIGHT:
					if (cl.activityLevel == ServerClient.ActivityLevel.IN_GAME && cl.isReady && !cl.universeSent)
					{
						cl.universeSent = true;
						sendSubspace(cl);
					}
					cl.updateActivityLevel(ServerClient.ActivityLevel.IN_FLIGHT);
					break;

				case KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_GAME:
					if (cl.activityLevel == ServerClient.ActivityLevel.INACTIVE) sendServerSync(cl);
					if (cl.activityLevel == ServerClient.ActivityLevel.IN_FLIGHT && cl.currentVessel != Guid.Empty)
					{
						try {
							SQLiteCommand cmd = universeDB.CreateCommand();
							string sql = "UPDATE kmpVessel SET Active = 0 WHERE Guid = '@id'";
							cmd.CommandText = sql;
                            cmd.Parameters.AddWithValue("id", cl.currentVessel);
							cmd.ExecuteNonQuery();
							cmd.Dispose();
						} catch { }
						sendVesselStatusUpdateToAll(cl,cl.currentVessel);
						cl.universeSent = false;
					}
					cl.updateActivityLevel(ServerClient.ActivityLevel.IN_GAME);
					break;

				case KMPCommon.ClientMessageID.PING:
					cl.queueOutgoingMessage(KMPCommon.ServerMessageID.PING_REPLY, null);
					break;
				
				case KMPCommon.ClientMessageID.UDP_PROBE:
					if (data != null)
					{
						double tick = BitConverter.ToDouble(data,0);
						double lastTick = tick;
						
						cl.lastTick = tick;
						if (!cl.warping)
						{
							SQLiteCommand cmd = universeDB.CreateCommand();
							string sql = "SELECT LastTick FROM kmpSubspace WHERE ID = @id;";
							cmd.CommandText = sql;
                            cmd.Parameters.AddWithValue("id", cl.currentSubspaceID);
							SQLiteDataReader reader = cmd.ExecuteReader();
						
							try 
							{ 	
								while(reader.Read()) 
								{ 
									lastTick = reader.GetDouble(0);
								} 
							}
							finally 
							{ 
								reader.Close();
								cmd.Dispose();
							}
						
							if (lastTick - tick > 0.1d)
							{
								sendSyncMessage(cl,lastTick+cl.syncOffset);
								cl.syncOffset += 0.05d;
								if (cl.lagWarning > 300) disconnectClient(cl,"Your game was running too slowly compared to other players. Please try reconnecting in a moment.");
								else cl.lagWarning++;
							}
							else
							{
								cl.lagWarning = 0;
								if (cl.syncOffset > 0.05d) cl.syncOffset -= 0.01d;
								cmd = universeDB.CreateCommand();
								sql = "UPDATE kmpSubspace SET LastTick = " + tick + " WHERE ID = " + cl.currentSubspaceID + " AND LastTick < " + tick;
								cmd.CommandText = sql;
								cmd.ExecuteNonQuery();
								cmd.Dispose();
								sendHistoricalVesselUpdates(cl.currentSubspaceID, tick, lastTick);
							}
						}
					}
					break;
				case KMPCommon.ClientMessageID.WARPING:
					if (data != null)
					{
						float rate = BitConverter.ToSingle(data,0);
						if (cl.warping)
						{
							if (rate < 1.1f)
							{
								//stopped warping-create subspace & add player to it
								SQLiteCommand cmd = universeDB.CreateCommand();
								string sql = "INSERT INTO kmpSubspace (LastTick) VALUES (@tick);";
								cmd.CommandText = sql;
                                cmd.Parameters.AddWithValue("tick", cl.lastTick.ToString("0.0").Replace(",","."));
								cmd.ExecuteNonQuery();
								cmd.Dispose();
								cmd = universeDB.CreateCommand();
								sql = "SELECT last_insert_rowid();";
								cmd.CommandText = sql;
								SQLiteDataReader reader = cmd.ExecuteReader();
								int newSubspace = -1;
								try 
								{ 	
									while(reader.Read()) 
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
					break;
				case KMPCommon.ClientMessageID.SSYNC:
					if (data != null)
					{
						int subspaceID = KMPCommon.intFromBytes(data,0);
						if (subspaceID == -1)
						{
							//Latest available subspace sync request	
							SQLiteCommand cmd = universeDB.CreateCommand();
							string sql = "SELECT ss1.ID FROM kmpSubspace ss1 LEFT JOIN kmpSubspace ss2 ON ss1.LastTick < ss2.LastTick WHERE ss2.ID IS NULL;";
							cmd.CommandText = sql;
							SQLiteDataReader reader = cmd.ExecuteReader(); 
							try 
							{ 
								while(reader.Read()) 
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
						Log.Info(cl.username + " sync request to subspace " + subspaceID);
						sendSubspace(cl, true);
					}
					break;
			}

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
            cmd.Parameters.AddWithValue("lastTick", lastTick.ToString("0.0").Replace(",","."));
            cmd.Parameters.AddWithValue("atTick", atTick.ToString("0.0").Replace(",","."));
            cmd.Parameters.AddWithValue("toSubspace", toSubspace);
			SQLiteDataReader reader = cmd.ExecuteReader(); 
			try 
			{ 
				while(reader.Read()) 
				{ 
					KMPVesselUpdate vessel_update = (KMPVesselUpdate) ByteArrayToObject(GetDataReaderBytes(reader,0));
					vessel_update.state = State.ACTIVE;
					vessel_update.isPrivate = reader.GetBoolean(1);
					vessel_update.isMine = false;
					vessel_update.relTime = RelativeTime.FUTURE;
					byte[] update = ObjectToByteArray(vessel_update);

                    foreach (var client in clients.Where(c => c.currentSubspaceID == toSubspace && !c.warping && c.currentVessel != vessel_update.kmpID))
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
		
		private void sendSubspace(ServerClient cl, bool excludeOwnActive = false)
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
					"  INNER JOIN kmpSubspace s ON s.ID = vu.Subspace AND s.LastTick <= (SELECT LastTick FROM kmpSubspace WHERE ID = " + cl.currentSubspaceID + ")" +
					"  GROUP BY vu.Guid) t ON t.Guid = vu.Guid AND t.LastTick = s.LastTick";
				if (excludeOwnActive) sql += " AND NOT v.Guid = '" + cl.currentVessel + "'";
				sql += ";";
				cmd.CommandText = sql;
				SQLiteDataReader reader = cmd.ExecuteReader(); 
				try 
				{ 
					while(reader.Read()) 
					{ 
						KMPVesselUpdate vessel_update = (KMPVesselUpdate) ByteArrayToObject(GetDataReaderBytes(reader,0));
						ConfigNode protoVessel = (ConfigNode) ByteArrayToObject(GetDataReaderBytes(reader,1));
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
		
		private void sendSubspaceSync(ServerClient cl, bool sendSync = true)
		{
			SQLiteCommand cmd = universeDB.CreateCommand();
				string sql = "SELECT LastTick FROM kmpSubspace WHERE ID = " + cl.currentSubspaceID + ";";
				cmd.CommandText = sql;
				SQLiteDataReader reader = cmd.ExecuteReader(); 
				double tick = 0d;
				try 
				{ 
					while(reader.Read()) 
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
		
		private void sendServerSync(ServerClient cl)
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
					while(reader.Read()) 
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
		
		public void handleClientTextMessage(ServerClient cl, String message_text)
		{
			StringBuilder sb = new StringBuilder();

			if (message_text.Length > 0 && message_text.First() == '!')
			{
				string message_lower = message_text.ToLower();

				if (message_lower == "!list")
				{
					//Compile list of usernames
					sb.Append("Connected users:\n");

                    foreach (var client in clients.Where(c => c.isReady))
                    {
                        sb.Append(client.username);
                        sb.Append('\n');
                    }

					sendTextMessage(cl, sb.ToString());
					return;
				}
				else if (message_lower == "!quit")
				{
					disconnectClient(cl, "Requested quit");
					return;
				}
				else if (message_lower.Length > (KMPCommon.GET_CRAFT_COMMAND.Length + 1)
					&& message_lower.Substring(0, KMPCommon.GET_CRAFT_COMMAND.Length) == KMPCommon.GET_CRAFT_COMMAND)
				{
					String player_name = message_lower.Substring(KMPCommon.GET_CRAFT_COMMAND.Length + 1);

					//Find the player with the given name
					ServerClient target_client = getClientByName(player_name);

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

								Log.Info("Sent craft " + target_client.sharedCraftName
									+ " to client " + cl.username);
							}
						}
					}
					
					return;
				}
			}

			//Compile full message
			sb.Append('[');
			sb.Append(cl.username);
			sb.Append("] ");
			sb.Append(message_text);

			String full_message = sb.ToString();

			//Console.SetCursorPosition(0, Console.CursorTop);
			Log.Info(full_message);

			//Send the update to all other clients
			sendTextMessageToAll(full_message, cl);
		}

		public static byte[] buildMessageArray(KMPCommon.ServerMessageID id, byte[] data)
		{
			//Construct the byte array for the message
			int msg_data_length = 0;
			if (data != null)
				msg_data_length = data.Length;

			byte[] message_bytes = new byte[KMPCommon.MSG_HEADER_LENGTH + msg_data_length];

			KMPCommon.intToBytes((int)id).CopyTo(message_bytes, 0);
			KMPCommon.intToBytes(msg_data_length).CopyTo(message_bytes, 4);
			if (data != null)
				data.CopyTo(message_bytes, KMPCommon.MSG_HEADER_LENGTH);

			return message_bytes;
		}

		private void sendMessageHeaderDirect(TcpClient client, KMPCommon.ServerMessageID id, int msg_length)
		{
			client.GetStream().Write(KMPCommon.intToBytes((int)id), 0, 4);
			client.GetStream().Write(KMPCommon.intToBytes(msg_length), 0, 4);

			Log.Debug("Sending message: " + id.ToString());
		}

		private void sendHandshakeRefusalMessageDirect(TcpClient client, String message)
		{
			try
			{

				//Encode message
				UnicodeEncoding encoder = new UnicodeEncoding();
				byte[] message_bytes = encoder.GetBytes(message);

				sendMessageHeaderDirect(client, KMPCommon.ServerMessageID.HANDSHAKE_REFUSAL, message_bytes.Length);

				client.GetStream().Write(message_bytes, 0, message_bytes.Length);

				client.GetStream().Flush();

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

				sendMessageHeaderDirect(client, KMPCommon.ServerMessageID.CONNECTION_END, message_bytes.Length);

				client.GetStream().Write(message_bytes, 0, message_bytes.Length);

				client.GetStream().Flush();

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

		private void sendHandshakeMessage(ServerClient cl)
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

        private void sendServerMessageToAll(String message, ServerClient exclude = null)
		{
			UnicodeEncoding encoder = new UnicodeEncoding();
			byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.SERVER_MESSAGE, encoder.GetBytes(message));

            foreach (var client in clients.Where(cl => cl.isReady && cl != exclude))
            {
                client.queueOutgoingMessage(message_bytes);
            }
		}

		private void sendServerMessage(ServerClient cl, String message)
		{
			UnicodeEncoding encoder = new UnicodeEncoding();
			cl.queueOutgoingMessage(KMPCommon.ServerMessageID.SERVER_MESSAGE, encoder.GetBytes(message));
		}

		private void sendTextMessageToAll(String message, ServerClient exclude = null)
		{
			UnicodeEncoding encoder = new UnicodeEncoding();
			byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.TEXT_MESSAGE, encoder.GetBytes(message));

            foreach (var client in clients.Where(cl => cl.isReady && cl != exclude))
            {
                client.queueOutgoingMessage(message_bytes);
            }
		}

		private void sendTextMessage(ServerClient cl, String message)
		{
			UnicodeEncoding encoder = new UnicodeEncoding();
			cl.queueOutgoingMessage(KMPCommon.ServerMessageID.SERVER_MESSAGE, encoder.GetBytes(message));
		}

		private void sendPluginUpdateToAll(byte[] data, bool secondaryUpdate, ServerClient cl = null)
		{
			//Extract the KMPVesselUpdate & ProtoVessel, if present, for universe DB
			byte[] infoOnly_data = new byte[data.Length];
			byte[] owned_data = new byte[data.Length];
			byte[] past_data = new byte[data.Length];
			data.CopyTo(infoOnly_data,0);
			data.CopyTo(owned_data,0);
			data.CopyTo(past_data,0);
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
								Log.Info("New vessel " + vessel_update.kmpID + " from " + cl.username + " added to universe");
								cmd = universeDB.CreateCommand();
								sql = "INSERT INTO kmpVessel (Guid, GameGuid, OwnerID, Private, Active, ProtoVessel, Subspace)" +
									"VALUES ('"+vessel_update.kmpID+"','"+vessel_update.id+"',"+cl.playerID+","+Convert.ToInt32(vessel_update.isPrivate)+","+Convert.ToInt32(vessel_update.state==State.ACTIVE)+",@protoVessel,"+cl.currentSubspaceID+")";
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
									sql = "UPDATE kmpVessel SET Private = "+Convert.ToInt32(vessel_update.isPrivate)+", Active = "+Convert.ToInt32(vessel_update.state==State.ACTIVE)+", OwnerID="+cl.playerID+", ProtoVessel = @protoVessel WHERE Guid = '" + vessel_update.kmpID + "';";
									cmd.Parameters.Add("@protoVessel", DbType.Binary, protoVesselBlob.Length).Value = protoVesselBlob;
									cmd.CommandText = sql;
									cmd.ExecuteNonQuery();
									cmd.Dispose();
								}
								else
								{
									
									cmd = universeDB.CreateCommand();
									sql = "UPDATE kmpVessel SET Private = "+Convert.ToInt32(vessel_update.isPrivate)+", Active = "+Convert.ToInt32(vessel_update.state==State.ACTIVE)+", OwnerID="+cl.playerID+", ProtoVessel = @protoVessel, Subspace = "+cl.currentSubspaceID+" WHERE Guid = '" + vessel_update.kmpID + "';";
									cmd.Parameters.Add("@protoVessel", DbType.Binary, protoVesselBlob.Length).Value = protoVesselBlob;
									cmd.CommandText = sql;
									cmd.ExecuteNonQuery();
									cmd.Dispose();
									bool emptySubspace = true;
									foreach (ServerClient client in clients)
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
										sql = "DELETE FROM kmpSubspace WHERE ID = " + cl.currentSubspaceID + " AND LastTick < (SELECT MIN(s.LastTick) FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID);";
										cmd.CommandText = sql;
										cmd.ExecuteNonQuery();
										cmd.Dispose();
									}
								}
							}
							
							if (cl != null && cl.currentVessel != vessel_update.kmpID && cl.currentVessel != Guid.Empty)
							{
								
								try {
									cmd = universeDB.CreateCommand();
									sql = "UPDATE kmpVessel SET Active = 0 WHERE Guid = '" + cl.currentVessel + "'";
									cmd.CommandText = sql;
									cmd.ExecuteNonQuery();
									cmd.Dispose();
								} catch { }
								
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
									sql = "UPDATE kmpVessel SET Private = "+Convert.ToInt32(vessel_update.isPrivate)+", Active = "+Convert.ToInt32(vessel_update.state==State.ACTIVE)+", OwnerID="+cl.playerID+" WHERE Guid = '" + vessel_update.kmpID + "';";
									cmd.CommandText = sql;
									cmd.ExecuteNonQuery();
									cmd.Dispose();
								}
								else
								{
									
									cmd = universeDB.CreateCommand();
									sql = "UPDATE kmpVessel SET Private = "+Convert.ToInt32(vessel_update.isPrivate)+", Active = "+Convert.ToInt32(vessel_update.state==State.ACTIVE)+", OwnerID="+cl.playerID+", Subspace = "+cl.currentSubspaceID+" WHERE Guid = '" + vessel_update.kmpID + "';";
									cmd.CommandText = sql;
									cmd.ExecuteNonQuery();
									cmd.Dispose();
									bool emptySubspace = true;
									foreach (ServerClient client in clients)
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
										sql = "DELETE FROM kmpSubspace WHERE ID = " + cl.currentSubspaceID + " AND LastTick < (SELECT MIN(s.LastTick) FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID);";
										cmd.CommandText = sql;
										cmd.ExecuteNonQuery();
										cmd.Dispose();
									}
								}
							}
							
							if (cl != null && cl.currentVessel != vessel_update.kmpID && cl.currentVessel != Guid.Empty)
							{
								
								try {
									cmd = universeDB.CreateCommand();
									sql = "UPDATE kmpVessel SET Active = 0 WHERE Guid = '" + cl.currentVessel + "'";
									cmd.CommandText = sql;
									cmd.ExecuteNonQuery();
									cmd.Dispose();
								} catch { }
								
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
						try {
							bool active = false;
							cmd = universeDB.CreateCommand();
							sql = "SELECT kmpVessel.OwnerID, kmpVessel.Active FROM kmpVessel LEFT JOIN kmpSubspace ON kmpSubspace.ID = kmpVessel.Subspace WHERE Guid = '" + vessel_update.kmpID + "' ORDER BY kmpSubspace.LastTick DESC LIMIT 1";
							cmd.CommandText = sql;
							SQLiteDataReader reader = cmd.ExecuteReader(); 
							try {
								while(reader.Read())
								{ 
									OwnerID = reader.GetInt32(0);
									active = reader.GetBoolean(1);
								}
							} catch { }
							cmd.Dispose();
							
							if (!active || OwnerID == cl.playerID) //Inactive vessel or this player was last in control of it
							{
								if (vessel_update.getProtoVesselNode() != null)
								{
									//Store included protovessel, update subspace
									byte[] protoVesselBlob = ObjectToByteArray(vessel_update.getProtoVesselNode());
									cmd = universeDB.CreateCommand();
									sql = "UPDATE kmpVessel SET ProtoVessel = @protoVessel, Subspace = "+cl.currentSubspaceID+" WHERE Guid = '" + vessel_update.kmpID + "';";
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
						} catch { }
						
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
				Log.Info("Vessel update error: " + e.Message  + " " + e.StackTrace);
			}
			
			//Build the message array
			byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.PLUGIN_UPDATE, data);
			byte[] owned_message_bytes = buildMessageArray(KMPCommon.ServerMessageID.PLUGIN_UPDATE, owned_data);
			byte[] past_message_bytes = buildMessageArray(KMPCommon.ServerMessageID.PLUGIN_UPDATE, past_data);

            foreach (var client in clients.Where(c => c != cl && c.isReady && c.activityLevel != ServerClient.ActivityLevel.INACTIVE && (c.activityLevel == ServerClient.ActivityLevel.IN_GAME || !secondaryUpdate)))
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
		
		private void storeVesselUpdate(KMPVesselUpdate vessel_update, ServerClient cl, bool isSecondary = false)
		{
			byte[] updateBlob = ObjectToByteArray(vessel_update);
			SQLiteCommand cmd = universeDB.CreateCommand();
			string sql = "DELETE FROM kmpVesselUpdate WHERE Guid = '" + vessel_update.kmpID + "' AND Subspace = " + cl.currentSubspaceID + ";" +
				" INSERT INTO kmpVesselUpdate (Guid, Subspace, UpdateMessage)" +
				" VALUES ('"+vessel_update.kmpID+"',"+cl.currentSubspaceID+",@update);";
			if (!isSecondary) sql += " INSERT INTO kmpVesselUpdateHistory (Guid, Subspace, Tick, UpdateMessage)" +
				" VALUES ('"+vessel_update.kmpID+"',"+cl.currentSubspaceID+","+vessel_update.tick+",@update);";
			cmd.Parameters.Add("@update", DbType.Binary, updateBlob.Length).Value = updateBlob;
			cmd.CommandText = sql;
			cmd.ExecuteNonQuery();
			cmd.Dispose();	
		}
		
		private bool checkVesselDestruction(KMPVesselUpdate vessel_update, ServerClient cl)
		{
			try {
				if (!recentlyDestroyed.ContainsKey(vessel_update.kmpID) || (recentlyDestroyed[vessel_update.kmpID] + 1500L) < currentMillisecond)
				{
					SQLiteCommand cmd = universeDB.CreateCommand();
					string sql = "UPDATE kmpVessel SET Destroyed = " + Convert.ToInt32(vessel_update.situation == Situation.DESTROYED) + " WHERE Guid = '" + vessel_update.kmpID + "'";
					cmd.CommandText = sql;
					cmd.ExecuteNonQuery();
					cmd.Dispose();
					if (vessel_update.situation == Situation.DESTROYED) 
					{
						Log.Activity("Vessel " + vessel_update.kmpID + " reported as destroyed");
						recentlyDestroyed[vessel_update.kmpID] = currentMillisecond;
					}
					return vessel_update.situation == Situation.DESTROYED;
				} else return true;
			} catch { }	
			return false;
		}
		
		private void sendVesselStatusUpdateToAll(ServerClient cl, Guid vessel)
		{
            foreach (var client in clients.Where(c => c.isReady && c != cl && c.activityLevel != ServerClient.ActivityLevel.INACTIVE))
            {
                sendVesselStatusUpdate(client, vessel);
            }
		}
		
		private void sendVesselStatusUpdate(ServerClient cl, Guid vessel)
		{
			SQLiteCommand cmd = universeDB.CreateCommand();
			string sql = "SELECT vu.UpdateMessage, v.ProtoVessel, v.Private, v.OwnerID, v.Active" +
				" FROM kmpVesselUpdate vu" +
				" INNER JOIN kmpVessel v ON v.Guid = vu.Guid" +
				" WHERE vu.Subspace = " + cl.currentSubspaceID + " AND v.Guid = '" + vessel.ToString() + "';";
			cmd.CommandText = sql;
			SQLiteDataReader reader = cmd.ExecuteReader(); 
			try 
			{ 
				while(reader.Read()) 
				{ 
					KMPVesselUpdate vessel_update = (KMPVesselUpdate) ByteArrayToObject(GetDataReaderBytes(reader,0));
					ConfigNode protoVessel = (ConfigNode) ByteArrayToObject(GetDataReaderBytes(reader,1));
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
		
		private void sendScreenshot(ServerClient cl, byte[] bytes)
		{
			Log.Info("Sending screenshot to player " + cl.username);
			cl.queueOutgoingMessage(KMPCommon.ServerMessageID.SCREENSHOT_SHARE, bytes);
		}
		
		private void sendScreenshotToWatchers(ServerClient cl, byte[] bytes)
		{
			//Create a list of valid watchers
			List<int> watcher_indices = new List<int>();

            foreach(var client in clients.Where(c => c != cl && c.isReady && c.activityLevel != ServerClient.ActivityLevel.INACTIVE))
            {
                bool match = false;

					lock (client.watchPlayerNameLock)
					{
						match = client.watchPlayerName == cl.username;
					}

					if (match)
						watcher_indices.Add(client.clientIndex);
            }

			if (watcher_indices.Count > 0)
			{
				//Build the message and send it to all watchers
				byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.SCREENSHOT_SHARE, bytes);
				foreach (int i in watcher_indices)
				{
					clients[i].queueOutgoingMessage(message_bytes);
				}
			}
		}

		private void sendCraftFile(ServerClient cl, String craft_name, byte[] data, byte type)
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

            foreach (var client in clients.Where(c => c.isValid))
            {
                client.queueOutgoingMessage(message_bytes);
            }
		}

		private void sendServerSettings(ServerClient cl)
		{
			cl.queueOutgoingMessage(KMPCommon.ServerMessageID.SERVER_SETTINGS, serverSettingBytes());
		}
		
		private void sendSyncMessage(ServerClient cl, double tick)
		{
			//Log.Info("Time sync for: " + cl.username);
			byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.SYNC, BitConverter.GetBytes(tick));
			cl.queueOutgoingMessage(message_bytes);
		}
		
		private void sendSyncCompleteMessage(ServerClient cl)
		{
			byte[] message_bytes = buildMessageArray(KMPCommon.ServerMessageID.SYNC_COMPLETE, null);
			cl.queueOutgoingMessage(message_bytes);
		}
		
		private void sendVesselMessage(ServerClient cl, byte[] data)
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
			
			Int32 version = 0;
			try {
				SQLiteCommand cmd = diskDB.CreateCommand();
				string sql = "SELECT version FROM kmpInfo";
				cmd.CommandText = sql;
				version = Convert.ToInt32(cmd.ExecuteScalar());
			}
			catch { Log.Info("Missing (or bad) universe database file."); }
			finally
			{
				if (version != UNIVERSE_VERSION)
				{
					Log.Info("Creating new universe...");
					try {
						File.Delete("KMP_universe.db");
					} catch {}
					SQLiteCommand cmd = universeDB.CreateCommand();
					string sql = "CREATE TABLE kmpInfo (Version INTEGER);" +
						"INSERT INTO kmpInfo (Version) VALUES (" + UNIVERSE_VERSION + ");" +
						"CREATE TABLE kmpSubspace (ID INTEGER PRIMARY KEY AUTOINCREMENT, LastTick DOUBLE);" +
						"INSERT INTO kmpSubspace (LastTick) VALUES (100);" +
						"CREATE TABLE kmpPlayer (ID INTEGER PRIMARY KEY AUTOINCREMENT, Name NVARCHAR(100), Guid CHAR(40));" +
						"CREATE TABLE kmpVessel (Guid CHAR(40), GameGuid CHAR(40), OwnerID INTEGER, Private BIT, Active BIT, ProtoVessel BLOB, Subspace INTEGER, Destroyed BIT);" +
						"CREATE TABLE kmpVesselUpdate (ID INTEGER PRIMARY KEY AUTOINCREMENT, Guid CHAR(40), Subspace INTEGER, UpdateMessage BLOB);" +
						"CREATE TABLE kmpVesselUpdateHistory (Guid CHAR(40), Subspace INTEGER, Tick DOUBLE, UpdateMessage BLOB);";
					cmd.CommandText = sql;
					cmd.ExecuteNonQuery();
				}
				else
				{
					Log.Info("Loading universe...");
					diskDB.BackupDatabase(universeDB, "main", "main",-1, null, 0);
				}
				diskDB.Close();
			}
			
			SQLiteCommand cmd2 = universeDB.CreateCommand();
			string sql2 = "UPDATE kmpVessel SET Active = 0;";
			cmd2.CommandText = sql2;
			cmd2.ExecuteNonQuery();
			Log.Info("Universe OK.");
		}
		
		public void backupDatabase()
		{
			File.Delete(DB_FILE);
			SQLiteConnection diskDB = new SQLiteConnection(DB_FILE_CONN);
			diskDB.Open();
			universeDB.BackupDatabase(diskDB, "main", "main",-1, null, 0);
			SQLiteCommand cmd = diskDB.CreateCommand();
			string sql = "DELETE FROM kmpSubspace WHERE LastTick < (SELECT MIN(s.LastTick) FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID);" + 
				" DELETE FROM kmpVesselUpdateHistory;" +
				" DELETE FROM kmpVesselUpdate WHERE ID IN (SELECT ID FROM kmpVesselUpdate vu WHERE Subspace != (SELECT ID FROM kmpSubspace WHERE LastTick = (SELECT MAX(LastTick) FROM kmpSubspace WHERE ID IN (SELECT Subspace FROM kmpVesselUpdate WHERE Guid = vu.Guid))));";
			cmd.CommandText = sql;
			cmd.ExecuteNonQuery();
			diskDB.Close();
			Log.Info("Universe saved to disk.");
		}
		
		public void cleanDatabase()
		{
			SQLiteCommand cmd = universeDB.CreateCommand();
			string sql = "DELETE FROM kmpSubspace WHERE LastTick < (SELECT MIN(s.LastTick) FROM kmpSubspace s INNER JOIN kmpVessel v ON v.Subspace = s.ID);" + 
				" DELETE FROM kmpVesselUpdateHistory;" +
				" DELETE FROM kmpVesselUpdate WHERE ID IN (SELECT ID FROM kmpVesselUpdate vu WHERE Subspace != (SELECT ID FROM kmpSubspace WHERE LastTick = (SELECT MAX(LastTick) FROM kmpSubspace WHERE ID IN (SELECT Subspace FROM kmpVesselUpdate WHERE Guid = vu.Guid))));";
			cmd.CommandText = sql;
			cmd.ExecuteNonQuery();
			Log.Info("Optimized in-memory universe database.");
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
		    if(obj == null)
		        return null;
		    BinaryFormatter bf = new BinaryFormatter();
		    MemoryStream ms = new MemoryStream();
		    bf.Serialize(ms, obj);
		    return ms.ToArray();
		}
		
		private object ByteArrayToObject(byte[] data)
		{
		    if(data == null)
		        return null;
		    BinaryFormatter bf = new BinaryFormatter();
			MemoryStream ms = new MemoryStream(data);
			return bf.Deserialize(ms);
		}

        private T ByteArrayToObject<T>(byte[] data)
        {
            if (data == null)
                return default(T);
            BinaryFormatter bf = new BinaryFormatter();
            MemoryStream ms = new MemoryStream(data);
            try
            {
                return (T)bf.Deserialize(ms);
            }
            catch
            {
                return default(T);
            }
        }
		
		private string CleanInput(string strIn)
	    {
	        // Replace invalid characters with empty strings. 
	        try {
	           return Regex.Replace(strIn, @"[\r\n\x00\x1a\\'""]", ""); 
	        } catch { return String.Empty; }
		}
	}
}
