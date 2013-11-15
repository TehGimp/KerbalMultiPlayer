using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

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
		public const String AUTO_RECONNECT_LABEL = "reconnect";
		public const String FAVORITE_LABEL = "pos";

		//public const String INTEROP_CLIENT_FILENAME = "interopclient.txt";
		//public const String INTEROP_PLUGIN_FILENAME = "interopplugin.txt";
		public const String CLIENT_CONFIG_FILENAME = "KMPClientConfig.xml";
		public const String CLIENT_TOKEN_FILENAME = "KMPPlayerToken.txt";
		public const String PART_LIST_FILENAME = "KMPPartList.txt";
		public const String CRAFT_FILE_EXTENSION = ".craft";
		
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

		public const int INTEROP_WRITE_INTERVAL = 333;
		public const int INTEROP_MAX_QUEUE_SIZE = 64;

		public const int MAX_QUEUED_CHAT_LINES = 8;
		public const int DEFAULT_PORT = 2076;

		//public const String PLUGIN_DIRECTORY = "PluginData/kerbalmultiplayer/";

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
		public static String hostname = "localhost";
		public static int updateInterval = 100;
		public static int screenshotInterval = 1000;
		public static bool autoReconnect = true;
		public static byte inactiveShipsPerUpdate = 0;
		public static ScreenshotSettings screenshotSettings = new ScreenshotSettings();
		public static String[] favorites = new String[8];

		//Connection
		public static int clientID;
		public static bool endSession;
		public static bool intentionalConnectionEnd;
		public static bool handshakeCompleted;
		public static Socket tcpSocket;
		public static long lastTCPMessageSendTime;
		public static bool quitHelperMessageShow;
		public static int reconnectAttempts;
		public static Socket udpSocket;
		public static bool udpConnected;
		public static long lastUDPMessageSendTime;
		public static long lastUDPAckReceiveTime;
		
		public static bool receivedSettings;

		//Plugin Interop

		public static Queue<byte[]> interopOutQueue;
		public static long lastInteropWriteTime;
		public static Queue<byte[]> interopInQueue;

		public static Queue<byte[]> pluginUpdateInQueue;
		public static Queue<InTextMessage> textMessageQueue;
		public static long lastScreenshotShareTime;

		public static byte[] queuedOutScreenshot;
		public static byte[] lastSharedScreenshot;

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

		//Threading

		public static object tcpSendLock = new object();
		public static object serverSettingsLock = new object();
		public static object screenshotOutLock = new object();
		public static object threadExceptionLock = new object();
		public static object clientDataLock = new object();
		public static object udpTimestampLock = new object();
		public static object receiveBufferLock = new object();
		public static object debugLogLock = new object();
		public static object interopOutQueueLock = new object();

		public static String threadExceptionStackTrace;
		public static Exception threadException;
		
		public static Thread serverThread;
		public static Thread interopThread;
		public static Thread chatThread;
		public static Thread connectionThread;

		public static Stopwatch stopwatch;
		public static Stopwatch pingStopwatch = new Stopwatch();
		
		public static KMPManager gameManager;
		public static long lastPing;
		public static bool debugging = true;
		
		public static List<string> partList = new List<string>();
		
		
		public static void InitMPClient(KMPManager manager)
		{
			gameManager = manager;
			UnityEngine.Debug.Log("KMP Client version " + KMPCommon.PROGRAM_VERSION);
			UnityEngine.Debug.Log("    Created by Shaun Esau");
			UnityEngine.Debug.Log("    Based on Kerbal LiveFeed created by Alfred Lam");

			stopwatch = new Stopwatch();
			stopwatch.Start();
			
			for (int i = 0; i < favorites.Length; i++)
				favorites[i] = String.Empty;
		}
		
		
		public static String GetUsername()
		{
			return username;
		}
		
		public static void SetUsername(String newUsername)
		{
			username = newUsername;
			if (username.Length > MAX_USERNAME_LENGTH)
				username = username.Substring(0, MAX_USERNAME_LENGTH); //Trim username

			writeConfigFile();
		}
		
		public static void SetServer(String newHostname)
		{
			hostname = newHostname;
			writeConfigFile();
		}

		public static void SetAutoReconnect(bool newAutoReconnect)
		{
			autoReconnect = newAutoReconnect;
			writeConfigFile();
		}
		
		public static String[] GetFavorites()
		{
			return favorites;
		}
		
		public static void SetFavorites(String[] newFavorites)
		{
			favorites = newFavorites;
			writeConfigFile();
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
					KSP.IO.TextWriter writer = KSP.IO.File.AppendText<KMPClientMain>("KMPClientlog.txt");
					writer.WriteLine(e.ToString());
					if (threadExceptionStackTrace != null && threadExceptionStackTrace.Length > 0)
					{
						writer.WriteLine("KMP Stacktrace: ");
						writer.WriteLine(threadExceptionStackTrace);
					}
					writer.Close();

					UnityEngine.Debug.LogError(e.ToString());
					if (threadExceptionStackTrace != null && threadExceptionStackTrace.Length > 0)
					{
						UnityEngine.Debug.Log(threadExceptionStackTrace);
					}

					UnityEngine.Debug.LogError("Unexpected exception encountered! Crash report written to KMPClientlog.txt");
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
			IPHostEntry host_entry = new IPHostEntry();
			try
			{
				host_entry = Dns.GetHostEntry(trimmed_hostname);
			}
			catch (SocketException)
			{
				host_entry = null;
			}
			catch (ArgumentException)
			{
				host_entry = null;
			}

			IPAddress address = null;
			if (host_entry != null && host_entry.AddressList.Length == 1)
				address = host_entry.AddressList.First();
			else
				IPAddress.TryParse(trimmed_hostname, out address);

			if (address == null) {
				SetMessage("Invalid server address.");
				return false;
			}

			IPEndPoint endpoint = new IPEndPoint(address, port);

			SetMessage("Connecting to server: " + address + ":" + port);

			try
			{
				TcpClient tcpClient = new TcpClient();
				tcpClient.NoDelay = true;
				tcpClient.Connect(endpoint);
				tcpSocket = tcpClient.Client;
                //tcpSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.KeepAlive, true);

				if (tcpSocket.Connected)
				{
					SetMessage("TCP connection established");
					clientID = -1;
					endSession = false;
					intentionalConnectionEnd = false;
					handshakeCompleted = false;
					receivedSettings = false;

					pluginUpdateInQueue = new Queue<byte[]>();
					textMessageQueue = new Queue<InTextMessage>();
					lock (interopOutQueueLock)
					{
						interopOutQueue = new Queue<byte[]>();
					}
					interopInQueue = new Queue<byte[]>();

					receivedMessageQueue = new Queue<ServerMessage>();

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
						udpSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
						udpSocket.Connect(endpoint);
					}
					catch
					{
						if (udpSocket != null)
							udpSocket.Close();

						udpSocket = null;
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
					connectionThread.Start();

					beginAsyncRead();

					SetMessage("Connected to server! Handshaking...");
					
					while (!endSession && !intentionalConnectionEnd && tcpSocket.Connected)
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

					return true;
				}

			}
			catch (Exception)
			{
				SetMessage("Disconnected");
				if (tcpSocket != null)
					tcpSocket.Close();

				tcpSocket = null;
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
						sendHandshakeMessage(); //Reply to the handshake
						lock (udpTimestampLock)
						{
							lastUDPMessageSendTime = stopwatch.ElapsedMilliseconds;
						}
						handshakeCompleted = true;
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

						//Queue the message
						enqueueTextMessage(in_message);
					}

					break;

				case KMPCommon.ServerMessageID.MOTD_MESSAGE:
					
					if(data != null)
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
							}
							
							receivedSettings = true;
							/*
							UnityEngine.Debug.Log("Update interval: " + updateInterval);
							UnityEngine.Debug.Log("Screenshot interval: " + screenshotInterval);
							UnityEngine.Debug.Log("Inactive ships per update: " + inactiveShipsPerUpdate);
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

					if (data != null && data.Length > 4)
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

							//Write the craft to a file
							String filename = getCraftFilename(craft_name, craft_type);
							if (filename != null)
							{
								try
								{
									//KSP.IO.File.WriteAllBytes<KMPClientMain>(craft_bytes, filename);
									System.IO.File.WriteAllBytes(filename,craft_bytes);
									enqueueTextMessage("Received craft file: " + craft_name);
								}
								catch
								{
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
					if (data != null) gameManager.targetTick = BitConverter.ToDouble(data,0) + Convert.ToDouble(lastPing);
				
					break;
				case KMPCommon.ServerMessageID.SYNC_COMPLETE:
					gameManager.HandleSyncCompleted();
					break;
			}
		}

		public static void clearConnectionState()
		{
			try {
				//Abort all threads
				DebugLog("Aborting chat thread...");
				safeAbort(chatThread, true);
				DebugLog("Aborting connection thread...");
				safeAbort(connectionThread, true);
				DebugLog("Aborting interop thread...");
				safeAbort(interopThread, true);
				DebugLog("Aborting client thread...");
				safeAbort(serverThread, true);
				
				
				DebugLog("Closing connections...");
				//Close the socket if it's still open
				if (tcpSocket != null)
					tcpSocket.Close();
				tcpSocket = null;
				
				if (udpSocket != null)
					udpSocket.Close();
				udpSocket = null;	
			}
			catch (ThreadAbortException) { }
			DebugLog("Disconnected");
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

					if (line_lower == "!quit")
					{
						handled = true;
						intentionalConnectionEnd = true;
						endSession = true;
						sendConnectionEndMessage("Quit");
					}
                    else if(line_lower == "!whereami")
                    {
                        DebugLog("Sending whereami request");
                        handled = true;
                        sendMessageTCP(KMPCommon.ClientMessageID.WHEREAMI, null);
                    }
					else if (line_lower == "!ping")
					{
						handled = true;
						if (!pingStopwatch.IsRunning)
						{
							sendMessageTCP(KMPCommon.ClientMessageID.PING, null);
							pingStopwatch.Start();
						}
					}
					else if (line_lower == "!debug")
					{
						handled = true;
						debugging = !debugging;
						enqueuePluginChatMessage("debug " + debugging);
					}
					else if (line_lower == "!bubble")
					{
						if(gameManager.horizontalDistanceToSafetyBubbleEdge() < 1 || gameManager.verticalDistanceToSafetyBubbleEdge() < 1)
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
					else if (line_lower.Length > (KMPCommon.SHARE_CRAFT_COMMAND.Length + 1)
						&& line_lower.Substring(0, KMPCommon.SHARE_CRAFT_COMMAND.Length) == KMPCommon.SHARE_CRAFT_COMMAND)
					{
						handled = true;
						//Share a craft file
						String craft_name = line.Substring(KMPCommon.SHARE_CRAFT_COMMAND.Length + 1);
						byte craft_type = 0;
						String filename = findCraftFilename(craft_name, ref craft_type);

						if (filename != null && filename.Length > 0)
						{
							try
							{
								//byte[] craft_bytes = KSP.IO.File.ReadAllBytes<KMPClientMain>(filename);
								byte[] craft_bytes = System.IO.File.ReadAllBytes(filename);
								sendShareCraftMessage(craft_name, craft_bytes, craft_type);
							}
							catch
							{
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

					if (stopwatch.ElapsedMilliseconds - lastInteropWriteTime >= INTEROP_WRITE_INTERVAL)
					{
						if (writePluginInterop())
						{
							lastInteropWriteTime = stopwatch.ElapsedMilliseconds;
						}
					}

					//Throttle the rate at which you can share screenshots
					if (stopwatch.ElapsedMilliseconds - lastScreenshotShareTime > screenshotInterval)
					{
						lock (screenshotOutLock)
						{
							if (queuedOutScreenshot != null)
							{
								DebugLog ("screenshot");
								//Share the screenshot
								sendShareScreenshotMessage(queuedOutScreenshot);
								lastSharedScreenshot = queuedOutScreenshot;
								queuedOutScreenshot = null;
								lastScreenshotShareTime = stopwatch.ElapsedMilliseconds;
								
								//Send the screenshot back to the plugin if the player is watching themselves
								if (watchPlayerName == username)
									enqueueClientInteropMessage(KMPCommon.ClientInteropMessageID.SCREENSHOT_RECEIVE, lastSharedScreenshot);
								DebugLog ("done screenshot");
							}
						}
					}

					Thread.Sleep(SLEEP_TIME);
				}

			}
			catch (ThreadAbortException)
			{
			}
			catch (Exception e)
			{
				DebugLog("Error in handlePluginInterop: " + e.Message);
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
			catch (ThreadAbortException)
			{
			}
			catch (Exception e)
			{
				passExceptionToMain(e);
			}
		}

		static void handleConnection()
		{
			try
			{
				while (true)
				{
					if (pingStopwatch.IsRunning && pingStopwatch.ElapsedMilliseconds > PING_TIMEOUT_DELAY)
					{
						enqueueTextMessage("Ping timed out.", true);
						pingStopwatch.Stop();
						pingStopwatch.Reset();
					}

					//Send a keep-alive message to prevent timeout
					if (stopwatch.ElapsedMilliseconds - lastTCPMessageSendTime >= KEEPALIVE_DELAY)
						sendMessageTCP(KMPCommon.ClientMessageID.KEEPALIVE, null);

					//Handle received messages
					while (receivedMessageQueue.Count > 0)
					{
						ServerMessage message;
						message = receivedMessageQueue.Dequeue();
						handleMessage(message.id, message.data);
					}

					if (udpSocket != null && handshakeCompleted)
					{

						//Update the status of the udp connection
						long last_udp_ack = 0;
						long last_udp_send = 0;
						lock (udpTimestampLock) {
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
							if ((stopwatch.ElapsedMilliseconds - last_udp_ack) > UDP_TIMEOUT_DELAY*10)
								throw new Exception("UDP connection lost and could not be reconnected.");
						}

						//Send a probe message to try to establish a udp connection
						if ((stopwatch.ElapsedMilliseconds - last_udp_send) > UDP_TIMEOUT_DELAY)
						{
							sendUDPProbeMessage(true);
							KMPClientMain.DebugLog("PROBE");
						}
						else if ((stopwatch.ElapsedMilliseconds - last_udp_send) > UDP_PROBE_DELAY)
							sendUDPProbeMessage(false);

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

								UnityEngine.Debug.Log(message.message);
							}
						}
						catch (KSP.IO.IOException)
						{
						}
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
			catch (ThreadAbortException) { }
			catch (ThreadStateException) { }
			catch (ThreadInterruptedException) { }
		}

		//Plugin Interop

		static bool writePluginInterop()
		{
			bool success = false;
			lock (interopOutQueueLock)
			{
				if (interopOutQueue.Count > 0)
				{
					try
					{
						while (interopOutQueue.Count > 0)
						{
							byte[] message;
							message = interopOutQueue.Dequeue();
							KSP.IO.MemoryStream ms = new KSP.IO.MemoryStream();
						    ms.Write(KMPCommon.intToBytes(KMPCommon.FILE_FORMAT_VERSION), 0, 4);
							ms.Write(message,0,message.Length);
							gameManager.acceptClientInterop(ms.ToArray());
						}
						success = true;
					}
					catch
					{
					}
				}
			}
			return success;
		}
		
		
		static void processPluginInterop()
		{
			if (interopInQueue.Count > 0 )
			{
				try
				{
					while (interopInQueue.Count > 0)
					{
						byte[] bytes;
						bytes = interopInQueue.Dequeue();
						if (bytes != null && bytes.Length > 0)
						{
							//Read the file-format version
							int file_version = KMPCommon.intFromBytes(bytes, 0);
			
							if (file_version != KMPCommon.FILE_FORMAT_VERSION)
							{
								//Incompatible client version
								DebugLog("KMP Client incompatible with plugin");
								return;
							}
			
							//Parse the messages
							int index = 4;
							while (index < bytes.Length - KMPCommon.INTEROP_MSG_HEADER_LENGTH)
							{
								//Read the message id
								int id_int = KMPCommon.intFromBytes(bytes, index);
			
								KMPCommon.PluginInteropMessageID id = KMPCommon.PluginInteropMessageID.NULL;
								if (id_int >= 0 && id_int < Enum.GetValues(typeof(KMPCommon.PluginInteropMessageID)).Length)
									id = (KMPCommon.PluginInteropMessageID)id_int;
								
								//Read the length of the message data
								int data_length = KMPCommon.intFromBytes(bytes, index+4);
			
								index += KMPCommon.INTEROP_MSG_HEADER_LENGTH;
			
								if (data_length <= 0)
									handleInteropMessage(id, null);
								else if (data_length <= (bytes.Length - index))
								{
									
									//Copy the message data
									byte[] data = new byte[data_length];
									Array.Copy(bytes, index, data, 0, data.Length);
			
									handleInteropMessage(id, data);
								}
			
								if (data_length > 0)
									index += data_length;
							}
						}
					}
				}
				catch { }
			}
		}
		public static void acceptPluginInterop(byte[] bytes)
		{
			try {
				interopInQueue.Enqueue(bytes);
			} catch { }
		}
		
//		static void readPluginInterop()
//		{
//
//			byte[] bytes = null;
//
//			if (KSP.IO.File.Exists<KMPClientMain>(INTEROP_PLUGIN_FILENAME))
//			{
//
//				try
//				{
//					bytes = KSP.IO.File.ReadAllBytes<KMPClientMain>(INTEROP_PLUGIN_FILENAME);
//					KSP.IO.File.Delete<KMPClientMain>(INTEROP_PLUGIN_FILENAME);
//				}
//				catch (KSP.IO.IOException)
//				{
//				}
//
//			}
//
//			if (bytes != null && bytes.Length > 0)
//			{
//				//Read the file-format version
//				int file_version = KMPCommon.intFromBytes(bytes, 0);
//
//				if (file_version != KMPCommon.FILE_FORMAT_VERSION)
//				{
//					//Incompatible client version
//					UnityEngine.Debug.Log("KMP Client incompatible with plugin");
//					return;
//				}
//
//				//Parse the messages
//				int index = 4;
//				while (index < bytes.Length - KMPCommon.INTEROP_MSG_HEADER_LENGTH)
//				{
//					//Read the message id
//					int id_int = KMPCommon.intFromBytes(bytes, index);
//
//					KMPCommon.PluginInteropMessageID id = KMPCommon.PluginInteropMessageID.NULL;
//					if (id_int >= 0 && id_int < Enum.GetValues(typeof(KMPCommon.PluginInteropMessageID)).Length)
//						id = (KMPCommon.PluginInteropMessageID)id_int;
//
//					//Read the length of the message data
//					int data_length = KMPCommon.intFromBytes(bytes, index+4);
//
//					index += KMPCommon.INTEROP_MSG_HEADER_LENGTH;
//
//					if (data_length <= 0)
//						handleInteropMessage(id, null);
//					else if (data_length <= (bytes.Length - index))
//					{
//						
//						//Copy the message data
//						byte[] data = new byte[data_length];
//						Array.Copy(bytes, index, data, 0, data.Length);
//
//						handleInteropMessage(id, data);
//					}
//
//					if (data_length > 0)
//						index += data_length;
//				}
//			}
//
//		}

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
							sendMessageTCP(KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_FLIGHT, null);
						else
							sendMessageTCP(KMPCommon.ClientMessageID.ACTIVITY_UPDATE_IN_GAME, null);
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
					sendMessageTCP(KMPCommon.ClientMessageID.WARPING, data);
					break;
				case KMPCommon.PluginInteropMessageID.SSYNC:
					sendMessageTCP(KMPCommon.ClientMessageID.SSYNC, data);
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
				interopOutQueue.Enqueue(message_bytes);

				//Enforce max queue size
				while (interopOutQueue.Count > INTEROP_MAX_QUEUE_SIZE)
				{
					interopOutQueue.Dequeue();
				}
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
				UnityEngine.Debug.Log(message);
		}

		static void safeDelete(String filename)
		{
			if (KSP.IO.File.Exists<KMPClientMain>(filename))
			{
				try
				{
					KSP.IO.File.Delete<KMPClientMain>(filename);
				}
				catch (KSP.IO.IOException)
				{
				}
			}
		}

		static String findCraftFilename(String craft_name, ref byte craft_type)
		{
			String vab_filename = getCraftFilename(craft_name, KMPCommon.CRAFT_TYPE_VAB);
			if (vab_filename != null && System.IO.File.Exists(vab_filename))
			{
				craft_type = KMPCommon.CRAFT_TYPE_VAB;
				return vab_filename;
			}

			String sph_filename = getCraftFilename(craft_name, KMPCommon.CRAFT_TYPE_SPH);
			if (sph_filename != null && System.IO.File.Exists(sph_filename))
			{
				craft_type = KMPCommon.CRAFT_TYPE_SPH;
				return sph_filename;
			}

			return null;

		}

		static String getCraftFilename(String craft_name, byte craft_type)
		{
			//Filter the craft name for illegal characters
			String filtered_craft_name = KMPCommon.filteredFileName(craft_name.Replace('.', '_'));

			if (currentGameTitle.Length <= 0 || filtered_craft_name.Length <= 0)
				return null;

			switch (craft_type)
			{
				case KMPCommon.CRAFT_TYPE_VAB:
					return "saves/" + currentGameTitle + "/Ships/VAB/" + filtered_craft_name + CRAFT_FILE_EXTENSION;
					
				case KMPCommon.CRAFT_TYPE_SPH:
					return "saves/" + currentGameTitle + "/Ships/SPH/" + filtered_craft_name + CRAFT_FILE_EXTENSION;
			}

			return null;

		}

		//Config

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
						xmlDoc.LoadXml(String.Format("<?xml version=\"1.0\"?><settings><global {0}=\"\" {1}=\"\" {2}=\"\"/><favourites></favourites></settings>", USERNAME_LABEL, IP_LABEL, AUTO_RECONNECT_LABEL));
						xmlDoc.Save(sPath);
					}
					
					xmlDoc.Load(sPath);
				}
				catch
				{
					try
					{
						xmlDoc.LoadXml(String.Format("<?xml version=\"1.0\"?><settings><global {0}=\"\" {1}=\"\" {2}=\"\"/><favourites></favourites></settings>", USERNAME_LABEL, IP_LABEL, AUTO_RECONNECT_LABEL));
						xmlDoc.Save(sPath);
						xmlDoc.Load(sPath);
					} catch { }
					
				}
				username = xmlDoc.SelectSingleNode("/settings/global/@"+USERNAME_LABEL).Value;
				hostname = xmlDoc.SelectSingleNode("/settings/global/@" + IP_LABEL).Value;
				bool.TryParse(xmlDoc.SelectSingleNode("/settings/global/@" + AUTO_RECONNECT_LABEL).Value, out autoReconnect);
	
				XmlNodeList elemList = xmlDoc.GetElementsByTagName("favourite");
				foreach(XmlNode xmlNode in elemList)
				{
					int nPos = -1;
					int.TryParse(xmlNode.Attributes[FAVORITE_LABEL].Value, out nPos);
					if(nPos >= 0 && nPos < favorites.Length)
					{
						favorites[nPos] = xmlNode.Attributes[IP_LABEL].Value;
					}
				}
			} catch {
				username = "";
				hostname = "";
				autoReconnect = true;
			}
				
			
			
			try
			{
				//Get the player's token if available
				KSP.IO.TextReader reader = KSP.IO.File.OpenText<KMPClientMain>(CLIENT_TOKEN_FILENAME);
				String line = reader.ReadLine();
				reader.Close();
				playerGuid = new Guid(line);
			}
			catch
			{
				//Generate a new token for server authentication
				playerGuid = Guid.NewGuid();
				KSP.IO.TextWriter writer = KSP.IO.File.CreateText<KMPClientMain>(CLIENT_TOKEN_FILENAME);
				writer.WriteLine(playerGuid.ToString());
				writer.Close();
			}
			
			try
			{
				//Get the part list if available
				KSP.IO.TextReader reader = KSP.IO.File.OpenText<KMPClientMain>(PART_LIST_FILENAME);
				List<string> lines = new List<string>();
				while (!reader.EndOfStream)
				{
					lines.Add(reader.ReadLine());
				}
				reader.Close();
				partList = lines;

				bool changed = false;
				if (!lines.Contains("kerbalEVA"))
				{
					partList.Add("kerbalEVA");
					changed = true;
				}
				if (!lines.Contains("flag"))
				{
					partList.Add("flag");
					changed = true;
				}
				if (changed)
				{
					KSP.IO.TextWriter writer = KSP.IO.File.CreateText<KMPClientMain>(PART_LIST_FILENAME);
					foreach (string part in partList)
						writer.WriteLine(part);
					writer.Close();
				}
			}
			catch
			{
				//Generate the stock part list
				partList = new List<string>();
				
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
				
				//Write to disk
				KSP.IO.TextWriter writer = KSP.IO.File.CreateText<KMPClientMain>(PART_LIST_FILENAME);
				foreach (string part in partList)
					writer.WriteLine(part);
				writer.Close();
			}
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
			
			for (int i = 0; i < favorites.Length; i++) // Rebuild the favourites from memory
			{
				if (!(favorites[i] == ""))
				{
					XmlElement xEl = xmlDoc.CreateElement("favourite");

					xEl.SetAttribute(FAVORITE_LABEL, "" + i);
					xEl.SetAttribute(IP_LABEL, favorites[i]);
					xFav.AppendChild(xEl);
				}
			}
			
			xmlDoc.Save(sPath); // Save :)
		}

		//Messages

		private static void beginAsyncRead()
		{
			try
			{
				if (tcpSocket != null)
				{
					currentMessageHeaderIndex = 0;
					currentMessageDataIndex = 0;
					receiveIndex = 0;
					receiveHandleIndex = 0;
					
				    StateObject state = new StateObject();
				    state.workSocket = tcpSocket;
				    tcpSocket.BeginReceive(state.buffer, 0, StateObject.BufferSize, 0,
				        new AsyncCallback(ReceiveCallback), state);
				}
			}
			catch (KSP.IO.IOException)
			{
			}
			catch (InvalidOperationException)
			{
			}
			catch (Exception e)
			{
				passExceptionToMain(e);
			}
		}
		
		private static void ReceiveCallback( IAsyncResult ar ) {
		    try {
		        // Retrieve the state object and the client socket 
		        // from the asynchronous state object.
		        StateObject state = (StateObject) ar.AsyncState;
		        Socket client = state.workSocket;
		        // Read data from the remote device.
		        int bytesRead = client.EndReceive(ar);
		        if (bytesRead > 0) {
		            // There might be more data, so store the data received so far.
					lock (receiveBufferLock) {
						KSP.IO.MemoryStream ms = new KSP.IO.MemoryStream();
	    				ms.Write(receiveBuffer, 0, receiveIndex);
						ms.Write(state.buffer, 0, bytesRead);
			            receiveBuffer = ms.ToArray();
						receiveIndex += bytesRead;
						handleReceive();
			            //  Get the rest of the data.
			            client.BeginReceive(state.buffer,0,StateObject.BufferSize,0,
			                new AsyncCallback(ReceiveCallback), state);
					}
		        } else {
//		            // All the data has arrived
//		            if (receiveIndex > 1) {
//						//LogAndShare("Done:" + System.Text.Encoding.ASCII.GetString(receiveBuffer));
//		                
//		            }
		        }
		    }
			catch (InvalidOperationException)
			{
			}
			catch (ThreadAbortException)
			{
			}
			catch (Exception e) {
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
			sendMessageTCP(KMPCommon.ClientMessageID.HANDSHAKE, message_data);
		}

		private static void sendTextMessage(String message)
		{
			//Encode message
			byte[] message_bytes = encoder.GetBytes(message);

			sendMessageTCP(KMPCommon.ClientMessageID.TEXT_MESSAGE, message_bytes);
		}

		private static void sendPluginUpdate(byte[] data, bool primary)
		{
			if (data != null && data.Length > 0)
			{
				KMPCommon.ClientMessageID id
					= primary ? KMPCommon.ClientMessageID.PRIMARY_PLUGIN_UPDATE : KMPCommon.ClientMessageID.SECONDARY_PLUGIN_UPDATE;
				
				if (udpConnected && data.Length < 100)
					sendMessageUDP(id, data);
				else
					sendMessageTCP(id, data);
			}
		}

		private static void sendShareScreenshotMessage(byte[] data)
		{
			if (data != null && data.Length > 0)
				sendMessageTCP(KMPCommon.ClientMessageID.SCREENSHOT_SHARE, data);
		}

		private static void sendScreenshotWatchPlayerMessage(String name)
		{
			//Encode name
			byte[] bytes = encoder.GetBytes(name);

			sendMessageTCP(KMPCommon.ClientMessageID.SCREEN_WATCH_PLAYER, bytes);
		}

		private static void sendConnectionEndMessage(String message)
		{
			//Encode message
			byte[] message_bytes = encoder.GetBytes(message);

			sendMessageTCP(KMPCommon.ClientMessageID.CONNECTION_END, message_bytes);
		}

		private static void sendShareCraftMessage(String craft_name, byte[] data, byte type)
		{
			//Encode message
			byte[] name_bytes = encoder.GetBytes(craft_name);

			byte[] bytes = new byte [5 + name_bytes.Length + data.Length];

			//Check size of data to make sure it's not too large
			if ((name_bytes.Length + data.Length) <= KMPCommon.MAX_CRAFT_FILE_BYTES)
			{
				//Copy data
				bytes[0] = type;
				KMPCommon.intToBytes(name_bytes.Length).CopyTo(bytes, 1);
				name_bytes.CopyTo(bytes, 5);
				data.CopyTo(bytes, 5 + name_bytes.Length);

				sendMessageTCP(KMPCommon.ClientMessageID.SHARE_CRAFT_FILE, bytes);
			}
			else
				enqueueTextMessage("Craft file is too large to send.", false, true);

			
		}

		private static void sendMessageTCP(KMPCommon.ClientMessageID id, byte[] data)
		{
			byte[] message_bytes = buildMessageByteArray(id, data);

			lock (tcpSendLock)
			{
				try
				{
					//Send message
					tcpSocket.Send(message_bytes, message_bytes.Length, SocketFlags.None);
//					Just do a blocking send
//					tcpSocket.BeginSend(message_bytes, 0, message_bytes.Length, SocketFlags.None,
//      					new AsyncCallback(SendCallback), tcpSocket); 
				}
				catch (System.InvalidOperationException) { }
				catch (KSP.IO.IOException) { }

			}

			lastTCPMessageSendTime = stopwatch.ElapsedMilliseconds;
		}
		
//		private static void SendCallback(IAsyncResult ar) {
//		    try {
//		        // Retrieve the socket from the state object.
//		        Socket client = (Socket) ar.AsyncState;
//		
//		        // Complete sending the data to the remote device.
//		        client.EndSend(ar);
//		
//		    } catch (Exception e) {
//		        passExceptionToMain(e);
//		    }
//		}

		private static void sendUDPProbeMessage(bool forceUDP)
		{
			byte[] time = null;
			if (gameManager.lastTick > 0d) time = BitConverter.GetBytes(gameManager.lastTick);
			if (udpConnected || forceUDP)//Always try UDP periodically
				sendMessageUDP(KMPCommon.ClientMessageID.UDP_PROBE, time);
			else sendMessageTCP(KMPCommon.ClientMessageID.UDP_PROBE, time);
		}

		private static void sendMessageUDP(KMPCommon.ClientMessageID id, byte[] data)
		{
			if (udpSocket != null)
			{
				//Send the packet
				try
				{
					udpSocket.Send(buildMessageByteArray(id, data, KMPCommon.intToBytes(clientID)));
				}
				catch { }

				lock (udpTimestampLock)
				{
					lastUDPMessageSendTime = stopwatch.ElapsedMilliseconds;
				}

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
			DebugLog(newMessage);
		}
		
		public static void DebugLog(String logEntry)
		{
			if (debugging)
				UnityEngine.Debug.Log(logEntry);
//			lock (debugLogLock)
//			{
//				logEntry = Thread.CurrentThread.ManagedThreadId + " " + logEntry;
//				KSP.IO.TextWriter debugLog = File.AppendText<KMPClientMain>("debug");
//				debugLog.WriteLine(logEntry);
//				debugLog.Close();
//			}
		}

		internal static void verifyShipsDirectory()
		{
			char cSep = '/';
			String sPath = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
			System.IO.DirectoryInfo dir = System.IO.Directory.GetParent(sPath);
			dir = dir.Parent.Parent;
			sPath = dir.FullName;
			sPath += "/saves/KMP/";
			if (!System.IO.Directory.Exists(sPath))
				System.IO.Directory.CreateDirectory(sPath);
			sPath += cSep+"Ships";
			//DebugLog(sPath);
			if(!System.IO.Directory.Exists(sPath))
				System.IO.Directory.CreateDirectory(sPath);

			if(!System.IO.Directory.Exists(sPath + cSep + "VAB"))
				System.IO.Directory.CreateDirectory(sPath + cSep + "VAB");

			if(!System.IO.Directory.Exists(sPath + cSep + "SPH"))
				System.IO.Directory.CreateDirectory(sPath + cSep + "SPH");
		}

		internal static bool startSaveExists()
		{
			String sPath = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
			System.IO.DirectoryInfo dir = System.IO.Directory.GetParent(sPath);
			dir = dir.Parent.Parent;
			sPath = dir.FullName;
			sPath += "/saves/kmp/";
			if (!System.IO.File.Exists(sPath + "start.sfs"))
				return false;
			else
				return true;
		}
	}
	
	public class StateObject {
	    // Client socket.
	    public Socket workSocket = null;
	    // Size of receive buffer.
	    public const int BufferSize = 8192;
	    // Receive buffer.
	    public byte[] buffer = new byte[BufferSize];
	    // Received data string.
	    public byte[] data = new byte[BufferSize];
	}
}
