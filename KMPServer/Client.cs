using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Net.Sockets;
using System.Threading;
using System.Net;
using System.Collections;
using System.Collections.Concurrent;

namespace KMPServer
{
	class Client
	{
		public enum ActivityLevel
		{
			INACTIVE,
			IN_GAME,
			IN_FLIGHT
		}

		//Repurpose SEND_BUFFER_SIZE to split large messages.
		public const int SEND_BUFFER_SIZE = 8192;
        public const int POLL_INTERVAL = 60000;

		//Properties

		public Server parent
		{
			private set;
			get;
		}
		public int clientIndex;
		public String username;
		public Guid guid;
		public int playerID;
		public Guid currentVessel = Guid.Empty;
		public int currentSubspaceID = -1;
		public double enteredSubspaceAt = 0d;
		public double lastTick = 0d;
		public double syncOffset = 0.01d;
		public int lagWarning = 0;
		public bool warping = false;
		public bool hasReceivedScenarioModules = false;
		public float averageWarpRate = 1f;
		
		public bool receivedHandshake;

		public byte[] screenshot;
		public String watchPlayerName;
		public byte[] sharedCraftFile;
		public String sharedCraftName;
		public KMPCommon.CraftType sharedCraftType;

		public long connectionStartTime;
		public long lastReceiveTime;
		public long lastSyncTime;
		public long lastUDPACKTime;
        public long lastPollTime = 0;

		public long lastInGameActivityTime;
		public long lastInFlightActivityTime;
		public ActivityLevel activityLevel;

		public TcpClient tcpClient;

		public object tcpClientLock = new object();
		public object timestampLock = new object();
		public object activityLevelLock = new object();
		public object screenshotLock = new object();
		public object watchPlayerNameLock = new object();
		public object sharedCraftLock = new object();
		public object sendOutgoingMessagesLock = new object();


		public byte[] currentMessage; //Switches between holding header and message data.
		public int currentBytesToReceive; //Switches between the bytes needed for a complete header or message.
		public bool currentMessageHeaderRecieved; //If false, receiving header, if true, reciving message.
		public KMPCommon.ClientMessageID currentMessageID;

		private bool isServerSendingData = false;
		private byte[] splitMessageData;
		private int splitMessageReceiveIndex;



		public ConcurrentQueue<byte[]> queuedOutMessagesHighPriority;
		public ConcurrentQueue<byte[]> queuedOutMessagesSplit;
		public ConcurrentQueue<byte[]> queuedOutMessages;
		
		public string disconnectMessage = "";
		
		public Client(Server parent)
		{
			this.parent = parent;
			resetProperties();
		}

        public bool isValid
        {
            get
            {
               // bool isConnected = false;
                if (this.tcpClient != null && this.tcpClient.Connected)
                {                   
                    Socket clientSocket = this.tcpClient.Client;
                    try
                    {
						if (receivedHandshake)
						{
							if ((parent.currentMillisecond - lastPollTime) > POLL_INTERVAL)
							{
							    lastPollTime = parent.currentMillisecond;
								//Removed redundant "Socket.Available" check and increased the Poll "Timeout" from 10ms to 500ms - Dani
								//Change SocketRead to SocketWrite. Also, no need for such high timeout.
								return clientSocket.Poll(200000, SelectMode.SelectWrite);
							}
							else
							{
							    // They have max 10 seconds to get their shit together. 
							    return true;
							}
						} else return true;
                    }
                    catch (SocketException)
                    {
                        // Unknown error
                        return false;
                    } catch (ObjectDisposedException)
                    {
                        // Socket closed
                        return false;
                    }
                    catch (Exception ex)
                    {
                        // Shouldn't happen, pass up.
                        parent.passExceptionToMain(ex);
                    }
                    
                    return true;
                }
                return false;
            }
        }

        public bool isReady
        {
            get
            {
                return (isValid && this.receivedHandshake);
            }
        }

        public IPAddress IPAddress
        {
            get
            {
                if (tcpClient == null) { return null; }
                return (tcpClient.Client.RemoteEndPoint as IPEndPoint).Address;
            }
        }

		public void resetProperties()
		{
			username = "";
			screenshot = null;
			watchPlayerName = String.Empty;
			receivedHandshake = false;

			sharedCraftFile = null;
			sharedCraftName = String.Empty;
			sharedCraftType = KMPCommon.CraftType.VAB;

			lastUDPACKTime = 0;

			queuedOutMessagesHighPriority = new ConcurrentQueue<byte[]>();
			queuedOutMessagesSplit = new ConcurrentQueue<byte[]>();
			queuedOutMessages = new ConcurrentQueue<byte[]>();

			lock (activityLevelLock)
			{
				activityLevel = Client.ActivityLevel.INACTIVE;
				lastInGameActivityTime = parent.currentMillisecond;
				lastInFlightActivityTime = parent.currentMillisecond;
			}

			lock (timestampLock)
			{
				lastReceiveTime = parent.currentMillisecond;
				connectionStartTime = parent.currentMillisecond;
				lastSyncTime = parent.currentMillisecond;
			}
		}

		public void updateReceiveTimestamp()
		{
			lock (timestampLock)
			{
				lastReceiveTime = parent.currentMillisecond;
			}
		}

		public void disconnected()
		{
			screenshot = null;
			watchPlayerName = String.Empty;

			sharedCraftFile = null;
			sharedCraftName = String.Empty;
		}

		//Async read

		private void beginAsyncRead()
		{
			try
			{
				if (tcpClient != null)
				{
					currentMessage = new byte[KMPCommon.MSG_HEADER_LENGTH]; //The first data we want to receive is the header size.
					currentMessageHeaderRecieved = false; //This is set to true while receiving the actual message and not its header.
					currentBytesToReceive = KMPCommon.MSG_HEADER_LENGTH; //We want to receive just the header.
					StateObject state = new StateObject();
					state.workClient = tcpClient;
					tcpClient.GetStream().BeginRead(currentMessage, 0, currentBytesToReceive, new AsyncCallback(asyncReceive), state);
				}
			}
			catch (InvalidOperationException)
			{
				//parent.disconnectClient(this, "InvalidOperationException");
				Log.Debug("Caught InvalidOperationException for player " + this.username + " in beginAsyncRead");
				//parent.markClientForDisconnect(this);
			}
			catch (System.IO.IOException)
			{
                //parent.disconnectClient(this, "IOException");
				Log.Debug("Caught IOException for player " + this.username + " in beginAsyncRead");
				//parent.markClientForDisconnect(this);
			}
			catch (Exception e)
			{
				parent.passExceptionToMain(e);
			}
		}

		private void asyncReceive(IAsyncResult ar)
		{
			try {
				// Retrieve the state object and the client socket 
				// from the asynchronous state object.
				StateObject state = (StateObject)ar.AsyncState;
				TcpClient client = state.workClient;
				int bytesRead = client.GetStream().EndRead(ar); // Read data from the remote device directly into the message buffer.
				updateReceiveTimestamp();
				currentBytesToReceive -= bytesRead; //Decrement how many bytes we have read.
				if (bytesRead > 0) { //This is just a shortcut really
					if (!currentMessageHeaderRecieved) {
						//We are receiving just the header
						if (currentBytesToReceive == 0) {
							//We have recieved the full message header, lets process it.
							currentMessageID = (KMPCommon.ClientMessageID)BitConverter.ToInt32(currentMessage, 0);
							currentBytesToReceive = BitConverter.ToInt32(currentMessage, 4);
							if (currentBytesToReceive == 0) {
								//We received the header of a empty message, process it and reset the buffers.
								messageReceived(currentMessageID, null);
								currentMessageID = KMPCommon.ClientMessageID.NULL;
								currentBytesToReceive = KMPCommon.MSG_HEADER_LENGTH;
								currentMessage = new byte[currentBytesToReceive];
							} else {
								//We received the header of a non-empty message, Let's give it a buffer and read again.
								currentMessage = new byte[currentBytesToReceive];
								currentMessageHeaderRecieved = true;
							}
						}
					} else {
						if (currentBytesToReceive == 0) {
							//We have received all the message data, lets decompress and process it
							byte[] decompressedData = KMPCommon.Decompress(currentMessage);
							messageReceived(currentMessageID, decompressedData);
							currentMessageHeaderRecieved = false;
							currentMessageID = KMPCommon.ClientMessageID.NULL;
							currentBytesToReceive = KMPCommon.MSG_HEADER_LENGTH;
							currentMessage = new byte[currentBytesToReceive];
						}
					}

				}
				if (currentBytesToReceive < 0) {
					throw new System.IO.IOException("You somehow managed to read more bytes then we asked for. Good for you. Open this up on the bugtracker now.");
				}
				if (client != null) {
					client.GetStream().BeginRead(currentMessage, currentMessage.Length - currentBytesToReceive, currentBytesToReceive, new AsyncCallback(asyncReceive), state);
				}
			}
			catch (Exception e) {
				//Basically, If anything goes wrong at all the stream is broken and there is no way to recover from it.
				Log.Debug("Exception thrown in ReceiveCallback(), catch 1, Exception: {0}", e.ToString());
			}
		}

		//Async send

		private void asyncSend(IAsyncResult result)
		{
			try
			{
                if (tcpClient.Connected)
                {
                    tcpClient.GetStream().EndWrite(result);
					isServerSendingData = false;
					if (queuedOutMessagesHighPriority.Count > 0 || queuedOutMessagesSplit.Count > 0 || queuedOutMessages.Count > 0) {
						sendOutgoingMessages();
					}
                }
                else
                {
                    //Do we care?!
                }
			}
			catch (InvalidOperationException)
			{
			}
			catch (System.IO.IOException)
			{
			}
			catch (ThreadAbortException)
			{
			}
			catch (Exception e)
			{
				parent.passExceptionToMain(e);
			}
		}
		
		//Messages

		private void messageReceived (KMPCommon.ClientMessageID id, byte[] data)
		{
			if (id == KMPCommon.ClientMessageID.SPLIT_MESSAGE) {
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
					//Parse the message and feed it into the client queue
					KMPCommon.ClientMessageID joined_message_id = (KMPCommon.ClientMessageID)KMPCommon.intFromBytes (splitMessageData, 0);
					int joined_message_length = KMPCommon.intFromBytes (splitMessageData, 4);
					byte[] joined_message_data = new byte[joined_message_length];
					Array.Copy (splitMessageData, 8, joined_message_data, 0, joined_message_length);
					byte[] joined_message_data_decompressed = KMPCommon.Decompress (joined_message_data);
					parent.queueClientMessage (this, joined_message_id, joined_message_data_decompressed);
					splitMessageReceiveIndex = 0;
				}
			} else {
				parent.queueClientMessage (this, id, data);
			}
		}

		public void sendOutgoingMessages()
		{

			try
			{
				lock (sendOutgoingMessagesLock) {
					if ((queuedOutMessagesHighPriority.Count > 0 || queuedOutMessagesSplit.Count > 0 || queuedOutMessages.Count > 0) && !isServerSendingData)
					{
						//Send high priority first, then any split messages (chopped up low priority messages), and then the normal queue.
						//Large low priorty messages get chopped up into a split message just before send.
						byte[] next_message = null;
						if (queuedOutMessagesHighPriority.Count > 0) {
							queuedOutMessagesHighPriority.TryDequeue(out next_message);
						} else {
							if (queuedOutMessagesSplit.Count > 0) {
									queuedOutMessagesSplit.TryDequeue(out next_message);
							} else {
								queuedOutMessages.TryDequeue(out next_message);
								splitOutgoingMessage(ref next_message);
							}
						}
						isServerSendingData = true;
						syncTimeRewrite(ref next_message);
						tcpClient.GetStream().BeginWrite(next_message, 0, next_message.Length, asyncSend, next_message);
					}
				}
			}
            // Socket closed or not connected.
            catch (System.InvalidOperationException)
            {
                //parent.disconnectClient(this, "InvalidOperationException");
				Log.Debug("Caught InvalidOperationException for player " + this.username + " in sendOutgoingMessages");
				//parent.markClientForDisconnect(this);
            }
            // Raised by BeginWrite, can mean socket is down.
            catch (System.IO.IOException)
            {
                //parent.disconnectClient(this, "IOException");
				Log.Debug("Caught IOException for player " + this.username + " in sendOutgoingMessages");
				//parent.markClientForDisconnect(this);
            }
            catch (System.NullReferenceException) { }
			
		}

		private void syncTimeRewrite(ref byte[] next_message) {
			//SYNC_TIME Rewriting
			int next_message_id = BitConverter.ToInt32(next_message, 0);
			if (next_message_id == (int)KMPCommon.ServerMessageID.SYNC_TIME) {
				byte[] next_message_stripped = new byte[next_message.Length - 8];
				Array.Copy (next_message, 8, next_message_stripped, 0, next_message.Length - 8);
				byte[] next_message_decompressed = KMPCommon.Decompress(next_message_stripped);
				byte[] time_sync_rewrite = new byte[24];
				next_message_decompressed.CopyTo(time_sync_rewrite, 0);
				BitConverter.GetBytes(DateTime.UtcNow.Ticks).CopyTo(time_sync_rewrite, 16);
				next_message = Server.buildMessageArray(KMPCommon.ServerMessageID.SYNC_TIME, time_sync_rewrite);
			}
		}

		public void splitOutgoingMessage(ref byte[] next_message)
		{
			//Only split messages bigger than SPLIT_MESSAGE_SIZE.
			if (next_message.Length > KMPCommon.SPLIT_MESSAGE_SIZE) {
				int split_index = 0;
				while (split_index < next_message.Length) {
					int bytes_to_read = Math.Min (next_message.Length - split_index, KMPCommon.SPLIT_MESSAGE_SIZE);
					byte[] split_message_bytes = new byte[bytes_to_read];
					Array.Copy (next_message, split_index, split_message_bytes, 0, bytes_to_read);
					byte[] split_message = Server.buildMessageArray (KMPCommon.ServerMessageID.SPLIT_MESSAGE, split_message_bytes);
					queuedOutMessagesSplit.Enqueue(split_message);
					split_index = split_index + bytes_to_read;
				}
				//Return the first split message if we just split.
				//Log.Debug("Split message into "  + queuedOutMessagesSplit.Count.ToString());
				queuedOutMessagesSplit.TryDequeue(out next_message);
			}
		}

		public void queueOutgoingMessage(KMPCommon.ServerMessageID id, byte[] data)
		{
			queueOutgoingMessage(Server.buildMessageArray(id, data));
		}


		public void queueOutgoingMessage(byte[] message_bytes)
		{
			//Figure out if this is a high or low priority message
			int sortMessageId = KMPCommon.intFromBytes(message_bytes, 0);
			switch (sortMessageId) {
						case (int)KMPCommon.ServerMessageID.HANDSHAKE:
						case (int)KMPCommon.ServerMessageID.HANDSHAKE_REFUSAL:
						case (int)KMPCommon.ServerMessageID.SERVER_MESSAGE:
						case (int)KMPCommon.ServerMessageID.TEXT_MESSAGE:
						case (int)KMPCommon.ServerMessageID.MOTD_MESSAGE:
						case (int)KMPCommon.ServerMessageID.SERVER_SETTINGS:
						case (int)KMPCommon.ServerMessageID.KEEPALIVE:
						case (int)KMPCommon.ServerMessageID.CONNECTION_END:
						case (int)KMPCommon.ServerMessageID.UDP_ACKNOWLEDGE:
						case (int)KMPCommon.ServerMessageID.PING_REPLY:
								queuedOutMessagesHighPriority.Enqueue(message_bytes);
								break;
						default:
								queuedOutMessages.Enqueue(message_bytes);
								break;
			}
		}

		internal void startReceivingMessages()
		{
			beginAsyncRead();
		}

		internal void endReceivingMessages()
		{
		}

		//Activity Level

		public void updateActivityLevel(ActivityLevel level)
		{
			bool changed = false;

			lock (activityLevelLock)
			{
				switch (level)
				{
					case ActivityLevel.IN_GAME:
						lastInGameActivityTime = parent.currentMillisecond;
						currentVessel = Guid.Empty;
						break;

					case ActivityLevel.IN_FLIGHT:
						lastInFlightActivityTime = parent.currentMillisecond;
						lastInGameActivityTime = parent.currentMillisecond;
						break;
				}

				if (level > activityLevel)
				{
					activityLevel = level;
					changed = true;
				}
			}

			if (changed)
				parent.clientActivityLevelChanged(this);
		}

	}

	public class StateObject
	{
		// Client socket.
		public TcpClient workClient = null;
	}
}
