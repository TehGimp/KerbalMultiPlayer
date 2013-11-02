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
	class ServerClient
	{
		public enum ActivityLevel
		{
			INACTIVE,
			IN_GAME,
			IN_FLIGHT
		}

		public const int SEND_BUFFER_SIZE = 8192;

		//Properties

		public Server parent
		{
			private set;
			get;
		}
		public int clientIndex;
		public String username;
		public String guid;
		public int playerID;
		public Guid currentVessel = Guid.Empty;
		public int currentSubspaceID = -1;
		public double enteredSubspaceAt = 0d;
		public double lastTick = 0d;
		public double syncOffset = 0.01d;
		public int lagWarning = 0;
		public bool universeSent = false;
		public bool warping = false;
		
		public bool receivedHandshake;

		public byte[] screenshot;
		public String watchPlayerName;
		public byte[] sharedCraftFile;
		public String sharedCraftName;
		public byte sharedCraftType;

		public long connectionStartTime;
		public long lastReceiveTime;
		public long lastUDPACKTime;

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

		private byte[] receiveBuffer = new byte[8192];
		private int receiveIndex = 0;
		private int receiveHandleIndex = 0;

		private byte[] currentMessageHeader = new byte[KMPCommon.MSG_HEADER_LENGTH];
		private int currentMessageHeaderIndex;
		private byte[] currentMessageData;
		private int currentMessageDataIndex;

		public KMPCommon.ClientMessageID currentMessageID;

		public ConcurrentQueue<byte[]> queuedOutMessages;

		public ServerClient(Server parent)
		{
			this.parent = parent;
			
			queuedOutMessages = new ConcurrentQueue<byte[]>();
		}

        public bool isValid
        {
            get
            {
                return (this.tcpClient != null && this.tcpClient.Connected);
            }
        }

        public bool isReady
        {
            get
            {
                return (isValid && this.receivedHandshake);
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
			sharedCraftType = 0;

			lastUDPACKTime = 0;

			queuedOutMessages = new ConcurrentQueue<byte[]>();

			lock (activityLevelLock)
			{
				activityLevel = ServerClient.ActivityLevel.INACTIVE;
				lastInGameActivityTime = parent.currentMillisecond;
				lastInFlightActivityTime = parent.currentMillisecond;
			}

			lock (timestampLock)
			{
				lastReceiveTime = parent.currentMillisecond;
				connectionStartTime = parent.currentMillisecond;
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
					currentMessageHeaderIndex = 0;
					currentMessageDataIndex = 0;
					receiveIndex = 0;
					receiveHandleIndex = 0;

					tcpClient.GetStream().BeginRead(
						receiveBuffer,
						receiveIndex,
						receiveBuffer.Length - receiveIndex,
						asyncReceive,
						receiveBuffer);
				}
			}
			catch (InvalidOperationException)
			{
			}
			catch (System.IO.IOException)
			{
			}
			catch (Exception e)
			{
				parent.passExceptionToMain(e);
			}
		}

		private void asyncReceive(IAsyncResult result)
		{
			try
			{
				int read = tcpClient.GetStream().EndRead(result);

				if (read > 0)
				{
					receiveIndex += read;
					//Console.WriteLine("Got data: " + System.Text.Encoding.ASCII.GetString(receiveBuffer));
					updateReceiveTimestamp();
					handleReceive();
				}

				tcpClient.GetStream().BeginRead(
					receiveBuffer,
					receiveIndex,
					receiveBuffer.Length - receiveIndex,
					asyncReceive,
					receiveBuffer);
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

		private void handleReceive()
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
						if (id_int >= 0 && id_int < Enum.GetValues(typeof(KMPCommon.ClientMessageID)).Length)
							currentMessageID = (KMPCommon.ClientMessageID)id_int;
						else
							currentMessageID = KMPCommon.ClientMessageID.NULL;
						
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
							messageReceived(currentMessageID, currentMessageData);

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

		//Asyc send

		private void asyncSend(IAsyncResult result)
		{
			try
			{
				tcpClient.GetStream().EndWrite(result);
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

		private void messageReceived(KMPCommon.ClientMessageID id, byte[] data)
		{
			parent.queueClientMessage(this, id, data);
		}

		public void sendOutgoingMessages()
		{

			try
			{
				if (queuedOutMessages.Count > 0)
				{
					//Check the size of the next message
					byte[] next_message = null;
					int send_buffer_index = 0;
					byte[] send_buffer = new byte[SEND_BUFFER_SIZE];
					
					while (queuedOutMessages.Count > 0)
					{
						queuedOutMessages.TryPeek(out next_message);
						if (send_buffer_index == 0 && next_message.Length >= send_buffer.Length)
						{
							//If the next message is too large for the send buffer, just send it
							queuedOutMessages.TryDequeue(out next_message);

							tcpClient.GetStream().BeginWrite(
								next_message,
								0,
								next_message.Length,
								asyncSend,
								next_message);
						}
						else if (next_message.Length <= (send_buffer.Length - send_buffer_index))
						{
							//If the next message is small enough, copy it to the send buffer
							queuedOutMessages.TryDequeue(out next_message);

							next_message.CopyTo(send_buffer, send_buffer_index);
							send_buffer_index += next_message.Length;
						}
						else
						{
							//If the next message is too big, send the send buffer
							tcpClient.GetStream().BeginWrite(
								send_buffer,
								0,
								send_buffer_index,
								asyncSend,
								next_message);

							send_buffer_index = 0;
							send_buffer = new byte[SEND_BUFFER_SIZE];
						}
					}

					//Send the send buffer
					if (send_buffer_index > 0)
					{
						tcpClient.GetStream().BeginWrite(
							send_buffer,
							0,
							send_buffer_index,
							asyncSend,
							next_message);
					}
				}
			}
			catch (System.InvalidOperationException) { }
			catch (System.IO.IOException) { }
			catch (System.NullReferenceException) { }
			
		}

		public void queueOutgoingMessage(KMPCommon.ServerMessageID id, byte[] data)
		{
			queueOutgoingMessage(Server.buildMessageArray(id, data));
		}

		public void queueOutgoingMessage(byte[] message_bytes)
		{
			queuedOutMessages.Enqueue(message_bytes);
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
}
