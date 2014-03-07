using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.IO;
using ICSharpCode.SharpZipLib.GZip;

public class KMPCommon
{

	public static String PROGRAM_VERSION
    {
        get
        {
            return Assembly.GetExecutingAssembly().GetName().Version.ToString();
        }
    }

	public const Int32 FILE_FORMAT_VERSION = 10000;
	public const Int32 NET_PROTOCOL_VERSION = 10016;
	public const int MSG_HEADER_LENGTH = 8;
    public const int MAX_MESSAGE_SIZE = 1024 * 1024; //Enough room for a max-size craft file
	public const int MESSAGE_COMPRESSION_THRESHOLD = 4096;
	public const int SPLIT_MESSAGE_SIZE = 8192; //Split big messages into smaller chunks, so high priority messages can get through faster.
	public const int INTEROP_MSG_HEADER_LENGTH = 8;

	public const int SERVER_SETTINGS_LENGTH = 23; //Length of fixed position information. Variable length settings start after this (including the header).

	public const int MAX_CRAFT_FILE_BYTES = 1024 * 1024;

	public const String SHARE_CRAFT_COMMAND = "!sharecraft";//"/" chat commands handled by client
	public const String GET_CRAFT_COMMAND = "!getcraft";	//"!" chat commands handled by server
	public const String RCON_COMMAND = "!rcon";

	public const String ADMIN_MARKER = "@";

	public static string filteredFileName(string filename)
	{
		const String illegal = "\\/:*?\"<>|";

 		StringBuilder sb = new StringBuilder();
		foreach (char c in filename)
		{
			//Filter illegal characters out of the player name
			if (!illegal.Contains(c))
				sb.Append(c);
		}

		return sb.ToString();
	}

	public static byte[] intToBytes(Int32 val)
	{
        return BitConverter.GetBytes(val);
	}

	public static Int32 intFromBytes(byte[] bytes, int offset = 0)
	{
        return BitConverter.ToInt32(bytes, offset);
	}
	
	public enum CraftType
	{
		VAB,
		SPH,
		SUBASSEMBLY
	}

	public enum ClientMessageID
	{
		HANDSHAKE /*Username Length : Username : Version*/,
		PRIMARY_PLUGIN_UPDATE /*data*/,
		SECONDARY_PLUGIN_UPDATE /*data*/,
		SCENARIO_UPDATE /*data*/,
		TEXT_MESSAGE /*Message text*/,
		SCREEN_WATCH_PLAYER /*Player name*/,
		SCREENSHOT_SHARE /*Description Length : Description : data*/,
		KEEPALIVE,
		CONNECTION_END /*Message*/ ,
		PROBE,
		NULL,
		SHARE_CRAFT_FILE /*Craft Type Byte : Craft name length : Craft Name : File bytes*/,
		ACTIVITY_UPDATE_IN_GAME,
		ACTIVITY_UPDATE_IN_FLIGHT,
		PING,
		WARPING,
		SSYNC,
		SPLIT_MESSAGE, /* Pre-emptive add for when message-queueing is implemented client side */
		SYNC_TIME /*NTP style sync*/
	}

	public enum ServerMessageID
	{
		HANDSHAKE /*Protocol Version : Version String Length : Version String : ClientID : Mode*/,
		HANDSHAKE_REFUSAL /*Refusal message*/,
		SERVER_MESSAGE /*Message text*/,
		TEXT_MESSAGE /*Message text*/,
		MOTD_MESSAGE /*Message text*/,
		PLUGIN_UPDATE /*data*/,
		SCENARIO_UPDATE /*data*/,
		SERVER_SETTINGS /*UpdateInterval (4) : Screenshot Interval (4) : Screenshot Height (4) :  Bubble Size (8) : InactiveShips (1)*/,
		SCREENSHOT_SHARE /*Description Length : Description : data*/,
		KEEPALIVE,
		CONNECTION_END /*Message*/,
		NULL,
		CRAFT_FILE /*Craft Type Byte : Craft name length : Craft Name : File bytes*/,
		PING_REPLY,
		SYNC /*tick*/,
		SYNC_COMPLETE,
		SPLIT_MESSAGE, /* This allows higher priority messages more entry points into the send queue */
		SYNC_TIME /*NTP style sync*/
	}

	public enum ClientInteropMessageID
	{
		NULL,
		CLIENT_DATA /*Byte - Inactive Vessels Per Update : Screenshot Height : UpdateInterval : Player Name*/,
		SCREENSHOT_RECEIVE /*Description Length : Description : data*/,
		CHAT_RECEIVE /*Message*/,
		PLUGIN_UPDATE /*data*/,
		SCENARIO_UPDATE /*data*/
	}

	public enum PluginInteropMessageID
	{
		NULL,
		PLUGIN_DATA /*Byte - In-Flight : Int32 - Current Game Title length : Current Game Title : Int32 - Screenshot watch player name length : Screenshot watch player name*/,
		SCREENSHOT_SHARE /*Description Length : Description : data*/,
		CHAT_SEND /*Message*/,
		PRIMARY_PLUGIN_UPDATE /*data*/,
		SECONDARY_PLUGIN_UPDATE /*data*/,
		SCENARIO_UPDATE /*data*/,
		WARPING /*data*/,
		SSYNC /*data*/,
		SYNC_TIME /*data*/
	}
	
	
	/* KMP message data format
	 * Uncompressed data: [bool-false : data]
	 * Compressed data: [bool-true : Int32-uncompressed length : compressed_data]
	 */
	
    public static byte[] Compress(byte[] data, bool forceUncompressed = false)
	{
		if (data == null) return null;
		byte[] compressedData = null;
        MemoryStream ms = null;
        GZipOutputStream gzip = null;
		try
        {
			ms = new MemoryStream();
			if (data.Length < MESSAGE_COMPRESSION_THRESHOLD || forceUncompressed)
			{
				//Small message, don't compress
				using (BinaryWriter writer = new BinaryWriter(ms))
	            {
					writer.Write(false);
	                writer.Write(data, 0, data.Length);
	                compressedData = ms.ToArray();
	                ms.Close();                
	                writer.Close();
	            }
			}
			else
			{
				//Compression enabled
	            Int32 size = data.Length;
	            using (BinaryWriter writer = new BinaryWriter(ms))
	            {
					writer.Write(true);
	                writer.Write(size);
	                gzip = new GZipOutputStream(ms);
	                gzip.Write(data, 0, data.Length);
	                gzip.Close();
	                compressedData = ms.ToArray();
	                ms.Close();   
	                writer.Close();
	            }
	        }
		}
		catch
		{
			return null;
		}
        finally
        {
            if (gzip != null) gzip.Dispose();
            if (ms != null) ms.Dispose();
        }
        return compressedData;
    }

    public static byte[] Decompress(byte[] data)
    {
		if (data == null) return null;
		byte[] decompressedData = null;
        MemoryStream ms = null;
        GZipInputStream gzip = null;
        try
		{
			ms = new MemoryStream(data,false);
        	using (BinaryReader reader = new BinaryReader(ms))
            {
				bool compressedFlag = reader.ReadBoolean();
				if (compressedFlag == false)
				{
					//Uncompressed
					decompressedData = reader.ReadBytes(data.Length - 1);
				}
				else
				{
					//Decompress
	                Int32 size = reader.ReadInt32();
	                gzip = new GZipInputStream(ms);
	                decompressedData = new byte[size];
	                gzip.Read(decompressedData, 0, decompressedData.Length);
	                gzip.Close();
	                ms.Close();
				}
				reader.Close();
            }
        }
		catch
		{
			return null;
		}
        finally
        {
            if (gzip != null) gzip.Dispose();
            if (ms != null) ms.Dispose();
        }
        return decompressedData;
    } 
}

