using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

public class KMPCommon
{

	public String PROGRAM_VERSION
    {
        get
        {
            return Assembly.GetExecutingAssembly().GetName().Version.ToString();
        }
    }

	public const Int32 FILE_FORMAT_VERSION = 10000;
	public const Int32 NET_PROTOCOL_VERSION = 10000;
	public const int MSG_HEADER_LENGTH = 8;
	public const int INTEROP_MSG_HEADER_LENGTH = 8;

	public const int SERVER_SETTINGS_LENGTH = 13;

	public const int MAX_CRAFT_FILE_BYTES = 1024 * 1024;

	public const String SHARE_CRAFT_COMMAND = "/sharecraft";
	public const String GET_CRAFT_COMMAND = "!getcraft";

	public const byte CRAFT_TYPE_VAB = 0;
	public const byte CRAFT_TYPE_SPH = 1;

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

	public enum ClientMessageID
	{
		HANDSHAKE /*Username Length : Username : Version*/,
		PRIMARY_PLUGIN_UPDATE /*data*/,
		SECONDARY_PLUGIN_UPDATE /*data*/,
		TEXT_MESSAGE /*Message text*/,
		SCREEN_WATCH_PLAYER /*Player name*/,
		SCREENSHOT_SHARE /*Description Length : Description : data*/,
		KEEPALIVE,
		CONNECTION_END /*Message*/ ,
		UDP_PROBE,
		NULL,
		SHARE_CRAFT_FILE /*Craft Type Byte : Craft name length : Craft Name : File bytes*/,
		ACTIVITY_UPDATE_IN_GAME,
		ACTIVITY_UPDATE_IN_FLIGHT,
		PING,
		WARPING,
		SSYNC
	}

	public enum ServerMessageID
	{
		HANDSHAKE /*Protocol Version : Version String Length : Version String : ClientID*/,
		HANDSHAKE_REFUSAL /*Refusal message*/,
		SERVER_MESSAGE /*Message text*/,
		TEXT_MESSAGE /*Message text*/,
		PLUGIN_UPDATE /*data*/,
		SERVER_SETTINGS /*UpdateInterval (4) : Screenshot Interval (4) : Screenshot Height (4): InactiveShips (1)*/,
		SCREENSHOT_SHARE /*Description Length : Description : data*/,
		KEEPALIVE,
		CONNECTION_END /*Message*/,
		UDP_ACKNOWLEDGE,
		NULL,
		CRAFT_FILE /*Craft Type Byte : Craft name length : Craft Name : File bytes*/,
		PING_REPLY,
		SYNC /*tick*/,
		SYNC_COMPLETE
	}

	public enum ClientInteropMessageID
	{
		NULL,
		CLIENT_DATA /*Byte - Inactive Vessels Per Update : Screenshot Height : UpdateInterval : Player Name*/,
		SCREENSHOT_RECEIVE /*Description Length : Description : data*/,
		CHAT_RECEIVE /*Message*/,
		PLUGIN_UPDATE /*data*/
	}

	public enum PluginInteropMessageID
	{
		NULL,
		PLUGIN_DATA /*Byte - In-Flight : Int32 - Current Game Title length : Current Game Title : Int32 - Screenshot watch player name length : Screenshot watch player name*/,
		SCREENSHOT_SHARE /*Description Length : Description : data*/,
		CHAT_SEND /*Message*/,
		PRIMARY_PLUGIN_UPDATE /*data*/,
		SECONDARY_PLUGIN_UPDATE /*data*/,
		WARPING /*data*/,
		SSYNC /*data*/
	}

}

