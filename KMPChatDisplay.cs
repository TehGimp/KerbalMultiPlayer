using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using UnityEngine;

namespace KMP
{
	class KMPChatDisplay
	{

		public struct ChatLine
		{
			public String message;
			public Color color;

			public ChatLine(String message)
			{
				this.message = message;
				color = Color.white;
			}
		}

		public const float WINDOW_WIDTH_NORMAL = 320;
		public const float WINDOW_WIDTH_WIDE = 440;
		public const float WINDOW_HEIGHT = 360;
		public const int MAX_CHAT_OUT_QUEUE = 4;
		public const int MAX_CHAT_LINES = 16;
		public const int MAX_CHAT_LINE_LENGTH = 128;
		public const float NAME_COLOR_SATURATION_FACTOR = 0.35f;
		public static GUILayoutOption[] layoutOptions;

		public static bool displayCommands = false;
		public static Rect windowPos = new Rect(Screen.width - WINDOW_WIDTH_NORMAL - 8, Screen.height / 2 - WINDOW_HEIGHT / 2, WINDOW_WIDTH_NORMAL, WINDOW_HEIGHT);
		public static Vector2 scrollPos = Vector2.zero;

		public static float windowWidth
		{
			get
			{
				if (KMPGlobalSettings.instance.chatWindowWide)
					return WINDOW_WIDTH_WIDE;
				else
					return WINDOW_WIDTH_NORMAL;
			}
		}

		public static Queue<ChatLine> chatLineQueue = new Queue<ChatLine>();
		public static String chatEntryString = String.Empty;

		public static void enqueueChatLine(String line)
		{
			ChatLine chat_line = new ChatLine(line);

			//Check if the message has a name
			if (line.Length > 3 && line.First() == '[')
			{
				int name_length = line.IndexOf(']');
				if (name_length > 0)
				{
					name_length = name_length - 1;
					String name = line.Substring(1, name_length);
					if (name == "Server")
						chat_line.color = new Color(0.65f, 1.0f, 1.0f);
           			else chat_line.color = KMPVessel.generateActiveColor(name) * NAME_COLOR_SATURATION_FACTOR
						+ Color.white * (1.0f-NAME_COLOR_SATURATION_FACTOR);
				}
			}

			chatLineQueue.Enqueue(chat_line);
			while (chatLineQueue.Count > MAX_CHAT_LINES)
				chatLineQueue.Dequeue();
			scrollPos.y += 100;
		}

	}
}
