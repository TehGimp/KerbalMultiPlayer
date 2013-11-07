using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using UnityEngine;


namespace KMP
{
    class KMPChatDX
    {

        /* New Chat */

        public const int MAX_CHAT_OUT_QUEUE = 4;
        public const int MAX_CHAT_LINES = 16;
        public const int MAX_CHAT_LINE_LENGTH = 128;
        public const float NAME_COLOR_SATURATION_FACTOR = 0.35f;

        public static float chatboxHeight = 300;
        public static float chatboxWidth = 500;

        public static bool showInput = false;

        public static bool displayCommands = false;
        public static Vector2 scrollPos = Vector2.zero;

        public static GUIStyle windowStyle = new GUIStyle();
        public static GUIStyle chatStyle = new GUIStyle();


        public static GUILayoutOption[] layoutOptions;
        public static Rect windowPos = new Rect(20, 0, chatboxWidth, chatboxHeight);

        public static Queue<ChatLine> chatLineQueue = new Queue<ChatLine>();
        public static String chatEntryString = String.Empty;

        public struct ChatLine
        {
            public String name;
            public String message;
            public Color color;

            public ChatLine(String line)
            {
                this.color = Color.yellow;
                this.name = "";
                this.message = line;

                //Check if the message has a name
                if (line.Length > 3 && line.First() == '[')
                {
                    int name_length = line.IndexOf(']');
                    if (name_length > 0)
                    {
                        name_length = name_length - 1;
                        this.name = line.Substring(1, name_length);
                        this.message = line.Substring(name_length + 2);

                        if (this.name == "Server")
                            this.color = Color.magenta;
                        else this.color = KMPVessel.generateActiveColor(name) * NAME_COLOR_SATURATION_FACTOR
                            + Color.white * (1.0f - NAME_COLOR_SATURATION_FACTOR);
                    }
                } 
            }
        }

        public static void enqueueChatLine(String line)
        {
            ChatLine chat_line = new ChatLine(line);

            chatLineQueue.Enqueue(chat_line);
            while (chatLineQueue.Count > MAX_CHAT_LINES)
                chatLineQueue.Dequeue();

            //scrollPos.y += 100;
        }

        public static void setStyle()
        {
            /* Setup Chat */
            chatStyle.fontStyle = FontStyle.Bold;
            chatStyle.wordWrap = true;
            chatStyle.padding.left = 5;
            chatStyle.fixedWidth = chatboxWidth;

        }

    }
}
