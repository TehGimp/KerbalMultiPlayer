using System;
using System.Collections.Generic;
using System.Linq;

using UnityEngine;

namespace KMP
{
    internal class KMPChatDX
    {
        /* New Chat */

        public const int MAX_CHAT_OUT_QUEUE = 4;
        public const int MAX_CHAT_LINES = 16;
        public const int MAX_CHAT_LINE_LENGTH = 128;
        public const float NAME_COLOR_SATURATION_FACTOR = 0.35f;

        public static float chatboxWidth = Screen.width / 4.5f;
        public static float chatboxHeight = Screen.height / 3.5f;

        public static float chatboxX = 0;
        public static float chatboxY = 20;

        public static bool offsettingEnabled = true;

        public static float trackerOffsetX = 200;
        public static float trackerOffsetY = 20;

        public static float editorOffsetX = 255;
        public static float editorOffsetY = 20;

        public static GameScenes lastScene = GameScenes.MAINMENU;

        public static bool showInput = false;
        public static bool draggable = false;

        public static bool displayCommands = false;
        public static Vector2 scrollPos = Vector2.zero;

        public static GUIStyle windowStyle = new GUIStyle();
        public static GUIStyle chatStyle = new GUIStyle();

        public static GUILayoutOption[] layoutOptions;
        public static Rect windowPos = new Rect(chatboxX, chatboxY, chatboxWidth, chatboxHeight);

        public static Queue<ChatLine> chatLineQueue = new Queue<ChatLine>();
        public static String chatEntryString = String.Empty;

        public struct ChatLine
        {
            public String name;
            public String message;
            public Color color;
            public bool isAdmin;

            public ChatLine(String line)
            {
                this.color = Color.yellow;
                this.name = "";
                this.message = line;
                this.isAdmin = false;

                //Check if the message has a name
                if (line.Length > 3 && (line.First() == '<' || (line.StartsWith("[" + KMPCommon.ADMIN_MARKER + "]") && line.Contains('<'))))
                {
                    int name_start = line.IndexOf('<');
                    int name_end = line.IndexOf('>');
                    int name_length = name_end - name_start - 1;
                    if (name_length > 0)
                    {
                        this.name = line.Substring(name_start + 1, name_length);
                        this.message = line.Substring(name_end + 1);

                        if (this.name == "Server")
                            this.color = Color.magenta;
                        else if (line.StartsWith("[" + KMPCommon.ADMIN_MARKER + "]"))
                        {
                            this.color = Color.red;
                            this.isAdmin = true;
                        }
                        else this.color = KMPVessel.generateActiveColor(name) * NAME_COLOR_SATURATION_FACTOR
                          + Color.white * (1.0f - NAME_COLOR_SATURATION_FACTOR);
                    }
                }
            }
        }

        public static Rect getWindowPos()
        {
            if (offsettingEnabled && !draggable)
            {
                switch (HighLogic.LoadedScene)
                {
                    case GameScenes.TRACKSTATION:
                        windowPos.x = chatboxX + trackerOffsetX;
                        windowPos.y = chatboxY + trackerOffsetY;

                        return windowPos;

                    case GameScenes.SPH:
                        windowPos.x = chatboxX + editorOffsetX;
                        windowPos.y = chatboxY + editorOffsetY;

                        return windowPos;

                    case GameScenes.EDITOR:
                        windowPos.x = chatboxX + editorOffsetX;
                        windowPos.y = chatboxY + editorOffsetY;

                        return windowPos;

                    default:
                        windowPos.x = chatboxX;
                        windowPos.y = chatboxY;

                        return windowPos;
                }
            }
            else
            {
                return windowPos;
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