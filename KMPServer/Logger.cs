using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace KMPServer
{
    public static class Log
    {
        private static string LogFolder = "logs";
        private static string LogFilename =  Path.Combine(LogFolder, "kmpserver " + DateTime.Now.ToString("dd-MM-yyyy HH-mm-ss") + ".log");
        
        public enum LogLevels : int
        {
            Debug = 0,
            Activity = 5,
            Info = 10,
            Chat = 11,
            Notice = 20,
            Warning = 30,
            Error = 40,
        }

        public static LogLevels MinLogLevel { get; set; }

        private static void WriteLog(LogLevels level, string format, params object[] args)
        {

            if (!Directory.Exists(LogFolder))
                Directory.CreateDirectory(LogFolder);

            if (level < MinLogLevel) { return; }

            lock (Console.Out)
            {
                string Line = string.Format("[{0}] [{1}] : {2}", DateTime.Now.ToString("HH:mm:ss"), level.ToString(), string.Format(format, args));
                Console.WriteLine(Line);
                try
                {
                    File.AppendAllText(LogFilename, Line + Environment.NewLine);
                }
                catch { } //What do we do about this?
                Console.ForegroundColor = ConsoleColor.Gray;
            }
        }

		private static void SendToAdmin(LogLevels level, string format, params object[] args)
		{
			try {
				string Line = string.Format("[{0}] : {1}", level.ToString (), string.Format(format, args));
				ServerMain.server.sendTextMessageToAdmins(Line);
			}
			catch (Exception) {};
		}

        public static void Debug(string format, params object[] args)
        {
            Console.ForegroundColor = ConsoleColor.DarkGreen;
            WriteLog(LogLevels.Debug, format, args);
        }

        public static void Activity(string format, params object[] args)
        {
            Console.ForegroundColor = ConsoleColor.DarkMagenta;
            WriteLog(LogLevels.Activity, format, args);
        }

        public static void Info(string format, params object[] args)
        {
            Console.ForegroundColor = ConsoleColor.Gray;
            WriteLog(LogLevels.Info, format, args);
			SendToAdmin(LogLevels.Info, format, args);
        }

        public static void Notice(string format, params object[] args)
        {
            Console.ForegroundColor = ConsoleColor.DarkBlue;
            WriteLog(LogLevels.Notice, format, args);
			SendToAdmin(LogLevels.Notice, format, args);
        }

        public static void Warning(string format, params object[] args)
        {
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            WriteLog(LogLevels.Warning, format, args);
			SendToAdmin(LogLevels.Warning, format, args);
        }

        public static void Error(string format, params object[] args)
        {
            Console.ForegroundColor = ConsoleColor.DarkRed;
            WriteLog(LogLevels.Error, format, args);
			SendToAdmin(LogLevels.Error, format, args);
        }

        public static void Chat(string who, string message)
        {
            Console.ForegroundColor = ConsoleColor.DarkCyan;
            WriteLog(LogLevels.Chat, "<{0}> {1}", who, message);
        }
    }
}

