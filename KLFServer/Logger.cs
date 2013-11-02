﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace KMPServer
{
    public static class Log
    {
        private static string LogFilename = "KMPServer.log";
        
        public enum LogLevels : int
        {
            Debug = 0,
            Activity = 5,
            Info = 10,
            Notice = 20,
            Warning = 30,
            Error = 40,
        }

        public static LogLevels MinLogLevel { get; set; }

        private static void WriteLog(LogLevels level, string format, params object[] args)
        {
            if (level < MinLogLevel) { return; }

            lock (Console.Out)
            {
                string Line = string.Format("[{0}] [{1}] : {2}", DateTime.Now.ToString("HH:mm:ss"), level.ToString(), string.Format(format, args));
                Console.WriteLine(Line);
                try
                {
                    File.AppendAllText(LogFilename, Line + "\n");
                }
                catch { } //What do we do about this?
            }
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
        }

        public static void Notice(string format, params object[] args)
        {
            Console.ForegroundColor = ConsoleColor.DarkBlue;
            WriteLog(LogLevels.Notice, format, args);
        }

        public static void Warning(string format, params object[] args)
        {
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            WriteLog(LogLevels.Warning, format, args);
        }

        public static void Error(string format, params object[] args)
        {
            Console.ForegroundColor = ConsoleColor.DarkRed;
            WriteLog(LogLevels.Error, format, args);
        }
    }
}
