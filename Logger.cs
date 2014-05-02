using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using UnityEngine;

namespace KMP
{
    public static class Log
    {
        private static bool useDelayedLogging = false;
        private static Queue<string> delayedDebugQueue = new Queue<string>();
        private static Queue<string> delayedWarningQueue = new Queue<string>();
        private static Queue<string> delayedErrorQueue = new Queue<string>();

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
            if (level < MinLogLevel)
            {
                return;
            }

            string output = string.Format("[{0}] : {1}", level.ToString(), string.Format(format, args));

            switch (level)
            {
                case LogLevels.Debug:
                case LogLevels.Activity:
                case LogLevels.Info:
                case LogLevels.Notice:
                    if (useDelayedLogging)
                    {
                        delayedDebugQueue.Enqueue(output);
                    }
                    else
                    {
                        UnityEngine.Debug.Log(output);
                    }
                    break;
                case LogLevels.Warning:
                    if (useDelayedLogging)
                    {
                        delayedWarningQueue.Enqueue(output);
                    }
                    else
                    {
                        UnityEngine.Debug.LogWarning(output);
                    }
                    break;
                case LogLevels.Error:
                    if (useDelayedLogging)
                    {
                        delayedErrorQueue.Enqueue(output);
                    }
                    else
                    {
                        UnityEngine.Debug.LogError(output);
                    }
                    break;
            }
        }

        public static void WriteDelayedLog()
        {
            while (delayedDebugQueue.Count > 0)
            {
                UnityEngine.Debug.Log(delayedDebugQueue.Dequeue());
            }
            while (delayedWarningQueue.Count > 0)
            {
                UnityEngine.Debug.LogWarning(delayedWarningQueue.Dequeue());
            }
            while (delayedErrorQueue.Count > 0)
            {
                UnityEngine.Debug.LogError(delayedErrorQueue.Dequeue());
            }
        }

        public static void Debug(string format, params object[] args)
        {
            WriteLog(LogLevels.Debug, format, args);
        }

        public static void Activity(string format, params object[] args)
        {
            WriteLog(LogLevels.Activity, format, args);
        }

        public static void Info(string format, params object[] args)
        {
            WriteLog(LogLevels.Info, format, args);
        }

        public static void Notice(string format, params object[] args)
        {
            WriteLog(LogLevels.Notice, format, args);
        }

        public static void Warning(string format, params object[] args)
        {
            WriteLog(LogLevels.Warning, format, args);
        }

        public static void Error(string format, params object[] args)
        {
            WriteLog(LogLevels.Error, format, args);
        }
    }
}

