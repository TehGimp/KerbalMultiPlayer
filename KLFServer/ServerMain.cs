using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Net;
using System.IO;

namespace KMPServer
{
	class ServerMain
	{

		public const int AUTO_RESTART_DELAY = 1000;

		static void Main(string[] args)
		{
            ServerSettings.ConfigStore settings = new ServerSettings.ConfigStore();
            ServerSettings.readFromFile(settings);

            Log.MinLogLevel = settings.LogLevel;

			Console.Title = "KMP Server " + KMPCommon.PROGRAM_VERSION;
            Log.Info("KMP Server version " + KMPCommon.PROGRAM_VERSION);
            Log.Info("    Created by Shaun Esau");
            Log.Info("    Based on Kerbal LiveFeed created by Alfred Lam");
            Log.Info("");

            if (settings.autoHost)
            {
                startServer(settings);
                return;
            }

            Log.Info("Current Configuration:");
            Log.Info("");

            foreach (var kvp in ServerSettings.GetCurrentValues(settings))
            {
                Log.Info("{0}\t: {1}", kvp.Key, kvp.Value);
            }

            Log.Info("");
            Log.Info("Enter /set [key] [value] to modify a setting.");
            Log.Info("/quit to exit, or /start to begin the server.");
            Log.Info("");

            bool running = true;

            while (running)
            {
                var line = Console.ReadLine();

                var parts = line.Split(' ');

                switch (parts[0].ToLowerInvariant())
                {
                    case "/quit":
                        return;
                    case "/set":
                        if (parts.Length != 3)
                        {
                            Log.Info("Invalid usage. Usage is /set [key] [value]");
                        }
                        else 
                        {
                            try
                            {
                                ServerSettings.modifySetting(settings, parts[1], parts[2]);
                                Log.Info("{0} changed to {1}", parts[1], parts[2]);
                                ServerSettings.writeToFile(settings);
                            }
                            catch
                            {
                                Log.Info("{0} cannot be set to {1}", parts[1], parts[2]);
                            }
                        }
                        break;
                    case "/start":
                        startServer(settings);
                        break;
                    default:
                        Log.Info("Unrecognised command: {0}", parts[0]);
                        break;
                }
            }
		}

        private static void startServer(ServerSettings.ConfigStore settings)
        {
            ServerStatus status = hostServer(settings);
            while (status == ServerStatus.RESTARTING)
            {
                System.Threading.Thread.Sleep(AUTO_RESTART_DELAY);
                status = hostServer(settings);
            }
             
            if (status == ServerStatus.QUIT)
            {

            }
            else
            {
                Console.WriteLine("Server " + Enum.GetName(typeof(ServerStatus), status).ToLower());
            }
        }

		static ServerStatus hostServer(ServerSettings.ConfigStore settings)
		{
			Server server = new Server(settings);

			try
			{
				server.hostingLoop();
			}
			catch (Exception e)
			{
				Log.Error("Unexpected exception encountered! Crash report written to log file");
				Log.Error(e.ToString());
				if (server.threadExceptionStackTrace != null && server.threadExceptionStackTrace.Length > 0)
				{
					Log.Error("Stacktrace: ");
					Log.Error(server.threadExceptionStackTrace);
				}
				//server.clearState();
				//return ServerStatus.CRASHED;
			}

			server.clearState();

			if (server.stop)
				return ServerStatus.STOPPED;

			if (!settings.autoRestart || server.quit)
				return ServerStatus.QUIT;

			return ServerStatus.RESTARTING;
		}
		public enum ServerStatus {
			STOPPED, QUIT, CRASHED, RESTARTING
		}
	}
}
