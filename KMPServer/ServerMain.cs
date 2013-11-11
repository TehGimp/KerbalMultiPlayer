﻿using System;
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
            ServerSettings.loadWhitelist(settings);
            ServerSettings.loadBans(settings);

            bool settingsChanged = false;

            if (args != null && args.Length > 0)
            {
                for (int i = 0; i < args.Length - 1; i++)
                {
                    if (args[i].StartsWith("+"))
                    {
                        var key = args[i].Substring(1);
                        var val = args[i++];
                        try
                        {
                            ServerSettings.modifySetting(settings, key, val);
                            settingsChanged = true;
                        }
                        catch { }
                    }
                }
            }

            if (settingsChanged) { ServerSettings.writeToFile(settings); }

            Log.MinLogLevel = settings.LogLevel;

			Console.Title = "KMP Server "+ KMPCommon.PROGRAM_VERSION;
            Log.Info("KMP Server version {0}", KMPCommon.PROGRAM_VERSION);
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
                var tabs = (kvp.Key.Length > 12) ? "\t" : "\t\t";
                Log.Info("{0}{2}: {1}", kvp.Key, kvp.Value, tabs);
            }

            Log.Info("");
            Log.Info("/set [key] [value] to modify a setting.");
            Log.Info("/whitelist [add|del] [user] to update whitelist.");
            Log.Info("/quit to exit, or /start to begin the server.");
            Log.Info("");

            //Check for missing files, try and copy from KSP installation if possible.
            string[] RequiredFiles = { "Assembly-CSharp.dll", "Assembly-CSharp-firstpass.dll", "UnityEngine.dll" };

            var missingFiles = RequiredFiles.Where(f => File.Exists(f) == false);

            foreach (var f in missingFiles)
            {
                var tryKSPpath = @"%programfiles(x86)%\Steam\SteamApps\common\Kerbal Space Program\KSP_Data\Managed\" + f;
                if (File.Exists(f))
                {
                    try
                    {
                        File.Copy(tryKSPpath, f);
                    }
                    catch
                    {
                        //Cannot copy.
                    }
                }
                else
                {
                    break;
                }
            }

            //Check again.
            missingFiles = RequiredFiles.Where(f => File.Exists(f) == false);

            if (missingFiles.Any())
            {
                Log.Error("The following required files are missing:");
                foreach (var f in missingFiles) { Log.Error(f); }
                Log.Error("Please place them in the KMP server directory. See README.txt for more information.");
            }
            
            bool running = true;
	    
            while (running)
            {
                var line = Console.ReadLine();

                var parts = line.Split(' ');

                switch (parts[0].ToLowerInvariant())
                {
                    case "/quit":
                        return;
                    case "/whitelist":
                        if (parts.Length != 3)
                        {
                            Log.Info("Invalid usage. /whitelist [add|del] [user]");
                        }

                        switch (parts[1])
                        {
                            case "add":
                                if (!settings.whitelist.Contains(parts[2], StringComparer.InvariantCultureIgnoreCase))
                                {
                                    settings.whitelist.Add(parts[2].ToLowerInvariant());
                                    Log.Info("{0} has been added to the whitelist", parts[2]);
                                }
                                else
                                {
                                    Log.Info("{0} is already on the whitelist", parts[2]);
                                }
                                break;
                            case "del":
                                if (settings.whitelist.Contains(parts[2], StringComparer.InvariantCultureIgnoreCase))
                                {
                                    settings.whitelist.Remove(parts[2].ToLowerInvariant());
                                    Log.Info("{0} has been removed from the whitelist", parts[2]);
                                }
                                else
                                {
                                    Log.Info("{0} was not already on the whitelist", parts[2]);
                                }
                                break;
                        }

                        ServerSettings.saveWhitelist(settings);
                        break;

                    case "/set":
                        if (parts.Length < 3)
                        {
                            Log.Info("Invalid usage. Usage is /set [key] [value]");
                        }
                        else 
                        {
                            string val = String.Join(" ", parts.Skip(2).ToArray());

                            try
                            {
                                ServerSettings.modifySetting(settings, parts[1], val);
                                Log.Info("{0} changed to {1}", parts[1], val);
                                ServerSettings.writeToFile(settings);
                                Log.MinLogLevel = settings.LogLevel;
                            }
                            catch
                            {
                                Log.Info("{0} cannot be set to {1}", parts[1], val);
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
                Log.Info("Server {0}",Enum.GetName(typeof(ServerStatus), status).ToLower()); 
            }
        }

		static ServerStatus hostServer(ServerSettings.ConfigStore settings)
		{
			Server server = new Server(settings);

			try
			{
				server.hostingLoop();
			}
			catch (System.Net.Sockets.SocketException e)
			{
				Log.Error("Unexpected exception encountered! Crash report written to log file");
				Log.Error(e.ToString());
                		Log.Error("NOTICE:");
                		Log.Error("This exception is usually caused by an incorrect ip binding. If this occurs again, try resetting the ip binding to 0.0.0.0");
                		Log.Error("");
				if (server.threadExceptionStackTrace != null && server.threadExceptionStackTrace.Length > 0)
				{
					Log.Error("Stacktrace: ");
					Log.Error(server.threadExceptionStackTrace);
				}
				//server.clearState();
				//return ServerStatus.CRASHED;
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
