﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Net;
using System.IO;
using System.Net.Sockets;

namespace KMPServer
{
	class ServerMain
	{

		public const int AUTO_RESTART_DELAY = 1000;

		public static Server server = null;

	    public static ServerSettings.ConfigStore settings;

		static void Main(string[] args)
		{
            if (!System.IO.Directory.Exists(Server.MODS_PATH))
            {
                System.IO.Directory.CreateDirectory(Server.MODS_PATH);
            }
			settings = new ServerSettings.ConfigStore();
			ServerSettings.readFromFile(settings);
			ServerSettings.loadWhitelist(settings);
			ServerSettings.loadBans(settings);
            ServerSettings.loadAdmins(settings);

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

            try
            {
                Console.WindowHeight = (int)(Console.WindowHeight * settings.consoleScale);
                Console.WindowWidth = (int)(Console.WindowWidth * settings.consoleScale);
            }
            catch
            {
            }//Fix for mono not needing window width stuff

		    Log.InitLogger();

            Console.Title = "KMP Server " + KMPCommon.PROGRAM_VERSION;
            Log.Info("KMP Server version {0}", KMPCommon.PROGRAM_VERSION);
            Log.Info("    Created by Shaun Esau and developed by the KMP team http://sesau.ca/ksp/KMP_contribs.html");
            Log.Info("    Based on Kerbal LiveFeed created by Alfred Lam");
            Log.Info("");

            if (settings.autoHost)
            {
                startServer(settings);
            }

            Log.Info("Current Configuration:");
            Log.Info("");

            foreach (var kvp in ServerSettings.GetCurrentValues(settings))
            {
                var tabs = (kvp.Key.Length > 11) ? "\t" : "\t\t";
                if (kvp.Key == "gameMode")
                {
                    Log.Info("");
                    Log.Info("Game Mode\t\t: {0}", kvp.Value == "0" ? "Sandbox" : "Career");
                }
                else Log.Info("{0}{2}: {1}", kvp.Key, kvp.Value, tabs);
            }

            Log.Info("");
            Log.Info("/set [key] [value] to modify a setting.");
            Log.Info("    /set help for information about each setting.");
            Log.Info("/whitelist [add|del] [user] to update whitelist.");
            Log.Info("/admin [add|del] [user] to update admin list.");
            Log.Info("/mode [sandbox|career] to set server game mode.");
            Log.Info("/dbdiag to run database performance diagnostics.");
			Log.Info("/modgen [blacklist|whitelist] [sha] to generate a KMPModControl.txt from the 'Mods' directory.");
			Log.Info("\tYou can use blacklist or whitelist mode, defaulting to blacklist.");
			Log.Info("\tYou can optionally specify sha to force required versions.");
            Log.Info("/quit to exit, or /start to begin the server.");

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
			
			string lastCommand = "";
            bool running = true;

            while (running)
            {
				ConsoleKeyInfo keypress;
				int inputIndex = 0;
				var input = "";
				
				while (true)
				{
					keypress = Console.ReadKey();
					if (keypress.Key == ConsoleKey.UpArrow)
					{
						input = lastCommand;
						inputIndex = input.Length;
						echoInput(input,inputIndex);
					}
					else if (keypress.Key == ConsoleKey.DownArrow)
					{
						//do nothing, but prevent key from counting as input
					}
					else if (keypress.Key == ConsoleKey.LeftArrow)
					{
						if (inputIndex > 0)
						{
							inputIndex--;
							Console.SetCursorPosition(inputIndex, Console.CursorTop);
						}
					}
					else if (keypress.Key == ConsoleKey.RightArrow)
					{
						if (inputIndex < input.Length)
						{
							inputIndex++;
							Console.SetCursorPosition(inputIndex, Console.CursorTop);
						}
					}
					else if (keypress.Key == ConsoleKey.Backspace && inputIndex > 0)
					{
						inputIndex--;
						input = input.Remove(inputIndex,1);
						echoInput(input + " ",inputIndex);
					}
					else if (keypress.Key == ConsoleKey.Delete && inputIndex < input.Length)
					{
						input = input.Remove(inputIndex,1);
						echoInput(input + " ",inputIndex);
					}
					else if (keypress.Key == ConsoleKey.Escape)
					{
						Console.WriteLine();
						input = "";
						break;
					}
					else if (keypress.Key == ConsoleKey.Enter)
					{
						break;
					}
					else
					{
						input = input.Insert(inputIndex,keypress.KeyChar.ToString());
						inputIndex++;
						echoInput(input,inputIndex);
					}
				}
				
				lastCommand = input;
				
                Log.Info("Command Input: {0}", input);

                var parts = input.Split(' ');

                switch (parts[0].ToLowerInvariant())
                {
                    case "/quit":
                        return;
                    case "/modgen":
                        Server.writeModControlCommand(parts);
                        break;
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

                    case "/admin":
                        if (parts.Length != 3)
                        {
                            Log.Info("Invalid usage. /admin [add|del] [user]");
                            break;
                        }

                        switch (parts[1])
                        {
                            case "add":
                                if (!settings.admins.Contains(parts[2], StringComparer.InvariantCultureIgnoreCase))
                                {
                                    settings.admins.Add(parts[2].ToLowerInvariant());
                                    Log.Info("{0} has been added to the admin list", parts[2]);
                                }
                                else
                                {
                                    Log.Info("{0} is already on the admin list", parts[2]);
                                }
                                break;
                            case "del":
                                if (settings.admins.Contains(parts[2], StringComparer.InvariantCultureIgnoreCase))
                                {
                                    settings.admins.Remove(parts[2].ToLowerInvariant());
                                    Log.Info("{0} has been removed from the admin list", parts[2]);
                                }
                                else
                                {
                                    Log.Info("{0} was not already on the admin list", parts[2]);
                                }
                                break;
                        }

                        ServerSettings.saveAdmins(settings);
                        break;

                    case "/mode":
                        if (parts.Length != 2)
                        {
                            Log.Info("Invalid usage. /mode [sandbox|career]");
                            break;
                        }
                        switch (parts[1].ToLowerInvariant())
                        {
                            case "sandbox":
                                settings.gameMode = 0;
                                Log.Info("Game mode set to sandbox");
                                break;
                            case "career":
                                settings.gameMode = 1;
                                Log.Info("Game mode set to career");
                                break;
                        }
                        ServerSettings.writeToFile(settings);
                        break;

                    case "/set":
                        if (parts.Length > 1 && parts[1].Equals("help"))
                        {
                            Log.Info("ipBinding - The IP address the server should bind to. Defaults to binding to all available IPs.");
                            Log.Info("port - The port used for connecting to the server.");
                            Log.Info("httpPort - The port used for viewing server information from a web browser.");
                            Log.Info("httpBroadcast - Enable simple http server for viewing server information from  a web browser.");
                            Log.Info("maxClients - The maximum number of players that can be connected to the server simultaneously.");

                            Log.Info("screenshotInterval - The minimum time a client must wait after sharing a screenshot before they can share another one.");
                            Log.Info("autoRestart - If true, the server will attempt to restart after catching an unhandled exception.");
                            Log.Info("autoHost - If true, the server will start hosting immediately rather than requiring the admin to enter the '/start' command.");
                            Log.Info("saveScreenshots - If true, the server will save all screenshots to the KMPScreenshots folder.");
                            Log.Info("hostIPV6 - If true, the server will be listening on a IPv6 address.");
						    
							Log.Info("useMySQL - If true, the server will use the configured MySQL connection string instead of the built-in SQLite database to store the universe.");
							Log.Info("mySQLConnString - The connection string to use when using a MySQL server to host the universe database.");
                            Log.Info("backupInterval - Time, in minutes, between universe database backups.");
                            Log.Info("maxDirtyBackups - The maximum number of backups the server will perform before forcing database optimization (which otherwise happens only when the server is empty).");
                            Log.Info("updatesPerSecond - CHANGING THIS VALUE IS NOT RECOMMENDED - The number of updates that will be received from all clients combined per second. The higher you set this number, the more frequently clients will send updates. As the number of active clients increases, the frequency of updates will decrease to not exceed this many updates per second. " + "WARNING: If this value is set too high then players will be more likely to be disconnected due to lag, while if it is set too low the gameplay experience will degrade significantly.");
						
                        	Log.Info("totalInactiveShips - CHANGING THIS VALUE IS NOT RECOMMENDED - The maximum number of inactive ships that can be updated by clients simultaneously.");    
							Log.Info("consoleScale - Changes the window size of the scale. Defaults to 1.0, requires restart.");	
							Log.Info("LogLevel - Log verbosity. Choose from: Debug, Activity, Info, Notice, Warning, or Error.");	
							Log.Info("maximumLogs - The maximum number of log files to store.");	
							Log.Info("screenshotHeight - The height of screenshots sent by players, in pixels.");
						
							Log.Info("autoDekessler - If true, server will clean up all debris in 'autoDekesslerTime'.");
                            Log.Info("autoDekesslerTime - Time, in minutes, that the server will clean up all debris.");
                        	Log.Info("profanityFilter - If true, enables the built-in profanity filter.");    
							Log.Info("profanityWords - List of profanity replacements. Replaces the first word with the second.");
							Log.Info("whitelisted - If true, enables the player whitelist.");
						
							Log.Info("joinMessage - A message shown to players when they join the server.");
                            Log.Info("serverInfo - A message displayed to anyone viewing server information in a browser.");
                            Log.Info("serverMotd - A message displayed to users when they login to the server that can be changed while the server is running.");
                            Log.Info("serverRules - A message displayed to users when they ask to view the server's rules.");
                            Log.Info("safetyBubbleRadius - The radius of the 'safety cylinder' which prevents collisions near KSC.");
							
							Log.Info("cheatsEnabled - If true, enable KSP's built-in debug cheats.");
                            Log.Info("allowPiracy - If true, a player can take control of another player's ship if they can accomplish manual docking (very difficult).");
                            Log.Info("freezeTimeWhenServerIsEmpty - If true, universe time is frozen when the server is empty (otherwise universe time runs continuously once a single player joins the server).");
                        }
                        else if (parts.Length < 3)
                        {
                            Log.Info("Invalid usage. Usage is /set [key] [value] or /set help");
                        }
                        else
                        {
                            string val = String.Join(" ", parts.Skip(2).ToArray());
                            string setKey = settings.MatchCaseInsensitive(parts[1]);
                            if (settings.Contains(setKey))
                            {
                                try
                                {
                                    ServerSettings.modifySetting(settings, setKey, val);
                                    Log.Info("{0} changed to {1}", setKey, val);
                                    ServerSettings.writeToFile(settings);
                                }
                                catch
                                {
                                    Log.Info("{0} cannot be set to {1}", parts[1], val);
                                }
                            }
                            else
                                Log.Info("No key found for {0}", parts[1]);
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
		
		private static void echoInput(string line, int index)
		{
			Console.SetCursorPosition(0, Console.CursorTop);
			Console.Write(line);
			Console.SetCursorPosition(index, Console.CursorTop);	
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
				Log.Info("Server {0}", Enum.GetName(typeof(ServerStatus), status).ToLower());
			}
		}

		static ServerStatus hostServer(ServerSettings.ConfigStore settings)
		{
			server = new Server(settings);

			try
			{
				server.hostingLoop();
			}
			catch (SocketException e)
			{
				var se = (SocketError)e.ErrorCode;
				switch (se)
				{
					case SocketError.AddressAlreadyInUse:
						Log.Error("Specified port number {0} is already in use by another process.", settings.port);
						break;
					case SocketError.AddressNotAvailable:
						Log.Error("Your specified IP binding ({0}) is not valid. You must use an IP address belonging to a network interface on this system. Use 0.0.0.0 to use all available interfaces.", settings.ipBinding);
						break;
					case SocketError.AccessDenied:
						Log.Error("You do not have permission to open a socket. Make sure the port number you are using is above 1000");
						break;
					default:
						Log.Error("Unable to start server. Error code was {0} ({1}).", e.ErrorCode, se.ToString());
						Log.Error(e.Message);
						break;
				}
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
		public enum ServerStatus
		{
			STOPPED, QUIT, CRASHED, RESTARTING
		}
	}
}
