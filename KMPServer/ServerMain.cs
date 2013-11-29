using System;
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

		static void Main(string[] args)
		{
			ServerSettings.ConfigStore settings = new ServerSettings.ConfigStore();
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

			Log.MinLogLevel = settings.LogLevel;

			Console.Title = "KMP Server " + KMPCommon.PROGRAM_VERSION;
			Log.Info("KMP Server version {0}", KMPCommon.PROGRAM_VERSION);
			Log.Info("    Created by Shaun Esau and developed by the KMP team http://sesau.ca/ksp/KMP_contribs.html");
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
			Log.Info("    /set help for information about each setting.");
			Log.Info("/whitelist [add|del] [user] to update whitelist.");
            Log.Info("/admin [add|del] [user] to update admin list.");
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

					case "/set":
                        if (parts[1].Equals("help"))
                        {
                            Log.Info("ipBinding - The IP address the server should bind to. Defaults to binding to all available IPs." + Environment.NewLine);
                            Log.Info("port - The port used for connecting to the server.");
                            Log.Info("httpPort - The port used for viewing server information from a web browser.");
                            Log.Info("httpBroadcast - ?"); // missing setting information
                            Log.Info("maxClients - The maximum number of players that can be connected to the server simultaneously." + Environment.NewLine);
                            Log.Info("updatesPerSecond - CHANGING THIS VALUE IS NOT RECOMMENDED - The number of updates that will be received from all clients combined per second. The higher you set this number, the more frequently clients will send updates. As the number of active clients increases, the frequency of updates will decrease to not exceed this many updates per second. " + Environment.NewLine + "WARNING: If this value is set too high then players will be more likely to be disconnected due to lag, while if it is set too low the gameplay experience will degrade significantly." + Environment.NewLine);
                            Log.Info("screenshotInterval - The minimum time a client must wait after sharing a screenshot before they can share another one." + Environment.NewLine);
                            Log.Info("autoRestart - If true, the server will attempt to restart after catching an unhandled exception." + Environment.NewLine);
                            Log.Info("autoHost - If true, the server will start hosting immediately rather than requiring the admin to enter the 'H' command." + Environment.NewLine);
                            Log.Info("saveScreenshots - If true, the server will save all screenshots to the KMPScreenshots folder." + Environment.NewLine);
                            Log.Info("hostIPV6 - If true, the server will be listening on a IPv6 address." + Environment.NewLine);
                            Log.Info("cheatsEnabled - If true, enable cheats." + Environment.NewLine);
                            Log.Info("joinMessage - A message shown to players when they join the server." + Environment.NewLine);
                            Log.Info("serverInfo - A message displayed to anyone viewing server information in a browser." + Environment.NewLine);
                            Log.Info("serverMotd - A message displayed to users when they login to the server that can be changed while the server is running." + Environment.NewLine);
                            Log.Info("serverRules - A message displayed to users when they ask to view the server's rules." + Environment.NewLine);
                            Log.Info("totalInactiveShips - The maximum number of inactive ships in the server." + Environment.NewLine);
                            Log.Info("LogLevel - Log verbosity. Choose from: Debug, Activity, Info, Notice, Warning, or Error" + Environment.NewLine);
                            Log.Info("whitelisted - If true, enable whitelist." + Environment.NewLine);
                            Log.Info("screenshotHeight - The height of screenshots took by players." + Environment.NewLine);
                            Log.Info("screenshotWidth - The width of screenshots took by players." + Environment.NewLine);
                            Log.Info("profanityFilter - Enable/disable the built-in profanity filter" + Environment.NewLine);
                            Log.Info("safetyBubbleRadius - The radius of the 'safety cylinder' which prevents collisions near KSC." + Environment.NewLine);
                            Log.Info("autoDekessler - If true, server will clean up all debris in 'autoDekesslerTime'." + Environment.NewLine);
                            Log.Info("autoDekesslerTime - Time, in minutes, that the server will clean up all debris." + Environment.NewLine);
                            Log.Info("profanityWords - Replaces the first word with the second." + Environment.NewLine);
                        }
						else if (parts.Length < 3)
						{
							Log.Info("Invalid usage. Usage is /set [key] [value]");
						}
                        else
                        {
                            string val = String.Join(" ", parts.Skip(2).ToArray());
                            if (settings.Contains(parts[1]))
                            {
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
