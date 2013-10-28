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

			Console.Title = "KMP Server " + KMPCommon.PROGRAM_VERSION;
			Console.WriteLine("KMP Server version " + KMPCommon.PROGRAM_VERSION);
			Console.WriteLine("    Created by Shaun Esau");
			Console.WriteLine("    Based on Kerbal LiveFeed created by Alfred Lam");
			Console.WriteLine();

			ServerSettings settings = new ServerSettings();
			settings.readConfigFile();
			bool firstLoop = true;

			while (true)
			{
				Console.WriteLine();

				Console.ForegroundColor = ConsoleColor.Green;
				Console.Write("Port: ");

				Console.ResetColor();
				Console.WriteLine(settings.port);

				Console.ForegroundColor = ConsoleColor.Green;
				Console.Write("HTTP Port: ");

				Console.ResetColor();
				Console.WriteLine(settings.httpPort);

				Console.ForegroundColor = ConsoleColor.Green;
				Console.Write("Max Clients: ");

				Console.ResetColor();
				Console.WriteLine(settings.maxClients);

				Console.ForegroundColor = ConsoleColor.Green;
				Console.Write("Join Message: ");

				Console.ResetColor();
				Console.WriteLine(settings.joinMessage);

				Console.ForegroundColor = ConsoleColor.Green;
				Console.Write("Server Info: ");

				Console.ResetColor();
				Console.WriteLine(settings.serverInfo);

				Console.ForegroundColor = ConsoleColor.Green;
				Console.Write("Updates Per Second: ");

				Console.ResetColor();
				Console.WriteLine(settings.updatesPerSecond);

//				Console.ForegroundColor = ConsoleColor.Green;
//				Console.Write("Total Inactive Ships: ");
//
//				Console.ResetColor();
//				Console.WriteLine(settings.totalInactiveShips);

				Console.ForegroundColor = ConsoleColor.Green;
				Console.Write("Screenshot Height: ");

				Console.ResetColor();
				Console.WriteLine(settings.screenshotSettings.maxHeight);

				Console.ForegroundColor = ConsoleColor.Green;
				Console.Write("Screenshot Interval: ");

				Console.ResetColor();
				Console.WriteLine(settings.screenshotInterval + "ms");

				Console.ForegroundColor = ConsoleColor.Green;
				Console.Write("Save Screenshots: ");

				Console.ResetColor();
				Console.WriteLine(settings.saveScreenshots);

				Console.ForegroundColor = ConsoleColor.Green;
				Console.Write("Auto-Restart: ");

				Console.ResetColor();
				Console.WriteLine(settings.autoRestart);

				Console.ForegroundColor = ConsoleColor.Green;
				Console.Write("Auto-Host: ");

				Console.ResetColor();
				Console.WriteLine(settings.autoHost);

				Console.ResetColor();
				Console.WriteLine();
				Console.WriteLine("P: change port, HP: change http port, M: change max clients");
				Console.WriteLine("J: join message, IF: server info, U: updates per second");//, IS: total inactive ships");
				Console.WriteLine("SH: screenshot height, SI: screenshot interval, SV: save screenshots");
				Console.WriteLine("AR: toggle auto-restart, AH: toggle auto-host");
				Console.WriteLine("H: begin hosting, Q: quit");

				String in_string;
				if (settings.autoHost && firstLoop)
				{
					in_string = "h";
				}
				else
				{
					in_string = Console.ReadLine().ToLower();
				}
				firstLoop = false;

				if (in_string == "q")
				{
					break;
				}
				else if (in_string == "p")
				{
					Console.Write("Enter the Port: ");

					int new_port;
					if (int.TryParse(Console.ReadLine(), out new_port) && ServerSettings.validPort(new_port))
					{
						settings.port = new_port;
						settings.writeConfigFile();
					}
					else
					{
						Console.WriteLine("Invalid port ["
							+ IPEndPoint.MinPort + '-'
							+ IPEndPoint.MaxPort + ']');
					}
				}
				else if (in_string == "hp")
				{
					Console.Write("Enter the HTTP Port: ");

					int new_port;
					if (int.TryParse(Console.ReadLine(), out new_port) && ServerSettings.validPort(new_port))
					{
						settings.httpPort = new_port;
						settings.writeConfigFile();
					}
					else
					{
						Console.WriteLine("Invalid port ["
							+ IPEndPoint.MinPort + '-'
							+ IPEndPoint.MaxPort + ']');
					}
				}
				else if (in_string == "m")
				{
					Console.Write("Enter the max number of clients: ");

					int new_value;
					if (int.TryParse(Console.ReadLine(), out new_value) && new_value > 0)
					{
						settings.maxClients = new_value;
						settings.writeConfigFile();
					}
					else
						Console.WriteLine("Invalid number of clients");
				}
				else if (in_string == "j")
				{
					Console.Write("Enter the join message: ");
					settings.joinMessage = Console.ReadLine();
					settings.writeConfigFile();
				}
				else if (in_string == "if")
				{
					Console.Write("Enter the server info message: ");
					settings.serverInfo = Console.ReadLine();
					settings.writeConfigFile();
				}
				else if (in_string == "u")
				{
					Console.Write("Enter the number of updates to receive per second: ");
					float new_value;
					if (float.TryParse(Console.ReadLine(), out new_value) && ServerSettings.validUpdatesPerSecond(new_value))
					{
						settings.updatesPerSecond = new_value;
						settings.writeConfigFile();
					}
					else
					{
						Console.WriteLine("Invalid updates per second ["
							+ ServerSettings.MIN_UPDATES_PER_SECOND + '-'
							+ ServerSettings.MAX_UPDATES_PER_SECOND + ']');
					}
				}
				else if (in_string == "sh")
				{
					Console.Write("Enter the screenshot height: ");
					int new_value;
					if (int.TryParse(Console.ReadLine(), out new_value))
					{
						settings.screenshotSettings.maxHeight = new_value;
						settings.writeConfigFile();
					}
					else
					{
						Console.WriteLine("Invalid screenshot height.");
					}
				}
				else if (in_string == "si")
				{
					Console.Write("Enter the screenshot interval: ");
					int new_value;
					if (int.TryParse(Console.ReadLine(), out new_value) && ServerSettings.validScreenshotInterval(new_value))
					{
						settings.screenshotInterval = new_value;
						settings.writeConfigFile();
					}
					else
					{
						Console.WriteLine("Invalid screenshot interval ["
							+ ServerSettings.MIN_SCREENSHOT_INTERVAL + '-'
							+ ServerSettings.MAX_SCREENSHOT_INTERVAL + ']');
					}
				}
//				else if (in_string == "is")
//				{
//					Console.Write("Enter the total number of inactive ships: ");
//					byte new_value;
//					if (byte.TryParse(Console.ReadLine(), out new_value))
//					{
//						settings.totalInactiveShips = new_value;
//						settings.writeConfigFile();
//					}
//					else
//					{
//						Console.WriteLine("Invalid total inactive ships ["
//							+ Byte.MinValue + '-'
//							+ Byte.MaxValue + ']');
//					}
//				}
				else if (in_string == "sv")
				{
					settings.saveScreenshots = !settings.saveScreenshots;
					settings.writeConfigFile();
				}
				else if (in_string == "ar")
				{
					settings.autoRestart = !settings.autoRestart;
					settings.writeConfigFile();
				}
				else if (in_string == "ah")
				{
					settings.autoHost = !settings.autoHost;
					settings.writeConfigFile();
				}
				else if (in_string == "h")
				{
					ServerStatus status = hostServer(settings);
					while (status == ServerStatus.RESTARTING)
					{
						System.Threading.Thread.Sleep(AUTO_RESTART_DELAY);
						status = hostServer(settings);
					}
					
					if (status == ServerStatus.QUIT)
					{
						Console.WriteLine("Press any key to quit");
						Console.ReadKey();
	
						break;
					}
					else
					{
						Console.WriteLine("Server "+Enum.GetName(typeof(ServerStatus), status).ToLower());
					}
				}

			}

		}

		static ServerStatus hostServer(ServerSettings settings)
		{

			Server server = new Server(settings);

			try
			{
				server.hostingLoop();
			}
			catch (Exception e)
			{
				//Write an error log
				TextWriter writer = File.CreateText("KMPServerlog.txt");

				writer.WriteLine(e.ToString());
				if (server.threadExceptionStackTrace != null && server.threadExceptionStackTrace.Length > 0)
				{
					writer.Write("Stacktrace: ");
					writer.WriteLine(server.threadExceptionStackTrace);
				}

				writer.Close();

				Console.WriteLine();

				Console.ForegroundColor = ConsoleColor.Red;
				Server.stampedConsoleWriteLine("Unexpected exception encountered! Crash report written to KMPServerlog.txt");
				Console.WriteLine(e.ToString());
				if (server.threadExceptionStackTrace != null && server.threadExceptionStackTrace.Length > 0)
				{
					Console.Write("Stacktrace: ");
					Console.WriteLine(server.threadExceptionStackTrace);
				}

				Console.WriteLine();
				Console.ResetColor();
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
