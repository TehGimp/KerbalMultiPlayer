using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.IO;
using System.Net;

namespace KMPServer
{
	public class ServerSettings
	{
		public const String SERVER_CONFIG_FILENAME = "KMPServerConfig.txt";
		public const String PORT_LABEL = "port";
		public const String HTTP_PORT_LABEL = "httpPort";
		public const String MAX_CLIENTS_LABEL = "maxClients";
		public const String JOIN_MESSAGE_LABEL = "joinMessage";
		public const String SERVER_INFO_LABEL = "serverInfo";
		public const String UPDATES_PER_SECOND_LABEL = "updatesPerSecond";
		public const String SCREENSHOT_INTERVAL_LABEL = "screenshotInterval";
		public const String SAVE_SCREENSHOTS_LABEL = "saveScreenshots";
		public const String AUTO_RESTART_LABEL = "autoRestart";
		public const String AUTO_HOST_LABEL = "autoHost";
		public const String TOTAL_INACTIVE_SHIPS_LABEL = "totalInactiveShips";
		public const String SCREENSHOT_HEIGHT_LABEL = "screenshotHeight";

		public int port = 2076;
		public int httpPort = 8081;
		public int maxClients = 8;
		public float updatesPerSecond = 60;
		public int screenshotInterval = 3000;
		public bool autoRestart = false;
		public bool autoHost = false;
		public bool saveScreenshots = false;
		public String joinMessage = String.Empty;
		public String serverInfo = String.Empty;
		public byte totalInactiveShips = 100;
		public ScreenshotSettings screenshotSettings = new ScreenshotSettings();

		public const int MIN_UPDATE_INTERVAL = 250;
		public const int MAX_UPDATE_INTERVAL = 500;

		public const float MIN_UPDATES_PER_SECOND = 30f;
		public const float MAX_UPDATES_PER_SECOND = 360.0f;

		public const int MIN_SCREENSHOT_INTERVAL = 500;
		public const int MAX_SCREENSHOT_INTERVAL = 10000;

		public static bool validUpdateInterval(int val)
		{
			return val >= MIN_UPDATE_INTERVAL && val <= MAX_UPDATE_INTERVAL;
		}

		public static bool validUpdatesPerSecond(float val)
		{
			return val >= MIN_UPDATES_PER_SECOND && val <= MAX_UPDATES_PER_SECOND;
		}

		public static bool validScreenshotInterval(int val)
		{
			return val >= MIN_SCREENSHOT_INTERVAL && val <= MAX_SCREENSHOT_INTERVAL;
		}

		public static bool validPort(int port)
		{
			return port >= IPEndPoint.MinPort && port <= IPEndPoint.MaxPort;
		}

		//Config

		public void readConfigFile()
		{
			try
			{
				TextReader reader = File.OpenText(SERVER_CONFIG_FILENAME);

				String line = reader.ReadLine();

				while (line != null)
				{
					String label = line; //Store the last line read as the label
					line = reader.ReadLine(); //Read the value from the next line

					if (line != null)
					{
						//Update the value with the given label
						if (label == PORT_LABEL)
						{
							int new_port;
							if (int.TryParse(line, out new_port) && validPort(new_port))
								port = new_port;
						}
						else if (label == HTTP_PORT_LABEL)
						{
							int new_port;
							if (int.TryParse(line, out new_port) && validPort(new_port))
								httpPort = new_port;
						}
						else if (label == MAX_CLIENTS_LABEL)
						{
							int new_max;
							if (int.TryParse(line, out new_max) && new_max > 0)
								maxClients = new_max;
						}
						else if (label == JOIN_MESSAGE_LABEL)
						{
							joinMessage = line;
						}
						else if (label == SERVER_INFO_LABEL)
						{
							serverInfo = line;
						}
						else if (label == UPDATES_PER_SECOND_LABEL)
						{
							int new_val;
							if (int.TryParse(line, out new_val))
								updatesPerSecond = new_val;
						}
						else if (label == SCREENSHOT_INTERVAL_LABEL)
						{
							int new_val;
							if (int.TryParse(line, out new_val) && validScreenshotInterval(new_val))
								screenshotInterval = new_val;
						}
						else if (label == AUTO_RESTART_LABEL)
						{
							bool new_val;
							if (bool.TryParse(line, out new_val))
								autoRestart = new_val;
						}
						else if (label == AUTO_HOST_LABEL)
						{
							bool new_val;
							if (bool.TryParse(line, out new_val))
								autoHost = new_val;
						}
						else if (label == SAVE_SCREENSHOTS_LABEL)
						{
							bool new_val;
							if (bool.TryParse(line, out new_val))
								saveScreenshots = new_val;
						}
						else if (label == TOTAL_INACTIVE_SHIPS_LABEL)
						{
							byte new_val;
							if (byte.TryParse(line, out new_val))
								totalInactiveShips = new_val;
						}
						else if (label == SCREENSHOT_HEIGHT_LABEL)
						{
							int new_val;
							if (int.TryParse(line, out new_val))
								screenshotSettings.maxHeight = new_val;
						}

					}

					line = reader.ReadLine();
				}

				reader.Close();
			}
			catch (FileNotFoundException)
			{
			}
			catch (UnauthorizedAccessException)
			{
			}

		}

		public void writeConfigFile()
		{
			TextWriter writer = File.CreateText(SERVER_CONFIG_FILENAME);

			//port
			writer.WriteLine(PORT_LABEL);
			writer.WriteLine(port);

			//port
			writer.WriteLine(HTTP_PORT_LABEL);
			writer.WriteLine(httpPort);

			//max clients
			writer.WriteLine(MAX_CLIENTS_LABEL);
			writer.WriteLine(maxClients);

			//join message
			writer.WriteLine(JOIN_MESSAGE_LABEL);
			writer.WriteLine(joinMessage);

			//server info
			writer.WriteLine(SERVER_INFO_LABEL);
			writer.WriteLine(serverInfo);

			//update interval
			writer.WriteLine(UPDATES_PER_SECOND_LABEL);
			writer.WriteLine(updatesPerSecond);

			//screenshot interval
			writer.WriteLine(SCREENSHOT_INTERVAL_LABEL);
			writer.WriteLine(screenshotInterval);

			//auto-restart
			writer.WriteLine(AUTO_RESTART_LABEL);
			writer.WriteLine(autoRestart);

			//auto-host
			writer.WriteLine(AUTO_HOST_LABEL);
			writer.WriteLine(autoHost);

			//upnp
			writer.WriteLine(TOTAL_INACTIVE_SHIPS_LABEL);
			writer.WriteLine(totalInactiveShips);

			//save screenshots
			writer.WriteLine(SAVE_SCREENSHOTS_LABEL);
			writer.WriteLine(saveScreenshots);

			//screenshot height
			writer.WriteLine(SCREENSHOT_HEIGHT_LABEL);
			writer.WriteLine(screenshotSettings.maxHeight);

			writer.Close();
		}
	}
}
