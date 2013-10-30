using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.IO;
using System.Net;
using System.Reflection;

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

        public class ConfigStore
        {
            public string ipBinding = "0.0.0.0";
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
            public Log.LogLevels LogLevel = Log.LogLevels.Info;

            private ScreenshotSettings _screenshotSettings = new ScreenshotSettings();
            public ScreenshotSettings screenshotSettings
            {
                get
                {
                    return _screenshotSettings;
                }
            }
        }

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

        public static void modifySetting(ConfigStore Store, string Key, string Value)
        {
            try
            {
                FieldInfo f = Store.GetType().GetFields().Where(fF => fF.Name.ToLowerInvariant() == Key).First();
                object newValue;

                if (f.FieldType.IsEnum)
                {
                    newValue = Enum.Parse(f.FieldType, Value.ToString(), true);
                }
                else
                {
                    newValue = (f.FieldType == typeof(bool)) ? getBool(Value.ToString()) : Convert.ChangeType(Value, f.FieldType);
                }

                f.SetValue(Store, newValue);
            }
            catch
            {
                throw new ArgumentException(string.Format("{0} is not a valid value for {1}", Value, Key));
            }
        }

        public static Dictionary<string, string> GetCurrentValues(ConfigStore Store)
        {
            var result = new Dictionary<string, string>();

            foreach (FieldInfo f in Store.GetType().GetFields().Where(f => f.IsPublic))
            {
                var value = f.GetValue(Store).ToString();
                result.Add(f.Name, value);
            }

            return result;
        }

        //Write the setting store out to a file by reflecting over its members.
        public static void writeToFile(ConfigStore Store)
        {
            string FileName = SERVER_CONFIG_FILENAME;

            try
            {
                if (File.Exists(FileName))
                {
                    File.SetAttributes(FileName, FileAttributes.Normal);
                }

                using (StreamWriter configWriter = new StreamWriter(FileName))
                {
                    foreach (FieldInfo f in Store.GetType().GetFields().Where(f => f.IsPublic))
                    {
                        var value = f.GetValue(Store).ToString();
                        string data = string.Format("{0}={1}", f.Name, value);
                        configWriter.WriteLine(data);
                    }
                }
            }
            catch (Exception ex)
            { 
                
            }
        }

        public static bool getBool(string Value)
        {
            if (Value == "1")
            {
                return (true);
            }
            else if (Value == "0")
            {
                return (false);
            }
            else
            {
                try
                {
                    return (Convert.ToBoolean(Value));
                }
                catch
                {
                    return (false);
                }
            }
        }

        public static void SetFieldValue(Dictionary<string, string> ConfigStore, object e, FieldInfo fV, string node)
        {
            object NArg;
            Type TestType = fV.FieldType;

            try
            {
                if (TestType.BaseType == typeof(Enum))
                {
                    if (String.IsNullOrEmpty(ConfigStore[node])) { return; }

                    NArg = Enum.Parse(TestType, ConfigStore[node], true);
                }

                else if (TestType == typeof(bool))
                {
                    NArg = getBool(ConfigStore[node]);
                }
                else
                {
                    NArg = Convert.ChangeType(ConfigStore[node], TestType);
                }

                fV.SetValue(e, NArg);
            }
            catch
            {
                //Failed to set field value, ignore.
            }
        }

        public static void readFromFile(ConfigStore Store)
        {
            string FileName = SERVER_CONFIG_FILENAME;

            Dictionary<string, string> ConfigStore = new Dictionary<string, string>();

            //Read the settings file into a dictionary before shoving the values in the setting store.
            try
            {
                using (StreamReader configReader = new StreamReader(FileName))
                {
                    string CurrentLine;
                    string[] LineParts;

                    while (configReader.EndOfStream == false)
                    {
                        CurrentLine = configReader.ReadLine();

                        if (CurrentLine.StartsWith("#") || String.IsNullOrEmpty(CurrentLine)) { continue; }

                        LineParts = CurrentLine.Split(new char[] { '=' }, 2);
                        if (LineParts.Length < 2) { continue; }

                        LineParts[0] = LineParts[0].ToLowerInvariant();

                        ConfigStore.Add(LineParts[0].Trim(), LineParts[1].Trim());
                    }

                    configReader.Close();
                }
            }
            catch (Exception ex)
            {
                
            }

            foreach (FieldInfo f in Store.GetType().GetFields())
            {
                string node = f.Name.ToLowerInvariant();

                if (ConfigStore.ContainsKey(node))
                {
                    SetFieldValue(ConfigStore, Store, f, node);
                }
                else
                {
                    //Missing a node, no matter - default value will remain.
                }
            }
        }
	}
}
