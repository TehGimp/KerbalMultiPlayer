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
		public const string SERVER_WHITELIST_FILENAME = "KMPWhitelist.txt";
		public const string SERVER_BANS_FILENAME = "KMPBans.txt";
		public const string SERVER_ADMINS_FILENAME = "KMPAdmins.txt";

		public class BanRecord
		{
			public DateTime When { get; set; }
			public DateTime Expires { get; set; }
			public string WhoBy { get; set; }
			public IPAddress BannedIP { get; set; }
			public Guid BannedGUID { get; set; }
			public string BannedName { get; set; }
			public string Why { get; set; }
		}

		public class ConfigStore
		{
			public string ipBinding = "0.0.0.0";
			public int port = 2076;
			public int httpPort = 8081;
			public bool httpBroadcast = true;
			public int maxClients = 8;
			public int screenshotInterval = 3000;
			public bool autoRestart = false;
			public bool autoHost = false;
			public bool saveScreenshots = true;
			public bool hostIPv6 = false;
			public bool cheatsEnabled = true;
			public int backupInterval = 5;
			public int maxDirtyBackups = 36;
			public float updatesPerSecond = 60;
			public String joinMessage = String.Empty;
			public String serverInfo = String.Empty;
			public String serverMotd = String.Empty;
			public String serverRules = String.Empty;
			public byte totalInactiveShips = 100;
			public Log.LogLevels LogLevel = Log.LogLevels.Info;
			public bool whitelisted = false;
			public int screenshotHeight = 600;
			public bool profanityFilter = true;
			public double safetyBubbleRadius = 20000d;
			public bool autoDekessler = false;
			public int autoDekesslerTime = 30;
			public string profanityWords = "fucker:kerper,faggot:kerpot,shit:kerp,fuck:guck,cunt:kump,piss:heph,fag:olp,dick:derp,cock:beet,asshole:hepderm,nigger:haggar";
		    public float consoleScale = 1.0f;
		    public int maximumLogs = 100;
			
			private int _gameMode = 0;
			public int gameMode
			{
				get
				{
					return _gameMode;
				}
				set
				{
					if (value >= 0 && value <= 1) _gameMode = value;
					else throw new ArgumentException("Invalid game mode specified");
				}
			}

			private IEnumerable<KeyValuePair<string, string>> _profanity = null;
			public IEnumerable<KeyValuePair<string, string>> Profanity
			{
				get
				{
					if (_profanity == null)
					{
						try
						{
							//Uses a Enumerable KVP instead of a dictionary to avoid an extra conversion.
							_profanity = profanityWords.Split(',').Select(ws =>
							{
								var wx = ws.Split(':');
								return new KeyValuePair<string, string>(wx[0], wx[1]);
							});
						}
						catch
						{
							_profanity = new Dictionary<string, string>();
						}
					}

					return _profanity;
				}
			}

			private List<BanRecord> _bans = new List<BanRecord>();
			internal List<BanRecord> bans
			{
				get
				{
					return _bans;
				}
			}

			private List<string> _whitelist = new List<string>();
			internal List<string> whitelist
			{
				get
				{
					return _whitelist;
				}
			}

			private List<string> _admins = new List<string>();
			internal List<string> admins
			{
				get
				{
					return _admins;
				}
			}

			private ScreenshotSettings _screenshotSettings = new ScreenshotSettings();
			internal ScreenshotSettings screenshotSettings
			{
				get
				{
					_screenshotSettings.maxHeight = screenshotHeight;
					return _screenshotSettings;

				}
			}

			public bool Contains(String sKey)
			{
				return ServerSettings.Contains(typeof(ServerSettings.ConfigStore), sKey);
			}
		}

		public const int MIN_UPDATE_INTERVAL = 250;
		public const int MAX_UPDATE_INTERVAL = 500;

		public const float MIN_UPDATES_PER_SECOND = 30f;
		public const float MAX_UPDATES_PER_SECOND = 360.0f;

		public const int MIN_SCREENSHOT_INTERVAL = 500;
		public const int MAX_SCREENSHOT_INTERVAL = 10000;

		public static bool Contains(Type T, String sKey)
		{
			return (T.GetField(sKey) != null);
		}

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
				FieldInfo f = Store.GetType().GetFields().Where(fF => fF.Name.ToLowerInvariant() == Key.ToLowerInvariant()).First();
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

		public static void saveBans(ConfigStore Store)
		{
			string FileName = SERVER_BANS_FILENAME;

			try
			{
				if (File.Exists(FileName))
				{
					File.SetAttributes(FileName, FileAttributes.Normal);
				}

				using (StreamWriter configWriter = new StreamWriter(FileName))
				{
					configWriter.WriteLine("#When\tExpires\tWhoBy\tBannedIP\tBannedGUID\tBannedName\tWhy");

					foreach (var b in Store.bans.Where(b => b.Expires > DateTime.Now))
					{
						configWriter.WriteLine("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}", b.When.ToString(), b.Expires.ToString(), b.WhoBy, b.BannedIP.ToString(), b.BannedGUID.ToString(), b.BannedName, b.Why);
					}
				}
			}
			catch { }
		}

		public static void loadBans(ConfigStore Store)
		{
			string FileName = SERVER_BANS_FILENAME;

			try
			{
				if (File.Exists(FileName))
				{
					Store.bans.Clear();

					foreach (var l in File.ReadAllLines(FileName))
					{
						var Now = DateTime.Now;

						try
						{
							if (l.StartsWith("#")) { continue; }
							var parts = l.Split('\t');
							var newBan = new BanRecord()
							{
								When = DateTime.Parse(parts[0]),
								Expires = DateTime.Parse(parts[1]),
								WhoBy = parts[2],
								BannedIP = IPAddress.Parse(parts[3]),
								BannedGUID = Guid.Parse(parts[4]),
								BannedName = parts[5],
								Why = parts[6],
							};

							if (newBan.Expires > Now)
							{
								Store.bans.Add(newBan);
							}
						}
						catch
						{
							//Bad ban line. Don't care?
						}
					}
				}
			}
			catch { }
		}

		public static void saveWhitelist(ConfigStore Store)
		{
			string FileName = SERVER_WHITELIST_FILENAME;

			try
			{
				if (File.Exists(FileName))
				{
					File.SetAttributes(FileName, FileAttributes.Normal);
				}

				using (StreamWriter configWriter = new StreamWriter(FileName))
				{
					foreach (var u in Store.whitelist)
					{
						configWriter.WriteLine(u.ToLowerInvariant());
					}
				}
			}
			catch { }
		}

		public static void loadWhitelist(ConfigStore Store)
		{
			string FileName = SERVER_WHITELIST_FILENAME;

			try
			{
				if (File.Exists(FileName))
				{
					Store.whitelist.Clear();
					Store.whitelist.AddRange(File.ReadAllLines(FileName));
				}
			}
			catch { }
		}

		public static void saveAdmins(ConfigStore Store)
		{
			string FileName = SERVER_ADMINS_FILENAME;

			try
			{
				if (File.Exists(FileName))
				{
					File.SetAttributes(FileName, FileAttributes.Normal);
				}

				using (StreamWriter configWriter = new StreamWriter(FileName))
				{
					foreach (var u in Store.admins)
					{
						configWriter.WriteLine(u);
					}
				}
			}
			catch { }
		}
			

		public static void loadAdmins(ConfigStore store)
		{
			string fileName = SERVER_ADMINS_FILENAME;

			try
			{
				if (File.Exists(fileName))
				{
					store.admins.Clear();
					store.admins.AddRange(File.ReadAllLines(fileName));
				}
			}
			catch { }
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
			catch (Exception)
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
				if (!File.Exists(FileName)) { return; }
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
			catch (Exception)
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
