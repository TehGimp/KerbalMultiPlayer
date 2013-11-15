using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;
using System.Xml.Serialization;
using System.Collections;

namespace KMP
{
    public class Bootstrap : KSP.Testing.UnitTest {
        public Bootstrap() {
			if (KMPManager.GameObjectInstance == null)
			{
				Debug.Log("*** KMP version " + KMPCommon.PROGRAM_VERSION + " started");
				KMPManager.GameObjectInstance = new GameObject("KMPManager", typeof(KMPManager));
				UnityEngine.Object.DontDestroyOnLoad(KMPManager.GameObjectInstance);
			}
        }
    }
	
	public class KMPManager : MonoBehaviour
	{
		
		public KMPManager()
		{
			//Initialize client
			KMPClientMain.InitMPClient(this);
			Debug.Log("Client Initialized.");
		}
		
		public struct VesselEntry
		{
			public KMPVessel vessel;
			public float lastUpdateTime;
		}

		public struct VesselStatusInfo
		{
			public string ownerName;
			public string vesselName;
			public string detailText;
			public Color color;
			public KMPVesselInfo info;
			public Orbit orbit;
			public float lastUpdateTime;
			public int currentSubspaceID;
		}

		//Singleton

		public static GameObject GameObjectInstance;

		//Properties

		public const String GLOBAL_SETTINGS_FILENAME = "globalsettings.txt";

		public const float INACTIVE_VESSEL_RANGE = 2500.0f;
		public const float DOCKING_TARGET_RANGE = 200.0f;
		public const int MAX_INACTIVE_VESSELS_PER_UPDATE = 16;
		public const int STATUS_ARRAY_MIN_SIZE = 2;
		public const int MAX_VESSEL_NAME_LENGTH = 32;
		public const float VESSEL_TIMEOUT_DELAY = 6.0f;
		public const float IDLE_DELAY = 120.0f;
		public const float PLUGIN_DATA_WRITE_INTERVAL = 0.333f;
		public const float GLOBAL_SETTINGS_SAVE_INTERVAL = 10.0f;
        public const double SAFETY_BUBBLE_CEILING = 35000d;

		public const int INTEROP_MAX_QUEUE_SIZE = 64;
		public const float INTEROP_WRITE_INTERVAL = 0.333f;
		public const float INTEROP_WRITE_TIMEOUT = 6.0f;
		
		public const float FULL_PROTOVESSEL_UPDATE_TIMEOUT = 45f;

		public const double PRIVATE_VESSEL_MIN_TARGET_DISTANCE = 500d;

		public UnicodeEncoding encoder = new UnicodeEncoding();

		public String playerName = String.Empty;
		public byte inactiveVesselsPerUpdate = 0;
		public float updateInterval = 1.0f;

		public Dictionary<String, VesselEntry> vessels = new Dictionary<string, VesselEntry>();
		public SortedDictionary<String, VesselStatusInfo> playerStatus = new SortedDictionary<string, VesselStatusInfo>();
		public RenderingManager renderManager;
		public PlanetariumCamera planetariumCam;

		public Queue<byte[]> interopOutQueue = new Queue<byte[]>();
		public Queue<byte[]> interopInQueue = new Queue<byte[]>();
		
		public static object interopInQueueLock = new object();
		
		private float lastGlobalSettingSaveTime = 0.0f;
		private float lastPluginDataWriteTime = 0.0f;
		private float lastPluginUpdateWriteTime = 0.0f;
		private float lastInteropWriteTime = 0.0f;
		private float lastKeyPressTime = 0.0f;
		private float lastFullProtovesselUpdate = 0.0f;

		private Queue<KMPVesselUpdate> vesselUpdateQueue = new Queue<KMPVesselUpdate>();
		private Queue<KMPVesselUpdate> newVesselUpdateQueue = new Queue<KMPVesselUpdate>();
		
		GUIStyle playerNameStyle, vesselNameStyle, stateTextStyle, chatLineStyle, screenshotDescriptionStyle;
		private bool isEditorLocked = false;

		private bool mappingGUIToggleKey = false;
		private bool mappingScreenshotKey = false;
        private bool mappingChatKey = false;
        private bool mappingChatDXToggleKey = false;

        PlatformID platform;
		
		private bool addPressed = false;
		private string newHost = "localhost";
		private string newPort = "2076";
		
		private bool forceQuit = false;
		private bool gameRunning = false;
		private bool activeTermination = false;
		
		//Vessel dictionaries
		public Dictionary<Guid, Vessel.Situations> sentVessels_Situations = new Dictionary<Guid, Vessel.Situations>();
		
		public Dictionary<Guid, Guid> serverVessels_RemoteID = new Dictionary<Guid, Guid>();
		public Dictionary<Guid, int> serverVessels_PartCounts = new Dictionary<Guid, int>();
		public Dictionary<Guid, List<Part>> serverVessels_Parts = new Dictionary<Guid, List<Part>>();
		public Dictionary<Guid, ConfigNode> serverVessels_ProtoVessels = new Dictionary<Guid, ConfigNode>();
		
		public Dictionary<Guid, bool> serverVessels_InUse = new Dictionary<Guid, bool>();
		public Dictionary<Guid, bool> serverVessels_IsPrivate = new Dictionary<Guid, bool>();
		public Dictionary<Guid, bool> serverVessels_IsMine = new Dictionary<Guid, bool>();
		
		public Dictionary<Guid, KeyValuePair<double,double>> serverVessels_LastUpdateDistanceTime = new Dictionary<Guid, KeyValuePair<double,double>>();
		public Dictionary<Guid, float> serverVessels_LoadDelay = new Dictionary<Guid, float>();
		public Dictionary<Guid, bool> serverVessels_InPresent = new Dictionary<Guid, bool>();
		public Dictionary<Guid, float> serverVessels_ObtSyncDelay = new Dictionary<Guid, float>();
		
		public Dictionary<Guid, KeyValuePair<double,double>> serverVessels_RendezvousSmoothPos = new Dictionary<Guid, KeyValuePair<double,double>>();
		public Dictionary<Guid, KeyValuePair<double,double>> serverVessels_RendezvousSmoothVel = new Dictionary<Guid, KeyValuePair<double,double>>();
		
		public Dictionary<Guid, float> newFlags = new Dictionary<Guid, float>();
		
		private Krakensbane krakensbane;
		
		public double lastTick = 0d;
		public double targetTick = 0d;
		
		public Vector3d kscPosition = Vector3d.zero;
		
		public Vector3d activeVesselPosition = Vector3d.zero;
		public Dictionary<Guid, Vector3d> dockingRelVel = new Dictionary<Guid, Vector3d>();
		
		public GameObject ksc = null;
		private bool warping = false;
		private bool syncing = false;
		private bool docking = false;
		private float lastWarpRate = 1f;
		private int chatMessagesWaiting = 0;
		private Vessel lastEVAVessel = null;
		private bool showServerSync = false;

		private bool configRead = false;

		public double safetyBubbleRadius = 20000d;
		private bool isVerified = false;
		
		public bool globalUIToggle
		{
			get
			{
				return renderManager == null || renderManager.uiElementsToDisable.Length < 1 || renderManager.uiElementsToDisable[0].activeSelf;
			}
		}

		public bool shouldDrawGUI
		{
			get
			{
				switch (HighLogic.LoadedScene)
				{
					case GameScenes.SPACECENTER:
					case GameScenes.EDITOR:
					case GameScenes.FLIGHT:
					case GameScenes.SPH:
					case GameScenes.TRACKSTATION:
						return KMPInfoDisplay.infoDisplayActive && globalUIToggle;

					default:
						return false;
				}
				
			}
		}

		public static bool isInFlight
		{
			get
			{
				return FlightGlobals.ready && FlightGlobals.ActiveVessel != null && KMPClientMain.handshakeCompleted && KMPClientMain.receivedSettings;
			}
		}
		
		public static bool isInFlightOrTracking
		{
			get
			{
				return HighLogic.LoadedScene == GameScenes.TRACKSTATION || isInFlight;
			}
		}

		public bool isIdle
		{
			get
			{
				return lastKeyPressTime > 0.0f && (UnityEngine.Time.realtimeSinceStartup - lastKeyPressTime) > IDLE_DELAY;
			}
		}

		//Keys

		public bool getAnyKeyDown(ref KeyCode key)
		{
			foreach (KeyCode keycode in Enum.GetValues(typeof(KeyCode)))
			{
				if (Input.GetKeyDown(keycode))
				{
					key = keycode;
					return true;
				}
			}

			return false;
		}

		//Updates

		public void updateStep()
		{
			try
			{
				if (HighLogic.LoadedScene == GameScenes.LOADING || !gameRunning)
					return; //Don't do anything while the game is loading or not in KMP game
				
				if (!isInFlight && HighLogic.LoadedScene == GameScenes.TRACKSTATION)
				{
					foreach (object button in GameObject.FindObjectsOfType(typeof(ScreenSafeUIButton)))
					{
						ScreenSafeUIButton ssUIButton = (ScreenSafeUIButton) button;
						if (ssUIButton.tooltip == "Terminate") ssUIButton.Unlock();
					}
				}

                if (isInFlight && FlightGlobals.ActiveVessel.mainBody.name == "Kerbin" && FlightGlobals.ActiveVessel.altitude < SAFETY_BUBBLE_CEILING)
                {
					if (ksc == null) ksc = GameObject.Find("KSC");
					kscPosition = new Vector3d(ksc.transform.position[0],ksc.transform.position[1],ksc.transform.position[2]);
				} else kscPosition = Vector3d.zero;
				
				if (lastWarpRate != TimeWarp.CurrentRate)
				{
					lastWarpRate = TimeWarp.CurrentRate;
					OnTimeWarpRateChanged();	
				}
				
				if (warping) {
					writeUpdates();
					return;
				}
				
				//Update Tracking Station names for unavailable vessels
				if (!isInFlight)
				{
					foreach (Vessel vessel in FlightGlobals.Vessels)
					{
						string baseName = vessel.vesselName;
						if (baseName.StartsWith("* ")) baseName = baseName.Substring(2);
						vessel.vesselName = (((serverVessels_InUse.ContainsKey(vessel.id) ? serverVessels_InUse[vessel.id] : false) || ((serverVessels_IsPrivate.ContainsKey(vessel.id) ? serverVessels_IsPrivate[vessel.id]: false) && (serverVessels_IsMine.ContainsKey(vessel.id) ? !serverVessels_IsMine[vessel.id] : false))) ? "* " : "") + baseName;
					}
				}
				else //Kill Kraken-debris, clean names
				{
					foreach (Vessel vessel in FlightGlobals.Vessels.FindAll(v => v.vesselName.Contains("> Debris")))
					{
						try { if (!vessel.isEVA) vessel.Die();} catch {}
					}
				}
				
				//Ensure player never touches something under another player's control
				if (isInFlight && !docking && serverVessels_InUse.ContainsKey(FlightGlobals.ActiveVessel.id))
				{
					if (serverVessels_InUse[FlightGlobals.ActiveVessel.id])
					{
						KMPClientMain.DebugLog("Selected occupied vessel");
						kickToTrackingStation();
						return;
					}
				}
				
				//Ensure player never touches a private vessel they don't own
				if (isInFlight && !docking && serverVessels_IsPrivate.ContainsKey(FlightGlobals.ActiveVessel.id) && serverVessels_IsMine.ContainsKey(FlightGlobals.ActiveVessel.id))
				{
					if (!serverVessels_IsMine[FlightGlobals.ActiveVessel.id] && serverVessels_IsPrivate[FlightGlobals.ActiveVessel.id])
					{
						KMPClientMain.DebugLog("Selected private vessel");
						kickToTrackingStation();
						return;
					}
				}
				if (isInFlight && !docking && FlightGlobals.fetch.VesselTarget != null)
				{
					//Get targeted vessel
					Vessel vesselTarget = null;
					if (FlightGlobals.fetch.VesselTarget is ModuleDockingNode)
					{
						ModuleDockingNode moduleTarget = (ModuleDockingNode) FlightGlobals.fetch.VesselTarget;
						if (moduleTarget.part.vessel != null) vesselTarget = moduleTarget.part.vessel;
					}
					if (FlightGlobals.fetch.VesselTarget is Vessel)
					{
						vesselTarget = (Vessel) FlightGlobals.fetch.VesselTarget;
					}

					if (vesselTarget != null) {

						double distanceToTarget = Vector3d.Distance(vesselTarget.GetWorldPos3D(), FlightGlobals.ship_position);

						//Check if target is private and too close
						if (distanceToTarget < PRIVATE_VESSEL_MIN_TARGET_DISTANCE && serverVessels_IsPrivate.ContainsKey(vesselTarget.id) && serverVessels_IsMine.ContainsKey(vesselTarget.id))
						{
							if (!serverVessels_IsMine[vesselTarget.id] && serverVessels_IsPrivate[vesselTarget.id])
							{
								KMPClientMain.DebugLog("Tried to target private vessel");
								ScreenMessages.PostScreenMessage("Can't dock - Target vessel is Private", 4f, ScreenMessageStyle.UPPER_CENTER);
								FlightGlobals.fetch.SetVesselTarget(null);
							}
						}
					}
				}

				//Update universe time
				try
				{
					double currentTick = Planetarium.GetUniversalTime();
					if (isInFlight && targetTick > currentTick+0.05d)
					{
						KMPClientMain.DebugLog("Syncing to new time " + targetTick + " from " + Planetarium.GetUniversalTime());
						if (FlightGlobals.ActiveVessel.situation != Vessel.Situations.PRELAUNCH
						    && FlightGlobals.ActiveVessel.situation != Vessel.Situations.LANDED
						    && FlightGlobals.ActiveVessel.situation != Vessel.Situations.SPLASHED)
						{
							Vector3d oldObtVel = FlightGlobals.ActiveVessel.obt_velocity;
							if (FlightGlobals.ActiveVessel.orbit.EndUT > 0)
							{
								double lastEndUT =  FlightGlobals.ActiveVessel.orbit.EndUT;
								while (FlightGlobals.ActiveVessel.orbit.EndUT > 0
								       && FlightGlobals.ActiveVessel.orbit.EndUT < targetTick
								       && FlightGlobals.ActiveVessel.orbit.EndUT > lastEndUT
								       && FlightGlobals.ActiveVessel.orbit.nextPatch != null)
								{
									KMPClientMain.DebugLog("orbit EndUT < target: " + FlightGlobals.ActiveVessel.orbit.EndUT + " vs " + targetTick);
									lastEndUT =  FlightGlobals.ActiveVessel.orbit.EndUT;
									FlightGlobals.ActiveVessel.orbitDriver.orbit = FlightGlobals.ActiveVessel.orbit.nextPatch;
									FlightGlobals.ActiveVessel.orbitDriver.UpdateOrbit();
									if (FlightGlobals.ActiveVessel.orbit.referenceBody == null) FlightGlobals.ActiveVessel.orbit.referenceBody = FlightGlobals.Bodies.Find(b => b.name == "Sun");
									KMPClientMain.DebugLog("updated to next patch");
								}
							}
							try
				            {
				                OrbitPhysicsManager.HoldVesselUnpack(1);
				            }
				            catch (NullReferenceException)
				            {
				            }
							//Krakensbane shift to new orbital location
							if (targetTick > currentTick+2.5d //if badly out of sync
							    && !(FlightGlobals.ActiveVessel.orbit.referenceBody.atmosphere && FlightGlobals.ActiveVessel.orbit.altitude < FlightGlobals.ActiveVessel.orbit.referenceBody.maxAtmosphereAltitude)) //and not in atmo
							{
								KMPClientMain.DebugLog("Krakensbane shift");
								Vector3d diffPos = FlightGlobals.ActiveVessel.orbit.getPositionAtUT(targetTick) - FlightGlobals.ship_position;
								foreach (Vessel otherVessel in FlightGlobals.Vessels.Where(v => v.packed == false && (v.id != FlightGlobals.ActiveVessel.id || (v.loaded && Vector3d.Distance(FlightGlobals.ship_position,v.GetWorldPos3D()) < INACTIVE_VESSEL_RANGE))))
		                			otherVessel.GoOnRails();
								getKrakensbane().setOffset(diffPos);
								//Update velocity
								FlightGlobals.ActiveVessel.ChangeWorldVelocity((-1 * oldObtVel) + FlightGlobals.ActiveVessel.orbitDriver.orbit.getOrbitalVelocityAtUT(targetTick).xzy);
	            				FlightGlobals.ActiveVessel.orbitDriver.vel = FlightGlobals.ActiveVessel.orbit.vel;
							}
						}
						Planetarium.SetUniversalTime(targetTick);
						KMPClientMain.DebugLog("sync completed");
					}
				} catch (Exception e) { KMPClientMain.DebugLog("error during sync: " + e.Message + " " + e.StackTrace); }

				writeUpdates();
				
				//Once all updates are processed, update the vesselUpdateQueue with new entries
				vesselUpdateQueue = newVesselUpdateQueue;
				
				//If in flight, check remote vessels, set position variable for docking-mode position updates
				if (isInFlight)
				{
					InputLockManager.ClearControlLocks();
					checkRemoteVesselIntegrity();
					activeVesselPosition = FlightGlobals.ActiveVessel.findWorldCenterOfMass();
					dockingRelVel.Clear();
				}
				
				//Handle all queued vessel updates
				while (vesselUpdateQueue.Count > 0)
				{
					handleVesselUpdate(vesselUpdateQueue.Dequeue());
				}

				processClientInterop();
				
				//Update the displayed player orbit positions
				List<String> delete_list = new List<String>();
	
				foreach (KeyValuePair<String, VesselEntry> pair in vessels) {
	
					VesselEntry entry = pair.Value;
	
					if ((UnityEngine.Time.realtimeSinceStartup-entry.lastUpdateTime) <= VESSEL_TIMEOUT_DELAY
						&& entry.vessel != null && entry.vessel.gameObj != null)
					{
						entry.vessel.updateRenderProperties(!KMPGlobalSettings.instance.showInactiveShips && entry.vessel.info.state != State.ACTIVE);
						entry.vessel.updatePosition();
					}
					else
					{
						delete_list.Add(pair.Key); //Mark the vessel for deletion
	
						if (entry.vessel != null && entry.vessel.gameObj != null)
							GameObject.Destroy(entry.vessel.gameObj);
					}
				}
	
				//Delete what needs deletin'
				foreach (String key in delete_list)
					vessels.Remove(key);
	
				delete_list.Clear();
	
				//Delete outdated player status entries
				foreach (KeyValuePair<String, VesselStatusInfo> pair in playerStatus)
				{
					if ((UnityEngine.Time.realtimeSinceStartup - pair.Value.lastUpdateTime) > VESSEL_TIMEOUT_DELAY)
					{
						KMPClientMain.DebugLog("deleted player status for timeout: " + pair.Key + " " + pair.Value.vesselName);
						delete_list.Add(pair.Key);
					}
				}
				
				foreach (String key in delete_list)
					playerStatus.Remove(key);
				
				
				//Prevent cases of remaining unfixed NREs from remote vessel updates from creating an inconsistent game state
				if (HighLogic.fetch.log.Count > 500 && isInFlight && !syncing)
				{
					bool forceResync = false; int nreCount = 0;
					foreach (HighLogic.LogEntry logEntry in HighLogic.fetch.log.GetRange(HighLogic.fetch.log.Count-100,100))
			        {
						if (logEntry.condition.Contains("NullReferenceException")) nreCount++;
						if (nreCount >= 60)
						{
							forceResync = true;
							break;
						}
					}
					if (forceResync)
					{
						KMPClientMain.DebugLog("Resynced due to NRE flood");
						ScreenMessages.PostScreenMessage("Unexpected error! Re-syncing...");
						GameEvents.onFlightReady.Remove(this.OnFlightReady);
						HighLogic.CurrentGame = GamePersistence.LoadGame("start",HighLogic.SaveFolder,false,true);
						HighLogic.CurrentGame.Start();
						docking = true;
						syncing = true;
						Invoke("OnFirstFlightReady",1f);	
					}
				}
			} catch (Exception ex) { KMPClientMain.DebugLog("uS err: " + ex.Message + " " + ex.StackTrace); }
		}
		
		private void dockedKickToTrackingStation()
		{
			if (syncing && docking)
			{
				GameEvents.onFlightReady.Remove(this.OnFlightReady);
				HighLogic.CurrentGame = GamePersistence.LoadGame("start",HighLogic.SaveFolder,false,true);
				HighLogic.CurrentGame.Start();
				Invoke("OnFirstFlightReady",1f);
			}
		}
		
		private void kickToTrackingStation()
		{
			if (!syncing)
			{
				KMPClientMain.DebugLog("Selected unavailable vessel, switching");
				ScreenMessages.PostScreenMessage("Selected vessel is occupied or private...", 5f,ScreenMessageStyle.UPPER_RIGHT);
				syncing = true;
				StartCoroutine(returnToTrackingStation());
			}
		}
		
		private void writeUpdates()
		{
			if ((UnityEngine.Time.realtimeSinceStartup - lastPluginUpdateWriteTime) > updateInterval
				&& (Time.realtimeSinceStartup - lastInteropWriteTime) < INTEROP_WRITE_TIMEOUT)
			{
				writePluginUpdate();
				lastPluginUpdateWriteTime = UnityEngine.Time.realtimeSinceStartup;
			}
			
			if ((UnityEngine.Time.realtimeSinceStartup - lastPluginDataWriteTime) > PLUGIN_DATA_WRITE_INTERVAL)
			{
				writePluginData();
				lastPluginDataWriteTime = UnityEngine.Time.realtimeSinceStartup;
			}

			//Write interop
			if ((UnityEngine.Time.realtimeSinceStartup - lastInteropWriteTime) > INTEROP_WRITE_INTERVAL)
			{
				if (writePluginInterop())
					lastInteropWriteTime = UnityEngine.Time.realtimeSinceStartup;
			}

			//Save global settings periodically

			if ((UnityEngine.Time.realtimeSinceStartup - lastGlobalSettingSaveTime) > GLOBAL_SETTINGS_SAVE_INTERVAL)
			{
				saveGlobalSettings();

				//Keep track of when the name was last read so we don't read it every time
				lastGlobalSettingSaveTime = UnityEngine.Time.realtimeSinceStartup;
			}	
		}
		
		private void checkRemoteVesselIntegrity()
		{
			try
			{
				if (!isInFlight || syncing || warping || docking) return;
				foreach (Vessel vessel in FlightGlobals.Vessels.FindAll(v => v.loaded && v.id != FlightGlobals.ActiveVessel.id && serverVessels_PartCounts.ContainsKey(v.id) && serverVessels_ProtoVessels.ContainsKey(v.id)))
				{
					if (serverVessels_PartCounts[vessel.id] > 0 && serverVessels_PartCounts[vessel.id] > vessel.Parts.Count)
					{
						KMPClientMain.DebugLog("checkRemoteVesselIntegrity killing vessel: " + vessel.id);
						serverVessels_PartCounts[vessel.id] = 0;
						foreach (Part part in serverVessels_Parts[vessel.id])
						{
							try { if (!part.vessel.isEVA) part.vessel.Die(); } catch {}
						}
						ProtoVessel protovessel = new ProtoVessel(serverVessels_ProtoVessels[vessel.id], HighLogic.CurrentGame);
						addRemoteVessel(protovessel,vessel.id);
						serverVessels_LoadDelay[vessel.id] = UnityEngine.Time.realtimeSinceStartup + 10f;
					}
				}
			}
			catch (Exception ex)
			{
				KMPClientMain.DebugLog("cRVI err: " + ex.Message + " " + ex.StackTrace);
			}
		}
		
		public void disconnect(string message = "")
		{
			if (HighLogic.LoadedSceneIsFlight || HighLogic.LoadedSceneIsEditor)
			{
				ScreenMessages.PostScreenMessage("You have been disconnected. Please return to the Main Menu to reconnect.",300f,ScreenMessageStyle.UPPER_CENTER);
				if (!String.IsNullOrEmpty(message)) ScreenMessages.PostScreenMessage(message, 300f,ScreenMessageStyle.UPPER_CENTER);
			}
			else
			{
				forceQuit = true;
				ScreenMessages.PostScreenMessage("You have been disconnected. Please return to the Main Menu to reconnect.",300f,ScreenMessageStyle.UPPER_CENTER);
				if (!String.IsNullOrEmpty(message)) ScreenMessages.PostScreenMessage(message, 300f,ScreenMessageStyle.UPPER_CENTER);
			}
			if (String.IsNullOrEmpty(message)) KMPClientMain.SetMessage("Disconnected");
			else KMPClientMain.SetMessage("Disconnected: " + message);
            saveGlobalSettings();
			gameRunning = false;
		}
		
		private void writePluginUpdate()
		{
			if (playerName == null || playerName.Length == 0)
				return;

			writePrimaryUpdate();
			
			//nearby vessels
            if (isInFlight && !syncing && !warping && !isInSafetyBubble(FlightGlobals.ship_position,FlightGlobals.ActiveVessel.mainBody,FlightGlobals.ActiveVessel.altitude))
			{
				writeSecondaryUpdates();
			}
		}

		private void writePrimaryUpdate()
		{
			if (!syncing && isInFlight && !warping
                && !isInSafetyBubble(FlightGlobals.ship_position,FlightGlobals.ActiveVessel.mainBody,FlightGlobals.ActiveVessel.altitude))
			{
				lastTick = Planetarium.GetUniversalTime();
				//Write vessel status
				KMPVesselUpdate update = getVesselUpdate(FlightGlobals.ActiveVessel); 
				if (FlightGlobals.ActiveVessel.vesselType == VesselType.EVA) lastEVAVessel = FlightGlobals.ActiveVessel;
				
				//Update the player vessel info
				VesselStatusInfo my_status = new VesselStatusInfo();
				my_status.info = update;
				my_status.orbit = FlightGlobals.ActiveVessel.orbit;
				my_status.color = KMPVessel.generateActiveColor(playerName);
				my_status.ownerName = playerName;
				if (FlightGlobals.ActiveVessel.vesselName.Contains(" <") && FlightGlobals.ActiveVessel.vesselName.Contains(">"))
					FlightGlobals.ActiveVessel.vesselName = FlightGlobals.ActiveVessel.vesselName.Substring(0,FlightGlobals.ActiveVessel.vesselName.IndexOf(" <"));
				if (String.IsNullOrEmpty(FlightGlobals.ActiveVessel.vesselName.Trim())) FlightGlobals.ActiveVessel.vesselName = "Unknown";
				my_status.vesselName = FlightGlobals.ActiveVessel.vesselName;
				my_status.lastUpdateTime = UnityEngine.Time.realtimeSinceStartup;
				
				if (playerStatus.ContainsKey(playerName))
					playerStatus[playerName] = my_status;
				else
					playerStatus.Add(playerName, my_status);
				
				KMPClientMain.DebugLog("sending primary update");
				try{
					enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.PRIMARY_PLUGIN_UPDATE, KSP.IO.IOUtils.SerializeToBinary(update));
				} catch (Exception e) { KMPClientMain.DebugLog("err: " + e.Message); }
			}
			else
			{
				lastTick = 0d;
				//Check if the player is building a ship
				bool building_ship = HighLogic.LoadedSceneIsEditor
					&& EditorLogic.fetch != null
					&& EditorLogic.fetch.ship != null && EditorLogic.fetch.ship.Count > 0
					&& EditorLogic.fetch.shipNameField != null
					&& EditorLogic.fetch.shipNameField.Text != null && EditorLogic.fetch.shipNameField.Text.Length > 0;

				String[] status_array = null;
				
				if (building_ship)
				{
					status_array = new String[3];
					//Vessel name
					String shipname = EditorLogic.fetch.shipNameField.Text;

					if (shipname.Length > MAX_VESSEL_NAME_LENGTH)
						shipname = shipname.Substring(0, MAX_VESSEL_NAME_LENGTH); //Limit vessel name length

					status_array[1] = "Building " + shipname;

					//Vessel details
					status_array[2] = "Parts: " + EditorLogic.fetch.ship.Count;
				}
				else if (warping)
				{
					status_array = new String[2];
					status_array[1] = "Warping";
				}
				else if (syncing)
				{
					status_array = new String[2];
					status_array[1] = "Synchronizing";
				}
				else
				{
					status_array = new String[2];

					switch (HighLogic.LoadedScene)
					{
						case GameScenes.FLIGHT:
							status_array[1] = "Preparing/launching from KSC";
							break;
						case GameScenes.SPACECENTER:
							status_array[1] = "At Space Center";
							break;
						case GameScenes.EDITOR:
							status_array[1] = "In Vehicle Assembly Building";
							break;
						case GameScenes.SPH:
							status_array[1] = "In Space Plane Hangar";
							break;
						case GameScenes.TRACKSTATION:
							status_array[1] = "At Tracking Station";
							break;
						default:
							status_array[1] = String.Empty;
							break;
					}
				}

				//Check if player is idle
				if (isIdle)
					status_array[1] = "(Idle) " + status_array[1];

				status_array[0] = playerName;

				//Serialize the update
				byte[] update_bytes = KSP.IO.IOUtils.SerializeToBinary(status_array);
				
				enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.PRIMARY_PLUGIN_UPDATE, update_bytes);

				VesselStatusInfo my_status = statusArrayToInfo(status_array);
				if (playerStatus.ContainsKey(playerName))
					playerStatus[playerName] = my_status;
				else
					playerStatus.Add(playerName, my_status);
			}
		}

		private void writeSecondaryUpdates()
		{
			if (inactiveVesselsPerUpdate > 0)
			{
				//Write the inactive vessels nearest the active vessel
				SortedList<float, Vessel> nearest_vessels = new SortedList<float, Vessel>();

				foreach (Vessel vessel in FlightGlobals.Vessels)
				{
					if (vessel != FlightGlobals.ActiveVessel && vessel.loaded && !vessel.name.Contains(" [Past]") && !vessel.name.Contains(" [Future]"))
					{
						float distance = (float)Vector3d.Distance(vessel.GetWorldPos3D(), FlightGlobals.ship_position);
						if (distance < INACTIVE_VESSEL_RANGE)
						{
							try
							{
								Part root = vessel.rootPart;
								bool include = true;
								if (serverVessels_InUse.ContainsKey(vessel.id) ? !serverVessels_InUse[vessel.id]: false)
								{
									foreach (Guid vesselID in serverVessels_Parts.Keys)
									{
										if (serverVessels_Parts[vesselID].Contains(root))
										{
											include=false;
											break;
										}
									}
								}
								if (include) nearest_vessels.Add(distance, vessel);
							}
							catch (ArgumentException)
							{
							}
						}
					}
				}

				int num_written_vessels = 0;

				//Write inactive vessels to file in order of distance from active vessel
				IEnumerator<KeyValuePair<float, Vessel>> enumerator = nearest_vessels.GetEnumerator();
				while (num_written_vessels < inactiveVesselsPerUpdate
					&& num_written_vessels < MAX_INACTIVE_VESSELS_PER_UPDATE && enumerator.MoveNext())
				{
					bool newVessel = !serverVessels_RemoteID.ContainsKey(enumerator.Current.Value.id);
					KMPVesselUpdate update = getVesselUpdate(enumerator.Current.Value);
					if (update != null)
					{
						if (! //Don't keep sending a secondary vessel that will stay destroyed for any other client:
						   ((update.situation == Situation.DESCENDING || update.situation == Situation.FLYING) //If other vessel is flying/descending
						    && enumerator.Current.Value.mainBody.atmosphere //and is near a body with atmo
						    && enumerator.Current.Value.altitude < enumerator.Current.Value.mainBody.maxAtmosphereAltitude //and is in atmo
						    && !newVessel)) //and isn't news to the server, then it shouldn't be sent
						{
							update.distance = enumerator.Current.Key;
							update.state = State.INACTIVE;
							if (enumerator.Current.Value.loaded && (serverVessels_InUse.ContainsKey(enumerator.Current.Value.id) ? serverVessels_InUse[enumerator.Current.Value.id] : false) && FlightGlobals.ActiveVessel.altitude > 10000d)
							{
								//Rendezvous relative position data
								KMPClientMain.DebugLog ("sending docking-mode update, distance: " + enumerator.Current.Key);
								update.relativeTo = FlightGlobals.ActiveVessel.id;
								Vector3d w_pos = Vector3d.zero;
								try
								{
									w_pos = enumerator.Current.Value.findWorldCenterOfMass() - FlightGlobals.ActiveVessel.findWorldCenterOfMass();
								} catch {
									KMPClientMain.DebugLog("couldn't get CoM!");
									w_pos = enumerator.Current.Value.GetWorldPos3D() - FlightGlobals.ship_position;
								}
								Vector3d o_vel = enumerator.Current.Value.GetObtVelocity() - FlightGlobals.ActiveVessel.GetObtVelocity();
								update.clearProtoVessel();
								for (int i = 0; i < 3; i++)
								{
									update.w_pos[i] = w_pos[i];
									update.o_vel[i] = o_vel[i];
								}
							}
							byte[] update_bytes = KSP.IO.IOUtils.SerializeToBinary(update);
							KMPClientMain.DebugLog ("sending secondary update for: " + enumerator.Current.Value.id);
							if (newVessel)
								enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.PRIMARY_PLUGIN_UPDATE, update_bytes);
							else
								enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.SECONDARY_PLUGIN_UPDATE, update_bytes);
							num_written_vessels++;
						}
					}
				}
			}
		}
		
		private void sendRemoveVesselMessage(Vessel vessel, bool isDocking = false)
		{
			KMPClientMain.DebugLog ("sendRemoveVesselMessage");
			KMPVesselUpdate update = getVesselUpdate(vessel);
			update.situation = Situation.DESTROYED;
			update.state = FlightGlobals.ActiveVessel.id == vessel.id ? State.ACTIVE : State.INACTIVE;
			update.isDockUpdate = isDocking;
			update.clearProtoVessel();
			byte[] update_bytes = KSP.IO.IOUtils.SerializeToBinary(update);
			enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.PRIMARY_PLUGIN_UPDATE, update_bytes);	
		}
		
		private void sendVesselMessage(Vessel vessel, bool isDocking = false)
		{
			if (isInFlight)
			{
				KMPClientMain.DebugLog ("sendVesselMessage");
				KMPVesselUpdate update = getVesselUpdate(vessel, true);
				update.state = FlightGlobals.ActiveVessel.id == vessel.id ? State.ACTIVE : State.INACTIVE;
				update.isDockUpdate = isDocking;
				byte[] update_bytes = KSP.IO.IOUtils.SerializeToBinary(update);
				enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.PRIMARY_PLUGIN_UPDATE, update_bytes);	
			}
		}
		
		private KMPVesselUpdate getVesselUpdate(Vessel vessel, bool forceFullUpdate = false)
		{
			if (vessel == null || vessel.mainBody == null)
				return null;
		
			if (vessel.id == Guid.Empty) vessel.id = Guid.NewGuid();
			
			//Create a KMPVesselUpdate from the vessel data
			KMPVesselUpdate update;
            //KMPClientMain.DebugLog("Vid: " + vessel.id);
            //KMPClientMain.DebugLog("foreFullUpdate: " + forceFullUpdate);
            //KMPClientMain.DebugLog("ParCountsContains: " + serverVessels_PartCounts.ContainsKey(vessel.id));
            //KMPClientMain.DebugLog("TimeDelta: " + ((UnityEngine.Time.realtimeSinceStartup - lastFullProtovesselUpdate) < FULL_PROTOVESSEL_UPDATE_TIMEOUT));
            //KMPClientMain.DebugLog("Throttle: " + (FlightGlobals.ActiveVessel.ctrlState.mainThrottle == 0f));

			//Check for new/forced update
			if (!forceFullUpdate //not a forced update
			    && (serverVessels_PartCounts.ContainsKey(vessel.id) ? 
			    	(vessel.id != FlightGlobals.ActiveVessel.id || (UnityEngine.Time.realtimeSinceStartup - lastFullProtovesselUpdate) < FULL_PROTOVESSEL_UPDATE_TIMEOUT) //not active vessel, or full protovessel timeout hasn't passed
			    	: false)) //have a serverVessels_PartCounts entry
			{
				if ((serverVessels_PartCounts.ContainsKey(vessel.id) ? serverVessels_PartCounts[vessel.id] == vessel.Parts.Count : false) //Part count is the same
					&& (sentVessels_Situations.ContainsKey(vessel.id) ? (sentVessels_Situations[vessel.id] == vessel.situation) : false)) //Situation hasn't changed
				{
					if (!newFlags.ContainsKey(vessel.id))	//Not an un-updated flag
						update = new KMPVesselUpdate(vessel,false);
					else if ((UnityEngine.Time.realtimeSinceStartup - newFlags[vessel.id]) < 65f) //Is a flag, but plaque timeout hasn't expired
                        update = new KMPVesselUpdate(vessel,false);
                    else //Is a flag, plaque timeout has expired so grab full update
					{
						update = new KMPVesselUpdate(vessel);
						newFlags.Remove(vessel.id);
					}
					
				}
				else
				{
					//Vessel has changed
					KMPClientMain.DebugLog("Full update: " + vessel.id);
					update = new KMPVesselUpdate(vessel);
					serverVessels_PartCounts[vessel.id] = vessel.Parts.Count;
				}
			}
			else 
			{
				//New vessel or forced protovessel update
				update = new KMPVesselUpdate(vessel);
				if (vessel.id == FlightGlobals.ActiveVessel.id) 
				{
					KMPClientMain.DebugLog("First or forced proto update for active vessel: " + vessel.id);
					lastFullProtovesselUpdate = UnityEngine.Time.realtimeSinceStartup;
				}
                if (!vessel.packed) serverVessels_PartCounts[vessel.id] = vessel.Parts.Count;
			}
			
			//Track vessel situation
			sentVessels_Situations[vessel.id] = vessel.situation;
			
			//Set privacy lock
			if (serverVessels_IsPrivate.ContainsKey(vessel.id)) update.isPrivate = serverVessels_IsPrivate[vessel.id];

			if (vessel.vesselName.Length <= MAX_VESSEL_NAME_LENGTH)
				update.name = vessel.vesselName;
			else
				update.name = vessel.vesselName.Substring(0, MAX_VESSEL_NAME_LENGTH);

			update.player = playerName;
			update.id = vessel.id;
			update.tick = Planetarium.GetUniversalTime();
			update.crewCount = vessel.GetCrewCount();
			
			if (serverVessels_RemoteID.ContainsKey(vessel.id)) update.kmpID = serverVessels_RemoteID[vessel.id];
			else
			{
				KMPClientMain.DebugLog("Generating new remote ID for vessel: " + vessel.id);
				Guid server_id = Guid.NewGuid();
				serverVessels_RemoteID[vessel.id] = server_id;
				update.kmpID = server_id;
				if (vessel.vesselType == VesselType.Flag)
				{
					newFlags[vessel.id] = UnityEngine.Time.realtimeSinceStartup;
				}
			}
			
			Vector3 pos = vessel.mainBody.transform.InverseTransformPoint(vessel.GetWorldPos3D());
			Vector3 dir = vessel.mainBody.transform.InverseTransformDirection(vessel.transform.up);
			Vector3 vel = vessel.mainBody.transform.InverseTransformDirection(vessel.GetObtVelocity());
			Vector3d o_vel = vessel.obt_velocity;
			Vector3d s_vel = vessel.srf_velocity;
			Quaternion rot = vessel.transform.rotation;
			
			for (int i = 0; i < 3; i++)
			{
				update.pos[i] = pos[i];
				update.dir[i] = dir[i];
				update.vel[i] = vel[i];
				update.o_vel[i] = o_vel[i];
				update.s_vel[i] = s_vel[i];
			}
			for (int i = 0; i < 4; i++)
			{
				update.rot[i] = rot[i];
			}
			update.w_pos[0] = vessel.orbit.LAN;
			if (vessel.situation == Vessel.Situations.LANDED || vessel.situation == Vessel.Situations.SPLASHED)
			{
				update.w_pos[1] = vessel.latitude;
				update.w_pos[2] = vessel.longitude;
			}
			
			//Determine situation
			if ((vessel.loaded && vessel.GetTotalMass() <= 0.0) || (vessel.vesselType == VesselType.Debris && vessel.situation == Vessel.Situations.SUB_ORBITAL))
				update.situation = Situation.DESTROYED;
			else
			{
				switch (vessel.situation)
				{

					case Vessel.Situations.LANDED:
						update.situation = Situation.LANDED;
						break;

					case Vessel.Situations.SPLASHED:
						update.situation = Situation.SPLASHED;
						break;

					case Vessel.Situations.PRELAUNCH:
						update.situation = Situation.PRELAUNCH;
						break;

					case Vessel.Situations.SUB_ORBITAL:
						if (vessel.orbit.timeToAp < vessel.orbit.period / 2.0)
							update.situation = Situation.ASCENDING;
						else
							update.situation = Situation.DESCENDING;
						break;

					case Vessel.Situations.ORBITING:
						update.situation = Situation.ORBITING;
						break;

					case Vessel.Situations.ESCAPING:
						if (vessel.orbit.timeToPe > 0.0)
							update.situation = Situation.ENCOUNTERING;
						else
							update.situation = Situation.ESCAPING;
						break;

					case Vessel.Situations.DOCKED:
						update.situation = Situation.DOCKED;
						break;

					case Vessel.Situations.FLYING:
						update.situation = Situation.FLYING;
						break;

					default:
						update.situation = Situation.UNKNOWN;
						break;

				}
			}

			if (vessel == FlightGlobals.ActiveVessel)
			{
				update.state = State.ACTIVE;

				//Set vessel details since it's the active vessel
				update.detail = getVesselDetail(vessel);
			}
			else if (vessel.isCommandable)
				update.state = State.INACTIVE;
			else
				update.state = State.DEAD;

			update.timeScale = (float)Planetarium.TimeScale;
			update.bodyName = vessel.mainBody.bodyName;

			return update;

		}

		private KMPVesselDetail getVesselDetail(Vessel vessel)
		{
			KMPVesselDetail detail = new KMPVesselDetail();

			detail.idle = isIdle;
			detail.mass = vessel.GetTotalMass();

			bool is_eva = false;
			bool parachutes_open = false;

			//Check if the vessel is an EVA Kerbal
			if (vessel.isEVA && vessel.parts.Count > 0 && vessel.parts.First().Modules.Count > 0)
			{
				foreach (PartModule module in vessel.parts.First().Modules)
				{
					if (module is KerbalEVA)
					{
						KerbalEVA kerbal = (KerbalEVA) module;

						detail.percentFuel = (byte)Math.Round(kerbal.Fuel / kerbal.FuelCapacity * 100);
						detail.percentRCS = byte.MaxValue;
						detail.numCrew = byte.MaxValue;

						is_eva = true;
						break;
					}

				}
			}

			if (!is_eva)
			{

				if (vessel.GetCrewCapacity() > 0)
					detail.numCrew = (byte)vessel.GetCrewCount();
				else
					detail.numCrew = byte.MaxValue;

				Dictionary<string, float> fuel_densities = new Dictionary<string, float>();
				Dictionary<string, float> rcs_fuel_densities = new Dictionary<string, float>();

				bool has_engines = false;
				bool has_rcs = false;

				foreach (Part part in vessel.parts)
				{

					foreach (PartModule module in part.Modules)
					{

						if (module is ModuleEngines)
						{
							//Determine what kinds of fuel this vessel can use and their densities
							ModuleEngines engine = (ModuleEngines)module;
							has_engines = true;

							foreach (ModuleEngines.Propellant propellant in engine.propellants)
							{
								if (propellant.name == "ElectricCharge" || propellant.name == "IntakeAir")
								{
									continue;
								}

								if (!fuel_densities.ContainsKey(propellant.name))
									fuel_densities.Add(propellant.name, PartResourceLibrary.Instance.GetDefinition(propellant.id).density);
							}
						}

						if (module is ModuleRCS)
						{
							ModuleRCS rcs = (ModuleRCS)module;
							if (rcs.requiresFuel)
							{
								has_rcs = true;
								if (!rcs_fuel_densities.ContainsKey(rcs.resourceName))
									rcs_fuel_densities.Add(rcs.resourceName, PartResourceLibrary.Instance.GetDefinition(rcs.resourceName).density);
							}
						}

						if (module is ModuleParachute)
						{
							ModuleParachute parachute = (ModuleParachute)module;
							if (parachute.deploymentState == ModuleParachute.deploymentStates.DEPLOYED)
								parachutes_open = true;
						}
					}


				}

				//Determine how much fuel this vessel has and can hold
				float fuel_capacity = 0.0f;
				float fuel_amount = 0.0f;
				float rcs_capacity = 0.0f;
				float rcs_amount = 0.0f;

				foreach (Part part in vessel.parts)
				{
					if (part != null && part.Resources != null)
					{
						foreach (PartResource resource in part.Resources)
						{
							float density = 0.0f;

							//Check that this vessel can use this type of resource as fuel
							if (has_engines && fuel_densities.TryGetValue(resource.resourceName, out density))
							{
								fuel_capacity += ((float)resource.maxAmount) * density;
								fuel_amount += ((float)resource.amount) * density;
							}

							if (has_rcs && rcs_fuel_densities.TryGetValue(resource.resourceName, out density))
							{
								rcs_capacity += ((float)resource.maxAmount) * density;
								rcs_amount += ((float)resource.amount) * density;
							}
						}
					}
				}

				if (has_engines && fuel_capacity > 0.0f)
					detail.percentFuel = (byte)Math.Round(fuel_amount / fuel_capacity * 100);
				else
					detail.percentFuel = byte.MaxValue;

				if (has_rcs && rcs_capacity > 0.0f)
					detail.percentRCS = (byte)Math.Round(rcs_amount / rcs_capacity * 100);
				else
					detail.percentRCS = byte.MaxValue;

			}

			//Determine vessel activity

			if (parachutes_open)
				detail.activity = Activity.PARACHUTING;

			//Check if the vessel is aerobraking
			if (vessel.orbit != null && vessel.orbit.referenceBody != null
				&& vessel.orbit.referenceBody.atmosphere && vessel.orbit.altitude < vessel.orbit.referenceBody.maxAtmosphereAltitude)
			{
				//Vessel inside its body's atmosphere
				switch (vessel.situation)
				{
					case Vessel.Situations.LANDED:
					case Vessel.Situations.SPLASHED:
					case Vessel.Situations.SUB_ORBITAL:
					case Vessel.Situations.PRELAUNCH:
						break;

					default:

						//If the apoapsis of the orbit is above the atmosphere, vessel is aerobraking
						if (vessel.situation == Vessel.Situations.ESCAPING || (float)vessel.orbit.ApA > vessel.orbit.referenceBody.maxAtmosphereAltitude)
							detail.activity = Activity.AEROBRAKING;

						break;
				}

			}

			//Check if the vessel is docking
			if (detail.activity == Activity.NONE && FlightGlobals.fetch.VesselTarget != null && FlightGlobals.fetch.VesselTarget is ModuleDockingNode
				&& Vector3.Distance(vessel.GetWorldPos3D(), FlightGlobals.fetch.VesselTarget.GetTransform().position) < DOCKING_TARGET_RANGE)
				detail.activity = Activity.DOCKING;

			return detail;
		}

		private void writePluginData()
		{
			//CurrentGameTitle
			String current_game_title = String.Empty;
			if (HighLogic.CurrentGame != null)
			{
				current_game_title = HighLogic.CurrentGame.Title;

				//Remove the (Sandbox) portion of the title
				const String remove = " (Sandbox)";
				if (current_game_title.Length > remove.Length)
					current_game_title = current_game_title.Remove(current_game_title.Length - remove.Length);
			}

			byte[] title_bytes = encoder.GetBytes(current_game_title);

			//Watch player name
			String watch_player_name = String.Empty;
			if (shouldDrawGUI && KMPScreenshotDisplay.windowEnabled)
				watch_player_name = KMPScreenshotDisplay.watchPlayerName;

			byte[] watch_bytes = encoder.GetBytes(watch_player_name);

			//Build update byte array
			byte[] update_bytes = new byte[1 + 4 + title_bytes.Length + 4 + watch_bytes.Length];

			int index = 0;

			//Activity
			update_bytes[index] = isInFlight ? (byte)1 : (byte)0;
			index++;

			//Game title
			KMPCommon.intToBytes(title_bytes.Length).CopyTo(update_bytes, index);
			index += 4;

			title_bytes.CopyTo(update_bytes, index);
			index += title_bytes.Length;

			//Watch player name
			KMPCommon.intToBytes(watch_bytes.Length).CopyTo(update_bytes, index);
			index += 4;

			watch_bytes.CopyTo(update_bytes, index);
			index += watch_bytes.Length;

			enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.PLUGIN_DATA, update_bytes);
		}

		private VesselStatusInfo statusArrayToInfo(String[] status_array)
		{
			if (status_array != null && status_array.Length >= STATUS_ARRAY_MIN_SIZE)
			{
				//Read status array
				VesselStatusInfo status = new VesselStatusInfo();
				status.info = null;
				status.ownerName = status_array[0];
				status.vesselName = status_array[1];

				if (status_array.Length >= 3)
					status.detailText = status_array[2];
				
				if (status_array.Length >= 4 && !String.IsNullOrEmpty(status_array[3]))
					status.currentSubspaceID = Int32.Parse(status_array[3]);
				else
					status.currentSubspaceID = -1;
				
				status.orbit = null;
				status.lastUpdateTime = UnityEngine.Time.realtimeSinceStartup;
				status.color = KMPVessel.generateActiveColor(status.ownerName);

				return status;
			}
			else
				return new VesselStatusInfo();
		}
		
		private IEnumerator<WaitForFixedUpdate> returnToSpaceCenter()
		{
			yield return new WaitForFixedUpdate();
			if (FlightGlobals.ClearToSave() == ClearToSaveStatus.CLEAR || !isInFlight || FlightGlobals.ActiveVessel.state == Vessel.State.DEAD)
			{
				if (!forceQuit)
				{
					GamePersistence.SaveGame("persistent",HighLogic.SaveFolder,SaveMode.OVERWRITE);
					HighLogic.LoadScene(GameScenes.SPACECENTER);
				}
				syncing = false;
			} else StartCoroutine(returnToSpaceCenter());
		}
		
		private IEnumerator<WaitForFixedUpdate> returnToTrackingStation()
		{
			yield return new WaitForFixedUpdate();
			if (HighLogic.LoadedSceneIsFlight && isInFlight && FlightGlobals.ready)
				HighLogic.LoadScene(GameScenes.TRACKSTATION);
			else if (HighLogic.LoadedScene != GameScenes.TRACKSTATION)
				StartCoroutine(returnToTrackingStation());
			docking = false;
			syncing = false;
		}
		
		private IEnumerator<WaitForFixedUpdate> setDockingTarget(Vessel vessel)
		{
			yield return new WaitForFixedUpdate();
			FlightGlobals.fetch.SetVesselTarget(vessel);
		}
		
		private IEnumerator<WaitForEndOfFrame> sendSubspaceSyncRequest(int subspace = -1, bool docking = false)
		{
			yield return new WaitForEndOfFrame();
			KMPClientMain.DebugLog("sending subspace sync request to subspace " + subspace);
			if (!docking) writePluginUpdate();
			byte[] update_bytes = KMPCommon.intToBytes(subspace);
			enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.SSYNC, update_bytes);
			showServerSync = false;
		}
		
		private IEnumerator<WaitForEndOfFrame> shareScreenshot()
		{
			yield return new WaitForEndOfFrame();
			//Determine the scaled-down dimensions of the screenshot
			int w = 0;
			int h = 0;

			KMPScreenshotDisplay.screenshotSettings.getBoundedDimensions(Screen.width, Screen.height, ref w, ref h);

			//Read the screen pixels into a texture
			Texture2D full_screen_tex = new Texture2D(Screen.width, Screen.height, TextureFormat.RGB24, false);
			full_screen_tex.filterMode = FilterMode.Bilinear;
			full_screen_tex.ReadPixels(new Rect(0, 0, Screen.width, Screen.height), 0, 0, false);
			full_screen_tex.Apply();

			RenderTexture render_tex = new RenderTexture(w, h, 24);
			render_tex.useMipMap = false;

			if (KMPGlobalSettings.instance.smoothScreens && (Screen.width > w * 2 || Screen.height > h * 2))
			{
				//Blit the full texture to a double-sized texture to improve final quality
				RenderTexture resize_tex = new RenderTexture(w * 2, h * 2, 24);
				Graphics.Blit(full_screen_tex, resize_tex);

				//Blit the double-sized texture to normal-sized texture
				Graphics.Blit(resize_tex, render_tex);
			}
			else
				Graphics.Blit(full_screen_tex, render_tex); //Blit the screen texture to a render texture
			
			//Destroy(full_screen_tex);
			full_screen_tex = null;

			RenderTexture.active = render_tex;
			
			//Read the pixels from the render texture into a Texture2D
			Texture2D resized_tex = new Texture2D(w, h, TextureFormat.RGB24, false);
			resized_tex.ReadPixels(new Rect(0, 0, w, h), 0, 0);
			resized_tex.Apply();
			
			RenderTexture.active = null;

			byte[] data = resized_tex.EncodeToPNG();

			//Build the description
			StringBuilder sb = new StringBuilder();
			sb.Append(playerName);
			if (isInFlight)
			{
				sb.Append(" - ");
				sb.Append(FlightGlobals.ActiveVessel.vesselName);
			}

			byte[] description = encoder.GetBytes(sb.ToString());

			//Build the message data
			byte[] bytes = new byte[4 + description.Length + data.Length];
			KMPCommon.intToBytes(description.Length).CopyTo(bytes, 0);
			description.CopyTo(bytes, 4);
			data.CopyTo(bytes, 4 + description.Length);
			
			enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.SCREENSHOT_SHARE, bytes);
		}

		private void handleUpdate(object obj)
		{

			if (obj is KMPVesselUpdate)
			{
				handleVesselUpdate((KMPVesselUpdate)obj);
			}
			else if (obj is String[])
			{
				String[] status_array = (String[])obj;
				VesselStatusInfo status = statusArrayToInfo(status_array);

				if (status.ownerName != null && status.ownerName.Length > 0)
				{
					if (playerStatus.ContainsKey(status.ownerName))
						playerStatus[status.ownerName] = status;
					else
						playerStatus.Add(status.ownerName, status);
				}
			}
		}

		private void handleVesselUpdate(KMPVesselUpdate vessel_update)
		{
			KMPClientMain.DebugLog("handleVesselUpdate");
			
			String vessel_key = vessel_update.id.ToString();

			KMPVessel vessel = null;

			//Try to find the key in the vessel dictionary
			VesselEntry entry;
			if (vessels.TryGetValue(vessel_key, out entry))
			{
				vessel = entry.vessel;

				if (vessel == null || vessel.gameObj == null || (vessel.vesselRef != null && vessel.vesselRef.id != vessel_update.id))
				{
					//Delete the vessel if it's null or needs to be renamed
					vessels.Remove(vessel_key);

					if (vessel != null && vessel.gameObj != null)
						GameObject.Destroy(vessel.gameObj);

					vessel = null;
				}
				else
				{
					//Update the entry's timestamp
					VesselEntry new_entry = new VesselEntry();
					new_entry.vessel = entry.vessel;
					new_entry.lastUpdateTime = UnityEngine.Time.realtimeSinceStartup;

					vessels[vessel_key] = new_entry;
				}
			}
				
			if (vessel == null) {
				//Add the vessel to the dictionary
				vessel = new KMPVessel(vessel_update.name, vessel_update.player, vessel_update.id);
				entry = new VesselEntry();
				entry.vessel = vessel;
				entry.lastUpdateTime = UnityEngine.Time.realtimeSinceStartup;

				if (vessels.ContainsKey(vessel_key))
					vessels[vessel_key] = entry;
				else
					vessels.Add(vessel_key, entry);
				
				/*Queue this update for the next update call because updating a vessel on the same step as
				 * creating it usually causes problems for some reason */
				newVesselUpdateQueue.Enqueue(vessel_update);
				KMPClientMain.DebugLog("vessel update queued");
			}
			else
			{
				applyVesselUpdate(vessel_update, vessel); //Apply the vessel update to the existing vessel
				KMPClientMain.DebugLog("vessel update applied");
			}
			
			KMPClientMain.DebugLog("handleVesselUpdate done");
		}

		private void applyVesselUpdate(KMPVesselUpdate vessel_update, KMPVessel vessel)
		{
			serverVessels_RemoteID[vessel_update.id] = vessel_update.kmpID;
			
			//Find the CelestialBody that matches the one in the update
			CelestialBody update_body = null;
			if (vessel.mainBody != null && vessel.mainBody.bodyName == vessel_update.bodyName)
				update_body = vessel.mainBody; //Vessel already has the correct body
			else
			{
				//Find the celestial body in the list of bodies
				foreach (CelestialBody body in FlightGlobals.Bodies)
				{
					if (body.bodyName == vessel_update.bodyName)
					{
						update_body = body;
						break;
					}
				}
			}
			
			Vector3 oldPosition = vessel.worldPosition;
			
			if (update_body != null)
			{
				//Convert float arrays to Vector3s
				Vector3 pos = new Vector3(vessel_update.pos[0], vessel_update.pos[1], vessel_update.pos[2]);
				Vector3 dir = new Vector3(vessel_update.dir[0], vessel_update.dir[1], vessel_update.dir[2]);
				Vector3 vel = new Vector3(vessel_update.vel[0], vessel_update.vel[1], vessel_update.vel[2]);
				vessel.info = vessel_update;
				vessel.setOrbitalData(update_body, pos, vel, dir);
			}
			
			KMPClientMain.DebugLog("vessel state: " + vessel_update.state.ToString() + ", tick=" + vessel_update.tick + ", realTick=" + Planetarium.GetUniversalTime());
			if (vessel_update.state == State.ACTIVE && !vessel_update.isSyncOnlyUpdate && vessel_update.relTime != RelativeTime.FUTURE && !vessel_update.isDockUpdate)
			{
				//Update the player status info
				VesselStatusInfo status = new VesselStatusInfo();
				if (vessel_update.relTime == RelativeTime.PRESENT) status.info = vessel_update;
				status.ownerName = vessel_update.player;
				status.vesselName = vessel_update.name;

				if (vessel.orbitValid)
					status.orbit = vessel.orbitRenderer.driver.orbit;

				status.lastUpdateTime = UnityEngine.Time.realtimeSinceStartup;
				status.color = KMPVessel.generateActiveColor(status.ownerName);

				if (playerStatus.ContainsKey(status.ownerName))
					playerStatus[status.ownerName] = status;
				else
					playerStatus.Add(status.ownerName, status);
			}
			if (!vessel_update.id.Equals(Guid.Empty) && !docking)
			{
				//Update vessel privacy locks
				if (!isInFlight || vessel_update.id != FlightGlobals.ActiveVessel.id)
				{
					serverVessels_InUse[vessel_update.id] = vessel_update.state == State.ACTIVE;
					serverVessels_IsPrivate[vessel_update.id] = vessel_update.isPrivate;
					serverVessels_IsMine[vessel_update.id] = vessel_update.isMine;
					KMPClientMain.DebugLog("status flags updated: " + (vessel_update.state == State.ACTIVE) + " " + vessel_update.isPrivate + " " + vessel_update.isMine);
					if (vessel_update.situation == Situation.DESTROYED)
					{
						KMPClientMain.DebugLog("killing vessel");
						Vessel extant_vessel = FlightGlobals.Vessels.Find(v => v.id == vessel_update.id);
						if (extant_vessel != null && !extant_vessel.isEVA) try { extant_vessel.Die(); } catch {}
						return;
					}
				}
				
				//Store protovessel if included
				if (vessel_update.getProtoVesselNode() != null && (!isInFlight || vessel_update.id != FlightGlobals.ActiveVessel.id)) serverVessels_ProtoVessels[vessel_update.id] = vessel_update.getProtoVesselNode();
			}
			if (isInFlightOrTracking)
			{
				if (vessel_update.id != FlightGlobals.ActiveVessel.id)
				{
					KMPClientMain.DebugLog("retrieving vessel: " + vessel_update.id.ToString());
					if (!vessel_update.id.Equals(Guid.Empty))
					{
						Vessel extant_vessel = vessel.vesselRef;
						if (extant_vessel == null) extant_vessel = FlightGlobals.Vessels.Find(v => v.id == vessel_update.id);
						if (isInFlight)
						{
							if (extant_vessel != null && vessel_update.state == State.ACTIVE && !vessel_update.isSyncOnlyUpdate) 
							{
								 extant_vessel.name = vessel_update.name + " <" + vessel_update.player + ">";
								 extant_vessel.vesselName = vessel_update.name + " <" + vessel_update.player + ">";
							}
							else if (extant_vessel != null)
							{
								extant_vessel.name = vessel_update.name;
								extant_vessel.vesselName = vessel_update.name;
							}
						}
						if (!serverVessels_LoadDelay.ContainsKey(vessel_update.id)
						    || (serverVessels_LoadDelay.ContainsKey(vessel_update.id) ? (serverVessels_LoadDelay[vessel_update.id] < UnityEngine.Time.realtimeSinceStartup) : false))
						{
							float incomingDistance = 2500f;
							if (!syncing && vessel.worldPosition != Vector3.zero && vessel_update.relTime == RelativeTime.PRESENT)
								incomingDistance = Vector3.Distance(vessel.worldPosition,FlightGlobals.ship_position);
							if (vessel_update.relTime != RelativeTime.PRESENT) incomingDistance = 3000f; //Never treat vessels from another time as close by
						 	if (vessel_update.state == State.ACTIVE
							    	|| vessel_update.isDockUpdate
							    	|| (incomingDistance > vessel_update.distance
							    		&& (serverVessels_LastUpdateDistanceTime.ContainsKey(vessel_update.id) ? (serverVessels_LastUpdateDistanceTime[vessel_update.id].Key > vessel_update.distance || serverVessels_LastUpdateDistanceTime[vessel_update.id].Value < Planetarium.GetUniversalTime()): true)))
							{
								serverVessels_LastUpdateDistanceTime[vessel_update.id] = new KeyValuePair<double, double>(vessel_update.distance,Planetarium.GetUniversalTime() + 0.75f);
								if (extant_vessel != null)
								{
									KMPClientMain.DebugLog("vessel found: " + extant_vessel.id);
									if (extant_vessel.vesselType != VesselType.Flag) //Special treatment for flags
									{
										vessel.vesselRef = extant_vessel;
										float ourDistance = 3000f;
										if (!extant_vessel.loaded)
										{
											if (KMPVessel.situationIsOrbital(vessel_update.situation))
												ourDistance = Vector3.Distance(extant_vessel.orbit.getPositionAtUT(Planetarium.GetUniversalTime()), FlightGlobals.ship_position);
											else ourDistance = Vector3.Distance(oldPosition, FlightGlobals.ship_position);
										}
										else ourDistance = Vector3.Distance(extant_vessel.GetWorldPos3D(), FlightGlobals.ship_position);
										bool countMismatch = false;
										ProtoVessel protovessel = null;
										if (serverVessels_ProtoVessels.ContainsKey(vessel_update.id))
										{
											protovessel = new ProtoVessel(serverVessels_ProtoVessels[vessel_update.id], HighLogic.CurrentGame);
										}
										if (serverVessels_PartCounts.ContainsKey(vessel_update.id))
										{
											//countMismatch = serverVessels_PartCounts[vessel_update.id] > 0 && extant_vessel.loaded && !extant_vessel.packed && serverVessels_PartCounts[vessel_update.id] != protovessel.protoPartSnapshots.Count;
											countMismatch = serverVessels_PartCounts[vessel_update.id] > 0 && serverVessels_PartCounts[vessel_update.id] != protovessel.protoPartSnapshots.Count;
										}
										if ((vessel_update.getProtoVesselNode() != null && (!KMPVessel.situationIsOrbital(vessel_update.situation) || ourDistance > 2500f || extant_vessel.altitude < 10000d)) || countMismatch)
										{
											KMPClientMain.DebugLog("updating from protovessel");
											serverVessels_PartCounts[vessel_update.id] = 0;
											if (protovessel != null)
											{
												if (vessel.orbitValid && KMPVessel.situationIsOrbital(vessel_update.situation) && protovessel.altitude > 10000f && protovessel.vesselType != VesselType.Flag && protovessel.vesselType != VesselType.EVA)
												{
													protovessel = syncOrbit(vessel, vessel_update.tick, protovessel, vessel_update.w_pos[0]);
					                            }
												addRemoteVessel(protovessel, vessel_update.id, vessel_update, incomingDistance);
												if (vessel_update.situation == Situation.FLYING) serverVessels_LoadDelay[vessel.id] = UnityEngine.Time.realtimeSinceStartup + 5f;
											} else { KMPClientMain.DebugLog("Protovessel missing!"); }
										}
										else
										{
											KMPClientMain.DebugLog("no protovessel");
											if (vessel.orbitValid && !extant_vessel.isActiveVessel)
											{
												KMPClientMain.DebugLog("updating from flight data, distance: " + ourDistance);
												//Update orbit to our game's time if necessary
												//bool throttled = serverVessels_ObtSyncDelay.ContainsKey(vessel_update.id) && serverVessels_ObtSyncDelay[vessel_update.id] > UnityEngine.Time.realtimeSinceStartup;
												bool throttled = false;
												if (KMPVessel.situationIsOrbital(vessel_update.situation) && extant_vessel.altitude > 10000f)
												{
													double tick = Planetarium.GetUniversalTime();
													//Update orbit whenever out of sync or other vessel in past/future, or not in docking range
													if (!throttled && (vessel_update.relTime == RelativeTime.PRESENT && ourDistance > (INACTIVE_VESSEL_RANGE+500f)) || (vessel_update.relTime != RelativeTime.PRESENT && Math.Abs(tick-vessel_update.tick) > 1.5d))
													{
														syncExtantVesselOrbit(vessel,vessel_update.tick,extant_vessel,vessel_update.w_pos[0]);
														serverVessels_ObtSyncDelay[vessel_update.id] = UnityEngine.Time.realtimeSinceStartup + 1f;
													}
												}
												
												if (FlightGlobals.ActiveVessel.mainBody == update_body && vessel_update.relTime == RelativeTime.PRESENT)
												{
													KMPClientMain.DebugLog("full update");
													if (serverVessels_InPresent.ContainsKey(vessel_update.id) ? !serverVessels_InPresent[vessel_update.id] : true)
													{
														serverVessels_InPresent[vessel_update.id] = true;
														foreach (Part part in extant_vessel.Parts)
														{
															part.setOpacity(1f);
														}
													}
													
													//Update rotation
													if (extant_vessel.loaded)
													{
														extant_vessel.transform.up = vessel.worldDirection;
														Quaternion rot = new Quaternion(vessel_update.rot[0],vessel_update.rot[1],vessel_update.rot[2],vessel_update.rot[3]);
														extant_vessel.SetRotation(rot);
														extant_vessel.angularMomentum = Vector3.zero;
														extant_vessel.VesselSAS.LockHeading(rot);
														extant_vessel.VesselSAS.currentRotation = rot;
														extant_vessel.VesselSAS.SetDampingMode(false);
													}
													
													if (!KMPVessel.situationIsOrbital(vessel_update.situation) || extant_vessel.altitude < 10000f || ourDistance > 2500f)
													{
														KMPClientMain.DebugLog ("velocity update");
														//Update velocity
														if (extant_vessel.loaded)
														{
															if (update_body.GetAltitude(vessel.worldPosition)<10000d)
															{
																//Set velocity by surface velocity
																Vector3d new_srf_vel = new Vector3d(vessel_update.s_vel[0],vessel_update.s_vel[1],vessel_update.s_vel[2]);
																if (new_srf_vel.sqrMagnitude>1d) extant_vessel.ChangeWorldVelocity((-1 * extant_vessel.srf_velocity) + new_srf_vel);
																else extant_vessel.ChangeWorldVelocity(-0.99f * extant_vessel.srf_velocity);
															}
															else
															{
																//Set velocity by orbit velocity
																Vector3d new_obt_vel = new Vector3d(vessel_update.o_vel[0],vessel_update.o_vel[1],vessel_update.o_vel[2]);
																if (new_obt_vel.sqrMagnitude>1d) extant_vessel.ChangeWorldVelocity((-1 * extant_vessel.obt_velocity) + new_obt_vel);
																else extant_vessel.ChangeWorldVelocity(-0.99f * extant_vessel.obt_velocity);
															}
														}
														
														//Update position
														if (extant_vessel.altitude < 10000f || !extant_vessel.loaded)
														{
															if (extant_vessel.loaded && (vessel_update.situation == Situation.LANDED || vessel_update.situation == Situation.SPLASHED))
															{
																//Update surface position
																KMPClientMain.DebugLog("surface position update");
																Vector3d newPos = update_body.GetWorldSurfacePosition(vessel_update.w_pos[1],vessel_update.w_pos[2],extant_vessel.altitude+0.001d);
																if ((newPos - extant_vessel.GetWorldPos3D()).sqrMagnitude > 1d) 
																	extant_vessel.SetPosition(newPos);
																else if (Vector3.Distance(vessel.worldPosition, extant_vessel.GetWorldPos3D()) > 25f)
																{
																	serverVessels_PartCounts[vessel_update.id] = 0;
																	addRemoteVessel(protovessel,vessel_update.id,vessel_update);
																}
															}
															else if (!throttled && Vector3.Distance(vessel.worldPosition, extant_vessel.GetWorldPos3D()) > 1d
															         && (extant_vessel.altitude < 10000f || ourDistance > 3000f)
															         && update_body.GetAltitude(vessel.worldPosition) > 1d)
															{
																//Update 3D position
																KMPClientMain.DebugLog("position update");
																extant_vessel.SetPosition(vessel.worldPosition);
															}
															else if (!extant_vessel.loaded && ourDistance > 1000 && update_body.GetAltitude(vessel.worldPosition) > 1d)
															{
																//Stretch packing thresholds to prevent excessive load/unloads during rendezvous initiation
																extant_vessel.distancePackThreshold += 250f;
																extant_vessel.distanceUnpackThreshold += 100f;
															} else
															{
																//Reset packing thresholds
																extant_vessel.distancePackThreshold = 7500f;
																extant_vessel.distanceUnpackThreshold = 1000f;
															}
														}
														
														//Update FlightCtrlState
														extant_vessel.ctrlState.CopyFrom(vessel_update.flightCtrlState.getAsFlightCtrlState(0.75f));
													}
													else 
													{
														//Orbital rendezvous
														KMPClientMain.DebugLog("orbital rendezvous");
														
														//Update FlightCtrlState
														extant_vessel.ctrlState.CopyFrom(vessel_update.flightCtrlState.getAsFlightCtrlState(0.6f));
													}
												}
												else if (FlightGlobals.ActiveVessel.mainBody == vessel.mainBody)
												{
													KMPClientMain.DebugLog("update from past/future");
													
													if (!serverVessels_InPresent.ContainsKey(vessel_update.id) || serverVessels_InPresent.ContainsKey(vessel_update.id) ? serverVessels_InPresent[vessel_update.id]: false)
													{
														serverVessels_InPresent[vessel_update.id] = false;
														foreach (Part part in extant_vessel.Parts)
														{
															part.setOpacity(0.3f);
														}
													}
													
													//Update rotation only
													extant_vessel.transform.up = vessel.worldDirection;
													extant_vessel.SetRotation(new Quaternion(vessel_update.rot[0],vessel_update.rot[1],vessel_update.rot[2],vessel_update.rot[3]));
												}
											}
										}
										KMPClientMain.DebugLog("updated");
									}
									else
									{
										//Update flag if needed
										if (vessel_update.getProtoVesselNode() != null)
										{
											ProtoVessel protovessel = new ProtoVessel(serverVessels_ProtoVessels[vessel_update.id], HighLogic.CurrentGame);
											addRemoteVessel(protovessel,vessel_update.id,vessel_update);
										}
									}
								}
								else
								{
									try
									{
										if (serverVessels_ProtoVessels.ContainsKey(vessel_update.id))
										{
											KMPClientMain.DebugLog("Adding new vessel: " + vessel_update.id);
											ProtoVessel protovessel = new ProtoVessel(serverVessels_ProtoVessels[vessel_update.id], HighLogic.CurrentGame);
											if (vessel.orbitValid && KMPVessel.situationIsOrbital(vessel_update.situation) && protovessel.vesselType != VesselType.Flag && protovessel.vesselType != VesselType.EVA)
											{
												protovessel = syncOrbit(vessel, vessel_update.tick, protovessel, vessel_update.w_pos[0]);
				                            }
											serverVessels_PartCounts[vessel_update.id] = 0;
											addRemoteVessel(protovessel, vessel_update.id, vessel_update, incomingDistance);
											HighLogic.CurrentGame.CrewRoster.ValidateAssignments(HighLogic.CurrentGame);
										}
										else 
										{
											KMPClientMain.DebugLog("New vessel, but no matching protovessel available");
										}
									} catch (Exception e) { KMPClientMain.DebugLog("Vessel add error: " + e.Message + "\n" + e.StackTrace); }
								}
							}
							else
							{
								KMPClientMain.DebugLog("Vessel update ignored: we are closer to target vessel or have recently updated from someone who was closer");
							}
						}
						else
						{
							KMPClientMain.DebugLog("Vessel update ignored: target vessel on load delay list");
						}
					}
				}
				else
				{
					//This is our vessel!
					if (vessel_update.getProtoVesselNode() != null && docking)
					{
						KMPClientMain.DebugLog("Received updated protovessel for active vessel");
						serverVessels_ProtoVessels[vessel_update.id] = vessel_update.getProtoVesselNode();
					}
					
					if (vessel_update.isDockUpdate && vessel_update.relTime == RelativeTime.PRESENT)
					{
						//Someone docked with us and has control
						docking = true;
						syncing = true;
						ScreenMessages.PostScreenMessage("Other player has control of newly docked vessel",2.5f,ScreenMessageStyle.UPPER_LEFT);
						KMPClientMain.DebugLog("Received docking update");
						serverVessels_PartCounts[FlightGlobals.ActiveVessel.id] = 0;
						
						//Return to tracking station
						Invoke("dockedKickToTrackingStation",0.25f);
						return;
					}
					//Try to negotiate our relative position with whatever sent this update
					if (FlightGlobals.ActiveVessel.altitude > 10000d && vessel_update.relativeTo != Guid.Empty && Math.Abs(Planetarium.GetUniversalTime() - vessel_update.tick) < 2d)
					{
						Vessel updateFrom = FlightGlobals.Vessels.Find (v => v.id == vessel_update.relativeTo);
						if (updateFrom != null && !updateFrom.loaded)
						{
							KMPClientMain.DebugLog("Rendezvous update from unloaded vessel");
							if (vessel_update.distance < INACTIVE_VESSEL_RANGE)
							{
								//We're not in normal secondary vessel range but other vessel is, send negotiating reply
								KMPVesselUpdate update = getVesselUpdate(updateFrom);
								update.distance = INACTIVE_VESSEL_RANGE;
								update.state = State.INACTIVE;
								//Rendezvous relative position data
								update.relativeTo = FlightGlobals.ActiveVessel.id;
								Vector3d w_pos = updateFrom.findWorldCenterOfMass() - activeVesselPosition;
								Vector3d o_vel = updateFrom.GetObtVelocity() - FlightGlobals.ActiveVessel.GetObtVelocity();
								for (int i = 0; i < 3; i++)
								{
									update.w_pos[i] = w_pos[i];
									update.o_vel[i] = o_vel[i];
								}
								
								byte[] update_bytes = KSP.IO.IOUtils.SerializeToBinary(update);
								enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.SECONDARY_PLUGIN_UPDATE, update_bytes);
								
								//updateFrom.distancePackThreshold += INACTIVE_VESSEL_RANGE/2;
							}
						}
						else if (updateFrom != null && updateFrom.loaded)
						{
							KMPClientMain.DebugLog("rendezvous positioning: " + updateFrom.id);
							
							Vector3d updateFromPos = updateFrom.findWorldCenterOfMass();
							Vector3d relPos = activeVesselPosition-updateFromPos;
							Vector3d updateRelPos = new Vector3d(vessel_update.w_pos[0],vessel_update.w_pos[1],vessel_update.w_pos[2]);
							
							if (!dockingRelVel.ContainsKey(updateFrom.id))
								dockingRelVel[updateFrom.id] = updateFrom.GetObtVelocity();

							Vector3d relVel = FlightGlobals.ActiveVessel.GetObtVelocity()-dockingRelVel[updateFrom.id];
							Vector3d updateRelVel = new Vector3d(vessel_update.o_vel[0],vessel_update.o_vel[1],vessel_update.o_vel[2]);
							Vector3d diffPos = updateRelPos - relPos;
							Vector3d diffVel = updateRelVel - relVel;
							diffPos *= 0.45d;
							diffVel *= 0.45d;
							Vector3d newPos = updateFromPos-diffPos;
							
							bool applyUpdate = true;
							double curTick = Planetarium.GetUniversalTime();
							if (serverVessels_RendezvousSmoothPos.ContainsKey(updateFrom.id) ? (relPos.sqrMagnitude > (serverVessels_RendezvousSmoothPos[updateFrom.id].Key * 25) && serverVessels_RendezvousSmoothPos[updateFrom.id].Value > (curTick-5d)): false)
								applyUpdate = false;
							if (serverVessels_RendezvousSmoothVel.ContainsKey(updateFrom.id) ? (relVel.sqrMagnitude > (serverVessels_RendezvousSmoothVel[updateFrom.id].Key * 25) && serverVessels_RendezvousSmoothVel[updateFrom.id].Value > (curTick-5d)): false)
								applyUpdate = false;
							
							double expectedDist = Vector3d.Distance(newPos, activeVesselPosition);
							
							if (applyUpdate)
							{
								serverVessels_RendezvousSmoothPos[updateFrom.id] = new KeyValuePair<double, double>(diffPos.sqrMagnitude,curTick);
								serverVessels_RendezvousSmoothVel[updateFrom.id] = new KeyValuePair<double, double>(diffVel.sqrMagnitude,curTick);
								
								try
					            {
					                OrbitPhysicsManager.HoldVesselUnpack(1);
					            }
					            catch (NullReferenceException)
					            {
					            }
	
								if (diffPos.sqrMagnitude < 1000000d && diffPos.sqrMagnitude > 0.5d)
								{
									KMPClientMain.DebugLog("Docking Krakensbane shift");
									foreach (Vessel otherVessel in FlightGlobals.Vessels.Where(v => v.packed == false && v.id != FlightGlobals.ActiveVessel.id && v.id == updateFrom.id))
			                			otherVessel.GoOnRails();
									getKrakensbane().setOffset(diffPos);
								}
								else if (diffPos.sqrMagnitude >= 1000000d)
								{
									KMPClientMain.DebugLog("Clamped docking Krakensbane shift");
									diffPos.Normalize();
									diffPos *= 1000d;
									foreach (Vessel otherVessel in FlightGlobals.Vessels.Where(v => v.packed == false && v.id != FlightGlobals.ActiveVessel.id))
			                			otherVessel.GoOnRails();
									getKrakensbane().setOffset(diffPos);
								}
								
								activeVesselPosition += diffPos;
								
								if (diffVel.sqrMagnitude > 0.0025d && diffVel.sqrMagnitude < 2500d)
								{
									KMPClientMain.DebugLog("Docking velocity update");
									if (updateFrom.packed) updateFrom.GoOffRails();
									updateFrom.ChangeWorldVelocity(-diffVel);
								}
								else if (diffVel.sqrMagnitude >= 2500d)
								{
									KMPClientMain.DebugLog("Damping large velocity differential");
									diffVel = diffVel.normalized;
									diffVel *= 50d;
									if (updateFrom.packed) updateFrom.GoOffRails();
									updateFrom.ChangeWorldVelocity(-diffVel);
								}
								
								dockingRelVel[updateFrom.id] -= diffVel;
								
								KMPClientMain.DebugLog("had dist:" + relPos.magnitude + " got dist:" + updateRelPos.magnitude);
								KMPClientMain.DebugLog("expected dist:" + expectedDist + " diffPos mag: " + diffPos.sqrMagnitude);
								KMPClientMain.DebugLog("had relVel:" + relVel.magnitude + " got relVel:" + updateRelVel.magnitude + " diffVel mag:" + diffVel.sqrMagnitude);
							}
						}  else KMPClientMain.DebugLog("Ignored docking position update: unexpected large pos/vel shift");
					} else KMPClientMain.DebugLog("Ignored docking position update: " + (FlightGlobals.ActiveVessel.altitude > 10000d) + " " + (vessel_update.relativeTo != Guid.Empty) + " " + (Math.Abs(Planetarium.GetUniversalTime() - vessel_update.tick) < 1d));
				}
			}
		}
		
		private ProtoVessel syncOrbit(KMPVessel kvessel, double fromTick, ProtoVessel protovessel, double LAN)
		{
			KMPClientMain.DebugLog("updating OrbitSnapshot");
			double tick = Planetarium.GetUniversalTime();
			
            //Update orbit
			Planetarium.SetUniversalTime(fromTick);
			
			Vector3 orbit_pos = kvessel.translationFromBody;
            Vector3 orbit_vel = kvessel.worldVelocity;
			
            //Swap the y and z values of the orbital position/velocities
            float temp = orbit_pos.y;
            orbit_pos.y = orbit_pos.z;
            orbit_pos.z = temp;
			
            temp = orbit_vel.y;
            orbit_vel.y = orbit_vel.z;
            orbit_vel.z = temp;
			
			OrbitDriver orbitDriver = new OrbitDriver();
			orbitDriver.orbit.UpdateFromStateVectors(orbit_pos, orbit_vel, kvessel.mainBody, fromTick);
			Orbit newOrbit = orbitDriver.orbit;
			newOrbit.LAN = LAN;
			
			Vessel victim = FlightGlobals.ActiveVessel;
			OrbitDriver oldDriver = victim.orbitDriver;
			victim.orbitDriver = orbitDriver;
			victim.patchedConicSolver.obtDriver = orbitDriver;
			victim.orbitDriver.UpdateOrbit();
			victim.patchedConicSolver.Update();
			
			newOrbit = victim.patchedConicSolver.orbit;
			if (newOrbit.referenceBody == null) newOrbit.referenceBody = FlightGlobals.Bodies.Find(b => b.name == "Sun");
			if (newOrbit.EndUT > 0)
			{
				double lastEndUT =  newOrbit.EndUT;
				while (newOrbit.EndUT > 0 && newOrbit.EndUT < tick && newOrbit.EndUT > lastEndUT && newOrbit.nextPatch != null)
				{
					KMPClientMain.DebugLog("orbit EndUT < target: " + newOrbit.EndUT + " vs " + tick);
					lastEndUT =  newOrbit.EndUT;
					newOrbit = newOrbit.nextPatch;
					if (newOrbit.referenceBody == null) newOrbit.referenceBody = FlightGlobals.Bodies.Find(b => b.name == "Sun");
					KMPClientMain.DebugLog("updated to next patch");
				}
			}
			
			victim.patchedConicSolver.obtDriver = oldDriver;
			victim.orbitDriver = oldDriver;
			
			Planetarium.SetUniversalTime(tick);
			protovessel.orbitSnapShot = new OrbitSnapshot(newOrbit);
			KMPClientMain.DebugLog("OrbitSnapshot updated");
			return protovessel;	
		}
		
		private void syncExtantVesselOrbit(KMPVessel kvessel, double fromTick, Vessel extant_vessel, double LAN)
		{
			KMPClientMain.DebugLog("updating Orbit: " + extant_vessel.id);
			
			bool victimAvailable = true;
			Vessel victim = FlightGlobals.ActiveVessel;
			
			foreach (ManeuverNode mNode in victim.patchedConicSolver.maneuverNodes)
			{
				if (mNode.attachedGizmo != null)
				{
					ManeuverGizmo mGizmo = mNode.attachedGizmo;
					if (mGizmo.handleAntiNormal.Drag) { victimAvailable = false; break; }
					if (mGizmo.handleNormal.Drag) { victimAvailable = false; break; }
					if (mGizmo.handlePrograde.Drag) { victimAvailable = false; break; }
					if (mGizmo.handleRadialIn.Drag) { victimAvailable = false; break; }
					if (mGizmo.handleRadialOut.Drag) { victimAvailable = false; break; }
					if (mGizmo.handleRetrograde.Drag) { victimAvailable = false; break; }
				}
			}
			
			if (victimAvailable)
			{
				double tick = Planetarium.GetUniversalTime();
				KMPClientMain.DebugLog("current vel mag: " + extant_vessel.orbit.getOrbitalVelocityAtUT(tick).magnitude);
				
				extant_vessel.GoOnRails();
				
	            //Update orbit
				Planetarium.SetUniversalTime(fromTick);
				Vector3 orbit_pos = kvessel.translationFromBody;
	            Vector3 orbit_vel = kvessel.worldVelocity;
				
	            //Swap the y and z values of the orbital position/velocities
	            float temp = orbit_pos.y;
	            orbit_pos.y = orbit_pos.z;
	            orbit_pos.z = temp;
				
	            temp = orbit_vel.y;
	            orbit_vel.y = orbit_vel.z;
	            orbit_vel.z = temp;
				
				OrbitDriver orbitDriver = extant_vessel.orbitDriver;
				orbitDriver.orbit.UpdateFromStateVectors(orbit_pos, orbit_vel, kvessel.mainBody, fromTick);
				Orbit newOrbit = orbitDriver.orbit;
				newOrbit.LAN = LAN;
				
				OrbitDriver oldDriver = victim.orbitDriver;
				victim.patchedConicSolver.obtDriver = orbitDriver;
				victim.orbitDriver = orbitDriver;
				victim.orbitDriver.UpdateOrbit();
				victim.patchedConicSolver.Update();
				
				newOrbit = victim.patchedConicSolver.orbit;
				
				if (newOrbit.referenceBody == null) newOrbit.referenceBody = FlightGlobals.Bodies.Find(b => b.name == "Sun");
//				KMPClientMain.DebugLog("aP:" + newOrbit.activePatch);
//				KMPClientMain.DebugLog("eUT:" + newOrbit.EndUT);
//				KMPClientMain.DebugLog("sUT:" + newOrbit.StartUT);
//				KMPClientMain.DebugLog("gOA:" + newOrbit.getObtAtUT(tick));
//				KMPClientMain.DebugLog("nPnull:" + (newOrbit.nextPatch == null));
//				KMPClientMain.DebugLog("pPnull:" + (newOrbit.previousPatch == null));
//				KMPClientMain.DebugLog("sI:" + newOrbit.sampleInterval);
//				KMPClientMain.DebugLog("UTsoi:" + newOrbit.UTsoi);
//				KMPClientMain.DebugLog("body:" + newOrbit.referenceBody.name);
				if (newOrbit.EndUT > 0)
				{
					double lastEndUT =  newOrbit.EndUT;
					while (newOrbit.EndUT > 0 && newOrbit.EndUT < tick && newOrbit.EndUT > lastEndUT && newOrbit.nextPatch != null)
					{
						KMPClientMain.DebugLog("orbit EndUT < target: " + newOrbit.EndUT + " vs " + tick);
						lastEndUT =  newOrbit.EndUT;
						newOrbit = newOrbit.nextPatch;
						if (newOrbit.referenceBody == null) newOrbit.referenceBody = FlightGlobals.Bodies.Find(b => b.name == "Sun");
						KMPClientMain.DebugLog("updated to next patch");
					}
				}
				newOrbit.UpdateFromUT(tick);
				
				//Swap orbits
				extant_vessel.orbitDriver = orbitDriver;
				extant_vessel.orbitDriver.orbit = newOrbit;
				victim.patchedConicSolver.obtDriver = oldDriver;
				victim.patchedConicRenderer.solver = victim.patchedConicSolver;
				victim.orbitDriver = oldDriver;
				victim.orbitDriver.UpdateOrbit();
				
				extant_vessel.orbitDriver.pos = extant_vessel.orbit.pos.xzy;
	            extant_vessel.orbitDriver.vel = extant_vessel.orbit.vel;
				
				Planetarium.SetUniversalTime(tick);
				KMPClientMain.DebugLog("new vel mag: " + extant_vessel.orbit.getOrbitalVelocityAtUT(tick).magnitude);
				KMPClientMain.DebugLog("Orbit updated to target: " + tick);
			} else { KMPClientMain.DebugLog("no victim available!"); }
		}
		
		private void addRemoteVessel(ProtoVessel protovessel, Guid vessel_id, KMPVesselUpdate update = null, double distance = 501d)
		{
			if (isInFlight && vessel_id == FlightGlobals.ActiveVessel.id)
			{
				KMPClientMain.DebugLog("Attempted to update controlled vessel!");
				return;
			}
			KMPClientMain.DebugLog("addRemoteVessel");
			Vector3 newWorldPos = Vector3.zero, newOrbitVel = Vector3.zero;
			bool setTarget = false, wasLoaded = false;
			try
			{
				//Ensure this vessel isn't already loaded
				Vessel oldVessel = FlightGlobals.Vessels.Find (v => v.id == vessel_id);
				if (oldVessel != null) {
					KMPClientMain.DebugLog("killing extant vessel");
					wasLoaded = oldVessel.loaded;
					if (protovessel.vesselType == VesselType.EVA && wasLoaded)
					{
						return; //Don't touch EVAs here
					}
					else
					{
						setTarget = FlightGlobals.fetch.VesselTarget != null && FlightGlobals.fetch.VesselTarget.GetVessel().id == vessel_id;
						if (oldVessel.loaded)
						{
							newWorldPos = oldVessel.transform.position;
							if (oldVessel.altitude > 10000d)
								newOrbitVel = oldVessel.GetObtVelocity();
						}
						oldVessel.Die();
					}
				}
				
				if (protovessel.vesselType != VesselType.EVA && serverVessels_Parts.ContainsKey(vessel_id))
				{
					KMPClientMain.DebugLog("killing known precursor vessels");
					foreach (Part part in serverVessels_Parts[vessel_id])
					{
						try { if (!part.vessel.isEVA) part.vessel.Die(); } catch {}
					}
				}
			} catch {}
			try
			{
				if ((protovessel.vesselType != VesselType.Debris && protovessel.vesselType != VesselType.Unknown) && protovessel.situation == Vessel.Situations.SUB_ORBITAL && protovessel.altitude < 25d)
				{
					//Land flags, vessels and EVAs that are on sub-orbital trajectory
					KMPClientMain.DebugLog("Placing sub-orbital protovessel on surface");
					protovessel.situation = Vessel.Situations.LANDED;
					protovessel.landed = true;
					if (protovessel.vesselType == VesselType.Flag) protovessel.height = -1;
				}
				//Don't bother with suborbital debris
				else if (protovessel.vesselType == VesselType.Debris && protovessel.situation == Vessel.Situations.SUB_ORBITAL) return;
				
				CelestialBody body = null;
				
				if (update != null)
				{
					body = FlightGlobals.Bodies.Find(b => b.name == update.bodyName);
					if (update.situation != Situation.LANDED && update.situation != Situation.SPLASHED)
					{
						if (body.atmosphere && body.maxAtmosphereAltitude > protovessel.altitude)
						{
							//In-atmo vessel--only load if within visible range
							if (distance > 500d)
								return;
						}
					}
				}

                if (isInSafetyBubble(protovessel.position, body, protovessel.altitude)) //refuse to load anything too close to the KSC
				{
					KMPClientMain.DebugLog("Tried to load vessel too close to KSC");
					return;
				}
				
				IEnumerator<ProtoCrewMember> crewEnum = HighLogic.CurrentGame.CrewRoster.GetEnumerator();
				int applicants = 0;
				while (crewEnum.MoveNext())
					if (crewEnum.Current.rosterStatus == ProtoCrewMember.RosterStatus.AVAILABLE) applicants++;
				
				if (protovessel.GetVesselCrew().Count * 5 > applicants)
				{
					KMPClientMain.DebugLog("Adding crew applicants");
					for (int i = 0; i < (protovessel.GetVesselCrew().Count * 5);)
					{
						ProtoCrewMember protoCrew = CrewGenerator.RandomCrewMemberPrototype();
						if (!HighLogic.CurrentGame.CrewRoster.ExistsInRoster(protoCrew.name))
						{
							HighLogic.CurrentGame.CrewRoster.AddCrewMember(protoCrew);
							i++;
						}
					}
				}
				if (vessels.ContainsKey(vessel_id.ToString()) && (!serverVessels_LoadDelay.ContainsKey(vessel_id) || (serverVessels_LoadDelay.ContainsKey(vessel_id) ? serverVessels_LoadDelay[vessel_id] < UnityEngine.Time.realtimeSinceStartup : false)))
				{
					protovessel.Load(HighLogic.CurrentGame.flightState);
					Vessel created_vessel = protovessel.vesselRef;
					if (created_vessel != null)
					{
						try
			            {
			                OrbitPhysicsManager.HoldVesselUnpack(1);
			            }
			            catch (NullReferenceException)
			            {
			            }
						if (!created_vessel.loaded) created_vessel.Load();
						KMPClientMain.DebugLog(created_vessel.id.ToString() + " initializing: ProtoParts=" + protovessel.protoPartSnapshots.Count+ ",Parts=" + created_vessel.Parts.Count + ",Sit=" + created_vessel.situation.ToString() + ",type=" + created_vessel.vesselType + ",alt=" + protovessel.altitude);
						
						vessels[vessel_id.ToString()].vessel.vesselRef = created_vessel;
						serverVessels_PartCounts[vessel_id] = created_vessel.Parts.Count;
						serverVessels_Parts[vessel_id] = new List<Part>();
						serverVessels_Parts[vessel_id].AddRange(created_vessel.Parts);
						
						bool distanceBlocksUnload = false;
						
						if (created_vessel.vesselType != VesselType.Flag && created_vessel.vesselType != VesselType.EVA)
						{
							foreach (Part part in created_vessel.Parts)
							{
								part.OnLoad();
								part.OnJustAboutToBeDestroyed += checkRemoteVesselIntegrity;
								part.explosionPotential = 0;
								part.terrainCollider = new PQS_PartCollider();
								part.terrainCollider.part = part;
								part.terrainCollider.useVelocityCollider = false;
								part.terrainCollider.useGravityCollider = false;
								part.breakingForce = float.MaxValue;
								part.breakingTorque = float.MaxValue;
							}
							if (update != null && update.bodyName == FlightGlobals.ActiveVessel.mainBody.name)
							{
								KMPClientMain.DebugLog("update included");
								
								//Update rotation
								
								created_vessel.SetRotation(new Quaternion(update.rot[0],update.rot[1],update.rot[2],update.rot[3]));
								
								if (update.relTime == RelativeTime.PRESENT)
								{	
									if (newWorldPos != Vector3.zero) 
									{
										KMPClientMain.DebugLog("repositioning");
										created_vessel.transform.position = newWorldPos;
										if (wasLoaded) distanceBlocksUnload = Vector3.Distance(created_vessel.transform.position,FlightGlobals.ActiveVessel.transform.position) < 2000f;
									}
									if (newOrbitVel != Vector3.zero) 
									{
										KMPClientMain.DebugLog("updating velocity");
										created_vessel.ChangeWorldVelocity((-1 * created_vessel.GetObtVelocity()) + newOrbitVel);
									}
									
									//Update FlightCtrlState
									if (created_vessel.ctrlState == null) created_vessel.ctrlState = new FlightCtrlState();
									created_vessel.ctrlState.CopyFrom(update.flightCtrlState.getAsFlightCtrlState(0.75f));
								}
								else
								{
									serverVessels_InPresent[update.id] = false;
									foreach (Part part in created_vessel.Parts)
									{
										part.setOpacity(0.3f);
									}
								}
							}
						}
						if (!syncing && !distanceBlocksUnload) //This explicit Unload helps correct the effects of "Can't remove Part (Script) because PartBuoyancy (Script) depends on it" errors and associated NREs seen during rendezvous mode switching, but for unknown reasons it causes problems if active during universe sync
							created_vessel.Unload();
						if (setTarget) StartCoroutine(setDockingTarget(created_vessel));
						KMPClientMain.DebugLog(created_vessel.id.ToString() + " initialized");
					}
				}
			}
			catch (Exception e)
			{
				KMPClientMain.DebugLog("Error adding remote vessel: " + e.Message + " " + e.StackTrace);
			}
		}
		
		private void writeIntToStream(KSP.IO.FileStream stream, Int32 val)
		{
			stream.Write(KMPCommon.intToBytes(val), 0, 4);
		}

		private Int32 readIntFromStream(KSP.IO.FileStream stream)
		{
			byte[] bytes = new byte[4];
			stream.Read(bytes, 0, 4);
			return KMPCommon.intFromBytes(bytes);
		}

		private void safeDelete(String filename)
		{
			if (KSP.IO.File.Exists<KMPManager>(filename))
			{
				try
				{
					KSP.IO.File.Delete<KMPManager>(filename);
				}
				catch { }
			}
		}

		private void enqueueChatOutMessage(String message)
		{
			String line = message.Replace("\n", ""); //Remove line breaks from message
			if (line.Length > 0)
			{
				enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.CHAT_SEND, encoder.GetBytes(line));
			}
		}

		//Interop

		public void acceptClientInterop(byte[] bytes) {
			lock (interopInQueueLock)
			{
				try {
//					int id_int = KMPCommon.intFromBytes(bytes, 4);
//					KMPCommon.ClientInteropMessageID id = KMPCommon.ClientInteropMessageID.NULL;
//					if (id_int >= 0 && id_int < Enum.GetValues(typeof(KMPCommon.ClientInteropMessageID)).Length)
//						id = (KMPCommon.ClientInteropMessageID)id_int;

					interopInQueue.Enqueue(bytes);
				} catch { }
			}
		}
		
		private void processClientInterop()
		{	
			if (interopInQueue.Count > 0 )
			{
				try
				{
					while (interopInQueue.Count > 0)
					{
						byte[] bytes;
						bytes = interopInQueue.Dequeue();
						if (bytes != null && bytes.Length > 4)
						{
							//Read the file-format version
							int file_version = KMPCommon.intFromBytes(bytes, 0);
			
							if (file_version != KMPCommon.FILE_FORMAT_VERSION)
							{
								//Incompatible client version
								Debug.LogError("KMP Client incompatible with plugin");
								return;
							}
			
							//Parse the messages
							int index = 4;
							while (index < bytes.Length - KMPCommon.INTEROP_MSG_HEADER_LENGTH)
							{
								//Read the message id
								int id_int = KMPCommon.intFromBytes(bytes, index);
			
								KMPCommon.ClientInteropMessageID id = KMPCommon.ClientInteropMessageID.NULL;
								if (id_int >= 0 && id_int < Enum.GetValues(typeof(KMPCommon.ClientInteropMessageID)).Length)
									id = (KMPCommon.ClientInteropMessageID)id_int;
			
								//Read the length of the message data
								int data_length = KMPCommon.intFromBytes(bytes, index + 4);
			
								index += KMPCommon.INTEROP_MSG_HEADER_LENGTH;
			
								if (data_length <= 0)
									handleInteropMessage(id, null);
								else if (data_length <= (bytes.Length - index))
								{
									//Copy the message data
									byte[] data = new byte[data_length];
									Array.Copy(bytes, index, data, 0, data.Length);
			
									handleInteropMessage(id, data);
								}
			
								if (data_length > 0)
									index += data_length;
							}
						}
					}
				}
				catch { }
			}
		}

		private bool writePluginInterop()
		{
			bool success = false;

			if (interopOutQueue.Count > 0 )
			{
				try
				{
					while (interopOutQueue.Count > 0)
					{
						byte[] message;
						message = interopOutQueue.Dequeue();
						KSP.IO.MemoryStream ms = new KSP.IO.MemoryStream();
					    ms.Write(KMPCommon.intToBytes(KMPCommon.FILE_FORMAT_VERSION), 0, 4);
						ms.Write(message,0,message.Length);

						KMPClientMain.acceptPluginInterop(ms.ToArray());
					}
					success = true;
				}
				catch { }
			}

			return success;
		}

		private void handleInteropMessage(KMPCommon.ClientInteropMessageID id, byte[] data)
		{
			try
			{
				switch (id)
				{
					case KMPCommon.ClientInteropMessageID.CHAT_RECEIVE:
						if (data != null)
						{
							KMPChatDisplay.enqueueChatLine(encoder.GetString(data));
                            KMPChatDX.enqueueChatLine(encoder.GetString(data));
							chatMessagesWaiting++;
						}
						break;
	
					case KMPCommon.ClientInteropMessageID.CLIENT_DATA:
	
						if (data != null && data.Length > 9)
						{
							//Read inactive vessels per update count
							inactiveVesselsPerUpdate = data[0];
	
							//Read screenshot height
							KMPScreenshotDisplay.screenshotSettings.maxHeight = KMPCommon.intFromBytes(data, 1);
	
							updateInterval = ((float)KMPCommon.intFromBytes(data, 5))/1000.0f;
	
							//Read username
							playerName = encoder.GetString(data, 9, data.Length - 9);
						}
	
						break;
	
					case KMPCommon.ClientInteropMessageID.PLUGIN_UPDATE:
						if (data != null)
						{
							//De-serialize and handle the update
							handleUpdate(KSP.IO.IOUtils.DeserializeFromBinary(data));
						}
						break;
	
					case KMPCommon.ClientInteropMessageID.SCREENSHOT_RECEIVE:
						if (data != null)
						{
							//Read description length
							int description_length = KMPCommon.intFromBytes(data, 0);
	
							//Read description
							String description = encoder.GetString(data, 4, description_length);
							
							//Read data
							byte[] image_data = new byte[data.Length - 4 - description_length];
							Array.Copy(data, 4 + description_length, image_data, 0, image_data.Length);		
							if (image_data.Length <= KMPScreenshotDisplay.screenshotSettings.maxNumBytes)
							{
								KMPScreenshotDisplay.description = description;
								StartCoroutine(applyScreenshotTexture(image_data));
							}
						}
						break;
				}
			} catch (Exception e) { KMPClientMain.DebugLog(e.Message); }
		}
		
		private IEnumerator<WaitForEndOfFrame> applyScreenshotTexture(byte[] image_data)
		{
			yield return new WaitForEndOfFrame();
			KMPScreenshotDisplay.texture = new Texture2D(4, 4, TextureFormat.RGB24, false, true);
			KMPClientMain.DebugLog("applying screenshot");
			if (KMPScreenshotDisplay.texture.LoadImage(image_data))
			{
				KMPScreenshotDisplay.texture.Apply();
				KMPClientMain.DebugLog("applied");
				//Make sure the screenshot texture does not exceed the size limits
				if (KMPScreenshotDisplay.texture.width > KMPScreenshotDisplay.screenshotSettings.maxWidth
					|| KMPScreenshotDisplay.texture.height > KMPScreenshotDisplay.screenshotSettings.maxHeight)
				{
					KMPScreenshotDisplay.texture = null;
					KMPScreenshotDisplay.description = String.Empty;
				}
			}
			else
			{
				KMPClientMain.DebugLog("image not loaded");
				KMPScreenshotDisplay.texture = null;
				KMPScreenshotDisplay.description = String.Empty;
			}	
		}
		
		private void enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID id, byte[] data)
		{
			int msg_data_length = 0;
			if (data != null)
				msg_data_length = data.Length;

			byte[] message_bytes = new byte[KMPCommon.INTEROP_MSG_HEADER_LENGTH + msg_data_length];

			KMPCommon.intToBytes((int)id).CopyTo(message_bytes, 0);
			KMPCommon.intToBytes(msg_data_length).CopyTo(message_bytes, 4);
			if (data != null)
				data.CopyTo(message_bytes, KMPCommon.INTEROP_MSG_HEADER_LENGTH);

			interopOutQueue.Enqueue(message_bytes);
			
			//Enforce max queue size
			while (interopOutQueue.Count > INTEROP_MAX_QUEUE_SIZE)
				interopOutQueue.Dequeue();
		}

		//Settings

		private void saveGlobalSettings()
		{
			//Get the global settings
			KMPGlobalSettings.instance.infoDisplayWindowX = KMPInfoDisplay.infoWindowPos.x;
			KMPGlobalSettings.instance.infoDisplayWindowY = KMPInfoDisplay.infoWindowPos.y;

			KMPGlobalSettings.instance.screenshotDisplayWindowX = KMPScreenshotDisplay.windowPos.x;
			KMPGlobalSettings.instance.screenshotDisplayWindowY = KMPScreenshotDisplay.windowPos.y;

			KMPGlobalSettings.instance.chatDisplayWindowX = KMPChatDisplay.windowPos.x;
			KMPGlobalSettings.instance.chatDisplayWindowY = KMPChatDisplay.windowPos.y;

            KMPGlobalSettings.instance.chatDXDisplayWindowX = KMPChatDX.windowPos.x;
            KMPGlobalSettings.instance.chatDXDisplayWindowY = KMPChatDX.windowPos.y;
            KMPGlobalSettings.instance.chatDXDisplayWindowWidth = KMPChatDX.chatboxWidth;
            KMPGlobalSettings.instance.chatDXDisplayWindowHeight = KMPChatDX.chatboxHeight;

            KMPGlobalSettings.instance.chatDXOffsetEnabled = KMPChatDX.offsetingEnabled;
            KMPGlobalSettings.instance.chatDXEditorOffsetX = KMPChatDX.editorOffsetX;
            KMPGlobalSettings.instance.chatDXEditorOffsetY = KMPChatDX.editorOffsetY;
            KMPGlobalSettings.instance.chatDXTrackingOffsetX = KMPChatDX.trackerOffsetX;
            KMPGlobalSettings.instance.chatDXTrackingOffsetY = KMPChatDX.trackerOffsetY;

			//Serialize global settings to file
			try
			{
				byte[] serialized = KSP.IO.IOUtils.SerializeToBinary(KMPGlobalSettings.instance);
				KSP.IO.File.WriteAllBytes<KMPManager>(serialized, GLOBAL_SETTINGS_FILENAME);
			}
			catch 
			{
			}
		}

		private void loadGlobalSettings()
		{
			bool success = false;
			try
			{
				//Deserialize global settings from file
				//byte[] bytes = KSP.IO.File.ReadAllBytes<KMPManager>(GLOBAL_SETTINGS_FILENAME); //Apparently KSP.IO.File.ReadAllBytes is broken
				String sPath = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
				sPath += "PluginData/";
				byte[] bytes = System.IO.File.ReadAllBytes(sPath + GLOBAL_SETTINGS_FILENAME);
				object deserialized = KSP.IO.IOUtils.DeserializeFromBinary(bytes);

				if (deserialized is KMPGlobalSettings)
				{
					KMPGlobalSettings.instance = (KMPGlobalSettings)deserialized;

					//Apply deserialized global settings
					KMPInfoDisplay.infoWindowPos.x = KMPGlobalSettings.instance.infoDisplayWindowX;
					KMPInfoDisplay.infoWindowPos.y = KMPGlobalSettings.instance.infoDisplayWindowY;

					if (KMPGlobalSettings.instance.guiToggleKey == KeyCode.None)
						KMPGlobalSettings.instance.guiToggleKey = KeyCode.F7;

					if (KMPGlobalSettings.instance.screenshotKey != KeyCode.None)
						KMPGlobalSettings.instance.screenshotKey = KeyCode.F8;

                    if (KMPGlobalSettings.instance.chatTalkKey == KeyCode.None)
                        KMPGlobalSettings.instance.chatTalkKey = KeyCode.BackQuote;

                    if (KMPGlobalSettings.instance.chatHideKey == KeyCode.None)
                        KMPGlobalSettings.instance.chatHideKey = KeyCode.F9;

					KMPScreenshotDisplay.windowPos.x = KMPGlobalSettings.instance.screenshotDisplayWindowX;
					KMPScreenshotDisplay.windowPos.y = KMPGlobalSettings.instance.screenshotDisplayWindowY;

					KMPChatDisplay.windowPos.x = KMPGlobalSettings.instance.chatDisplayWindowX;
					KMPChatDisplay.windowPos.y = KMPGlobalSettings.instance.chatDisplayWindowY;

                    KMPChatDX.chatboxX = KMPGlobalSettings.instance.chatDXDisplayWindowX;
                    KMPChatDX.chatboxY = KMPGlobalSettings.instance.chatDXDisplayWindowY;

                    KMPChatDX.chatboxWidth = KMPGlobalSettings.instance.chatDXDisplayWindowWidth;
                    KMPChatDX.chatboxHeight = KMPGlobalSettings.instance.chatDXDisplayWindowHeight;

                    KMPChatDX.offsetingEnabled = KMPGlobalSettings.instance.chatDXOffsetEnabled;
                    KMPChatDX.editorOffsetX = KMPGlobalSettings.instance.chatDXEditorOffsetX;
                    KMPChatDX.editorOffsetY = KMPGlobalSettings.instance.chatDXEditorOffsetY;
                    KMPChatDX.trackerOffsetX = KMPGlobalSettings.instance.chatDXTrackingOffsetX;
                    KMPChatDX.trackerOffsetY = KMPGlobalSettings.instance.chatDXTrackingOffsetY;


					success = true;
				}
			}
			catch (KSP.IO.IOException)
			{
				success = false;
			}
			catch (System.IO.IOException)
			{
				success = false;
			}
			catch (System.IO.IsolatedStorage.IsolatedStorageException e)
			{
				success = false;
			}
			if (!success)
			{
				try
				{
					KSP.IO.File.Delete<KMPManager>(GLOBAL_SETTINGS_FILENAME);
				} catch {}
				KMPGlobalSettings.instance = new KMPGlobalSettings();
			}
		}

		//MonoBehaviour

		public void Awake()
		{
			DontDestroyOnLoad(this);
			CancelInvoke();
			InvokeRepeating("updateStep", 1/30.0f, 1/30.0f);
			loadGlobalSettings();

            try
            {
                platform = Environment.OSVersion.Platform;
            } 
            catch(Exception)
            {
                platform = PlatformID.Unix;
            }
			Debug.Log("KMP loaded");
		}
		
		private void Start()
		  {
            if (ScaledSpace.Instance == null || ScaledSpace.Instance.scaledSpaceTransforms == null) { return; }
            KMPClientMain.DebugLog("Clearing ScaledSpace transforms, count: " + ScaledSpace.Instance.scaledSpaceTransforms.Count);
            ScaledSpace.Instance.scaledSpaceTransforms.RemoveAll(t => t == null);
            if (HighLogic.LoadedScene == GameScenes.MAINMENU)
            {
                ScaledSpace.Instance.scaledSpaceTransforms.RemoveAll(t => !FlightGlobals.Bodies.Exists(b => b.name == t.name));
            }
            KMPClientMain.DebugLog("New count: " + ScaledSpace.Instance.scaledSpaceTransforms.Count);
        }
		
		private void OnPartCouple(GameEvents.FromToAction<Part,Part> data)
		{
			docking = true;
			KMPClientMain.DebugLog("Dock event: " + data.to.vessel.id + " " + data.from.vessel.id);
			//Destroy old vessels for other players
			removeDockedVessel(data.to.vessel);
			removeDockedVessel(data.from.vessel);
			//Fix displayed crew
			while (KerbalGUIManager.ActiveCrew.Count > 0)
			{
				KMPClientMain.DebugLog("Removed extra displayed crew member");
				KerbalGUIManager.RemoveActiveCrew(KerbalGUIManager.ActiveCrew.Find(k => true));
			}
			Invoke("setDoneDocking",3f);
		}
		
		private void setNotDocking()
		{
			docking = false;	
		}
		
		private void removeDockedVessel(Vessel vessel)
		{
			serverVessels_InUse[vessel.id] = false;
			serverVessels_IsPrivate.Remove(vessel.id);
			serverVessels_IsMine.Remove(vessel.id);
			sendRemoveVesselMessage(vessel, true);
			serverVessels_PartCounts[vessel.id] = 0;
			serverVessels_ProtoVessels.Remove(vessel.id);
		}
		
		private void OnPartUndock(Part data)
		{
			docking = true;
			KMPClientMain.DebugLog("Undock event");
			if (data.vessel != null)
			{
				serverVessels_PartCounts[data.vessel.id] = 0;
				serverVessels_ProtoVessels.Remove(data.vessel.id);
			}
			docking = false;
		}
		
		private void OnCrewOnEva(GameEvents.FromToAction<Part,Part> data)
		{
			KMPClientMain.DebugLog("EVA event");
			if (data.from.vessel != null) sendVesselMessage(data.from.vessel);
		}
		
		private void OnCrewBoardVessel(GameEvents.FromToAction<Part,Part> data)
		{
			KMPClientMain.DebugLog("End EVA event");
			if (data.to.vessel != null) sendVesselMessage(data.to.vessel);
			if (lastEVAVessel != null) sendRemoveVesselMessage(lastEVAVessel);
		}
		
		private void OnVesselLoaded(Vessel data)
		{
			KMPClientMain.DebugLog("Vessel loaded: " + data.id);
			//data.distancePackThreshold = Vector3.Distance(data.orbit.getPositionAtUT(Planetarium.GetUniversalTime()), FlightGlobals.ship_position) + 100f;
		}
		
		private void OnVesselTerminated(ProtoVessel data)
		{
            KMPClientMain.DebugLog("Vessel termination: " + data.vesselID + " " + serverVessels_RemoteID.ContainsKey(data.vesselID) + " " + (HighLogic.LoadedScene == GameScenes.TRACKSTATION) + " " + (data.vesselType == VesselType.Debris || (serverVessels_IsMine.ContainsKey(data.vesselID) ? serverVessels_IsMine[data.vesselID] : true)));
			if (serverVessels_RemoteID.ContainsKey(data.vesselID) //"activeTermination" only if this is remote vessel
			    && HighLogic.LoadedScene == GameScenes.TRACKSTATION //and at TrackStation
			    && (data.vesselType == VesselType.Debris || (serverVessels_IsMine.ContainsKey(data.vesselID) ? serverVessels_IsMine[data.vesselID] : true))) //and is debris or owned vessel
			{
				activeTermination = true;
			}
		}
		
		private void OnVesselDestroy(Vessel data)
		{
			if (!docking //Send destroy message to server if not currently in docking event
			    && serverVessels_RemoteID.ContainsKey(data.id) //and is a remote vessel
			    && ((isInFlight && data.id == FlightGlobals.ActiveVessel.id) //and is in-flight/ours OR
			    	|| (HighLogic.LoadedScene == GameScenes.TRACKSTATION //still at trackstation
			    			&& activeTermination //and activeTermination is set
			    			&& (data.vesselType == VesselType.Debris || (serverVessels_IsMine.ContainsKey(data.id) ? serverVessels_IsMine[data.id] : true))))) //and target is debris or owned vessel
			{
				activeTermination = false;
				KMPClientMain.DebugLog("Vessel destroyed: " + data.id);
				sendRemoveVesselMessage(data);
			}
		}
			
		private void OnTimeWarpRateChanged()
		{
			KMPClientMain.DebugLog("OnTimeWarpRateChanged");
			if (TimeWarp.WarpMode == TimeWarp.Modes.LOW) TimeWarp.SetRate(0,true);
			else
			{
				KMPClientMain.DebugLog("sending: " + TimeWarp.CurrentRate);
				byte[] update_bytes = BitConverter.GetBytes(TimeWarp.CurrentRate);
				enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.WARPING, update_bytes);
				if (TimeWarp.CurrentRate <= 1) 
				{
					Invoke("setNotWarping",1f);
					KMPClientMain.DebugLog("done warping");
				}
				else
				{
					warping = true;
					KMPClientMain.DebugLog("warping");
				}
			}
		}
		
		private void setNotWarping()
		{
			warping = false;	
		}
		
		private void OnFirstFlightReady()
		{
			if (syncing)
			{
				//Enable debug log for sync
				KMPClientMain.debugging = true;
				KMPClientMain.DebugLog("Requesting initial sync");
				GameEvents.onFlightReady.Remove(this.OnFirstFlightReady);
				GameEvents.onFlightReady.Add(this.OnFlightReady);
				MapView.EnterMapView();
				MapView.MapCamera.SetTarget("Kerbin");
				ScreenMessages.PostScreenMessage("Synchronizing universe, please wait...",30f,ScreenMessageStyle.UPPER_CENTER);
				StartCoroutine(sendSubspaceSyncRequest(-1,true));
				Invoke("handleSyncTimeout",55f);
				docking = false;
			}
		}
			
		private void OnFlightReady()
		{
			InputLockManager.ClearControlLocks();
			//Ensure vessel uses only stock parts in lieu of proper mod support
			if (!FlightGlobals.ActiveVessel.isEVA && !FlightGlobals.ActiveVessel.protoVessel.protoPartSnapshots.TrueForAll(pps => KMPClientMain.partList.Contains(pps.partName)))
			{
				KMPClientMain.DebugLog("Loaded vessel has prohibited parts!");
				foreach (ProtoPartSnapshot pps in FlightGlobals.ActiveVessel.protoVessel.protoPartSnapshots)
					KMPClientMain.DebugLog(pps.partName);
				syncing = true;
				GameEvents.onFlightReady.Add(this.OnFirstFlightReady);
				GameEvents.onFlightReady.Remove(this.OnFlightReady);
				HighLogic.CurrentGame.Start();
				ScreenMessages.PostScreenMessage("Can't start flight - Vessel has prohibited parts! Sorry!",10f,ScreenMessageStyle.UPPER_CENTER);
			}
		}
		
		public void HandleSyncCompleted()
		{
			if (!forceQuit && syncing) Invoke("finishSync",5f);
		}
		
		private void handleSyncTimeout()
		{
			if (!forceQuit && syncing) Invoke("finishSync",5f);
		}
		
		private void finishSync()
		{
			if (!forceQuit && syncing)
			{
				ScreenMessages.PostScreenMessage("Universe synchronized",1f,ScreenMessageStyle.UPPER_RIGHT);
				StartCoroutine(returnToSpaceCenter());
				//Disable debug logging once synced unless explicitly enabled
				KMPClientMain.debugging = false;
			}
		}

		private void OnGameSceneLoadRequested(GameScenes data)
		{
			KMPClientMain.DebugLog("OnGameSceneLoadRequested");
			if (gameRunning && (data == GameScenes.SPACECENTER || data == GameScenes.MAINMENU))
			{
				writePluginUpdate();
			}
		}
		
		public void Update()
		{
			try
			{
				if (!gameRunning) return;
				if (FlightDriver.Pause) FlightDriver.SetPause(false);

                //Find an instance of the game's RenderingManager
                if (renderManager == null)
                    renderManager = (RenderingManager)FindObjectOfType(typeof(RenderingManager));

                //Find an instance of the game's PlanetariumCamera
                if (planetariumCam == null)
                    planetariumCam = (PlanetariumCamera)FindObjectOfType(typeof(PlanetariumCamera));

                if (Input.GetKeyDown(KMPGlobalSettings.instance.guiToggleKey))
                    KMPInfoDisplay.infoDisplayActive = !KMPInfoDisplay.infoDisplayActive;

                if (Input.GetKeyDown(KMPGlobalSettings.instance.screenshotKey))
                    StartCoroutine(shareScreenshot());

                if (Input.GetKeyDown(KMPGlobalSettings.instance.chatTalkKey))
                    KMPChatDX.showInput = true;

                if (Input.GetKeyDown(KMPGlobalSettings.instance.chatHideKey))
                {
                    KMPGlobalSettings.instance.chatDXWindowEnabled = !KMPGlobalSettings.instance.chatDXWindowEnabled;
                    if (KMPGlobalSettings.instance.chatDXWindowEnabled) KMPChatDX.enqueueChatLine("Press Chat key (" + (KMPGlobalSettings.instance.chatTalkKey == KeyCode.BackQuote ? "~" : KMPGlobalSettings.instance.chatTalkKey.ToString()) + ") to send a message");
                }

                if (Input.anyKeyDown)
                    lastKeyPressTime = UnityEngine.Time.realtimeSinceStartup;

                //Handle key-binding
                if (mappingGUIToggleKey)
                {
                    KeyCode key = KeyCode.F7;
                    if (getAnyKeyDown(ref key))
                    {
                        if (key != KeyCode.Mouse0)
                        {
                            KMPGlobalSettings.instance.guiToggleKey = key;
                            mappingGUIToggleKey = false;
                        }
                    }
                }

                if (mappingScreenshotKey)
                {
                    KeyCode key = KeyCode.F8;
                    if (getAnyKeyDown(ref key))
                    {
                        if (key != KeyCode.Mouse0)
                        {
                            KMPGlobalSettings.instance.screenshotKey = key;
                            mappingScreenshotKey = false;
                        }
                    }
                }

                if (mappingChatKey)
                {
                    KeyCode key = KeyCode.Y;
                    if (getAnyKeyDown(ref key))
                    {
                        KMPGlobalSettings.instance.chatTalkKey = key;
                        mappingChatKey = false;
                    }
                }

                if (mappingChatDXToggleKey)
                {
                    KeyCode key = KeyCode.F9;
                    if (getAnyKeyDown(ref key))
                    {
                        KMPGlobalSettings.instance.chatHideKey = key;
                        mappingChatDXToggleKey = false;
                    }
                }
			} catch (Exception ex) { KMPClientMain.DebugLog ("u err: " + ex.Message + " " + ex.StackTrace); }
		}

		public void OnGUI()
		{
			drawGUI();
		}

		//GUI

		public void drawGUI()
		{
			if (forceQuit)
			{
				forceQuit = false;
				gameRunning = false;
				HighLogic.LoadScene(GameScenes.MAINMENU);
			}
			
			//Init info display options
			if (KMPInfoDisplay.layoutOptions == null)
				KMPInfoDisplay.layoutOptions = new GUILayoutOption[6];

			KMPInfoDisplay.layoutOptions[0] = GUILayout.ExpandHeight(true);
			KMPInfoDisplay.layoutOptions[1] = GUILayout.ExpandWidth(true);

			if (KMPInfoDisplay.infoDisplayMinimized)
			{
				KMPInfoDisplay.layoutOptions[2] = GUILayout.MinHeight(KMPInfoDisplay.WINDOW_HEIGHT_MINIMIZED);
				KMPInfoDisplay.layoutOptions[3] = GUILayout.MaxHeight(KMPInfoDisplay.WINDOW_HEIGHT_MINIMIZED);

				KMPInfoDisplay.layoutOptions[4] = GUILayout.MinWidth(KMPInfoDisplay.WINDOW_WIDTH_MINIMIZED);
				KMPInfoDisplay.layoutOptions[5] = GUILayout.MaxWidth(KMPInfoDisplay.WINDOW_WIDTH_MINIMIZED);
			}
			else
			{

				if (KMPGlobalSettings.instance.infoDisplayBig)
				{
					KMPInfoDisplay.layoutOptions[4] = GUILayout.MinWidth(KMPInfoDisplay.WINDOW_WIDTH_BIG);
					KMPInfoDisplay.layoutOptions[5] = GUILayout.MaxWidth(KMPInfoDisplay.WINDOW_WIDTH_BIG);

					KMPInfoDisplay.layoutOptions[2] = GUILayout.MinHeight(KMPInfoDisplay.WINDOW_HEIGHT_BIG);
					KMPInfoDisplay.layoutOptions[3] = GUILayout.MaxHeight(KMPInfoDisplay.WINDOW_HEIGHT_BIG);
				}
				else
				{
					KMPInfoDisplay.layoutOptions[4] = GUILayout.MinWidth(KMPInfoDisplay.WINDOW_WIDTH_DEFAULT);
					KMPInfoDisplay.layoutOptions[5] = GUILayout.MaxWidth(KMPInfoDisplay.WINDOW_WIDTH_DEFAULT);

					KMPInfoDisplay.layoutOptions[2] = GUILayout.MinHeight(KMPInfoDisplay.WINDOW_HEIGHT);
					KMPInfoDisplay.layoutOptions[3] = GUILayout.MaxHeight(KMPInfoDisplay.WINDOW_HEIGHT);
				}
			}

			CheckEditorLock();

//			//Init chat display options
//			if (KMPChatDisplay.layoutOptions == null)
//				KMPChatDisplay.layoutOptions = new GUILayoutOption[2];
//
//			KMPChatDisplay.layoutOptions[0] = GUILayout.MinWidth(KMPChatDisplay.windowWidth);
//			KMPChatDisplay.layoutOptions[1] = GUILayout.MaxWidth(KMPChatDisplay.windowWidth);

            // Chat DX
            if (KMPChatDX.layoutOptions == null)
                KMPChatDX.layoutOptions = new GUILayoutOption[4];

            KMPChatDX.layoutOptions[0] = GUILayout.MinWidth(KMPChatDX.chatboxWidth);
            KMPChatDX.layoutOptions[1] = GUILayout.MaxWidth(KMPChatDX.chatboxWidth);
            KMPChatDX.layoutOptions[2] = GUILayout.MinHeight(KMPChatDX.chatboxHeight);
            KMPChatDX.layoutOptions[3] = GUILayout.MaxHeight(KMPChatDX.chatboxHeight);

            KMPChatDX.windowStyle.normal.background = null;

			//Init screenshot display options
			if (KMPScreenshotDisplay.layoutOptions == null)
				KMPScreenshotDisplay.layoutOptions = new GUILayoutOption[2];

			KMPScreenshotDisplay.layoutOptions[0] = GUILayout.MaxHeight(KMPScreenshotDisplay.MIN_WINDOW_HEIGHT);
			KMPScreenshotDisplay.layoutOptions[1] = GUILayout.MaxWidth(KMPScreenshotDisplay.MIN_WINDOW_WIDTH);
			
			//Init connection display options
			if (KMPConnectionDisplay.layoutOptions == null)
				KMPConnectionDisplay.layoutOptions = new GUILayoutOption[2];

			KMPConnectionDisplay.layoutOptions[0] = GUILayout.MaxHeight(KMPConnectionDisplay.MIN_WINDOW_HEIGHT);
			KMPConnectionDisplay.layoutOptions[1] = GUILayout.MaxWidth(KMPConnectionDisplay.MIN_WINDOW_WIDTH);
			
			//Init lock display options
			if (KMPVesselLockDisplay.layoutOptions == null)
				KMPVesselLockDisplay.layoutOptions = new GUILayoutOption[2];

			KMPVesselLockDisplay.layoutOptions[0] = GUILayout.MaxHeight(KMPVesselLockDisplay.MIN_WINDOW_HEIGHT);
			KMPVesselLockDisplay.layoutOptions[1] = GUILayout.MaxWidth(KMPVesselLockDisplay.MIN_WINDOW_WIDTH);
			
			GUI.skin = HighLogic.Skin;
			
			if (!KMPConnectionDisplay.windowEnabled && HighLogic.LoadedScene == GameScenes.MAINMENU) KMPClientMain.clearConnectionState();
			
			KMPConnectionDisplay.windowEnabled = (HighLogic.LoadedScene == GameScenes.MAINMENU) && globalUIToggle;

            
            if (KMPGlobalSettings.instance.chatDXWindowEnabled)
            {
                KMPChatDX.windowPos = GUILayout.Window(
                    999994,
                    KMPChatDX.getWindowPos(),
                    chatWindowDX,
                    "",
                    KMPChatDX.windowStyle,
                    KMPChatDX.layoutOptions
                    );
            }

			
			if (KMPConnectionDisplay.windowEnabled)
			{
				gameRunning = false;
				try
				{
					GameEvents.onGameSceneLoadRequested.Remove(this.OnGameSceneLoadRequested);
					GameEvents.onFlightReady.Remove(this.OnFlightReady);
					GameEvents.onPartCouple.Remove(this.OnPartCouple);
					GameEvents.onPartUndock.Remove(this.OnPartUndock);
					GameEvents.onCrewOnEva.Remove(this.OnCrewOnEva);
					GameEvents.onCrewBoardVessel.Remove(this.OnCrewBoardVessel);
					GameEvents.onVesselLoaded.Remove(this.OnVesselLoaded);
					GameEvents.onVesselTerminated.Remove(this.OnVesselTerminated);
					GameEvents.onVesselDestroy.Remove(this.OnVesselDestroy);
				} catch { }
				GUILayout.Window(
					999996,
					KMPConnectionDisplay.windowPos,
					connectionWindow,
					"Connection Settings",
					KMPConnectionDisplay.layoutOptions
					);
			}
			
			if (!KMPConnectionDisplay.windowEnabled && KMPClientMain.handshakeCompleted && KMPClientMain.tcpSocket != null)
			{
				if(KMPInfoDisplay.infoDisplayActive)
				{
					KMPInfoDisplay.infoWindowPos = GUILayout.Window(
						999999,
						KMPInfoDisplay.infoWindowPos,
						infoDisplayWindow,
						KMPInfoDisplay.infoDisplayMinimized ? "KMP" : "KerbalMP v"+KMPCommon.PROGRAM_VERSION+" ("+KMPGlobalSettings.instance.guiToggleKey+")",
						KMPInfoDisplay.layoutOptions
						);
					
					if (isInFlight && !syncing && !KMPInfoDisplay.infoDisplayMinimized)
					{
						GUILayout.Window(
							999995,
							KMPVesselLockDisplay.windowPos,
							lockWindow,
							"Lock",
							KMPVesselLockDisplay.layoutOptions
							);
					}
				}	
			}

			
		    if(!gameRunning)
     	    {
         		//close the windows if not connected to a server 
       	 	    KMPScreenshotDisplay.windowEnabled = false;
      		    KMPGlobalSettings.instance.chatDXWindowEnabled = false;
 		    }
			
			if (KMPScreenshotDisplay.windowEnabled)
			{
				KMPScreenshotDisplay.windowPos = GUILayout.Window(
					999998,
					KMPScreenshotDisplay.windowPos,
					screenshotWindow,
					"KerbalMP Viewer",
					KMPScreenshotDisplay.layoutOptions
					);
			}

//			if (KMPGlobalSettings.instance.chatWindowEnabled)
//			{
//				KMPChatDisplay.windowPos = GUILayout.Window(
//					999997,
//					KMPChatDisplay.windowPos,
//					chatWindow,
//					"KerbalMP Chat",
//					KMPChatDisplay.layoutOptions
//					);
//			}


            KMPChatDX.windowPos = enforceWindowBoundaries(KMPChatDX.windowPos);
			KMPInfoDisplay.infoWindowPos = enforceWindowBoundaries(KMPInfoDisplay.infoWindowPos);
			KMPScreenshotDisplay.windowPos = enforceWindowBoundaries(KMPScreenshotDisplay.windowPos);
//			KMPChatDisplay.windowPos = enforceWindowBoundaries(KMPChatDisplay.windowPos);
            
		}
		
		private void lockWindow(int windowID)
		{
			try
			{
				bool wasLocked;
				if (!serverVessels_IsPrivate.ContainsKey(FlightGlobals.ActiveVessel.id))
				{
					//Must be ours
					serverVessels_IsPrivate[FlightGlobals.ActiveVessel.id] = false;
					serverVessels_IsMine[FlightGlobals.ActiveVessel.id] = true;
					sendVesselMessage(FlightGlobals.ActiveVessel);
					wasLocked = false;
				}
				else
				{
					wasLocked = serverVessels_IsPrivate[FlightGlobals.ActiveVessel.id];
				}
				GUILayout.BeginVertical();
				GUIStyle lockButtonStyle = new GUIStyle(GUI.skin.button);
				lockButtonStyle.fontSize = 10;
				bool locked =
					GUILayout.Toggle(wasLocked,
					wasLocked ? "Private" : "Public",
					lockButtonStyle);
				if (wasLocked != locked)
				{
					serverVessels_IsPrivate[FlightGlobals.ActiveVessel.id] = locked;
					serverVessels_IsMine[FlightGlobals.ActiveVessel.id] = true;
					if (locked) ScreenMessages.PostScreenMessage("Your vessel is now marked Private",5,ScreenMessageStyle.UPPER_CENTER);
					else ScreenMessages.PostScreenMessage("Your vessel is now marked Public",5,ScreenMessageStyle.UPPER_CENTER);
					sendVesselMessage(FlightGlobals.ActiveVessel);
				}
				GUILayout.EndVertical();
			}
			catch {}
		}
		
		private void infoDisplayWindow(int windowID)
		{
			GUILayout.BeginVertical();

			bool minimized = KMPInfoDisplay.infoDisplayMinimized;
			bool big = KMPGlobalSettings.instance.infoDisplayBig;

			if (!minimized)
				GUILayout.BeginHorizontal();
			
			KMPInfoDisplay.infoDisplayMinimized = GUILayout.Toggle(
				KMPInfoDisplay.infoDisplayMinimized,
				KMPInfoDisplay.infoDisplayMinimized ? "[-]" : "X",
				GUI.skin.button);

			if (!minimized)
			{
				KMPGlobalSettings.instance.infoDisplayBig = GUILayout.Toggle(
					KMPGlobalSettings.instance.infoDisplayBig,
					KMPGlobalSettings.instance.infoDisplayBig ? "- " : "+ ",
					GUI.skin.button);
				KMPInfoDisplay.infoDisplayDetailed = GUILayout.Toggle(KMPInfoDisplay.infoDisplayDetailed, "Detail", GUI.skin.button);
				KMPInfoDisplay.infoDisplayOptions = GUILayout.Toggle(KMPInfoDisplay.infoDisplayOptions, "Options", GUI.skin.button);
				GUILayout.EndHorizontal();

				KMPInfoDisplay.infoScrollPos = GUILayout.BeginScrollView(KMPInfoDisplay.infoScrollPos);
				GUILayout.BeginVertical();

				//Init label styles
				playerNameStyle = new GUIStyle(GUI.skin.label);
				playerNameStyle.normal.textColor = Color.white;
				playerNameStyle.hover.textColor = Color.white;
				playerNameStyle.active.textColor = Color.white;
				playerNameStyle.alignment = TextAnchor.MiddleLeft;
				playerNameStyle.margin = new RectOffset(0, 0, 2, 0);
				playerNameStyle.padding = new RectOffset(0, 0, 0, 0);
				playerNameStyle.stretchWidth = true;
				playerNameStyle.fontStyle = FontStyle.Bold;

				vesselNameStyle = new GUIStyle(GUI.skin.label);
				vesselNameStyle.normal.textColor = Color.white;
				vesselNameStyle.stretchWidth = true;
				vesselNameStyle.fontStyle = FontStyle.Bold;
				if (big)
				{
					vesselNameStyle.margin = new RectOffset(0, 4, 2, 0);
					vesselNameStyle.alignment = TextAnchor.LowerRight;
				}
				else
				{
					vesselNameStyle.margin = new RectOffset(4, 0, 0, 0);
					vesselNameStyle.alignment = TextAnchor.LowerLeft;
				}

				vesselNameStyle.padding = new RectOffset(0, 0, 0, 0);

				stateTextStyle = new GUIStyle(GUI.skin.label);
				stateTextStyle.normal.textColor = new Color(0.75f, 0.75f, 0.75f);
				stateTextStyle.margin = new RectOffset(4, 0, 0, 0);
				stateTextStyle.padding = new RectOffset(0, 0, 0, 0);
				stateTextStyle.stretchWidth = true;
				stateTextStyle.fontStyle = FontStyle.Normal;
				stateTextStyle.fontSize = 12;

				//Write vessel's statuses
				foreach (KeyValuePair<String, VesselStatusInfo> pair in playerStatus)
					vesselStatusLabels(pair.Value, big);

				GUILayout.EndVertical();
				GUILayout.EndScrollView();

				GUILayout.BeginHorizontal();
				GUIStyle chatButtonStyle = new GUIStyle(GUI.skin.button);
				if (chatMessagesWaiting > 5)
				{
					chatButtonStyle.normal.textColor = new Color(0.92f, 0.09f, 0.09f);
				}
				else if (chatMessagesWaiting > 2)
				{
					chatButtonStyle.normal.textColor = new Color(0.92f, 0.60f, 0.09f);
				}
				else if (chatMessagesWaiting > 0)
				{
					chatButtonStyle.normal.textColor = new Color(0.27f, 0.92f, 0.09f);
				}
				KMPGlobalSettings.instance.chatDXWindowEnabled = GUILayout.Toggle(KMPGlobalSettings.instance.chatDXWindowEnabled, "Chat ("+KMPGlobalSettings.instance.chatHideKey+")", chatButtonStyle);
				KMPScreenshotDisplay.windowEnabled = GUILayout.Toggle(KMPScreenshotDisplay.windowEnabled, "Viewer", GUI.skin.button);
				if (GUILayout.Button("Share Screen ("+KMPGlobalSettings.instance.screenshotKey+")"))
					StartCoroutine(shareScreenshot());
				
				GUIStyle syncButtonStyle = new GUIStyle(GUI.skin.button);
				if (showServerSync && isInFlight && FlightGlobals.ActiveVessel.ctrlState.mainThrottle == 0f)
				{
					syncButtonStyle.normal.textColor = new Color(0.28f, 0.86f, 0.94f);
					syncButtonStyle.hover.textColor = new Color(0.48f, 0.96f, 0.96f);
					if (GUILayout.Button("Sync",syncButtonStyle))
						StartCoroutine(sendSubspaceSyncRequest());
				}
				else
				{
					syncButtonStyle.normal.textColor = new Color(0.5f,0.5f,0.5f);
					GUI.enabled = false;
					GUILayout.Button("Sync",syncButtonStyle);
					GUI.enabled = true;
				}
				GUILayout.EndHorizontal();

				if (KMPInfoDisplay.infoDisplayOptions)
				{
					//Connection
					GUILayout.Label("Connection");

					GUILayout.BeginHorizontal();
					
					if (GUILayout.Button("Disconnect & Exit"))
					{	
						KMPClientMain.sendConnectionEndMessage("Quit");
						KMPClientMain.clearConnectionState();
						KMPClientMain.intentionalConnectionEnd = true;
						KMPClientMain.endSession = true;
						gameRunning = false;
						forceQuit = true;
					}

					GUILayout.EndHorizontal();
					
					//Settings
					GUILayout.Label("Settings");

					GUILayout.BeginHorizontal();

					KMPGlobalSettings.instance.smoothScreens = GUILayout.Toggle(
						KMPGlobalSettings.instance.smoothScreens,
						"Smooth Screenshots",
						GUI.skin.button);

					KMPGlobalSettings.instance.chatColors
						= GUILayout.Toggle(KMPGlobalSettings.instance.chatColors, "Chat Colors", GUI.skin.button);
					
					GUILayout.EndHorizontal();

					//Key mapping
					GUILayout.Label("Key-Bindings");


					GUILayout.BeginHorizontal();

					mappingGUIToggleKey = GUILayout.Toggle(
						mappingGUIToggleKey,
						mappingGUIToggleKey ? "Press key" : "Menu Toggle: " + KMPGlobalSettings.instance.guiToggleKey,
						GUI.skin.button);

					mappingScreenshotKey = GUILayout.Toggle(
						mappingScreenshotKey,
						mappingScreenshotKey ? "Press key" : "Screenshot: " + KMPGlobalSettings.instance.screenshotKey,
						GUI.skin.button);

					GUILayout.EndHorizontal();

                    

                    GUILayout.BeginHorizontal();

                    mappingChatKey = GUILayout.Toggle(
                        mappingChatKey,
                        mappingChatKey ? "Press key" : "Send Chat: " + KMPGlobalSettings.instance.chatTalkKey,
                        GUI.skin.button);

                    mappingChatDXToggleKey = GUILayout.Toggle(
                        mappingChatDXToggleKey,
                        mappingChatDXToggleKey ? "Press key" : "Chat Toggle: " + KMPGlobalSettings.instance.chatHideKey,
                        GUI.skin.button);

                    GUILayout.EndHorizontal();

                    // Chat map & reset
                    GUILayout.Label("Reset Chat Window");
                    if (GUILayout.Button("Reset Chat"))
                    {
                        KMPChatDX.windowPos.x = 0;
                        KMPChatDX.windowPos.y = 0;
                    }

                    

				}
			}

			GUILayout.EndVertical();

			GUI.DragWindow();

		}
		
		private void connectionWindow(int windowID)
		{
			if(!configRead)
			{
				KMPClientMain.readConfigFile();
				configRead = true;
			}
			if (!isVerified)
			{
				KMPClientMain.verifyShipsDirectory();
				isVerified = true;
			}
			if (KMPClientMain.handshakeCompleted && KMPClientMain.tcpSocket != null)
			{
				if (KMPClientMain.tcpSocket.Connected && !gameRunning)
				{
					//Clear dictionaries
					sentVessels_Situations.Clear();
		
					serverVessels_RemoteID.Clear();
					serverVessels_PartCounts.Clear();
					serverVessels_Parts.Clear();
					serverVessels_ProtoVessels.Clear();
					
					serverVessels_InUse.Clear();
					serverVessels_IsPrivate.Clear();
					serverVessels_IsMine.Clear();
					
					serverVessels_LastUpdateDistanceTime.Clear();
					serverVessels_LoadDelay.Clear();
					serverVessels_InPresent.Clear();
					serverVessels_ObtSyncDelay.Clear();
					
					serverVessels_RendezvousSmoothPos.Clear();
					serverVessels_RendezvousSmoothVel.Clear();
	
					newFlags.Clear();
					
					//Start MP game
					KMPConnectionDisplay.windowEnabled = false;
					gameRunning = true;
					GameSettings.MAX_PHYSICS_DT_PER_FRAME = 1.0f;
					HighLogic.SaveFolder = "KMP";
					HighLogic.CurrentGame = GamePersistence.LoadGame("start",HighLogic.SaveFolder,false,true);
					HighLogic.CurrentGame.Parameters.Flight.CanAutoSave = false;
					HighLogic.CurrentGame.Parameters.Flight.CanLeaveToEditor = false;
					HighLogic.CurrentGame.Parameters.Flight.CanLeaveToMainMenu = false;
					HighLogic.CurrentGame.Parameters.Flight.CanQuickLoad = false;
					HighLogic.CurrentGame.Parameters.Flight.CanRestart = false;
					HighLogic.CurrentGame.Parameters.Flight.CanTimeWarpLow = false;
					HighLogic.CurrentGame.Parameters.Flight.CanSwitchVesselsFar = false;
					HighLogic.CurrentGame.Title = "KMP";
					HighLogic.CurrentGame.Description = "Kerbal Multi Player session";
					HighLogic.CurrentGame.flagURL = "KMP/Flags/default";
					GamePersistence.SaveGame("persistent",HighLogic.SaveFolder,SaveMode.OVERWRITE);
					GameEvents.onFlightReady.Add(this.OnFirstFlightReady);
					syncing = true;
					HighLogic.CurrentGame.Start();
					for (int i=0; i<50;)
					{
						ProtoCrewMember protoCrew = CrewGenerator.RandomCrewMemberPrototype();
						if (!HighLogic.CurrentGame.CrewRoster.ExistsInRoster(protoCrew.name))
						{
							HighLogic.CurrentGame.CrewRoster.AddCrewMember(protoCrew);
							i++;
						}
					}
					GameEvents.onGameSceneLoadRequested.Add(this.OnGameSceneLoadRequested);
					GameEvents.onPartCouple.Add(this.OnPartCouple);
					GameEvents.onPartUndock.Add(this.OnPartUndock);
					GameEvents.onCrewOnEva.Add(this.OnCrewOnEva);
					GameEvents.onCrewBoardVessel.Add(this.OnCrewBoardVessel);
					GameEvents.onVesselLoaded.Add(this.OnVesselLoaded);
					GameEvents.onVesselTerminated.Add(this.OnVesselTerminated);
					GameEvents.onVesselDestroy.Add(this.OnVesselDestroy);
					writePluginData();
					//Make sure user knows how to use new chat
					KMPChatDX.enqueueChatLine("Press Chat key (" + (KMPGlobalSettings.instance.chatTalkKey == KeyCode.BackQuote ? "~" : KMPGlobalSettings.instance.chatTalkKey.ToString()) + ") to send a message");
					KMPGlobalSettings.instance.chatDXWindowEnabled = true;
					
					return;
				}
			}
			
			GUILayout.BeginHorizontal();
				if (addPressed)
				{
					GUILayoutOption[] field_options = new GUILayoutOption[1];
					field_options[0] = GUILayout.MaxWidth(50);
					GUILayout.Label("Address:");
					newHost = GUILayout.TextField(newHost);
					GUILayout.Label("Port:");
					newPort = GUILayout.TextField(newPort,field_options);
					bool addHostPressed = GUILayout.Button("Add",field_options);
					if (addHostPressed)
					{
						KMPClientMain.SetServer(newHost.Trim());
						ArrayList favorites = KMPClientMain.GetFavorites();

						if (favorites.Contains(newHost.Trim() + ":" + newPort.Trim()))
						{
							ScreenMessages.PostScreenMessage("This server is already on the list", 300f, ScreenMessageStyle.UPPER_CENTER);
						}
						else
						{
							String sHostname = newHost.Trim() + ":" + newPort.Trim();
							favorites.Add(sHostname);

							//Close the add server bar after a server has been added and select the new server
							addPressed = false;
							KMPConnectionDisplay.activeHostname = sHostname;
							KMPClientMain.SetFavorites(favorites);
						}
					}
				}
			GUILayout.EndHorizontal();
			
			GUILayout.BeginHorizontal();
			
				GUILayoutOption[] connection_list_options = new GUILayoutOption[1];
				connection_list_options[0] = GUILayout.MinWidth(290);
			
				GUILayout.BeginVertical(connection_list_options);

					GUILayout.BeginHorizontal();
						GUILayoutOption[] label_options = new GUILayoutOption[1];
						label_options[0] = GUILayout.MinWidth(75);
						GUILayout.Label("Username:", label_options);
						KMPClientMain.SetUsername(GUILayout.TextField(KMPClientMain.GetUsername()));
					GUILayout.EndHorizontal();
			
					KMPConnectionDisplay.scrollPos = GUILayout.BeginScrollView(KMPConnectionDisplay.scrollPos, connection_list_options);
						foreach (String hostname in KMPClientMain.GetFavorites())
						{
							if (!String.IsNullOrEmpty(hostname))
								connectionButton(hostname);
						}
					GUILayout.EndScrollView();
			
				GUILayout.EndVertical();
			
				GUILayoutOption[] pane_options = new GUILayoutOption[1];
				pane_options[0] = GUILayout.MaxWidth(50);
			
				GUILayout.BeginVertical(pane_options);
			
					bool allowConnect = true;
					if (String.IsNullOrEmpty(KMPConnectionDisplay.activeHostname) || String.IsNullOrEmpty(KMPClientMain.GetUsername()))
						allowConnect = false;
			
					if (!allowConnect) GUI.enabled = false;
					bool connectPressed = GUILayout.Button("Connect");
					GUI.enabled = true;
			
					if (connectPressed && allowConnect)
					{
						KMPClientMain.SetMessage("");
						KMPClientMain.SetServer(KMPConnectionDisplay.activeHostname);
						KMPClientMain.Connect();
					}
					
					if (KMPClientMain.GetFavorites().Count < 1) addPressed = true;
					
					addPressed = GUILayout.Toggle(
						addPressed,
						"Add Server",
						GUI.skin.button);
					
					if (String.IsNullOrEmpty(KMPConnectionDisplay.activeHostname)) GUI.enabled = false;
					bool deletePressed = GUILayout.Button("Remove");
					if (deletePressed)
					{
						ArrayList favorites = KMPClientMain.GetFavorites();
						if (favorites.Contains(KMPConnectionDisplay.activeHostname))
						{
							favorites.Remove(KMPConnectionDisplay.activeHostname);
							KMPConnectionDisplay.activeHostname = "";
							KMPClientMain.SetFavorites(favorites);
						}
					}
					GUI.enabled = true;
			
				GUILayout.EndVertical();
			
			GUILayout.EndHorizontal();
			
			GUILayout.BeginHorizontal();
				GUILayout.BeginVertical();
					GUILayoutOption[] status_options = new GUILayoutOption[1];
					status_options[0] = GUILayout.MaxWidth(310);

					if (String.IsNullOrEmpty(KMPClientMain.GetUsername()))
						GUILayout.Label("Please specify a username", status_options);
					else if (String.IsNullOrEmpty(KMPConnectionDisplay.activeHostname))
						GUILayout.Label("Please add or select a server", status_options);
					else if (!KMPClientMain.startSaveExists())
						GUILayout.Label("ERROR!  Start save missing!  Verify client installation!", status_options);
					else
						GUILayout.Label(KMPClientMain.message, status_options);
			
				GUILayout.EndVertical();
			GUILayout.EndHorizontal();
		}
		
		private void screenshotWindow(int windowID)
		{

			GUILayout.BeginHorizontal();
			GUILayout.BeginVertical();

			GUILayoutOption[] screenshot_box_options = new GUILayoutOption[4];
			screenshot_box_options[0] = GUILayout.MinWidth(KMPScreenshotDisplay.screenshotSettings.maxWidth);
			screenshot_box_options[1] = GUILayout.MaxWidth(KMPScreenshotDisplay.screenshotSettings.maxWidth);
			screenshot_box_options[2] = GUILayout.MinHeight(KMPScreenshotDisplay.screenshotSettings.maxHeight);
			screenshot_box_options[3] = GUILayout.MaxHeight(KMPScreenshotDisplay.screenshotSettings.maxHeight);

			//Init label styles
			screenshotDescriptionStyle = new GUIStyle(GUI.skin.label);
			screenshotDescriptionStyle.normal.textColor = Color.white;
			screenshotDescriptionStyle.alignment = TextAnchor.MiddleCenter;
			screenshotDescriptionStyle.stretchWidth = true;
			screenshotDescriptionStyle.fontStyle = FontStyle.Normal;
			screenshotDescriptionStyle.margin.bottom = 0;
			screenshotDescriptionStyle.margin.top = 0;
			screenshotDescriptionStyle.padding.bottom = 0;
			screenshotDescriptionStyle.padding.top = 4;

			//Screenshot
			if (KMPScreenshotDisplay.texture != null)
			{
				GUILayout.Box(KMPScreenshotDisplay.texture, screenshot_box_options);
				GUILayout.Label(KMPScreenshotDisplay.description, screenshotDescriptionStyle);
			}
			else
				GUILayout.Box(GUIContent.none, screenshot_box_options);

			GUILayoutOption[] user_list_options = new GUILayoutOption[1];
			user_list_options[0] = GUILayout.MinWidth(150);

			GUILayout.EndVertical();

			//User list
			KMPScreenshotDisplay.scrollPos = GUILayout.BeginScrollView(KMPScreenshotDisplay.scrollPos, user_list_options);
			GUILayout.BeginVertical();

			foreach (KeyValuePair<String, VesselStatusInfo> pair in playerStatus)
			{
				screenshotWatchButton(pair.Key);
			}

			GUILayout.EndVertical();
			GUILayout.EndScrollView();

			GUILayout.EndHorizontal();

			GUI.DragWindow();
		}

		private void chatWindow(int windowID)
		{
			//Stay on map screen if in flight to prevent accidental input
			//if (isInFlight && !MapView.MapIsEnabled) MapView.EnterMapView();
			
			chatMessagesWaiting = 0;
			
			//Init label styles
			chatLineStyle = new GUIStyle(GUI.skin.label);
			chatLineStyle.normal.textColor = Color.white;
			chatLineStyle.margin = new RectOffset(0, 0, 0, 0);
			chatLineStyle.padding = new RectOffset(0, 0, 0, 0);
			chatLineStyle.alignment = TextAnchor.LowerLeft;
			chatLineStyle.wordWrap = true;
			chatLineStyle.stretchWidth = true;
			chatLineStyle.fontStyle = FontStyle.Normal;

			GUILayoutOption[] entry_field_options = new GUILayoutOption[1];
			entry_field_options[0] = GUILayout.MaxWidth(KMPChatDisplay.windowWidth-58);

			GUIStyle chat_entry_style = new GUIStyle(GUI.skin.textField);
			chat_entry_style.stretchWidth = true;

			GUILayout.BeginVertical();

			//Mode toggles
			GUILayout.BeginHorizontal();
			KMPGlobalSettings.instance.chatWindowWide = GUILayout.Toggle(KMPGlobalSettings.instance.chatWindowWide, "Wide", GUI.skin.button);
			KMPChatDisplay.displayCommands = GUILayout.Toggle(KMPChatDisplay.displayCommands, "Help", GUI.skin.button);
			GUILayout.EndHorizontal();

			//Commands
			if (KMPChatDisplay.displayCommands)
			{
				chatLineStyle.normal.textColor = Color.white;

				GUILayout.Label("/quit - Leave the server", chatLineStyle);
				GUILayout.Label(KMPCommon.SHARE_CRAFT_COMMAND + " <craftname> - Share a craft", chatLineStyle);
				GUILayout.Label(KMPCommon.GET_CRAFT_COMMAND + " <playername> - Get the craft the player last shared", chatLineStyle);
				GUILayout.Label("!list - View players on the server", chatLineStyle);
			}

			KMPChatDisplay.scrollPos = GUILayout.BeginScrollView(KMPChatDisplay.scrollPos);

			//Chat text
			GUILayout.BeginVertical();

			foreach (KMPChatDisplay.ChatLine line in KMPChatDisplay.chatLineQueue)
			{
				if (KMPGlobalSettings.instance.chatColors)
					chatLineStyle.normal.textColor = line.color;
				GUILayout.Label(line.message, chatLineStyle);
			}

			GUILayout.EndVertical();

			GUILayout.EndScrollView();

			GUILayout.BeginHorizontal();

			//Entry text field
			KMPChatDisplay.chatEntryString = GUILayout.TextField(
				KMPChatDisplay.chatEntryString,
				KMPChatDisplay.MAX_CHAT_LINE_LENGTH,
				chat_entry_style,
				entry_field_options);

			if (KMPChatDisplay.chatEntryString.Contains('\n') || GUILayout.Button("Send"))
			{
				enqueueChatOutMessage(KMPChatDisplay.chatEntryString);
				KMPChatDisplay.chatEntryString = String.Empty;
			}

			GUILayout.EndHorizontal();

			GUILayout.EndVertical();

			GUI.DragWindow();
		}

        private void chatWindowDX(int windowID)
        {
            // Show the button for everyone.
            platform = PlatformID.Unix;

            GUILayoutOption[] entry_field_options = new GUILayoutOption[2];
            
            entry_field_options[0] = GUILayout.MaxWidth(KMPChatDX.chatboxWidth);
            entry_field_options[1] = GUILayout.MinWidth(KMPChatDX.chatboxWidth);

            if (platform == PlatformID.Unix)
            {
                entry_field_options[0] = GUILayout.MaxWidth(KMPChatDX.chatboxWidth - 75);
                entry_field_options[1] = GUILayout.MinWidth(KMPChatDX.chatboxWidth - 75);
            }

            GUIStyle chat_entry_style = new GUIStyle(GUI.skin.textField);
            chat_entry_style.stretchWidth = true;
            chat_entry_style.alignment = TextAnchor.LowerLeft;

            /* Display Chat */


        

            GUILayout.BeginVertical();
            GUILayout.MinHeight(KMPChatDX.chatboxHeight);
            GUILayout.Space(1);

            KMPChatDX.setStyle();

            GUILayout.FlexibleSpace();


            foreach (KMPChatDX.ChatLine line in KMPChatDX.chatLineQueue)
            {
                GUILayout.BeginHorizontal();
                KMPChatDX.chatStyle.normal.textColor = line.color;
                if (line.name == "")
                {
                    GUILayout.Label(line.message, KMPChatDX.chatStyle);

                    var position = GUILayoutUtility.GetLastRect();

                    var style = KMPChatDX.chatStyle;
                    style.normal.textColor = new Color(0, 0, 0);

                    position.x--;
                    GUI.Label(position, line.message, style);
                    position.x += 2;
                    GUI.Label(position, line.message, style);
                    position.x--;
                    position.y--;
                    GUI.Label(position, line.message, style);
                    position.y += 2;
                    GUI.Label(position, line.message, style);

                    KMPChatDX.chatStyle.normal.textColor = line.color;
                    position.y--;
                    GUI.Label(position, line.message, style);
                }
                else
                {
                    var text = line.name + ": " + line.message;
                    GUILayout.Label(text, KMPChatDX.chatStyle);

                    var position = GUILayoutUtility.GetLastRect();

                    var style = KMPChatDX.chatStyle;
                    style.normal.textColor = new Color(0, 0, 0);

                    position.x--;
                    GUI.Label(position, text, style);
                    position.x += 2;
                    GUI.Label(position, text, style);
                    position.x--;
                    position.y--;
                    GUI.Label(position, text, style);
                    position.y += 2;
                    GUI.Label(position, text, style);

                    KMPChatDX.chatStyle.normal.textColor = line.color;
                    position.y--;
                    GUI.Label(position, text, style);

                }

                GUILayout.EndHorizontal();
                GUILayout.Space(1);
            }


            if(KMPChatDX.showInput)
            {
                GUI.SetNextControlName("inputField");
                //Entry text field

                GUILayout.BeginHorizontal();

                KMPChatDX.chatEntryString = GUILayout.TextField(
                    KMPChatDX.chatEntryString,
                    KMPChatDX.MAX_CHAT_LINE_LENGTH,
                    chat_entry_style,
                    entry_field_options);

                if (KMPChatDX.chatEntryString.Contains('\n') || (platform == PlatformID.Unix && GUILayout.Button("Send")))
                {
                    enqueueChatOutMessage(KMPChatDX.chatEntryString);
                    KMPChatDX.chatEntryString = String.Empty;
                    KMPChatDX.showInput = false;
                }

                if (GUI.GetNameOfFocusedControl() != "inputField")
                {
                    GUI.FocusControl("inputField");
                }

                GUILayout.EndHorizontal();
            }

            GUILayout.EndVertical();

            if (KMPChatDX.draggable)
            {
                GUI.depth = 0;
                GUI.BringWindowToFront(windowID);
                GUI.DragWindow();
            }
            else
            {
                GUI.depth = 2;
                GUI.BringWindowToBack(windowID);
            }
        }



		private void vesselStatusLabels(VesselStatusInfo status, bool big)
		{
			bool name_pressed = false;
			bool showSync = false;
			playerNameStyle.normal.textColor = status.color * 0.75f + Color.white * 0.25f;

			if (big)
				GUILayout.BeginHorizontal();

			if (status.ownerName != null)
				name_pressed |= GUILayout.Button(status.ownerName, playerNameStyle);

			if (status.vesselName != null && status.vesselName.Length > 0)
			{
				String vessel_name = status.vesselName;
				
				if (status.currentSubspaceID > 0)
				{
					showSync = true;
					showServerSync = true;
				}
				
				if (status.info != null && status.info.detail != null && status.info.detail.idle)
					vessel_name = "(Idle) " + vessel_name;

				name_pressed |= GUILayout.Button(vessel_name, vesselNameStyle);
			}
			
			GUIStyle syncButtonStyle = new GUIStyle(GUI.skin.button);
			syncButtonStyle.normal.textColor = new Color(0.28f, 0.86f, 0.94f);
			syncButtonStyle.hover.textColor = new Color(0.48f, 0.96f, 0.96f);
			syncButtonStyle.margin = new RectOffset(150,10,0,0);
			syncButtonStyle.fixedHeight = 22f;
			
			if (big)
			{
				GUILayout.EndHorizontal();
				GUILayout.BeginHorizontal();
			}
			bool syncRequest = false;
			if (!isInFlight) GUI.enabled = false;
			if (showSync && FlightGlobals.ActiveVessel.ctrlState.mainThrottle == 0f) syncRequest |= GUILayout.Button("Sync",syncButtonStyle);
			GUI.enabled = true;
			
			if (big)
				GUILayout.EndHorizontal();
			
			//Build the detail text
			StringBuilder sb = new StringBuilder();

			//Check if the status has specific detail text
			if (status.detailText != null && status.detailText.Length > 0 && KMPInfoDisplay.infoDisplayDetailed)
				sb.Append(status.detailText);
			else if (status.info != null && status.info.detail != null)
			{

				bool exploded = false;
				bool situation_determined = false;

				if (status.info.situation == Situation.DESTROYED || status.info.detail.mass <= 0.0f)
				{
					sb.Append("Exploded at ");
					exploded = true;
					situation_determined = true;
				}
				else
				{

					//Check if the vessel's activity overrides the situation
					switch (status.info.detail.activity)
					{
						case Activity.AEROBRAKING:
							sb.Append("Aerobraking at ");
							situation_determined = true;
							break;

						case Activity.DOCKING:
							if (KMPVessel.situationIsGrounded(status.info.situation))
								sb.Append("Docking on ");
							else
								sb.Append("Docking above ");
							situation_determined = true;
							break;

						case Activity.PARACHUTING:
							sb.Append("Parachuting to ");
							situation_determined = true;
							break;
					}

					if (!situation_determined)
					{
						switch (status.info.situation)
						{
							case Situation.DOCKED:
								sb.Append("Docked at ");
								break;

							case Situation.ENCOUNTERING:
								sb.Append("Encountering ");
								break;

							case Situation.ESCAPING:
								sb.Append("Escaping ");
								break;

							case Situation.FLYING:
								sb.Append("Flying at ");
								break;

							case Situation.LANDED:
								sb.Append("Landed at ");
								break;

							case Situation.ORBITING:
								sb.Append("Orbiting ");
								break;

							case Situation.PRELAUNCH:
								sb.Append("Prelaunch at ");
								break;

							case Situation.SPLASHED:
								sb.Append("Splashed at ");
								break;

							case Situation.ASCENDING:
								sb.Append("Ascending from ");
								break;

							case Situation.DESCENDING:
								sb.Append("Descending to ");
								break;
						}
					}

				}

				sb.Append(status.info.bodyName);

				if (!exploded && KMPInfoDisplay.infoDisplayDetailed)
				{

					bool show_mass = status.info.detail.mass >= 0.05f;
					bool show_fuel = status.info.detail.percentFuel < byte.MaxValue;
					bool show_rcs = status.info.detail.percentRCS < byte.MaxValue;
					bool show_crew = status.info.detail.numCrew < byte.MaxValue;

					if (show_mass || show_fuel || show_rcs || show_crew)
						sb.Append(" - ");

					if (show_mass)
					{
						sb.Append("Mass: ");
						sb.Append(status.info.detail.mass.ToString("0.0"));
						sb.Append(' ');
					}

					if (show_fuel)
					{
						sb.Append("Fuel: ");
						sb.Append(status.info.detail.percentFuel);
						sb.Append("% ");
					}

					if (show_rcs)
					{
						sb.Append("RCS: ");
						sb.Append(status.info.detail.percentRCS);
						sb.Append("% ");
					}

					if (show_crew)
					{
						sb.Append("Crew: ");
						sb.Append(status.info.detail.numCrew);
					}
				}

			}

			if (sb.Length > 0)
				GUILayout.Label(sb.ToString(), stateTextStyle);
			
			//If the name was pressed, then focus on that players' reference body
			if (name_pressed
				&& HighLogic.LoadedSceneHasPlanetarium && planetariumCam != null
					&& status.info != null
					&& status.info.bodyName.Length > 0)
			{
				if (!MapView.MapIsEnabled)
					MapView.EnterMapView();

				foreach (MapObject target in planetariumCam.targets)
				{
					if (target.name == status.info.bodyName)
					{
						planetariumCam.SetTarget(target);
						break;
					}
				}
			}
		
			if (syncRequest)
				StartCoroutine(sendSubspaceSyncRequest(status.currentSubspaceID));
		}

		private void screenshotWatchButton(String name)
		{
			bool player_selected = GUILayout.Toggle(KMPScreenshotDisplay.watchPlayerName == name, name, GUI.skin.button);
			if (player_selected != (KMPScreenshotDisplay.watchPlayerName == name))
			{
				if (KMPScreenshotDisplay.watchPlayerName != name)
					KMPScreenshotDisplay.watchPlayerName = name; //Set watch player name
				else
					KMPScreenshotDisplay.watchPlayerName = String.Empty;

				lastPluginDataWriteTime = 0.0f; //Force re-write of plugin data
			}
		}
		
		private void connectionButton(String name)
		{
			bool player_selected = GUILayout.Toggle(KMPConnectionDisplay.activeHostname == name, name, GUI.skin.button);
			if (player_selected != (KMPConnectionDisplay.activeHostname == name))
			{
				if (KMPConnectionDisplay.activeHostname != name)
					KMPConnectionDisplay.activeHostname = name;
				else
					KMPConnectionDisplay.activeHostname = String.Empty;
			}
		}
		
		private Rect enforceWindowBoundaries(Rect window)
		{
			const int padding = 20;

			if (window.x < -window.width + padding)
				window.x = -window.width + padding;

			if (window.x > Screen.width - padding)
				window.x = Screen.width - padding;

			if (window.y < -window.height + padding)
				window.y = -window.height + padding;

			if (window.y > Screen.height - padding)
				window.y = Screen.height - padding;

			return window;
		}
		
		private bool isInSafetyBubble(Vector3d pos, CelestialBody body, double altitude)
		{
			//Assume Kerbin if body isn't supplied for some reason
			if (body == null) body = FlightGlobals.Bodies.Find(b => b.name == "Kerbin");
			
			//If KSC out of range, syncing, not at Kerbin, or past ceiling we're definitely clear
			if (kscPosition == Vector3d.zero || syncing || body.name != "Kerbin" || altitude > SAFETY_BUBBLE_CEILING)
				return false;
			
			//Cylindrical safety bubble -- project vessel position to a plane positioned at KSC with normal pointed away from surface
			Vector3d kscNormal = body.GetSurfaceNVector(-0.102668048654,-74.5753856554);
			double projectionDistance = Vector3d.Dot(kscNormal, (pos - kscPosition)) * -1;
			Vector3d projectedPos = pos + (Vector3d.Normalize(kscNormal)*projectionDistance);
			
			return Vector3d.Distance(kscPosition, projectedPos) < safetyBubbleRadius;
		}
		
		public double horizontalDistanceToSafetyBubbleEdge()
		{
			CelestialBody body = FlightGlobals.ActiveVessel.mainBody;
			Vector3d pos = FlightGlobals.ship_position;
			double altitude = FlightGlobals.ActiveVessel.altitude;
			//Assume Kerbin if body isn't supplied for some reason
			if (body == null) body = FlightGlobals.Bodies.Find(b => b.name == "Kerbin");
			
			//If KSC out of range, syncing, not at Kerbin, or past ceiling we're definitely clear
			if (kscPosition == Vector3d.zero || syncing || body.name != "Kerbin" || altitude > SAFETY_BUBBLE_CEILING)
				return -1d;
			
			//Cylindrical safety bubble -- project vessel position to a plane positioned at KSC with normal pointed away from surface
			Vector3d kscNormal = body.GetSurfaceNVector(-0.102668048654,-74.5753856554);
			double projectionDistance = Vector3d.Dot(kscNormal, (pos - kscPosition)) * -1;
			Vector3d projectedPos = pos + (Vector3d.Normalize(kscNormal)*projectionDistance);
			
			return safetyBubbleRadius - Vector3d.Distance(kscPosition, projectedPos);
		}
		
		public double verticalDistanceToSafetyBubbleEdge()
		{
			CelestialBody body = FlightGlobals.ActiveVessel.mainBody;
			double altitude = FlightGlobals.ActiveVessel.altitude;
			//Assume Kerbin if body isn't supplied for some reason
			if (body == null) body = FlightGlobals.Bodies.Find(b => b.name == "Kerbin");
			
			//If KSC out of range, syncing, not at Kerbin, or past ceiling we're definitely clear
			if (kscPosition == Vector3d.zero || syncing || body.name != "Kerbin" || altitude > SAFETY_BUBBLE_CEILING)
				return -1d;
			
			
			return SAFETY_BUBBLE_CEILING - altitude;
		}
		
		//This code adapted from Kerbal Engineer Redux source
		private void CheckEditorLock()
		{
			Vector2 mousePos = Input.mousePosition;
			mousePos.y = Screen.height - mousePos.y;

			bool should_lock = HighLogic.LoadedSceneIsEditor && shouldDrawGUI && (
					KMPInfoDisplay.infoWindowPos.Contains(mousePos)
					|| (KMPScreenshotDisplay.windowEnabled && KMPScreenshotDisplay.windowPos.Contains(mousePos))
					|| (KMPGlobalSettings.instance.chatDXWindowEnabled && KMPChatDisplay.windowPos.Contains(mousePos))
					);

			if (should_lock && !isEditorLocked && !EditorLogic.editorLocked)
			{
				EditorLogic.fetch.Lock(true, true, true);
				isEditorLocked = true;
			}
			else if (!should_lock && isEditorLocked && EditorLogic.editorLocked)
			{
				EditorLogic.fetch.Unlock();
				isEditorLocked = false;
			}
		}
        
		private Krakensbane getKrakensbane()
        {
			if (krakensbane == null) krakensbane = (Krakensbane)FindObjectOfType(typeof(Krakensbane));
			return krakensbane;
        }
	}
}
