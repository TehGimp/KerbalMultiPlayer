using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;
using System.Xml.Serialization;
using System.Collections;
using System.Threading;

namespace KMP
{
    public class LoadedFileInfo
    {
        /// <summary>
        /// The full absolute path on this computer. Uses the local system's directory separator character ('/' for Unix, '\' for Windows).
        /// </summary>
        public string FullPath;
        /// <summary>
        /// The relative path starting at the KSP main directory. Uses Unix directory separator ('/') so it will match the server's mod config file.
        /// </summary>
        public string LoadedPath;
        /// <summary>
        /// The directory that this mod has been loaded into (usually 'GameData', but could be 'Plugins' or 'Parts').
        /// </summary>
        public string ModDirectory;
        /// <summary>
        /// The relative path starting in the 'GameData', 'Plugins', or 'Parts' directory (will not have that directory name included). Uses Unix directory separator ('/') so it will match the server's mod config file.
        /// </summary>
        public string ModPath;
        /// <summary>
        /// The SHA256 hash of this file.
        /// </summary>
        public string SHA256;

        public LoadedFileInfo(string filepath)
        {
            FullPath = filepath.Replace('\\', '/');
            string location = new System.IO.DirectoryInfo(KSPUtil.ApplicationRootPath).FullName;
            LoadedPath = filepath.Substring(location.Length).Replace('\\', '/');
            ModPath = LoadedPath.Substring(LoadedPath.IndexOf('/') + 1); // +1 is to cut off remaining directory separator character
            ModDirectory = LoadedPath.Substring(0, LoadedPath.IndexOf('/'));
        }

        public void HandleHash(System.Object state)
        {
            try
            {
                using (System.IO.Stream hashStream = new System.IO.FileStream(FullPath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))
                {
                    using (System.Security.Cryptography.SHA256Managed sha = new System.Security.Cryptography.SHA256Managed())
                    {
                        byte[] hash = sha.ComputeHash(hashStream);
                        SHA256 = BitConverter.ToString(hash).Replace("-", String.Empty);
                    }
                    Log.Debug("Added and hashed: " + ModPath + "=" + SHA256);
                }
            }
            catch (Exception e)
            {
                Log.Debug("Failed to hash: " + ModPath + ", exception: " + e.Message.ToString());
            }
            if (Interlocked.Decrement(ref KMPManager.numberOfFilesToCheck) == 0)
            {
                Log.Debug("All SHA hashing completed!");
                KMPManager.ShaFinishedEvent.Set();
            }
        }
    }

    [KSPAddon(KSPAddon.Startup.Instantly, true)]
    public class KMPManager : MonoBehaviour
    {
		
        public KMPManager()
        {
            //Initialize client
            KMPClientMain.InitMPClient(this);
            Log.Debug("Client Initialized.");
        }

        public class VesselEntry
        {
            public KMPVessel vessel;
            public float lastUpdateTime;
        }

        public class VesselStatusInfo
        {
            public string ownerName;
            public string vesselName;
            public string detailText;
            public Color color;
            public KMPVesselInfo info;
            public Orbit orbit;
            public float lastUpdateTime;
            public int currentSubspaceID;
            public Guid vesselID;
        }
        //Singleton
        public static GameObject GameObjectInstance;
        //Properties
        public const String GLOBAL_SETTINGS_FILENAME = "globalsettings.txt";
        public const int MAX_SHA_THREADS = 20;
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
        public const float SCENARIO_UPDATE_INTERVAL = 30.0f;
        private const string SYNC_PLATE_ID = "14ccd14d-32d3-4f51-a021-cb020ca9cbfe";
        public const float FULL_PROTOVESSEL_UPDATE_TIMEOUT = 45f;
        public const double PRIVATE_VESSEL_MIN_TARGET_DISTANCE = 500d;
        public const ControlTypes BLOCK_ALL_CONTROLS = ControlTypes.ALL_SHIP_CONTROLS | ControlTypes.ACTIONS_ALL | ControlTypes.EVA_INPUT | ControlTypes.TIMEWARP | ControlTypes.MISC | ControlTypes.GROUPS_ALL | ControlTypes.CUSTOM_ACTION_GROUPS;
        public UnicodeEncoding encoder = new UnicodeEncoding();
        public String playerName = String.Empty;
        public byte inactiveVesselsPerUpdate = 0;
        public float updateInterval = 1.0f;
        public Dictionary<String, VesselEntry> vessels = new Dictionary<String, VesselEntry>();
        public SortedDictionary<String, VesselStatusInfo> playerStatus = new SortedDictionary<string, VesselStatusInfo>();
        public RenderingManager renderManager;
        public PlanetariumCamera planetariumCam;
        public PauseMenu pauseMenu;
        public static List<LoadedFileInfo> LoadedModfiles;
        public Queue<byte[]> interopInQueue = new Queue<byte[]>();
        public static object interopInQueueLock = new object();
        public int numberOfShips = 0;
        public int gameMode = 0;
        //0=Sandbox, 1=Career
        public bool gameCheatsEnabled = false;
        //Allow built-in KSP cheats
        public bool gameArrr = false;
        //Allow private vessels to be taken if other user can successfully dock manually
        public static int numberOfFilesToCheck = 0;
        public static ManualResetEvent ShaFinishedEvent;
        private float lastGlobalSettingSaveTime = 0.0f;
        private float lastPluginDataWriteTime = 0.0f;
        private float lastPluginUpdateWriteTime = 0.0f;
        private float lastKeyPressTime = 0.0f;
        private float lastFullProtovesselUpdate = 0.0f;
        private float lastScenarioUpdateTime = 0.0f;
        private float lastTimeSyncTime = 0.0f;
        public float lastSubspaceLockChange = 0.0f;
        //NTP-style time syncronize settings
        private bool isSkewingTime = false;
        private Int64 offsetSyncTick = 0;
        //The difference between the servers system clock and ours.
        private Int64 latencySyncTick = 0;
        //The network lag detected by NTP.
        private Int64 estimatedServerLag = 0;
        //The server lag detected by NTP.
        private List<Int64> listClientTimeSyncLatency = new List<Int64>();
        //Holds old sync time messages so we can filter bad ones
        private List<Int64> listClientTimeSyncOffset = new List<Int64>();
        //Holds old sync time messages so we can filter bad ones
        private List<float> listClientTimeWarp = new List<float>();
        //Holds the average time skew so we can tell the server how badly we are lagging.
        public float listClientTimeWarpAverage = 1;
        //Uses this varible to avoid locking the queue.
        private bool isTimeSyncronized;
        public bool displayNTP = false;
        //Show NTP stats on the client
        private const Int64 SYNC_TIME_LATENCY_FILTER = 5000000;
        //500 milliseconds, Must receive reply within this time or the message is discarded
        private const float SYNC_TIME_INTERVAL = 30f;
        //How often to sync time.
        private const int SYNC_TIME_VALID_COUNT = 4;
        //Number of SYNC_TIME's to receive until time is valid.
        private const int MAX_TIME_SYNC_HISTORY = 10;
        //The last 10 SYNC_TIME's are used for the offset filter.
        private ScreenMessage skewMessage;
        private ScreenMessage vesselLoadedMessage;
        private Queue<KMPVesselUpdate> vesselUpdateQueue = new Queue<KMPVesselUpdate>();
        GUIStyle playerNameStyle, vesselNameStyle, stateTextStyle, chatLineStyle, screenshotDescriptionStyle;
        private bool isEditorLocked = false;
        private bool mappingGUIToggleKey = false;
        private bool mappingScreenshotKey = false;
        private bool mappingScreenshotToggleKey = false;
        private bool mappingChatKey = false;
        private bool mappingChatDXToggleKey = false;
        private bool isGameHUDHidden = false;
        PlatformID platform;
        private bool addPressed = false;
        private string newHost = "localhost";
        private string newPort = "2076";
        private string newFamiliar = "Server";
        public bool forceQuit = false;
        public bool delayForceQuit = true;
        public bool gameStart = false;
        public bool terminateConnection = true;
        public bool gameRunning = false;
        private bool activeTermination = false;
        private bool clearEditorPartList = false;
        //Vessel dictionaries
        public Dictionary<Guid, Vessel.Situations> sentVessels_Situations = new Dictionary<Guid, Vessel.Situations>();
        public Dictionary<Guid, Guid> serverVessels_RemoteID = new Dictionary<Guid, Guid>();
        public Dictionary<Guid, int> serverVessels_PartCounts = new Dictionary<Guid, int>();
        //public Dictionary<Guid, List<Part>> serverVessels_Parts = new Dictionary<Guid, List<Part>>();
        public Dictionary<Guid, ConfigNode> serverVessels_ProtoVessels = new Dictionary<Guid, ConfigNode>();
        public Dictionary<Guid, bool> serverVessels_InUse = new Dictionary<Guid, bool>();
        public Dictionary<Guid, bool> serverVessels_IsPrivate = new Dictionary<Guid, bool>();
        public Dictionary<Guid, bool> serverVessels_IsMine = new Dictionary<Guid, bool>();
        public Dictionary<Guid, KeyValuePair<double,double>> serverVessels_LastUpdateDistanceTime = new Dictionary<Guid, KeyValuePair<double,double>>();
        public Dictionary<Guid, float> serverVessels_AddDelay = new Dictionary<Guid, float>();
        public Dictionary<Guid, bool> serverVessels_InPresent = new Dictionary<Guid, bool>();
        public Dictionary<Guid, float> newFlags = new Dictionary<Guid, float>();
        public Dictionary<uint, int> serverParts_CrewCapacity = new Dictionary<uint, int>();
        public double lastTick = 0d;
        public double skewTargetTick = 0;
        public long skewServerTime = 0;
        public float skewSubspaceSpeed = 1f;
        public Vector3d kscPosition = Vector3d.zero;
        public Vector3 activeVesselPosition = Vector3d.zero;
        public GameObject ksc = null;
        private bool warping = false;
        private bool syncing = false;
        private bool docking = false;
        private float lastWarpRate = 1f;
        private int chatMessagesWaiting = 0;
        private Vessel lastEVAVessel = null;
        private bool showServerSync = false;
        private bool inGameSyncing = false;
        private List<Guid> vesselUpdatesHandled = new List<Guid>();
        private bool configRead = false;
        public double safetyBubbleRadius = 2000d;
        public double minSafetyBubbleRadius = 100d;
        private bool safetyTransparency;
        private bool isVerified = false;
        private ToolbarButtonWrapper KMPToggleButton;
        private bool KMPToggleButtonState = true;
        private bool KMPToggleButtonInitialized;
        public static bool showConnectionWindow = false;

        public bool globalUIToggle
        {
            get
            {
                if (renderManager == null)
                {
                    return true;
                }
                return renderManager.uiElementsToDisable.Length < 1 || renderManager.uiElementsToDisable[0].activeSelf;
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
                        return KMPInfoDisplay.infoDisplayActive && globalUIToggle && KMPToggleButtonState;

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

        public bool isObserving
        {
            get
            {

                bool isInUse = serverVessels_InUse.ContainsKey(FlightGlobals.ActiveVessel.id) ? serverVessels_InUse[FlightGlobals.ActiveVessel.id] : false;
                bool isPrivate = serverVessels_IsPrivate.ContainsKey(FlightGlobals.ActiveVessel.id) ? serverVessels_IsPrivate[FlightGlobals.ActiveVessel.id] : false;
                bool isMine = serverVessels_IsMine.ContainsKey(FlightGlobals.ActiveVessel.id) ? serverVessels_IsMine[FlightGlobals.ActiveVessel.id] : false;
                return isInFlight && isInUse && (isPrivate || !isMine);
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
                {
                    //Don't do anything while the game is loading or not in KMP game
                    return;
                }
				
                if (syncing)
                {
                    if (vesselLoadedMessage != null)
                    {
                        vesselLoadedMessage.duration = 0f;
                    }
                    if (isTimeSyncronized)
                    {
                        if (!inGameSyncing)
                        {
                            if (numberOfShips != 0)
                            {
                                vesselLoadedMessage = ScreenMessages.PostScreenMessage("Synchronizing vessels: " + vesselUpdatesHandled.Count + "/" + numberOfShips + " (" + (vesselUpdatesHandled.Count * 100 / numberOfShips) + "%)", 1f, ScreenMessageStyle.UPPER_RIGHT);
                            }
                            else
                            {
                                vesselLoadedMessage = ScreenMessages.PostScreenMessage("Synchronized new universe!", 1f, ScreenMessageStyle.UPPER_RIGHT);
                            }
                        }
                        else
                        {
                            vesselLoadedMessage = ScreenMessages.PostScreenMessage("Synchronizing vessels: " + FlightGlobals.Vessels.Count, 1f, ScreenMessageStyle.UPPER_RIGHT);
                        }
                    }
                    else
                    {
                        vesselLoadedMessage = ScreenMessages.PostScreenMessage("Synchronizing to server clock: " + listClientTimeSyncOffset.Count + "/" + SYNC_TIME_VALID_COUNT + " (" + (listClientTimeSyncOffset.Count * 100 / SYNC_TIME_VALID_COUNT) + "%)", 1f, ScreenMessageStyle.UPPER_RIGHT);
                    }
                }
				
                if (!isInFlight && HighLogic.LoadedScene == GameScenes.TRACKSTATION)
                {
                    try
                    {
                        //Check this out later, It threw errors in the tracking station and may be what we need to update the tracking list.
                        //KnowledgeBase kb = (KnowledgeBase)GameObject.FindObjectOfType(typeof(KnowledgeBase));

                        SpaceTracking st = (SpaceTracking)GameObject.FindObjectOfType(typeof(SpaceTracking));
						
                        if (st.mainCamera.target.vessel != null && (serverVessels_IsMine[st.mainCamera.target.vessel.id] || !serverVessels_IsPrivate[st.mainCamera.target.vessel.id]))
                        {
                            //Public/owned vessel
                            st.FlyButton.Unlock();
                            st.DeleteButton.Unlock();
                            if (st.mainCamera.target.vessel.mainBody.bodyName == "Kerbin" && (st.mainCamera.target.vessel.situation == Vessel.Situations.LANDED || st.mainCamera.target.vessel.situation == Vessel.Situations.SPLASHED))
                                st.RecoverButton.Unlock();
                            else
                                st.RecoverButton.Lock();
                        }
                        else
                        {
                            //Private unowned vessel
                            st.FlyButton.Lock();
                            st.DeleteButton.Lock();
                            st.RecoverButton.Lock();
                        }
                    }
                    catch
                    {
                    }
                }
				
                if (lastWarpRate != TimeWarp.CurrentRate)
                {
                    lastWarpRate = TimeWarp.CurrentRate;
                    OnTimeWarpRateChanged();	
                }
				
                if (warping)
                {
                    writeUpdates();
                    return;
                }
				
                if (EditorPartList.Instance != null && clearEditorPartList)
                {
                    clearEditorPartList = false;
                    EditorPartList.Instance.Refresh();
                }
				
                if (syncing)
                    lastScenarioUpdateTime = UnityEngine.Time.realtimeSinceStartup;
                else if ((UnityEngine.Time.realtimeSinceStartup - lastScenarioUpdateTime) >= SCENARIO_UPDATE_INTERVAL)
                {
                    sendScenarios();
                }
				
                //Update vessel names in the tracking station
                if (!isInFlight)
                {
                    foreach (Vessel vessel in FlightGlobals.fetch.vessels)
                    {
                        if (vessel.id.ToString() != SYNC_PLATE_ID)
                        {
                            string baseName = vessel.vesselName;
                            if (baseName.StartsWith("* "))
                                baseName = baseName.Substring(2);
                            bool inUse = serverVessels_InUse.ContainsKey(vessel.id) ? serverVessels_InUse[vessel.id] : false;
                            bool isPrivate = serverVessels_IsPrivate.ContainsKey(vessel.id) ? serverVessels_IsPrivate[vessel.id] : false;
                            bool isMine = serverVessels_IsMine.ContainsKey(vessel.id) ? !serverVessels_IsMine[vessel.id] : false;
                            if (inUse && !isMine || isPrivate && !isMine)
                            {
                                vessel.name = "* " + baseName;
                            }
                            else
                            {
                                vessel.name = baseName;
                            }
                        }
                    }
                }
                else //Kill Kraken-debris, clean names
                {
                    foreach (Vessel vessel in FlightGlobals.fetch.vessels.FindAll(v => v.vesselName.Contains("> ") || (v.vesselName.Contains("<") && !v.vesselName.Contains(">"))))
                    {
                        try
                        {
                            if (!vessel.isEVA)
                            {
                                Log.Debug("Killed kraken debris: " + vessel.name);
                                killVessel(vessel.id);
                                sendRemoveVesselMessage(vessel, false);
                            }
                        }
                        catch (Exception e)
                        {
                            Log.Debug("Exception thrown in updateStep(), catch 1, Exception: {0}", e.ToString());
                        }
                    }
                }
				
                //Ensure player never touches something under another player's control
                bool controlsLocked = false;
                if (isInFlight && !docking && serverVessels_InUse.ContainsKey(FlightGlobals.ActiveVessel.id))
                {
                    if (serverVessels_InUse[FlightGlobals.ActiveVessel.id])
                    {
                        ScreenMessages.PostScreenMessage("This vessel is currently controlled by another player...", 2.5f, ScreenMessageStyle.UPPER_CENTER);
                        InputLockManager.SetControlLock(BLOCK_ALL_CONTROLS, "KMP_Occupied");
                        controlsLocked = true;
                    }
                    else
                    {
                        InputLockManager.RemoveControlLock("KMP_Occupied");
                    }
                }
				
                //Ensure player never touches a private vessel they don't own
                if (isInFlight && !docking && serverVessels_IsPrivate.ContainsKey(FlightGlobals.ActiveVessel.id) && serverVessels_IsMine.ContainsKey(FlightGlobals.ActiveVessel.id))
                {
                    if (!serverVessels_IsMine[FlightGlobals.ActiveVessel.id] && serverVessels_IsPrivate[FlightGlobals.ActiveVessel.id])
                    {
                        ScreenMessages.PostScreenMessage("This vessel is private...", 2.5f, ScreenMessageStyle.UPPER_CENTER);
                        InputLockManager.SetControlLock(BLOCK_ALL_CONTROLS, "KMP_Private");
                        controlsLocked = true;
                    }
                    else
                    {
                        InputLockManager.RemoveControlLock("KMP_Private");
                    }
                }
                if (isInFlight && !docking && FlightGlobals.fetch.VesselTarget != null)
                {
                    //Get targeted vessel
                    Vessel vesselTarget = null;
                    if (FlightGlobals.fetch.VesselTarget is ModuleDockingNode)
                    {
                        ModuleDockingNode moduleTarget = (ModuleDockingNode)FlightGlobals.fetch.VesselTarget;
                        if (moduleTarget.part.vessel != null)
                        {
                            vesselTarget = moduleTarget.part.vessel;
                        }
                    }
                    if (FlightGlobals.fetch.VesselTarget is Vessel)
                    {
                        vesselTarget = (Vessel)FlightGlobals.fetch.VesselTarget;
                    }

                    if (vesselTarget != null)
                    {

                        double distanceToTarget = Vector3d.Distance(vesselTarget.GetWorldPos3D(), FlightGlobals.ship_position);

                        //Check if target is private and too close
                        if (distanceToTarget < PRIVATE_VESSEL_MIN_TARGET_DISTANCE && serverVessels_IsPrivate.ContainsKey(vesselTarget.id) && serverVessels_IsMine.ContainsKey(vesselTarget.id))
                        {
                            if (!serverVessels_IsMine[vesselTarget.id] && serverVessels_IsPrivate[vesselTarget.id])
                            {
                                Log.Debug("Tried to target private vessel");
                                ScreenMessages.PostScreenMessage("Can't dock - Target vessel is Private", 4f, ScreenMessageStyle.UPPER_CENTER);
                                FlightGlobals.fetch.SetVesselTarget(null);
                            }
                        }
                    }
                }
                if (isInFlight && !docking && !gameArrr)
                {
                    foreach (Vessel possible_target in FlightGlobals.Vessels.ToList())
                    {
                        if (possible_target.id.ToString() != SYNC_PLATE_ID)
                        {
                            checkVesselPrivacy(possible_target);
                        }
                    }
                }

                //Reset the safety bubble transparency setting so it works when we go back into flight.
                if (!isInFlight)
                {
                    safetyTransparency = false;
                }

                //Let's let the user know they are actually inside the bubble.
                if (FlightGlobals.fetch.activeVessel != null)
                {
                    if (isInSafetyBubble(FlightGlobals.fetch.activeVessel.GetWorldPos3D()) != safetyTransparency)
                    {
                        safetyTransparency = !safetyTransparency;
                        foreach (Part part in FlightGlobals.fetch.activeVessel.parts)
                        {
                            if (safetyTransparency)
                            {
                                setPartOpacity(part, 0.75f);
                            }
                            else
                            {
                                setPartOpacity(part, 1f);
                            }
                        }
                    }
                }


                writeUpdates();
				
                //If in flight, check remote vessels
                if (isInFlight)
                {
                    VesselRecoveryButton vrb = null;
                    try
                    {
                        vrb = (VesselRecoveryButton)GameObject.FindObjectOfType(typeof(VesselRecoveryButton));
                    }
                    catch
                    {
                    }
                    if (controlsLocked)
                    {
                        //Prevent EVA'ing crew or vessel recovery
                        lockCrewGUI();
                        //Log.Debug("il: " + InputLockManager.PrintLockStack());
                        if (vrb != null)
                            vrb.ssuiButton.Lock();
                    }
                    else
                    {
                        //Clear locks
                        if (!KMPChatDX.showInput)
                        {
                            InputLockManager.RemoveControlLock("KMP_ChatActive");
                        }
                        unlockCrewGUI();
                        if (vrb != null && FlightGlobals.ActiveVessel.mainBody.bodyName == "Kerbin" && (FlightGlobals.ActiveVessel.situation == Vessel.Situations.LANDED || FlightGlobals.ActiveVessel.situation == Vessel.Situations.SPLASHED))
                            vrb.ssuiButton.Unlock();
                        else if (vrb != null)
                            vrb.ssuiButton.Lock();
                    }
                    activeVesselPosition = FlightGlobals.ship_CoM;
                }
				
                //Handle all queued vessel updates
                //Give KSP a change to do other things too.
                System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
                stopwatch.Start();
                while (vesselUpdateQueue.Count > 0)
                {
                    handleVesselUpdate(vesselUpdateQueue.Dequeue());
                    if (stopwatch.ElapsedMilliseconds > 100)
                    {
                        stopwatch.Stop();
                        break;
                    }
                }

                processClientInterop();

                //Update the displayed player orbit positions
                List<String> delete_list = new List<String>();

                foreach (KeyValuePair<String, VesselEntry> pair in vessels)
                {
                    VesselEntry entry = pair.Value;

                    if ((UnityEngine.Time.realtimeSinceStartup - entry.lastUpdateTime) <= VESSEL_TIMEOUT_DELAY && entry.vessel != null && entry.vessel.gameObj != null)
                    {
                        Log.Debug("Updating player marker");
                        entry.vessel.updatePackDistance();
                        entry.vessel.updatePlayerMarker();
                        entry.vessel.updateRenderProperties();
                    }
                    else
                    {
                        delete_list.Add(pair.Key); //Mark the vessel for deletion
                        entry.vessel.updatePackDistance(true);
                        if (entry.vessel != null && entry.vessel.gameObj != null)
                        {
                            GameObject.Destroy(entry.vessel.gameObj);
                        }
                    }
                }

                //Delete what needs deletin'
                foreach (String key in delete_list)
                {
                    vessels.Remove(key);
                }

                delete_list.Clear();

                //Delete outdated player status entries

                foreach (KeyValuePair<String, VesselStatusInfo> pair in playerStatus)
                {
                    if ((UnityEngine.Time.realtimeSinceStartup - pair.Value.lastUpdateTime) > VESSEL_TIMEOUT_DELAY)
                    {
                        Log.Debug("Deleted player status for timeout: " + pair.Key + " " + pair.Value.vesselName);
                        delete_list.Add(pair.Key);
                    }
                }
				
                foreach (String key in delete_list)
                {
                    playerStatus.Remove(key);
                }

                //Queue a time sync if needed
                if ((UnityEngine.Time.realtimeSinceStartup - lastTimeSyncTime) > SYNC_TIME_INTERVAL)
                {
                    SyncTime();
                }

                //Do the Phys-warp NTP time sync dance.
                SkewTime();

            }
            catch (Exception ex)
            {
                Log.Debug("Exception thrown in updateStep(), catch 4, Exception: {0}", ex.ToString());
                Log.Debug("uS err: " + ex.Message + " " + ex.StackTrace);
            }
        }

        private void removeKMPControlLocks()
        {
            InputLockManager.RemoveControlLock("KMP_Occupied");
            InputLockManager.RemoveControlLock("KMP_Private");
            InputLockManager.RemoveControlLock("KMP_ChatActive");
        }

        private void checkVesselPrivacy(Vessel vessel)
        {
            if (!vessel.packed && serverVessels_IsPrivate.ContainsKey(vessel.id) && serverVessels_IsMine.ContainsKey(vessel.id))
            {
                foreach (Part part in vessel.Parts)
                {
                    bool enabled = !serverVessels_IsPrivate[vessel.id] || serverVessels_IsMine[vessel.id];
                    if (!enabled && !serverParts_CrewCapacity.ContainsKey(part.uid))
                    {
                        serverParts_CrewCapacity[part.uid] = part.CrewCapacity;
                    }
                    if (!enabled)
                    {
                        part.CrewCapacity = 0;	
                    }
                    else if (serverParts_CrewCapacity.ContainsKey(part.uid))
                    {
                        part.CrewCapacity = serverParts_CrewCapacity[part.uid];
                        serverParts_CrewCapacity.Remove(part.uid);
                    }
                    foreach (PartModule module in part.Modules)
                    {
                        if (module is ModuleDockingNode)
                        {
                            ModuleDockingNode dmodule = (ModuleDockingNode)module;
                            float absCaptureRange = Math.Abs(dmodule.captureRange);
                            dmodule.captureRange = (enabled ? 1 : -1) * absCaptureRange;
                            dmodule.isEnabled = enabled;
                        }
                    }
                }
            }	
        }

        private void dockedKickToTrackingStation()
        {
            if (syncing && docking)
            {
                GameEvents.onFlightReady.Remove(this.OnFlightReady);
                HighLogic.CurrentGame = GamePersistence.LoadGame("start", HighLogic.SaveFolder, false, true);
                HighLogic.CurrentGame.Start();
                Invoke("OnFirstFlightReady", 1f);
            }
        }

        private void kickToTrackingStation()
        {
            if (!syncing)
            {
                Log.Debug("Selected unavailable vessel, switching");
                ScreenMessages.PostScreenMessage("Selected vessel is controlled from past or destroyed!", 5f, ScreenMessageStyle.UPPER_RIGHT);
                syncing = true;
                StartCoroutine(returnToTrackingStation());
            }
        }

        private void writeUpdates()
        {
            if ((UnityEngine.Time.realtimeSinceStartup - lastPluginUpdateWriteTime) > updateInterval)
            {
                writePluginUpdate();
                lastPluginUpdateWriteTime = UnityEngine.Time.realtimeSinceStartup;
            }
			
            if ((UnityEngine.Time.realtimeSinceStartup - lastPluginDataWriteTime) > PLUGIN_DATA_WRITE_INTERVAL)
            {
                writePluginData();
                lastPluginDataWriteTime = UnityEngine.Time.realtimeSinceStartup;
            }

            //Save global settings periodically

            if ((UnityEngine.Time.realtimeSinceStartup - lastGlobalSettingSaveTime) > GLOBAL_SETTINGS_SAVE_INTERVAL)
            {
                saveGlobalSettings();

                //Keep track of when the name was last read so we don't read it every time
                lastGlobalSettingSaveTime = UnityEngine.Time.realtimeSinceStartup;
            }	
        }

        public void disconnect(string message = "")
        {
            KMPClientMain.handshakeCompleted = false;
            forceQuit = delayForceQuit; //If we get disconnected straight away, we should forceQuit anyway.
            if (HighLogic.LoadedSceneIsFlight || HighLogic.LoadedSceneIsEditor)
            {
                ScreenMessages.PostScreenMessage("You have been disconnected. Please return to the Main Menu to reconnect.", 300f, ScreenMessageStyle.UPPER_CENTER);
                if (!String.IsNullOrEmpty(message))
                    ScreenMessages.PostScreenMessage(message, 300f, ScreenMessageStyle.UPPER_CENTER);
            }
            else
            {
                forceQuit = true;
                ScreenMessages.PostScreenMessage("You have been disconnected. Please return to the Main Menu to reconnect.", 300f, ScreenMessageStyle.UPPER_CENTER);
                if (!String.IsNullOrEmpty(message))
                {
                    ScreenMessages.PostScreenMessage(message, 300f, ScreenMessageStyle.UPPER_CENTER);
                }
            }
            if (String.IsNullOrEmpty(message))
            {
                KMPClientMain.SetMessage("Disconnected");
            }
            else
            {
                KMPClientMain.SetMessage("Disconnected: " + message);
            }
            saveGlobalSettings();
            gameRunning = false;
            terminateConnection = true;
            //Clear any left over locks.
            InputLockManager.ClearControlLocks();
        }

        private void writePluginUpdate()
        {
            if (playerName == null || playerName.Length == 0)
            {
                return;
            }

            if (!docking)
            {
                StartCoroutine(writePrimaryUpdate());
            }
			
            //nearby vessels
            Vector3d ship_position = (FlightGlobals.fetch.activeVessel != null) ? FlightGlobals.fetch.activeVessel.GetWorldPos3D() : Vector3d.zero;
            if (isInFlight && !isObserving && !syncing && !warping && !isInSafetyBubble(ship_position) && (serverVessels_IsMine.ContainsKey(FlightGlobals.fetch.activeVessel.id) ? serverVessels_IsMine[FlightGlobals.fetch.activeVessel.id] : true))
            {
                StartCoroutine(writeSecondaryUpdates());
            }
        }

        private IEnumerator<WaitForFixedUpdate> writePrimaryUpdate()
        {
            yield return new WaitForFixedUpdate();
            Vessel activeVessel = FlightGlobals.fetch.activeVessel;
            Vector3d ship_position = (activeVessel != null) ? activeVessel.GetWorldPos3D() : Vector3d.zero;
            if (!syncing && isInFlight && !warping && (activeVessel != null ? !activeVessel.packed : false) && (HighLogic.LoadedScene != GameScenes.LOADING) && !isInSafetyBubble(ship_position) && !isObserving)
            {

                lastTick = Planetarium.GetUniversalTime();
                //Write vessel status
                KMPVesselUpdate update = getVesselUpdate(activeVessel, false);

                if (update == null)
                {
                    Log.Warning("writePrimaryUpdate failure: Update is null.");
                    yield break;
                }
                if (activeVessel.vesselType == VesselType.EVA)
                {
                    lastEVAVessel = activeVessel;
                }
				
                //Update the player vessel info
                VesselStatusInfo my_status = new VesselStatusInfo();
                my_status.info = update;
                my_status.orbit = activeVessel.orbit;
                my_status.color = KMPVessel.generateActiveColor(playerName);
                my_status.ownerName = playerName;
                if (activeVessel.vesselName.Contains(" <"))
                {
                    activeVessel.vesselName = activeVessel.vesselName.Substring(0, activeVessel.vesselName.IndexOf(" <"));
                }
                if (String.IsNullOrEmpty(activeVessel.vesselName.Trim()))
                {
                    activeVessel.vesselName = "Unknown";
                }
                my_status.vesselName = activeVessel.vesselName;
                my_status.lastUpdateTime = UnityEngine.Time.realtimeSinceStartup;

                if (playerStatus.ContainsKey(playerName))
                {
                    playerStatus[playerName] = my_status;
                }
                else
                {
                    playerStatus.Add(playerName, my_status);
                }
				
                try
                {
                    enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.PRIMARY_PLUGIN_UPDATE, KSP.IO.IOUtils.SerializeToBinary(update));
                }
                catch (Exception e)
                {
                    Log.Debug("Exception thrown in writePrimaryUpdate(), catch 1, Exception: {0}", e.ToString());
                    Log.Debug("err: " + e.Message);
                }
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
                            if (serverVessels_IsMine.ContainsKey(activeVessel.id) ? serverVessels_IsMine[activeVessel.id] : true)
                                status_array[1] = "Preparing/launching from KSC";
                            else
                                status_array[1] = "Spectating " + activeVessel.vesselName;
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

        private IEnumerator<WaitForFixedUpdate> writeSecondaryUpdates()
        {
            yield return new WaitForFixedUpdate();
            Vessel activeVessel = FlightGlobals.fetch.activeVessel;
            if (activeVessel != null)
            {
               
         
                if (inactiveVesselsPerUpdate > 0)
                {
                    //Write the inactive vessels nearest the active vessel
                    SortedList<float, Vessel> nearest_vessels = new SortedList<float, Vessel>();

                    foreach (Vessel vessel in FlightGlobals.fetch.vessels)
                    {
                        if (vessel == null)
                        {
                            //Protect against null vessels
                            Log.Warning("writeSecondryUpdates: FlightGlobals.Vessel is null!");
                            continue;
                        }
                        if (vessel.id == activeVessel.id)
                        {
                            //Don't send our vessel
                            continue;
                        }
                        if (!vessel.loaded)
                        {
                            //Obvious
                            continue;
                        }
                        if (vessel.id.ToString() == SYNC_PLATE_ID || vessel.name.Contains(" [Past]") || vessel.name.Contains(" [Future]") || vessel.name.Contains("<") || vessel.name.Contains(">") || vessel.name.Contains("*"))
                        {
                            //Skip player vessels and the sync plate.
                            continue;
                        }


                        float distance = Vector3.Distance(activeVessel.GetWorldPos3D(), vessel.GetWorldPos3D());
                        if (distance < INACTIVE_VESSEL_RANGE)
                        {
                            //Skip far vessles
                            continue;
                        }

                        Part root = vessel.rootPart;
                        if (root == null)
                        {
                            //Not sure...
                            continue;
                        }

                        try
                        {
                            bool include = true;
                            /*
                        if (serverVessels_InUse.ContainsKey(vessel.id) ? !serverVessels_InUse[vessel.id] : false)
                        {
                            foreach (Guid vesselID in serverVessels_Parts.Keys)
                            {
                                if (serverVessels_Parts.ContainsKey(vesselID) ? serverVessels_Parts[vesselID].Contains(root) : false)
                                {
                                    include = false;
                                    break;
                                }

                            }
                        }
                        */
                            if (include)
                            {
                                nearest_vessels.Add(distance, vessel);
                            }
                        }
                        catch (ArgumentException e)
                        {
                            Log.Debug("Exception thrown in writeSecondaryUpdates(), catch 1, Exception: {0}", e.ToString());
                        }

                
                    }

                    int num_written_vessels = 0;

                    foreach (KeyValuePair<float,Vessel> secondryVessel in nearest_vessels)
                    {
                        KMPVesselUpdate update = getVesselUpdate(secondryVessel.Value, false);
                        if (update == null)
                        {
                            //Failed to get update. This is normal if the other vessel is in the safety bubble for instance.
                            continue;
                        }

                        if (secondryVessel.Value.mainBody == null)
                        {
                            Log.Warning("Skipping secondry update for vessel with no mainBody.");
                        }

                        bool isInAtmo = secondryVessel.Value.mainBody.atmosphere ? secondryVessel.Value.altitude < secondryVessel.Value.mainBody.maxAtmosphereAltitude : false;
                          
                        if (!isInAtmo && (update.situation == Situation.DESCENDING || update.situation == Situation.FLYING))
                        {
                            //update.distance = secondryVessel.Key;
                            update.state = State.INACTIVE;
                            byte[] update_bytes = KSP.IO.IOUtils.SerializeToBinary(update);
                            bool newVessel = !serverVessels_RemoteID.ContainsKey(secondryVessel.Value.id);
                            if (newVessel)
                            {
                                enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.PRIMARY_PLUGIN_UPDATE, update_bytes);
                            }
                            else
                            {
                                enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.SECONDARY_PLUGIN_UPDATE, update_bytes);
                            }
                            num_written_vessels++;
                        }
                        if (num_written_vessels > MAX_INACTIVE_VESSELS_PER_UPDATE)
                        {
                            break;
                        }
                    }
                }
            }
            else
            {
                Log.Debug("Refusing to send secondry updates: Active vessel is null");
            }
        }

        private void sendRemoveVesselMessage(Vessel vessel, bool isDocking = false)
        {
            Log.Debug("sendRemoveVesselMessage");
            KMPVesselUpdate update = getVesselUpdate(vessel, false);
            if (update == null)
            {
                Log.Debug("sendRemoveVesselMessage failure: Update is null.");
                return;
            }
            update.situation = Situation.DESTROYED;
            update.state = FlightGlobals.fetch.activeVessel.id == vessel.id ? State.ACTIVE : State.INACTIVE;
            update.isDockUpdate = isDocking;
            update.clearProtoVessel();
            byte[] update_bytes = KSP.IO.IOUtils.SerializeToBinary(update);
            enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.PRIMARY_PLUGIN_UPDATE, update_bytes);	
        }

        private void sendVesselMessage(Vessel vessel, bool isDocking = false)
        {
            if (isInFlight)
            {
                Log.Debug("sendVesselMessage");
                KMPVesselUpdate update = getVesselUpdate(vessel, true);
                if (update == null)
                {
                    Log.Debug("Failed to get update for vessel: Update is null.");
                    return;
                }
                update.state = FlightGlobals.ActiveVessel.id == vessel.id ? State.ACTIVE : State.INACTIVE;
                update.isDockUpdate = isDocking;
                byte[] update_bytes = KSP.IO.IOUtils.SerializeToBinary(update);
                enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.PRIMARY_PLUGIN_UPDATE, update_bytes);	
            }
        }

        private void sendScenarios()
        {
            Log.Debug("sendScenarios");
            if (!syncing)
            {
                lastScenarioUpdateTime = UnityEngine.Time.realtimeSinceStartup;
                double tick = Planetarium.GetUniversalTime();
                foreach (ProtoScenarioModule proto in HighLogic.CurrentGame.scenarios)
                {
                    if (proto != null && proto.moduleName != null && proto.moduleRef != null)
                    {
                        KMPScenarioUpdate update = new KMPScenarioUpdate(proto.moduleName, proto.moduleRef, tick);
                        byte[] update_bytes = KSP.IO.IOUtils.SerializeToBinary(update);
                        enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.SCENARIO_UPDATE, update_bytes);
                    }
                }
            }
        }

        private KMPVesselUpdate getVesselUpdate(Vessel vessel, bool forceFullUpdate)
        {
            try
            {
                //WARNING: These checks will cause you to drop off everyones player list if any of the primary updates hits this.

                #region getVesselUpdate - Sanity checks
                if (vessel == null)
                {
                    Log.Debug("Skipping getVesselUpdate, vessel is null.");
                    return null;
                }

                if (!vessel.loaded)
                {
                    Log.Debug("Skipping getVesselUpdate, vessel is unloaded.");
                    return null;
                }

                if (vessel.packed)
                {
                    Log.Debug("Skipping getVesselUpdate, vessel is packed.");
                    return null;
                }

                if (vessel.mainBody == null)
                {
                    Log.Debug("Skipping getVesselUpdate: vessel.mainBody is null.");
                    return null;
                }

                if (vessel.id.ToString() == SYNC_PLATE_ID)
                {
                    Log.Debug("Skipping getVesselUpdate: We are the SyncPlate");
                    return null;
                }
                bool isMine = serverVessels_IsMine.ContainsKey(vessel.id) ? serverVessels_IsMine[vessel.id] : false;
                bool isPrivate = serverVessels_IsPrivate.ContainsKey(vessel.id) ? serverVessels_IsPrivate[vessel.id] : false;
                bool isInUse = serverVessels_InUse.ContainsKey(vessel.id) ? serverVessels_InUse[vessel.id] : false;
                //In use and not ours
                if (isInUse && !isMine)
                {
                    Log.Debug("Skipping getVesselUpdate: Don't update other players vessels");
                    return null;
                }

                //Private and not ours
                if (isPrivate && !isMine)
                {
                    Log.Debug("Skipping getVesselUpdate: Don't update other players vessels");
                    return null;
                }

                #endregion

                if (vessel.id == Guid.Empty)
                {
                    vessel.id = Guid.NewGuid();
                    Log.Debug("Creating new vessel ID, " + vessel.id + ", name: " + vessel.name);
                }

                if (vessel.vesselType == VesselType.Flag && !newFlags.ContainsKey(vessel.id) && !serverVessels_ProtoVessels.ContainsKey(vessel.id))
                {
                    newFlags.Add(vessel.id, UnityEngine.Time.realtimeSinceStartup + 65f);
                }

                //Log.Debug("Vid: " + vessel.id);
                //Log.Debug("foreFullUpdate: " + forceFullUpdate);
                //Log.Debug("ParCountsContains: " + serverVessels_PartCounts.ContainsKey(vessel.id));
                //Log.Debug("TimeDelta: " + ((UnityEngine.Time.realtimeSinceStartup - lastFullProtovesselUpdate) < FULL_PROTOVESSEL_UPDATE_TIMEOUT));
                //Log.Debug("Throttle: " + (FlightGlobals.ActiveVessel.ctrlState.mainThrottle == 0f));
			
                //Ensure privacy protections don't affect server's version of vessel
                foreach (Part part in vessel.Parts)
                {
                    if (serverParts_CrewCapacity.ContainsKey(part.uid) ? serverParts_CrewCapacity[part.uid] != 0 : false)
                    {
                        part.CrewCapacity = serverParts_CrewCapacity[part.uid];
                    }
                    foreach (PartModule module in part.Modules)
                    {
                        if (module is ModuleDockingNode)
                        {
                            ModuleDockingNode dmodule = (ModuleDockingNode)module;
                            float absCaptureRange = Math.Abs(dmodule.captureRange);
                            dmodule.captureRange = absCaptureRange;
                            dmodule.isEnabled = true;
                        }
                    }
                }
                //Do a full update under these circumstances:
                //  We need to send another periodic active vessel update
                //  It's a forced update.
                //  We just docked and we need to update the other vessel
                //  The vessel does not have a serverVessels_PartCounts entry
                bool sendFullUpdate = false;

                bool activeVesselUpdateNeeded = (vessel.id == FlightGlobals.ActiveVessel.id) && ((UnityEngine.Time.realtimeSinceStartup - lastFullProtovesselUpdate) > FULL_PROTOVESSEL_UPDATE_TIMEOUT);
                if (forceFullUpdate || docking || !serverVessels_PartCounts.ContainsKey(vessel.id) || activeVesselUpdateNeeded)
                { 
                    sendFullUpdate = true;
                    //New vessel or forced protovessel update
                }
                else
                {
                    //Part count has changed
                    bool hasPartCountChanged = serverVessels_PartCounts.ContainsKey(vessel.id) ? (serverVessels_PartCounts[vessel.id] != vessel.Parts.Count) : true;
                    //Situation has changed
                    bool hasSituationChanged = sentVessels_Situations.ContainsKey(vessel.id) ? (sentVessels_Situations[vessel.id] != vessel.situation) : true;

                    if (hasPartCountChanged || hasSituationChanged)
                    { 
                        Log.Debug("Full update: " + vessel.id);
                        sendFullUpdate = true;
                        serverVessels_PartCounts[vessel.id] = vessel.Parts.Count;
                        sentVessels_Situations[vessel.id] = vessel.situation;
                    }
                    else
                    {
                        //Is a new flag and the plaque timeout has expired
                        if (newFlags.ContainsKey(vessel.id) ? (UnityEngine.Time.realtimeSinceStartup > newFlags[vessel.id]) : false)
                        {
                            sendFullUpdate = true;
                            newFlags.Remove(vessel.id);
                        }
                    }
                }

                if (isInSafetyBubble(vessel.GetWorldPos3D()))
                {
                    sendFullUpdate = false;
                }

                KMPVesselUpdate update = new KMPVesselUpdate(vessel, sendFullUpdate);
                if (vessel.id == FlightGlobals.ActiveVessel.id && sendFullUpdate)
                {
                    Log.Debug("Full update for active vessel: " + vessel.id + ", name: " + vessel.name);
                    lastFullProtovesselUpdate = UnityEngine.Time.realtimeSinceStartup;
                    serverVessels_PartCounts[vessel.id] = vessel.Parts.Count;
                }

                //Track vessel situation
                sentVessels_Situations[vessel.id] = vessel.situation;
			
                //Set privacy lock
                update.isPrivate = serverVessels_IsPrivate.ContainsKey(vessel.id) ? serverVessels_IsPrivate[vessel.id] : false;

                //Shorten the name if it's too long.
                update.name = (vessel.vesselName.Length <= MAX_VESSEL_NAME_LENGTH) ? update.name = vessel.vesselName : vessel.vesselName.Substring(0, MAX_VESSEL_NAME_LENGTH);

                update.player = playerName;
                update.id = vessel.id;
                update.tick = Planetarium.GetUniversalTime();
                update.crewCount = vessel.GetCrewCount();
                if (serverVessels_RemoteID.ContainsKey(vessel.id))
                {
                    update.kmpID = serverVessels_RemoteID[vessel.id];
                }
                else
                {
                    Log.Debug("Generating new remote ID for vessel: " + vessel.id);
                    Guid server_id = Guid.NewGuid();
                    update.kmpID = server_id;
                    serverVessels_RemoteID.Add(vessel.id, server_id);
				
                    if (vessel.vesselType == VesselType.Flag)
                    {
                        newFlags[vessel.id] = UnityEngine.Time.realtimeSinceStartup;
                    }
                }

                update.orbitActivePatch = vessel.orbit.activePatch;
                update.orbitINC = vessel.orbit.inclination;
                update.orbitECC = vessel.orbit.eccentricity;
                update.orbitSMA = vessel.orbit.semiMajorAxis;
                update.orbitLAN = vessel.orbit.LAN;
                //Thank KSP for their wonderful documentation. w is the new ω.
                update.orbitW = vessel.orbit.argumentOfPeriapsis;
                update.orbitMEP = vessel.orbit.meanAnomalyAtEpoch;
                update.orbitT = vessel.orbit.epoch;
                //Log.Debug("Sent epoch is: " + update.orbitT);
                update.surface_position[0] = vessel.latitude;
                update.surface_position[1] = vessel.longitude;
                update.surface_position[2] = vessel.altitude;
                //Quaternion transformRotation = vessel.transform.localRotation;
                Vector3 transformForward = vessel.mainBody.transform.InverseTransformDirection(vessel.transform.forward);
                Vector3 transformUp = vessel.mainBody.transform.InverseTransformDirection(vessel.transform.up);
                Vector3d transformSurfaceVelocity = vessel.srf_velocity;
                Vector3 transformAngularVelocity = vessel.angularVelocity;
                Vector3d transformAcceleration = vessel.acceleration;
                for (int i = 0; i < 3; i++)
                {
                    //update.rotation[i] = transformRotation[i];
                    update.forward[i] = transformForward[i];
                    update.up[i] = transformUp[i];
                    update.surface_velocity[i] = transformSurfaceVelocity[i];
                    update.angular_velocity[i] = transformAngularVelocity[i];
                    update.acceleration[i] = transformAcceleration[i];
                }
                //Vector3d orbitPos = vessel.orbit.getTruePositionAtUT(Planetarium.GetUniversalTime());
                //Vector3d truePos = vessel.GetWorldPos3D();
                //Vector3d diffPos = truePos - orbitPos;
                //Log.Debug("Send diff: " + diffPos);
                //Log.Debug("Send pertubation: " + vessel.perturbation);

                //Rotation is a quat and I don't feel like putting this in a 4 loop...
                //update.rotation[3] = transformRotation[3];

                //Determine situation
                if ((vessel.loaded && vessel.GetTotalMass() <= 0.0) || (vessel.vesselType == VesselType.Debris && vessel.situation == Vessel.Situations.SUB_ORBITAL))
                {
                    update.situation = Situation.DESTROYED;
                }
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
                if (vessel.id == FlightGlobals.ActiveVessel.id)
                {
                    update.state = State.ACTIVE;
                    //Set vessel details since it's the active vessel
                    update.detail = getVesselDetail(vessel);
                }
                else if (vessel.isCommandable)
                    update.state = State.INACTIVE;
                else
                {
                    serverVessels_InUse[vessel.id] = false;
                    update.state = State.DEAD;
                }
                update.timeScale = (float)Planetarium.TimeScale;
                update.bodyName = vessel.mainBody.bodyName;
                //Reset vessel privacy locks in case they were changed
                checkVesselPrivacy(vessel);
                return update;
            }
            catch (Exception e)
            {
                Log.Warning("Exception caught!: " + e.ToString());
            }
            return null;
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
                        KerbalEVA kerbal = (KerbalEVA)module;

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

                            foreach (Propellant propellant in engine.propellants)
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
				
                if (status_array.Length >= 5)
                {
                    status.vesselID = new Guid(status_array[4]);
                }
				
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
            FlightInputHandler.state.mainThrottle = 0;
            if (FlightGlobals.ClearToSave() == ClearToSaveStatus.CLEAR || !isInFlight || FlightGlobals.ActiveVessel.state == Vessel.State.DEAD)
            {
                if (!forceQuit)
                {
                    GamePersistence.SaveGame("persistent", HighLogic.SaveFolder, SaveMode.OVERWRITE);
                    HighLogic.LoadScene(GameScenes.SPACECENTER);
                }
                syncing = false;
            }
            else
                StartCoroutine(returnToSpaceCenter());
        }

        private IEnumerator<WaitForFixedUpdate> returnToTrackingStation()
        {
            yield return new WaitForFixedUpdate();
            FlightInputHandler.state.mainThrottle = 0;
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

        private IEnumerator<WaitForFixedUpdate> setActiveVessel(Vessel vessel)
        {
            yield return new WaitForFixedUpdate();
            FlightGlobals.ForceSetActiveVessel(vessel);
        }

        private IEnumerator<WaitForFixedUpdate> setNewVesselNotInPresent(Vessel vessel)
        {
            yield return new WaitForFixedUpdate();
            serverVessels_InPresent[vessel.id] = false;
            foreach (Part part in vessel.Parts)
            {
                setPartOpacity(part, 0.3f);
            }
        }

        private void killVessel(Guid vesselID)
        {
            if (vesselID.ToString() == SYNC_PLATE_ID)
            {
                Log.Debug("Tried to kill SyncPlate");
                return;
            }
            try
            {
                OrbitPhysicsManager.HoldVesselUnpack(1);
            }
            catch
            {
            }
            foreach (Vessel vessel in FlightGlobals.fetch.vessels.FindAll(v => v.id == vesselID))
            {
                if (vessel.isActiveVessel)
                {
                    vessel.MakeInactive();
                    setActiveVessel(FlightGlobals.fetch.vessels.Find(v => v.id.ToString() == SYNC_PLATE_ID));
                }
                if (!vessel.packed)
                {
                    vessel.GoOnRails();
                    setNoClip(vessel);
                }
                //FlightGlobals.fetch.vessels.Remove(vessel);
                StartCoroutine(killVesselOnNextUpdate(vessel));
            }
        }

        private IEnumerator<WaitForFixedUpdate> killVesselOnNextUpdate(Vessel vessel)
        {
            yield return new WaitForFixedUpdate();
            if (vessel.loaded)
            {
                vessel.Unload();
            }
            vessel.Die();
        }

        private IEnumerator<WaitForEndOfFrame> sendSubspaceSyncRequest(int subspace = -1, bool docking = false)
        {
            yield return new WaitForEndOfFrame();
            Log.Debug("sending subspace sync request to subspace " + subspace);
            showServerSync = false;
            if (!syncing)
                inGameSyncing = true;
            syncing = true;
            byte[] update_bytes = KMPCommon.intToBytes(subspace);
            enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.SSYNC, update_bytes);
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
										
                    if (isInFlight && status.vesselID == FlightGlobals.ActiveVessel.id && status.currentSubspaceID > 0)
                    {
                        StartCoroutine(sendSubspaceSyncRequest(status.currentSubspaceID));
                    }
                }
            }
        }

        /// <summary>
        /// Determines if a scenario module with the specified name exists already. Only checks the name.
        /// This is used to determine when the scenarios list was not cleared properly (creating a new game does not seem to clear the list - it only sets the modules'
        /// moduleRefs to null, which caused failure to sync on career servers, by creating another two modules and then attempting to write data into all four,
        /// and might have caused career mode on non-career servers if you weren't restarting KSP after exiting career servers).
        /// </summary>
        /// <param name="modName">The module name, such as "ResearchAndDevelopment"</param>
        /// <returns></returns>
        private bool HasModule(string modName)
        {
            Game g = HighLogic.CurrentGame;
            bool retval = false;

            if (g == null)
            {
                Log.Warning("HasModule called when there is no current game.");
            }
            else if (g.scenarios.Count > 0)
            {
                for (int i = 0; i < g.scenarios.Count; i++)
                {
                    ProtoScenarioModule proto = g.scenarios[i];
                    if (proto != null)
                    {
                        if (proto.moduleName.CompareTo(modName) == 0)
                        {
                            retval = true;
                        }
                    }
                    else
                    {
                        Log.Warning("Null protoScenario found by HasModule!");
                    }
                }
            }
            return retval;
        }

        /// <summary>
        /// Log current game science, and log some debug information.
        /// Logs the science amount, or -1 if there is no current game, or no scenarios, or no R&D proto scenario module.
        /// This was used for debugging to figure out what and where things were going wrong with connecting after disconnecting (#578, #579).
        /// </summary>
        private void LogScience()
        {
            Game g = HighLogic.CurrentGame;
            if (g == null)
            {
                Log.Debug("No current game.");
                return;
            }
            Log.Debug("Game status=" + g.Status + " modes=" + g.Mode + " IsResumable=" + g.IsResumable() + " startScene=" + g.startScene + " NumScenarios=" + g.scenarios.Count);
            float science = -1;
            if (g.scenarios.Count > 0)
            {
                for (int i = 0; i < g.scenarios.Count; i++)
                {
                    ProtoScenarioModule proto = g.scenarios[i];
                    if (proto != null)
                    {
                        Log.Debug("g.scenarios[" + i + "].moduleName=" + g.scenarios[i].moduleName + ", and moduleRef=" + (g.scenarios[i].moduleRef != null ? g.scenarios[i].moduleRef.ClassName : "null"));
                        if (proto.moduleRef != null && proto.moduleRef is ResearchAndDevelopment)
                        {
                            ResearchAndDevelopment rd = (ResearchAndDevelopment)proto.moduleRef;
                            if (science > -1)
                            {
                                //This was happening later on when there were four "scenarios" and the ones with null moduleRefs had somehow been replaced by actual moduleRefs.
                                //Upon trying to handle another science update when there were two valid R&D moduleRefs in scenarios,
                                //KSP/KMP exploded causing the desync which triggered the disconnect and message to restart KSP (the bug symptoms).
                                //The root cause, of course, was that scenarios was never cleared properly. That is fixed now.
                                Log.Error("More than one ResearchAndDevelopment scenario module in the game! Science was already " + science + ", now we've found another which says it is " + rd.Science + "!");
                            }
                            science = rd.Science;
                        }
                        else if (proto.moduleName.CompareTo("ResearchAndDevelopment") == 0)
                        {
                            //This was happening - after disconnecting from a career mode server, then making a new game to connect again, 
                            //there would already be two "scenarios" - both with null moduleRefs - 
                            //BEFORE we had tried to add two for career mode.
                            Log.Error("ProtoScenarioModule claims to be a ResearchAndDevelopment but contains no such thing! moduleRef is " + (proto.moduleRef != null ? proto.moduleRef.ClassName : "null"));
                        }
                    }
                    else
                    {
                        Log.Debug("Null protoScenario!");
                    }
                }
            }
            else
            {
                Log.Debug("No scenarios.");
            }
            if (science > -1)
            {
                Log.Debug("Science = " + science);
            }
            else if (g.scenarios.Count > 0)
            {
                Log.Debug("No ResearchAndDevelopment scenario modules.");
            }
        }

        private void handleScenarioUpdate(object obj)
        {
            if (obj is KMPScenarioUpdate)
            {
                KMPScenarioUpdate update = (KMPScenarioUpdate)obj;
                bool loaded = false;
                foreach (ProtoScenarioModule proto in HighLogic.CurrentGame.scenarios)
                {
                    if (proto != null && proto.moduleName == update.name && proto.moduleRef != null && update.getScenarioNode() != null)
                    {
                        Log.Debug("Loading scenario data for existing module: " + update.name);
                        if (update.name == "ResearchAndDevelopment")
                        {
                            ResearchAndDevelopment rd = (ResearchAndDevelopment)proto.moduleRef;
                            Log.Debug("pre-R&D: {0}", rd.Science);
                        }
                        try
                        {
                            proto.moduleRef.Load(update.getScenarioNode());
                        }
                        catch (Exception e)
                        {
                            KMPClientMain.sendConnectionEndMessage("Error in handling scenario data. Please restart your client. ");
                            Log.Debug(e.ToString());
                        }
                        if (update.name == "ResearchAndDevelopment")
                        {
                            ResearchAndDevelopment rd = (ResearchAndDevelopment)proto.moduleRef;
                            Log.Debug("post-R&D: {0}", rd.Science);
                        }
                        loaded = true;
                        break;
                    }
                }
                if (!loaded)
                {
                    Log.Debug("Loading new scenario module data: " + update.name);
                    ProtoScenarioModule newScenario = new ProtoScenarioModule(update.getScenarioNode());
                    newScenario.Load(ScenarioRunner.fetch);
                }
                clearEditorPartList = true;
            }
        }

        private void handleVesselUpdate(KMPVesselUpdate vessel_update)
        {
            String vessel_key = vessel_update.id.ToString();
            //Try to find the key in the vessel dictionary
            if (vessels.ContainsKey(vessel_key))
            {
                if (vessels[vessel_key].vessel.gameObj != null)
                {
                    vessels[vessel_key].lastUpdateTime = UnityEngine.Time.realtimeSinceStartup;
                    applyVesselUpdate(vessel_update, vessels[vessel_key].vessel);
                }
                else
                {
                    Log.Debug("Existing vessel update entry replaced.");
                    VesselEntry new_entry = new VesselEntry();
                    new_entry.vessel = new KMPVessel(vessel_update.name, vessel_update.player, vessel_update.id);
                    new_entry.lastUpdateTime = UnityEngine.Time.realtimeSinceStartup;
                    vessels[vessel_key] = new_entry;
                    applyVesselUpdate(vessel_update, new_entry.vessel);
                }
            }
            else
            {
                VesselEntry new_entry = new VesselEntry();
                new_entry.vessel = new KMPVessel(vessel_update.name, vessel_update.player, vessel_update.id);
                new_entry.lastUpdateTime = UnityEngine.Time.realtimeSinceStartup;
                vessels[vessel_key] = new_entry;
                applyVesselUpdate(vessel_update, new_entry.vessel);
            }
        }

        private void applyVesselUpdate(KMPVesselUpdate vessel_update, KMPVessel vessel)
        {
            //The load counter.

            if (!vesselUpdatesHandled.Contains(vessel_update.kmpID))
            {                
                vesselUpdatesHandled.Add(vessel_update.kmpID);
            }

            if (vessel_update.id.ToString() == SYNC_PLATE_ID)
            {
                Log.Debug("Refusing to update SyncPlate");
                return;
            }

            if (vessel_update.id == Guid.Empty)
            {
                Log.Debug("Skipping update without ID");
                return;
            }

            #region Grab extant vessel
            Vessel extant_vessel = null;
            if (FlightGlobals.fetch != null)
            {
                extant_vessel = FlightGlobals.fetch.vessels.Find(v => v.id == vessel_update.id);
            }
            else
            {
                Log.Debug("Flightglobals fetch is null?");
            }
            #endregion

            #region applyVesselUpdate - Setting initial parameters
            serverVessels_RemoteID[vessel_update.id] = vessel_update.kmpID;

            CelestialBody updateBody = FlightGlobals.Bodies.Find(b => b.bodyName == vessel_update.bodyName);
            if (updateBody == null)
            {
                Log.Debug("applyVesselUpdate can not find body: " + vessel_update.bodyName);
                updateBody = FlightGlobals.Bodies.Find(b => b.bodyName == "Sun");
            }

            //Save the new orbital data / positioning data.
            vessel.referenceOrbit = (new Orbit(vessel_update.orbitINC, vessel_update.orbitECC, vessel_update.orbitSMA, vessel_update.orbitLAN, vessel_update.orbitW, vessel_update.orbitMEP, vessel_update.orbitT, updateBody));
            //vessel.referenceRotation = (new Quaternion(vessel_update.rotation[0], vessel_update.rotation[1], vessel_update.rotation[2], vessel_update.rotation[3]));
            vessel.referenceForward = (new Vector3(vessel_update.forward[0], vessel_update.forward[1], vessel_update.forward[2]));
            vessel.referenceUp = (new Vector3(vessel_update.up[0], vessel_update.up[1], vessel_update.up[2]));
            vessel.referenceSurfacePosition = (new Vector3d(vessel_update.surface_position[0], vessel_update.surface_position[1], vessel_update.surface_position[2]));
            vessel.referenceSurfaceVelocity = (new Vector3d(vessel_update.surface_velocity[0], vessel_update.surface_velocity[1], vessel_update.surface_velocity[2]));
            vessel.referenceAngularVelocity = (new Vector3(vessel_update.angular_velocity[0], vessel_update.angular_velocity[1], vessel_update.angular_velocity[2]));
            vessel.referenceAcceleration = (new Vector3d(vessel_update.acceleration[0], vessel_update.acceleration[1], vessel_update.acceleration[2]));
            vessel.referenceUT = vessel_update.tick;
            vessel.info = new KMPVesselInfo();
            vessel.info.bodyName = vessel_update.bodyName;
            vessel.info.situation = vessel_update.situation;
            vessel.info.state = vessel_update.state;
            vessel.info.timeScale = vessel_update.timeScale;
            vessel.info.detail = vessel_update.detail;
            if (!vessel.orbitValid)
            {
                Log.Warning("Vessel orbit is not valid for " + vessel.id + ", skipping!");
                return;
            }

            #endregion

            #region applyVesselUpdate - Player status updates
            if (vessel_update.state == State.ACTIVE && vessel_update.relTime != RelativeTime.FUTURE && !vessel_update.isDockUpdate)
            {
                //Update the player status info
                VesselStatusInfo status = new VesselStatusInfo();
                if (vessel_update.relTime == RelativeTime.PRESENT)
                {
                    status.info = vessel_update;
                }
                status.ownerName = vessel_update.player;
                status.vesselName = vessel_update.name;

                status.lastUpdateTime = UnityEngine.Time.realtimeSinceStartup;
                status.color = KMPVessel.generateActiveColor(status.ownerName);

                playerStatus[status.ownerName] = status;
            }
            #endregion

            #region applyVesselUpdate - Delete destroyed vessels

            if (!docking)
            {
                //Update vessel privacy locks
                serverVessels_InUse[vessel_update.id] = ((vessel_update.state == State.ACTIVE) && !vessel_update.isMine);
                serverVessels_IsPrivate[vessel_update.id] = vessel_update.isPrivate;
                serverVessels_IsMine[vessel_update.id] = vessel_update.isMine;
                //Log.Debug("status flags updated: " + (vessel_update.state == State.ACTIVE) + " " + vessel_update.isSyncOnlyUpdate + " " + vessel_update.isPrivate + " " + vessel_update.isMine);
                if (vessel_update.situation == Situation.DESTROYED && vessel_update.id != FlightGlobals.ActiveVessel.id && FlightGlobals.fetch != null)
                {
                    Log.Debug("Vessel reported destroyed, killing vessel");
                    foreach (Vessel kill_vessel in FlightGlobals.fetch.vessels.FindAll(v => v.id == vessel_update.id))
                    {
                        if (kill_vessel != null)
                        {
                            try
                            {
                                killVessel(kill_vessel.id);
                            }
                            catch (Exception e)
                            {
                                Log.Debug("Exception thrown in applyVesselUpdate(), catch 1, Exception: {0}", e.ToString());
                            }
                        }
                        return;
                    }
                }
            }
            #endregion

            #region applyVesselUpdate - Save protovessel confignode
            ConfigNode protonode = vessel_update.getProtoVesselNode();
            //Store protovessel ConfigNode if included
            if (protonode != null)
            {
                Log.Debug("Saving protovessel ConfigNode for " + vessel_update.id);
                serverVessels_ProtoVessels[vessel_update.id] = protonode;
            }
            #endregion

            #region applyVesselUpdate - In flight check
            //Apply update if able
            if (!isInFlightOrTracking)
            {
                Log.Debug("Skipping vessel update, Not in flight or tracking station");
                return;
            }
            #endregion

            #region applyVesselUpdate - Relative time sanity check
            if ((FlightGlobals.ActiveVessel != null ? vessel_update.id == FlightGlobals.ActiveVessel.id : false) && vessel_update.relTime == RelativeTime.PAST)
            {
                kickToTrackingStation();
                return;
            }
            #endregion

            #region applyVesselUpdate - Rename active player vessel
            if (extant_vessel != null)
            {
                if (vessel_update.state == State.ACTIVE)
                {
                    extant_vessel.name = vessel_update.name + " <" + vessel_update.player + ">";
                    extant_vessel.vesselName = vessel_update.name + " <" + vessel_update.player + ">";
                }
                else
                {
                    extant_vessel.name = vessel_update.name;
                    extant_vessel.vesselName = vessel_update.name;
                }
            }
            #endregion

            //Update the vessel under the following conditions:
            //
            //The vessel is not the current player-controlled vessel
            //We are spectating.
            //The update is a docking update
            //We are in the tracking station

            bool activeVessel = false;

            if (FlightGlobals.ActiveVessel != null)
            {
                activeVessel = (FlightGlobals.ActiveVessel.id == vessel.id);
            }

            if (activeVessel && (!isObserving || vessel_update.isDockUpdate) && HighLogic.LoadedScene != GameScenes.TRACKSTATION)
            {
                Log.Debug("Got update we cannot apply!");
                return;
            }

            double ourDistance = 3000f;
            if (FlightGlobals.ActiveVessel != null && extant_vessel != null)
            {
                ourDistance = Vector3d.Distance(FlightGlobals.ActiveVessel.GetWorldPos3D(), extant_vessel.GetWorldPos3D());
            }

            #region applyVesselUpdate - Update protovessel on existing vessel
            if (extant_vessel != null)
            {
                //TODO: Fix aircraft.
                if (protonode != null && vessel_update.situation != Situation.FLYING)
                {
                    if (protonode != null)
                    {
                        Log.Debug("Updating from protovessel, New Protonode");
                    }
                    serverVessels_PartCounts.Remove(vessel_update.id);
                    if (serverVessels_ProtoVessels.ContainsKey(vessel_update.id))
                    {
                        ProtoVessel protovessel = new ProtoVessel(serverVessels_ProtoVessels[vessel_update.id], HighLogic.CurrentGame);
                        if (!vessel.useSurfacePositioning)
                        {
                            if (checkOrbitForCollision(vessel.currentOrbit, Planetarium.GetUniversalTime(), vessel_update.tick))
                            {
                                Log.Debug("Did not load vessel, has collided with surface, killing vessel");
                                killVessel(vessel.id);
                                return;
                            }
                        }
                        if (protovessel == null)
                        {
                            Log.Debug("Failed to get protovessel");
                            return;
                        }
                        addRemoteVessel(protovessel, vessel, vessel_update, ourDistance);
                    }
                    #endregion
                }
                else
                {
                    #region applyVesselUpdate - Update existing vessel from flight data
                    StartCoroutine(UpdateVesselOnNextFixedUpdate(extant_vessel, vessel, vessel_update));
                    #endregion
                }
            }
            else
            {
                #region applyVesselUpdate - Try to load a new vessel from protovessel
                if (serverVessels_ProtoVessels.ContainsKey(vessel_update.id))
                {
                    if (serverVessels_AddDelay.ContainsKey(vessel_update.kmpID) ? serverVessels_AddDelay[vessel_update.kmpID] < UnityEngine.Time.realtimeSinceStartup : true)
                    {
                        if (!vessel.useSurfacePositioning)
                        {
                            if (checkOrbitForCollision(vessel.currentOrbit, Planetarium.GetUniversalTime(), vessel_update.tick))
                            {
                                Log.Debug("Did not load vessel, has collided with surface");
                                return;
                            }
                        }
                        Log.Debug("Adding new vessel: " + vessel_update.id);
                        ConfigNode protoNode = serverVessels_ProtoVessels[vessel_update.id];
                        checkProtoNodeCrew(protoNode);
                        ProtoVessel protovessel = new ProtoVessel(protoNode, HighLogic.CurrentGame);
                        serverVessels_PartCounts[vessel_update.id] = 0;
                        addRemoteVessel(protovessel, vessel, vessel_update, 0f);
                        HighLogic.CurrentGame.CrewRoster.ValidateAssignments(HighLogic.CurrentGame);
                    }
                }
                else
                {
                    Log.Debug("New vessel, but no matching protovessel available");
                }
                #endregion
            }
        }

        private IEnumerator<WaitForFixedUpdate> UpdateVesselOnNextFixedUpdate(Vessel extant_vessel, KMPVessel kvessel, KMPVesselUpdate vessel_update)
        {
            yield return new WaitForFixedUpdate();
            if (extant_vessel != null)
            {
                Log.Debug("UVONFU Name: " + extant_vessel.name + ", Type: " + extant_vessel.vesselType + ", Loaded: " + extant_vessel.loaded + ", Packed: " + extant_vessel.packed);
                //Loaded means within the 2.5km load limit.
                if (extant_vessel.loaded)
                {
                    //Set the flight control state
                    if (FlightGlobals.ActiveVessel != null && isInFlight)
                    { 
                        FlightCtrlState flightState = vessel_update.flightCtrlState.getAsFlightCtrlState();
                        if (extant_vessel.loaded && !extant_vessel.packed && flightState != null)
                        {
                            if (isObserving && extant_vessel.id == FlightGlobals.ActiveVessel.id)
                            {
                                FlightInputHandler.state.CopyFrom(flightState);
                            }
                            if (extant_vessel.id != FlightGlobals.ActiveVessel.id)
                            {
                                extant_vessel.ctrlState.CopyFrom(flightState);
                            }
                        }
                        if (extant_vessel.loaded && !extant_vessel.packed && vessel_update.actionState != null)
                        {
                            //Doing this because Bad Things (TM) might happen if we keep setting stage to true - Although that's worth looking at too.
                            extant_vessel.ActionGroups.SetGroup(KSPActionGroup.Gear, vessel_update.actionState.GetValue("Gear").StartsWith("True"));
                            extant_vessel.ActionGroups.SetGroup(KSPActionGroup.Light, vessel_update.actionState.GetValue("Light").StartsWith("True"));
                            extant_vessel.ActionGroups.SetGroup(KSPActionGroup.Brakes, vessel_update.actionState.GetValue("Brakes").StartsWith("True"));
                            extant_vessel.ActionGroups.SetGroup(KSPActionGroup.SAS, vessel_update.actionState.GetValue("SAS").StartsWith("True"));
                            extant_vessel.ActionGroups.SetGroup(KSPActionGroup.RCS, vessel_update.actionState.GetValue("RCS").StartsWith("True"));
                        }

                    }
                    //Vessel is loaded and not on rails.
                    if (!extant_vessel.packed)
                    {
                        //Needed: Someone who understands quaterions so we can use quaterion rotation.
                        //extant_vessel.SetRotation(kvessel.referenceRotation);

                        //This is the old way. It works.
                        Vector3 transformForward = extant_vessel.mainBody.transform.TransformDirection(kvessel.referenceForward.normalized);
                        Vector3 transformUp = extant_vessel.mainBody.transform.TransformDirection(kvessel.referenceUp);
                        extant_vessel.transform.LookAt(extant_vessel.transform.position + transformForward, transformUp);
                        extant_vessel.SetRotation(extant_vessel.transform.rotation);


                        //Someone will figure this out one day :-/
                        //extant_vessel.angularVelocity = Vector3.zero;
                        extant_vessel.angularVelocity = kvessel.referenceAngularVelocity;

                        if (kvessel.useSurfacePositioning)
                        {
                            //Set the position from the reference lat/long/alt, includes prediciton if the update is close to our universe time.
                            extant_vessel.SetPosition(kvessel.surfaceModePosition);
                            //Set the velocity from the reference velocity - includes prediction if the update is close to our universe time.
                            Vector3d deltaVelocity = kvessel.surfaceModeVelocity - extant_vessel.srf_velocity;
                            extant_vessel.ChangeWorldVelocity(deltaVelocity);
                        }
                        else
                        {
                            extant_vessel.SetPosition(kvessel.referenceOrbit.getTruePositionAtUT(Planetarium.GetUniversalTime()));
                            Vector3d deltaVelocity = kvessel.referenceOrbit.getOrbitalVelocityAtUT(Planetarium.GetUniversalTime()).xzy - extant_vessel.orbit.getOrbitalVelocityAtUT(Planetarium.GetUniversalTime()).xzy;
                            extant_vessel.ChangeWorldVelocity(deltaVelocity);
                        }

                    }
                    else
                    {
                        //Vessel is loaded but on rails
                        if (extant_vessel.Landed)
                        {
                            extant_vessel.SetPosition(kvessel.surfaceModePosition);
                        }
                        else
                        {
                            //Only set the orbit if the update time difference isn't too great.
                            if (Math.Abs(Planetarium.GetUniversalTime() - kvessel.referenceUT) < 10)
                            {
                                SetOrbit(extant_vessel.orbitDriver.orbit, kvessel.referenceOrbit);
                            }
                        }
                    }
                }
                else
                {
                    //Vessel isn't loaded (by definintion, it's also on rails).
                    if (extant_vessel.Landed)
                    {
                        extant_vessel.SetPosition(kvessel.surfaceModePosition);
                    }
                    else
                    {
                        SetOrbit(extant_vessel.orbitDriver.orbit, kvessel.referenceOrbit);
                    }
                }
                if (kvessel.referenceOrbit != null)
                {
                    if (kvessel.orbitRenderer.driver.orbit == null)
                    {
                        kvessel.orbitRenderer.driver.orbit = new Orbit();
                    }
                    SetOrbit(kvessel.orbitRenderer.driver.orbit, kvessel.referenceOrbit);
                }
            }
        }
        //Hyperedit's idea
        private void SetOrbit(Orbit oldOrbit, Orbit newOrbit)
        {
            oldOrbit.inclination = newOrbit.inclination;
            oldOrbit.eccentricity = newOrbit.eccentricity;
            oldOrbit.semiMajorAxis = newOrbit.semiMajorAxis;
            oldOrbit.LAN = newOrbit.LAN;
            oldOrbit.meanAnomalyAtEpoch = newOrbit.meanAnomalyAtEpoch;
            oldOrbit.epoch = newOrbit.epoch;
            oldOrbit.argumentOfPeriapsis = newOrbit.argumentOfPeriapsis;
            oldOrbit.referenceBody = newOrbit.referenceBody;
            //Init+UpdateFromUT brings the orbit into our time frame.
            oldOrbit.Init();
            //oldOrbit.UpdateFromUT(Planetarium.GetUniversalTime());
        }

        private void checkProtoNodeCrew(ConfigNode protoNode)
        {
            IEnumerator<ProtoCrewMember> crewEnum = HighLogic.CurrentGame.CrewRoster.GetEnumerator();
            int applicants = 0;
            while (crewEnum.MoveNext())
                if (crewEnum.Current.rosterStatus == ProtoCrewMember.RosterStatus.AVAILABLE)
                    applicants++;
		
            foreach (ConfigNode partNode in protoNode.GetNodes("PART"))
            {
                foreach (string crew in partNode.GetValues("crew"))
                {
                    int crewValue = Convert.ToInt32(crew);
                    crewValue++;
                    if (crewValue > applicants)
                    {
                        Log.Debug("Adding crew applicants");
                        for (int i = 0; i < (crewValue-applicants);)
                        {
                            ProtoCrewMember protoCrew = CrewGenerator.RandomCrewMemberPrototype();
                            if (!HighLogic.CurrentGame.CrewRoster.ExistsInRoster(protoCrew.name))
                            {
                                HighLogic.CurrentGame.CrewRoster.AddCrewMember(protoCrew);
                                i++;
                            }
                        }
                    }
                }
            }	
        }

        private void setPartOpacity(Part part, float opacity)
        {
            try
            {
                if (part != null ? part.vessel != null : false)
                {
                    part.setOpacity(opacity);
                }
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in setPartOpacity(), Exception: {0}", e.ToString());
            }
        }

        private bool checkOrbitForCollision(Orbit orbit, double tick, double fromTick)
        {
            CelestialBody body = orbit.referenceBody;
            bool boom = orbit.PeA < body.maxAtmosphereAltitude && orbit.timeToPe < (tick - fromTick);
            if (boom)
                Log.Debug("Orbit collided with surface");
            //else Log.Debug("Orbit does not collide with body: {0} {1} {2} {3} {4}",orbit.PeA,body.maxAtmosphereAltitude,orbit.timeToPe,tick,fromTick);
            return boom;
        }

        private void addRemoteVessel(ProtoVessel protovessel, KMPVessel kvessel, KMPVesselUpdate update, double distance)
        {
            #region addRemoteVessel - Sanity checks
            if (protovessel == null)
            {
                Log.Warning("Skipped adding null protovessel!");
                return;
            }

            Log.Debug("addRemoteVessel: " + update.id.ToString() + ", name: " + protovessel.vesselName.ToString() + ", type: " + protovessel.vesselType.ToString());            

            if (update.id == FlightGlobals.ActiveVessel.id && !serverVessels_InUse.ContainsKey(update.id))
            {
                Log.Debug("Vessel add skipped, Active and not in use.");
                return;
            }

            if (serverVessels_AddDelay.ContainsKey(update.kmpID) ? serverVessels_AddDelay[update.kmpID] >= UnityEngine.Time.realtimeSinceStartup : false)
            {
                Log.Debug("Skipped recently added vessel.");
                return;
            }
            serverVessels_AddDelay[update.kmpID] = UnityEngine.Time.realtimeSinceStartup + 5f;

            #endregion

            if (protovessel.vesselType == VesselType.Flag)
            {
                //This is a workaround - Sometimes a flags lock was getting incorrectly set.
                Invoke("ClearFlagLock", 5f);
            }
            bool wasLoaded = false;
            bool wasActive = false;
            bool setTarget = false;

            Vessel extant_vessel = FlightGlobals.Vessels.Find(v => v.id == update.id);

            if (extant_vessel != null)
            {
                wasLoaded = extant_vessel.loaded;

                if (protovessel.vesselType == VesselType.EVA && wasLoaded)
                {
                    return; //Don't touch EVAs here
                }

                if (FlightGlobals.fetch.VesselTarget != null ? extant_vessel.id == FlightGlobals.fetch.VesselTarget.GetVessel().id : false)
                {
                    setTarget = true;
                }

                if (extant_vessel.id == FlightGlobals.ActiveVessel.id)
                {
                    wasActive = true;
                }
            }

            try
            {
                if ((protovessel.vesselType != VesselType.Debris && protovessel.vesselType != VesselType.Unknown) && protovessel.situation == Vessel.Situations.SUB_ORBITAL && protovessel.altitude < 25d)
                {
                    //Land flags, vessels and EVAs that are on sub-orbital trajectory
                    Log.Debug("Placing sub-orbital protovessel on surface.");
                    protovessel.situation = Vessel.Situations.LANDED;
                    protovessel.landed = true;
                    if (protovessel.vesselType == VesselType.Flag)
                    {
                        protovessel.height = -1;
                    }
                }
                else
                {
                    if (protovessel.vesselType == VesselType.Debris && protovessel.situation == Vessel.Situations.SUB_ORBITAL)
                    {
                        //Don't bother with suborbital debris
                        return;
                    }
                }
				
                CelestialBody body = null;
				
                if (update != null)
                {
                    body = FlightGlobals.Bodies.Find(b => b.name == update.bodyName);
                    if (update.situation == Situation.FLYING)
                    {
                        if (body.atmosphere && body.maxAtmosphereAltitude > protovessel.altitude)
                        {
                            //In-atmo vessel--only load if within visible range.
                            if (distance > 2000d && !wasActive)
                            {
                                Log.Debug("Refusing to load in-atmo flying vessel, distance: " + distance);
                                return;
                            }
                        }
                    }
                }

                if (isProtoVesselInSafetyBubble(protovessel)) //refuse to load anything too close to the KSC
                {
                    Log.Debug("Refusing to load protovessel in safety bubble.");
                    return;
                }

                if (extant_vessel != null)
                {
                    if (wasActive)
                    {
                        extant_vessel.MakeInactive();
                        serverVessels_InUse[extant_vessel.id] = false;
                        serverVessels_IsPrivate[extant_vessel.id] = false;
                        serverVessels_IsMine[extant_vessel.id] = true;
                    }
                }

                StartCoroutine(loadProtovessel(wasLoaded, wasActive, setTarget, protovessel, kvessel, update, distance));
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in addRemoteVessel(), catch 3, Exception: {0}", e.ToString());
                Log.Debug("Error adding remote vessel: " + e.Message + " " + e.StackTrace);
            }
        }

        private IEnumerator<WaitForFixedUpdate> loadProtovessel(bool wasLoaded, bool wasActive, bool setTarget, ProtoVessel protovessel, KMPVessel kvessel, KMPVesselUpdate update, double distance = 501d)
        {
            yield return new WaitForFixedUpdate();

            killVessel(kvessel.id);

            serverVessels_PartCounts[update.id] = protovessel.protoPartSnapshots.Count;
            try
            {
                OrbitPhysicsManager.HoldVesselUnpack(1);
            }
            catch
            {
            }
            protovessel.Load(HighLogic.CurrentGame.flightState);
            Vessel created_vessel = protovessel.vesselRef;
            kvessel.id = created_vessel.id;
            if (created_vessel != null)
            {
                /*
                FlightGlobals.fetch.vessels.Add(created_vessel);
                */
                if (!created_vessel.loaded && wasLoaded)
                {
                    created_vessel.Load();
                }
                Log.Debug(created_vessel.id.ToString() + " initializing: ProtoParts=" + protovessel.protoPartSnapshots.Count + ",Parts=" + created_vessel.Parts.Count + ",Sit=" + created_vessel.situation.ToString() + ",type=" + created_vessel.vesselType + ",alt=" + protovessel.altitude);
                if (created_vessel.Parts != null)
                {
                    serverVessels_PartCounts[update.id] = created_vessel.Parts.Count;
                    /*
                    serverVessels_Parts[vessel_id] = new List<Part>();
                    if (created_vessel.parts != null)
                    {
                        serverVessels_Parts[vessel_id].AddRange(created_vessel.parts);
                    }
                    */
                }
                else
                {
                    Log.Debug("Parts is null.");
                }
                if (created_vessel.vesselType != VesselType.Flag && created_vessel.vesselType != VesselType.EVA)
                {
                    /*
                        foreach (Part part in created_vessel.Parts)
                        {
                            part.OnLoad();
                            part.explosionPotential = 0;
                            part.terrainCollider = new PQS_PartCollider();
                            part.terrainCollider.part = part;
                            part.terrainCollider.useVelocityCollider = false;
                            part.terrainCollider.useGravityCollider = false;
                            part.breakingForce = float.MaxValue;
                            part.breakingTorque = float.MaxValue;
                        
                        }
                    */
                }
                if (setTarget)
                {
                    StartCoroutine(setDockingTarget(created_vessel));
                }

                if (wasActive)
                {
                    StartCoroutine(setActiveVessel(created_vessel));
                }

                Log.Debug(created_vessel.id.ToString() + " initialized");
            }
            else
            {
                Log.Debug("New protovessel failed to load.");
            }
        }

        private void setNoClip(Vessel vessel)
        {
            try
            {
                if (vessel != null ? vessel.parts != null : false)
                {
                    foreach (Part part in vessel.parts)
                    {
                        if (part != null)
                        {
                            part.rigidbody.detectCollisions = false;
                            part.rigidbody.isKinematic = false;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log.Warning("Failed to noclip parts: " + e.ToString());
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
                catch (Exception e)
                {
                    Log.Debug("Exception thrown in safeDelete(), catch 1, Exception: {0}", e.ToString());
                }
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
        public void acceptClientInterop(byte[] bytes)
        {
            lock (interopInQueueLock)
            {
                try
                {
//					int id_int = KMPCommon.intFromBytes(bytes, 4);
//					KMPCommon.ClientInteropMessageID id = KMPCommon.ClientInteropMessageID.NULL;
//					if (id_int >= 0 && id_int < Enum.GetValues(typeof(KMPCommon.ClientInteropMessageID)).Length)
//						id = (KMPCommon.ClientInteropMessageID)id_int;

                    interopInQueue.Enqueue(bytes);
                }
                catch (Exception e)
                {
                    Log.Debug("Exception thrown in acceptClientInterop(), catch 1, Exception: {0}", e.ToString());
                }
            }
        }

        private void processClientInterop()
        {	
            if (interopInQueue.Count > 0)
            {
                try
                {
                    while (interopInQueue.Count > 0)
                    {
                        byte[] bytes;
                        bytes = interopInQueue.Dequeue();

                        //Read the message id
                        int id_int = KMPCommon.intFromBytes(bytes, 0);
		
                        KMPCommon.ClientInteropMessageID id = KMPCommon.ClientInteropMessageID.NULL;
                        if (id_int >= 0 && id_int < Enum.GetValues(typeof(KMPCommon.ClientInteropMessageID)).Length)
                            id = (KMPCommon.ClientInteropMessageID)id_int;
		
                        //Read the length of the message data
                        int data_length = KMPCommon.intFromBytes(bytes, 4);
		
                        if (data_length <= 0)
                            handleInteropMessage(id, null);
                        else
                        {
                            //Copy the message data
                            byte[] data = new byte[data_length];
                            Array.Copy(bytes, 8, data, 0, data.Length);
                            handleInteropMessage(id, data);
                        }
                    }
                }
                catch (Exception e)
                {
                    Log.Debug("Exception thrown in processClientInterop(), catch 1, Exception: {0}", e.ToString());
                }
            }
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
	
                            updateInterval = ((float)KMPCommon.intFromBytes(data, 5)) / 1000.0f;
	
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
	
                    case KMPCommon.ClientInteropMessageID.SCENARIO_UPDATE:
                        if (data != null)
                        {
                            //De-serialize and handle the update
                            handleScenarioUpdate(KSP.IO.IOUtils.DeserializeFromBinary(data));
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
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in handleInteropMessage(), catch 1, Exception: {0}", e.ToString());
                Log.Debug(e.Message);
            }
        }

        private IEnumerator<WaitForEndOfFrame> applyScreenshotTexture(byte[] image_data)
        {
            yield return new WaitForEndOfFrame();
            KMPScreenshotDisplay.texture = new Texture2D(4, 4, TextureFormat.RGB24, false, true);
            Log.Debug("applying screenshot");
            if (KMPScreenshotDisplay.texture.LoadImage(image_data))
            {
                KMPScreenshotDisplay.texture.Apply();
                Log.Debug("applied");
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
                Log.Debug("image not loaded");
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

            KMPClientMain.acceptPluginInterop(message_bytes);
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

            KMPGlobalSettings.instance.chatDXDisplayWindowX = KMPChatDX.chatboxX;
            KMPGlobalSettings.instance.chatDXDisplayWindowY = KMPChatDX.chatboxY;
            KMPGlobalSettings.instance.chatDXDisplayWindowWidth = KMPChatDX.chatboxWidth;
            KMPGlobalSettings.instance.chatDXDisplayWindowHeight = KMPChatDX.chatboxHeight;

            KMPGlobalSettings.instance.chatDXOffsetEnabled = KMPChatDX.offsettingEnabled;
            KMPGlobalSettings.instance.chatDXEditorOffsetX = KMPChatDX.editorOffsetX;
            KMPGlobalSettings.instance.chatDXEditorOffsetY = KMPChatDX.editorOffsetY;
            KMPGlobalSettings.instance.chatDXTrackingOffsetX = KMPChatDX.trackerOffsetX;
            KMPGlobalSettings.instance.chatDXTrackingOffsetY = KMPChatDX.trackerOffsetY;

            //Serialize global settings to file
            try
            {
                byte[] serialized = KSP.IO.IOUtils.SerializeToBinary(KMPGlobalSettings.instance);
                KSP.IO.File.WriteAllBytes<KMPManager>(serialized, GLOBAL_SETTINGS_FILENAME);
                Log.Debug("Saved Global Settings to file.");
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in saveGlobalSettings(), catch 1, Exception: {0}", e.ToString());
                Log.Debug(e.Message);
            }
        }

        private void loadGlobalSettings()
        {
            bool success = false;
            try
            {
                //Deserialize global settings from file
                String sPath = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location);
                sPath += "/PluginData/KerbalMultiPlayer/";
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

                    if (KMPGlobalSettings.instance.screenshotKey == KeyCode.None)
                        KMPGlobalSettings.instance.screenshotKey = KeyCode.F8;

                    if (KMPGlobalSettings.instance.chatTalkKey == KeyCode.None)
                        KMPGlobalSettings.instance.chatTalkKey = KeyCode.BackQuote;

                    if (KMPGlobalSettings.instance.chatHideKey == KeyCode.None)
                        KMPGlobalSettings.instance.chatHideKey = KeyCode.F9;

                    if (KMPGlobalSettings.instance.screenshotToggleKey == KeyCode.None)
                        KMPGlobalSettings.instance.screenshotToggleKey = KeyCode.F10;


                    KMPScreenshotDisplay.windowPos.x = KMPGlobalSettings.instance.screenshotDisplayWindowX;
                    KMPScreenshotDisplay.windowPos.y = KMPGlobalSettings.instance.screenshotDisplayWindowY;

                    KMPChatDisplay.windowPos.x = KMPGlobalSettings.instance.chatDisplayWindowX;
                    KMPChatDisplay.windowPos.y = KMPGlobalSettings.instance.chatDisplayWindowY;

                    KMPChatDX.chatboxX = KMPGlobalSettings.instance.chatDXDisplayWindowX;
                    KMPChatDX.chatboxY = KMPGlobalSettings.instance.chatDXDisplayWindowY;

                    KMPChatDX.chatboxWidth = KMPGlobalSettings.instance.chatDXDisplayWindowWidth;
                    KMPChatDX.chatboxHeight = KMPGlobalSettings.instance.chatDXDisplayWindowHeight;

                    KMPChatDX.offsettingEnabled = KMPGlobalSettings.instance.chatDXOffsetEnabled;
                    KMPChatDX.editorOffsetX = KMPGlobalSettings.instance.chatDXEditorOffsetX;
                    KMPChatDX.editorOffsetY = KMPGlobalSettings.instance.chatDXEditorOffsetY;
                    KMPChatDX.trackerOffsetX = KMPGlobalSettings.instance.chatDXTrackingOffsetX;
                    KMPChatDX.trackerOffsetY = KMPGlobalSettings.instance.chatDXTrackingOffsetY;


                    success = true;
                }
            }
            catch (KSP.IO.IOException e)
            {
                Log.Debug("Exception thrown in loadGlobalSettings(), catch 1, Exception: {0}", e.ToString());
                success = false;
                Log.Debug(e.Message);
            }
            catch (System.IO.IOException e)
            {
                Log.Debug("Exception thrown in loadGlobalSettings(), catch 2, Exception: {0}", e.ToString());
                success = false;
                Log.Debug(e.Message);
            }
            catch (System.IO.IsolatedStorage.IsolatedStorageException e)
            {
                Log.Debug("Exception thrown in loadGlobalSettings(), catch 3, Exception: {0}", e.ToString());
                success = false;
                Log.Debug(e.Message);
            }
            if (!success)
            {
                try
                {
                    KSP.IO.File.Delete<KMPManager>(GLOBAL_SETTINGS_FILENAME);
                }
                catch (Exception e)
                {
                    Log.Debug("Exception thrown in loadGlobalSettings(), catch 4, Exception: {0}", e.ToString());
                }
                KMPGlobalSettings.instance = new KMPGlobalSettings();
            }
        }
        //MonoBehaviour
        public void Awake()
        {
            DontDestroyOnLoad(this.gameObject);
            CancelInvoke();
            InvokeRepeating("updateStep", 1 / 30.0f, 1 / 30.0f);
            loadGlobalSettings();
            try
            {
                platform = Environment.OSVersion.Platform;
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in Awake(), catch 1, Exception: {0}", e.ToString());
                platform = PlatformID.Unix;
            }
            LoadedModfiles = new List<LoadedFileInfo>();
            try
            {
                List<string> filenames = System.IO.Directory.GetFiles(KSPUtil.ApplicationRootPath + "GameData", "*.dll", System.IO.SearchOption.AllDirectories).ToList(); // add files that weren't immediately loaded (e.g. files that plugins use later)
                filenames = filenames.ConvertAll(x => new System.IO.DirectoryInfo(x).FullName);
                filenames = filenames.Distinct(StringComparer.CurrentCultureIgnoreCase).ToList();
                ShaFinishedEvent = new ManualResetEvent(false);
                numberOfFilesToCheck = filenames.Count;
                foreach (string file in filenames)
                {
                    LoadedFileInfo Entry = new LoadedFileInfo(file);
                    LoadedModfiles.Add(Entry);
                    ThreadPool.QueueUserWorkItem(new WaitCallback(Entry.HandleHash));
                }
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in Awake(), catch 2, Exception: {0}", e.ToString());
            }
            Log.Debug("KMP loaded");
        }

        private void Start()
        {
            if (ScaledSpace.Instance == null ? ScaledSpace.Instance.scaledSpaceTransforms == null : false)
            {
                return;
            }
            Log.Debug("Clearing ScaledSpace transforms, count: " + ScaledSpace.Instance.scaledSpaceTransforms.Count);
            ScaledSpace.Instance.scaledSpaceTransforms.RemoveAll(t => t == null);
            if (HighLogic.LoadedScene == GameScenes.MAINMENU)
            {
                ScaledSpace.Instance.scaledSpaceTransforms.RemoveAll(t => !FlightGlobals.Bodies.Exists(b => b.name == t.name));
            }
            Log.Debug("New count: " + ScaledSpace.Instance.scaledSpaceTransforms.Count);
        }

        private void OnPartCouple(GameEvents.FromToAction<Part,Part> data)
        {
            docking = true;
            Log.Debug("Dock event: " + data.to.vessel.id + " " + data.from.vessel.id);
            //Destroy old vessels for other players
            removeDockedVessel(data.from.vessel);
            removeDockedVessel(data.to.vessel);
            //Fix displayed crew
            clearCrewGUI();
            Invoke("setMidDocking", 2f);
        }

        private void setMidDocking()
        {
            if (FlightGlobals.ActiveVessel != null ? !FlightGlobals.ActiveVessel.packed : false)
            {
                StartCoroutine(writePrimaryUpdate());
                Invoke("setFinishDocking", 2f);
            }
            else
            {
                Invoke("setMidDocking", 1f);
            }
        }

        private void setFinishDocking()
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
            //docking = true;
            Log.Debug("Undock event");
            if (data.vessel != null)
            {
                serverVessels_PartCounts[data.vessel.id] = 0;
                serverVessels_ProtoVessels.Remove(data.vessel.id);
                serverVessels_InUse[data.vessel.id] = false;
                serverVessels_IsMine[data.vessel.id] = true;
                sendVesselMessage(data.vessel, true);
            }
            //Invoke("setFinishDocking",1f);
        }

        private void OnCrewOnEva(GameEvents.FromToAction<Part,Part> data)
        {
            Log.Debug("EVA event");
            if (data.from.vessel != null)
                sendVesselMessage(data.from.vessel);
        }

        private void OnCrewBoardVessel(GameEvents.FromToAction<Part,Part> data)
        {
            Log.Debug("End EVA event");
            if (data.to.vessel != null)
                sendVesselMessage(data.to.vessel);
            if (lastEVAVessel != null)
                sendRemoveVesselMessage(lastEVAVessel);
        }

        private void OnVesselLoaded(Vessel data)
        {
            Log.Debug("Vessel loaded: " + data.id);
        }

        private void OnVesselTerminated(ProtoVessel data)
        {
            Log.Debug("Vessel termination: " + data.vesselID + " " + serverVessels_RemoteID.ContainsKey(data.vesselID) + " " + (HighLogic.LoadedScene == GameScenes.TRACKSTATION) + " " + (data.vesselType == VesselType.Debris || (serverVessels_IsMine.ContainsKey(data.vesselID) ? serverVessels_IsMine[data.vesselID] : true)));
            if (serverVessels_RemoteID.ContainsKey(data.vesselID) && HighLogic.LoadedScene == GameScenes.TRACKSTATION)
            {
                activeTermination = true;
            }
        }

        private void OnVesselDestroy(Vessel data)
        {
            #region OnVesselDestroy sanity checks
            if (data == null)
            {
                return;
            }
            if (data.id.ToString() == SYNC_PLATE_ID)
            {
                return;
            }
            if (!isInFlightOrTracking)
            {
                return;
            }
            #endregion

            Log.Debug("Vessel " + data.id + " was destroyed, name: " + data.name);

            if (!serverVessels_RemoteID.ContainsKey(data.id))
            {
                Log.Debug("Non-KMP vessel was destroyed!");
                return;
            }

            if (!docking) //Don't worry about destruction events during docking, could be other player updating us
            {
                //Mark vessel to stay unloaded for a bit, to help prevent any performance impact from vessels that are still in-universe, but that can't load under current conditions
                if (serverVessels_RemoteID.ContainsKey(data.id))
                {
                    Log.Debug("Keeping " + data.id + " unloaded for 10 seconds.");
                    serverVessels_AddDelay[serverVessels_RemoteID[data.id]] = UnityEngine.Time.realtimeSinceStartup + 10f;
                }

                //Is our current vessel
                bool isCurrentVessel = (data == FlightGlobals.ActiveVessel);
                //Is debris
                bool isDebris = (data.vesselType == VesselType.Debris);
                //Is ours
                bool isMine = (serverVessels_IsMine.ContainsKey(data.id) ? serverVessels_IsMine[data.id] : false);
                //Is public
                bool isPrivate = (serverVessels_IsPrivate.ContainsKey(data.id) ? serverVessels_IsPrivate[data.id] : false);
                //!isInFlight means isInTracking due to the return above.
                if ((isInFlight && (isCurrentVessel || isDebris)) || (!isInFlight && activeTermination && (isDebris || isMine || !isPrivate)))
                {
                    activeTermination = false;
                    Log.Debug("Vessel destroyed: " + data.id + ", name: " + data.name);
                    sendRemoveVesselMessage(data);
                }
            }
        }

        private void OnProgressComplete(ProgressNode data)
        {
            sendScenarios();
        }

        private void OnProgressReached(ProgressNode data)
        {
            sendScenarios();
        }

        private void OnGUIRnDComplexDespawn()
        {
            sendScenarios();
        }

        private void OnTechnologyResearched(GameEvents.HostTargetAction<RDTech,RDTech.OperationResult> data)
        {
            sendScenarios();
        }

        private void OnVesselRecovered(ProtoVessel data)
        {
            Vessel vessel = FlightGlobals.Vessels.Find(v => v.id == data.vesselID);
            sendRemoveVesselMessage(vessel, false);
            sendScenarios();
        }

        private void OnTimeWarpRateChanged()
        {
            if (TimeWarp.CurrentRate <= 1)
            {
                syncing = true;
                inGameSyncing = true;
                Invoke("setNotWarping", 1f);
                Log.Debug("Done warping");
            }
            else
            {
                if (!warping)
                {
                    warping = true;
                    skewServerTime = 0;
                    skewTargetTick = 0;
                    StartCoroutine(writePrimaryUpdate()); //Ensure server catches any vessel switch before warp
                    Log.Debug("Started warping");
                }
            }
            //Log.Debug("sending: " + TimeWarp.CurrentRate + ", " + Planetarium.GetUniversalTime());
            byte[] update_bytes = new byte[12]; //warp rate float (4) + current tick double (8)
            BitConverter.GetBytes(TimeWarp.CurrentRate).CopyTo(update_bytes, 0);
            BitConverter.GetBytes(Planetarium.GetUniversalTime()).CopyTo(update_bytes, 4);
            enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.WARPING, update_bytes);
        }

        private void setNotWarping()
        {
            warping = false;	
        }

        private void OnFirstFlightReady()
        {
            if (syncing && !forceQuit)
            {
                GameEvents.onFlightReady.Remove(this.OnFirstFlightReady);
                GameEvents.onFlightReady.Add(this.OnFlightReady);
                MapView.EnterMapView();
                MapView.MapCamera.SetTarget("Kerbin");
                StartCoroutine(HandleSyncTimeout(300));
                docking = false;
                //NTP will start the syncronize after the clock is syncronized.
            }
            delayForceQuit = false;
        }

        private void sendInitialSyncRequest()
        {
            if (isInFlightOrTracking)
            {
                Log.Debug("Requesting initial sync");
                StartCoroutine(sendSubspaceSyncRequest(-1, true));
            }
            else
            {
                Invoke("sendInitialSyncRequest", 0.25f);
            }
        }

        private void OnFlightReady()
        {
            removeKMPControlLocks();
            //Ensure vessel uses only stock parts in lieu of proper mod support
            if (!FlightGlobals.ActiveVessel.isEVA && !FlightGlobals.ActiveVessel.protoVessel.protoPartSnapshots.TrueForAll(pps => KMPClientMain.partList.Contains(pps.partName)))
            {
                Log.Debug("Loaded vessel has prohibited parts!");
                foreach (ProtoPartSnapshot pps in FlightGlobals.ActiveVessel.protoVessel.protoPartSnapshots)
                    Log.Debug(pps.partName);
                syncing = true;
                GameEvents.onFlightReady.Add(this.OnFirstFlightReady);
                GameEvents.onFlightReady.Remove(this.OnFlightReady);
                HighLogic.CurrentGame.Start();
                ScreenMessages.PostScreenMessage("Can't start flight - Vessel has prohibited parts! Sorry!", 10f, ScreenMessageStyle.UPPER_CENTER);
            }
        }

        public void HandleSyncCompleted()
        {
            if (gameRunning && !forceQuit && syncing)
            {
                if (!inGameSyncing)
                {
                    Invoke("finishSync", 1f);
                }
                else
                {
                    Invoke("finishInGameSync", 1f);
                }
            }

        }

        private void finishInGameSync()
        {
            syncing = false;
            inGameSyncing = false;
            showServerSync = false;
        }

        private IEnumerator<WaitForSeconds> HandleSyncTimeout(int timeout)
        {
            yield return new WaitForSeconds(1f);
            timeout--;
            if (timeout == 0)
            {
                disconnect("Sync Timeout");
                KMPClientMain.sendConnectionEndMessage("Sync Timeout");
                KMPClientMain.endSession = true;
                forceQuit = true;
                KMPClientMain.SetMessage("Disconnected: Sync timeout");
            }
            else
            {
                if (syncing)
                {
                    StartCoroutine(HandleSyncTimeout(timeout));
                }
            }
        }

        private void finishSync()
        {
            if (!forceQuit && syncing && gameRunning)
            {
                if (vesselLoadedMessage != null)
                {
                    vesselLoadedMessage.duration = 0f;
                }
                vesselLoadedMessage = ScreenMessages.PostScreenMessage("Universe synchronized!", 1f, ScreenMessageStyle.UPPER_RIGHT);
                StartCoroutine(returnToSpaceCenter());
                //Disable debug logging once synced unless explicitly enabled
            }
        }

        private void ClearFlagLock()
        {
            Log.Debug("Clearing flag locks");
            InputLockManager.RemoveControlLock("Flag_NoInterruptWhileDeploying");
        }

        private void krakensBaneWarp(double newTick)
        {
            if (warping)
            {
                return;
            }

            //Warp our vessel if we are orbital.

            bool putActiveVesselOnRails = false;
            if (isInFlight && FlightGlobals.ActiveVessel != null)
            {
                putActiveVesselOnRails = KMPVessel.situationIsOrbital((Situation)FlightGlobals.ActiveVessel.situation);
            }

            //Prevent vessel unpacks for 1 update
            try
            {
                OrbitPhysicsManager.HoldVesselUnpack(1);
            }
            catch
            {
            }

            //Put all the other vessels on rails
            foreach (Vessel vessel in FlightGlobals.Vessels.Where(v => v.packed == false))
            {
                if (FlightGlobals.ActiveVessel != vessel || putActiveVesselOnRails)
                {
                    vessel.GoOnRails();
                }
            }

            //Set the time
            Planetarium.SetUniversalTime(newTick);
        }

        private void SkewTime()
        {
            if (syncing || warping)
            {
                return;
            }
            if (!isTimeSyncronized)
            {
                return;
            }

            if (HighLogic.LoadedScene == GameScenes.EDITOR || HighLogic.LoadedScene == GameScenes.SPH)
                return; //Time does not advance in the VAB or SPH

            if (!isInFlightOrTracking && isSkewingTime)
            {
                isSkewingTime = false;
                Time.timeScale = 1f;
                return;
            }


            //This brings the computers MET timer in to line with the server.
            if (isInFlightOrTracking && skewServerTime != 0 && skewTargetTick != 0)
            {
                long timeFromLastSync = (DateTime.UtcNow.Ticks + offsetSyncTick) - skewServerTime;
                double timeFromLastSyncSeconds = (double)timeFromLastSync / 10000000;
                double timeFromLastSyncSecondsAdjusted = timeFromLastSyncSeconds * skewSubspaceSpeed;
                double currentError = Planetarium.GetUniversalTime() - (skewTargetTick + timeFromLastSyncSecondsAdjusted); //Ticks are integers of 100ns, Planetarium camera is a float in seconds.
                double currentErrorMs = Math.Round(currentError * 1000, 2);

                if (Math.Abs(currentError) > 5)
                {
                    if (skewMessage != null)
                    {
                        skewMessage.duration = 0f;
                    }
                    krakensBaneWarp(skewTargetTick + timeFromLastSyncSecondsAdjusted);
                    return;
                }

                //Dynamic warp.
                float timeWarpRate = (float)Math.Pow(2, -currentError);
                if (timeWarpRate > 1.5f)
                    timeWarpRate = 1.5f;
                if (timeWarpRate < 0.5f)
                    timeWarpRate = 0.5f;

                if (Math.Abs(currentError) > 0.2)
                {
                    isSkewingTime = true;
                    Time.timeScale = timeWarpRate;
                }

                if (Math.Abs(currentError) < 0.05 && isSkewingTime)
                {
                    isSkewingTime = false;
                    Time.timeScale = 1;
                }

                //Let's give the client a little bit of time to settle before being able to request a different rate.
                if (UnityEngine.Time.realtimeSinceStartup > lastSubspaceLockChange + 10f)
                {
                    float requestedRate = (1 / timeWarpRate) * skewSubspaceSpeed;
                    listClientTimeWarp.Add(requestedRate);
                    listClientTimeWarpAverage = listClientTimeWarp.Average();
                }
                else
                {
                    listClientTimeWarp.Add(skewSubspaceSpeed);
                    listClientTimeWarpAverage = listClientTimeWarp.Average();
                }

                //Keeps the last 10 seconds (300 update steps) of clock speed history to report to the server
                if (listClientTimeWarp.Count > 300)
                {
                    listClientTimeWarp.RemoveAt(0);
                }


                if (displayNTP)
                {
                    if (skewMessage != null)
                    {
                        //Hide the old message.
                        skewMessage.duration = 0;
                    }
                    //Current clock error in milliseconds
                    String skewMessageText;
                    skewMessageText = "\n\nClock error: " + currentErrorMs + "ms.\n";
                    skewMessageText += "Game speed: " + Math.Round(Time.timeScale, 3) + "x.\n";
                    //Current client latency detected by NTP (latency - server processing time)
                    long latencySyncTickMs = latencySyncTick / 10000;
                    skewMessageText += "Network latency: " + latencySyncTickMs + "ms.\n";
                    //Current system clock offset
                    skewMessageText += "Clock offset: ";

                    long tempOffset = offsetSyncTick;
                    long offsetHours = tempOffset / 36000000000;
                    tempOffset -= offsetHours * 36000000000;
                    long offsetMinutes = tempOffset / 600000000;
                    tempOffset -= offsetMinutes * 600000000;
                    long offsetSeconds = tempOffset / 10000000;
                    tempOffset -= offsetSeconds * 10000000;
                    long offsetMilliseconds = tempOffset / 10000;

                    if (offsetHours > 0)
                    {
                        skewMessageText += offsetHours + "h, ";
                    }
                    if (offsetMinutes > 0)
                    {
                        skewMessageText += offsetMinutes + "m, ";
                    }
                    if (offsetSeconds > 0)
                    {
                        skewMessageText += offsetSeconds + "s, ";
                    }
                    skewMessageText += offsetMilliseconds + "ms.\n";
                    //Current subspace speed
                    skewMessageText += "Subspace Speed: " + Math.Round(skewSubspaceSpeed, 3) + "x.\n";
                    //Estimated server lag
                    skewMessageText += "Server lag: ";
                    long tempServerLag = estimatedServerLag;
                    long serverLagSeconds = tempServerLag / 10000000;
                    tempServerLag -= serverLagSeconds * 10000000;
                    if (serverLagSeconds > 0)
                    {
                        skewMessageText += serverLagSeconds + "s, ";
                    }
                    long serverLagMilliseconds = tempServerLag / 10000;
                    skewMessageText += serverLagMilliseconds + "ms.\n";
                    skewMessageText += "Universe Time:" + Planetarium.GetUniversalTime() + "\n";
					
                    skewMessage = ScreenMessages.PostScreenMessage(skewMessageText, 1f, ScreenMessageStyle.UPPER_RIGHT);
                }
            }
        }

        private void SyncTime()
        {
            //Have to write the actual time just before sending.
            lastTimeSyncTime = UnityEngine.Time.realtimeSinceStartup;
            enqueuePluginInteropMessage(KMPCommon.PluginInteropMessageID.SYNC_TIME, null);
        }

        public void HandleSyncTimeCompleted(byte[] data)
        {
            Int64 clientSend = BitConverter.ToInt64(data, 0);
            Int64 serverReceive = BitConverter.ToInt64(data, 8);
            Int64 serverSend = BitConverter.ToInt64(data, 16);
            Int64 clientReceive = DateTime.UtcNow.Ticks;
            //Fancy NTP algorithm
            Int64 clientLatency = (clientReceive - clientSend) - (serverSend - serverReceive);
            Int64 clientOffset = ((serverReceive - clientSend) + (serverSend - clientReceive)) / 2;
            estimatedServerLag = serverSend - serverReceive;

            //If time is synced, throw out outliers.
            if (isTimeSyncronized)
            {
                if (clientLatency < SYNC_TIME_LATENCY_FILTER)
                {
                    listClientTimeSyncOffset.Add(clientOffset);
                    listClientTimeSyncLatency.Add(clientLatency);
                }
                //If time is not synced, add all data (as there can be no outliers).
            }
            else
            {
                listClientTimeSyncOffset.Add(clientOffset);
                listClientTimeSyncLatency.Add(clientLatency);
                //Queue another sync time.
                SyncTime();
            }

            //If received enough TIME_SYNC messages, set time to syncronized.
            if ((listClientTimeSyncOffset.Count >= SYNC_TIME_VALID_COUNT) && !isTimeSyncronized)
            {
                offsetSyncTick = (Int64)listClientTimeSyncOffset.Average();
                latencySyncTick = (Int64)listClientTimeSyncLatency.Average();
                isTimeSyncronized = true;
                KMP.Log.Debug("Initial client time syncronized: " + (latencySyncTick / 10000).ToString() + "ms latency, " + (offsetSyncTick / 10000).ToString() + "ms offset");
                //Ask for the initial sync now
                sendInitialSyncRequest();
            }

            if (listClientTimeSyncOffset.Count > MAX_TIME_SYNC_HISTORY)
            {
                listClientTimeSyncOffset.RemoveAt(0);
            }

            if (listClientTimeSyncLatency.Count > MAX_TIME_SYNC_HISTORY)
            {
                listClientTimeSyncLatency.RemoveAt(0);
            }

            //Update offset timer so the physwrap skew can use it
            if (isTimeSyncronized)
            {
                offsetSyncTick = (Int64)listClientTimeSyncOffset.Average();
                latencySyncTick = (Int64)listClientTimeSyncLatency.Average();
            }
        }

        private void OnGameSceneLoadRequested(GameScenes data)
        {
            Log.Debug("OnGameSceneLoadRequested");
            if (gameRunning && data == GameScenes.SPACECENTER)
            {
                writePluginUpdate();
            }
            if (gameRunning && data == GameScenes.MAINMENU)
            {
                writePluginUpdate();
                disconnect("Quit");
            }
        }

        private void clearCrewGUI()
        {
            while (KerbalGUIManager.ActiveCrew.Count > 0)
            {
                KerbalGUIManager.RemoveActiveCrew(KerbalGUIManager.ActiveCrew.Find(k => true));
            }	
        }

        private void lockCrewGUI()
        {
            FlightGlobals.ActiveVessel.DespawnCrew();
            FlightEVA.fetch.DisableInterface();
        }

        private void unlockCrewGUI()
        {
            if (FlightGlobals.ActiveVessel.GetVesselCrew().Count > 0 && KerbalGUIManager.ActiveCrew.Count < 1)
            {
                FlightGlobals.ActiveVessel.SpawnCrew();
                FlightEVA.fetch.EnableInterface();
            }
        }

        public void Update()
        {
            try
            {
                //Don't do anything if kmp isn't running or we are loading.
                if (!gameRunning || HighLogic.LoadedScene == GameScenes.LOADING)
                {
                    return;
                }

                if (pauseMenu != null)
                {
                    bool closePauseMenu = false;
                    if (syncing)
                    {
                        if (PauseMenu.isOpen)
                        {
                            if (KMPClientMain.tcpClient != null && isTimeSyncronized)
                            {
                                closePauseMenu = true;
                            }
                            else
                            {
                                disconnect("Connection terminated during sync");
                                forceQuit = true;
                            }
                        }
                        else
                        {
                            if (PauseMenu.isOpen && closePauseMenu)
                            {
                                closePauseMenu = false;
                                PauseMenu.Close();
                            }
                        }
                    }

                    if (PauseMenu.isOpen && closePauseMenu)
                    {
                        closePauseMenu = false;
                        PauseMenu.Close();
                    }
                }

                //Disable pause
                if (FlightDriver.Pause)
                    FlightDriver.SetPause(false);

                //If server cheats are disabled, turn them off.
                if (gameCheatsEnabled == false)
                {
                    CheatOptions.InfiniteFuel = false;
                    CheatOptions.InfiniteEVAFuel = false;
                    CheatOptions.InfiniteRCS = false;
                    CheatOptions.NoCrashDamage = false;
                    Destroy(FindObjectOfType(typeof(DebugToolbar)));
                }

                //Find an instance of the game's PauseMenu
                if (pauseMenu == null)
                {
                    pauseMenu = (PauseMenu)FindObjectOfType(typeof(PauseMenu));
                }

                //Find an instance of the game's RenderingManager
                if (renderManager == null)
                {
                    renderManager = (RenderingManager)FindObjectOfType(typeof(RenderingManager));
                }

                //Find an instance of the game's PlanetariumCamera
                if (planetariumCam == null)
                {
                    planetariumCam = (PlanetariumCamera)FindObjectOfType(typeof(PlanetariumCamera));
                }
                if (Input.GetKeyDown(KeyCode.F2))
                {
                    isGameHUDHidden = !isGameHUDHidden;
                }

                if (Input.GetKeyDown(KMPGlobalSettings.instance.guiToggleKey) && !isGameHUDHidden && KMPToggleButtonState)
                    KMPInfoDisplay.infoDisplayActive = !KMPInfoDisplay.infoDisplayActive;

                if (Input.GetKeyDown(KMPGlobalSettings.instance.screenshotKey))
                    StartCoroutine(shareScreenshot());
                    
                if (Input.GetKeyDown(KMPGlobalSettings.instance.screenshotToggleKey) && !isGameHUDHidden && KMPToggleButtonState)
                    KMPScreenshotDisplay.windowEnabled = !KMPScreenshotDisplay.windowEnabled;
				
                if (Input.GetKeyDown(KMPGlobalSettings.instance.chatTalkKey))
                {
                    KMPChatDX.showInput = true;
                    //DISABLE SHIP CONTROL
                    InputLockManager.SetControlLock(ControlTypes.All, "KMP_ChatActive");
                }
				
                if (Input.GetKeyDown(KeyCode.Escape) && KMPChatDX.showInput)
                {
                    KMPChatDX.showInput = false;
                    //ENABLE SHIP CONTROL
                    InputLockManager.RemoveControlLock("KMP_ChatActive");
                }

                if (Input.GetKeyDown(KMPGlobalSettings.instance.chatHideKey) && !isGameHUDHidden && KMPToggleButtonState)
                {
                    KMPGlobalSettings.instance.chatDXWindowEnabled = !KMPGlobalSettings.instance.chatDXWindowEnabled;
                    if (KMPGlobalSettings.instance.chatDXWindowEnabled)
                        KMPChatDX.enqueueChatLine("Press Chat key (" + (KMPGlobalSettings.instance.chatTalkKey == KeyCode.BackQuote ? "~" : KMPGlobalSettings.instance.chatTalkKey.ToString()) + ") to send a message");
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
                
                if (mappingScreenshotToggleKey)
                {
                    KeyCode key = KeyCode.F10;
                    if (getAnyKeyDown(ref key))
                    {
                        if (key != KeyCode.Mouse0)
                        {
                            KMPGlobalSettings.instance.screenshotToggleKey = key;
                            mappingScreenshotToggleKey = false;
                        }
                    }
                }

                if (mappingChatKey)
                {
                    KeyCode key = KeyCode.Y;
                    if (getAnyKeyDown(ref key))
                    {
                        if (key != KeyCode.Mouse0)
                        {
                            KMPGlobalSettings.instance.chatTalkKey = key;
                            mappingChatKey = false;
                        }
                    }
                }

                if (mappingChatDXToggleKey)
                {
                    KeyCode key = KeyCode.F9;
                    if (getAnyKeyDown(ref key))
                    {
                        if (key != KeyCode.Mouse0)
                        {
                            KMPGlobalSettings.instance.chatHideKey = key;
                            mappingChatDXToggleKey = false;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Debug("Exception thrown in Update(), catch 2, Exception: {0}", ex.ToString());
                Log.Debug("u err: " + ex.Message + " " + ex.StackTrace);
            }
        }

        public void OnGUI()
        {
            drawGUI();
        }
        //GUI
        public void drawGUI()
        {
            //KSP Toolbar integration - Can't chuck it in the bootstrap because Toolbar does not instantate early enough.
            if (!KMPToggleButtonInitialized)
            {
                if (ToolbarButtonWrapper.ToolbarManagerPresent)
                {
                    KMPToggleButton = ToolbarButtonWrapper.TryWrapToolbarButton("KMP", "Toggle");
                    KMPToggleButton.TexturePath = "KMP/KMPButton/KMPEnabled";
                    KMPToggleButton.ToolTip = "Toggle KMP Windows";
                    KMPToggleButton.AddButtonClickHandler((e) =>
                    {
                        KMPToggleButtonState = !KMPToggleButtonState;
                        KMPToggleButton.TexturePath = KMPToggleButtonState ? "KMP/KMPButton/KMPEnabled" : "KMP/KMPButton/KMPDisabled";
                    });
                }
                KMPToggleButtonInitialized = true;
            }

            if (forceQuit && !delayForceQuit)
            {
                Log.Debug("Force quit");
                forceQuit = false;
                gameRunning = false;
                if (HighLogic.LoadedScene != GameScenes.MAINMENU)
                {
                    HighLogic.LoadScene(GameScenes.MAINMENU);
                }
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
			
            if (!KMPGlobalSettings.instance.useNewUiSkin)
            {
                GUI.skin = HighLogic.Skin;
            }
			
            KMPConnectionDisplay.windowEnabled = (HighLogic.LoadedScene == GameScenes.MAINMENU) && globalUIToggle;

            
            if (KMPGlobalSettings.instance.chatDXWindowEnabled && !isGameHUDHidden && KMPToggleButtonState)
            {
                KMPChatDX.windowPos = GUILayout.Window(
                    GUIUtility.GetControlID(999994, FocusType.Passive),
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
                    GameEvents.OnProgressComplete.Remove(this.OnProgressComplete);
                    GameEvents.OnProgressReached.Remove(this.OnProgressReached);
                    GameEvents.onGUIRnDComplexDespawn.Remove(this.OnGUIRnDComplexDespawn);
                    GameEvents.OnTechnologyResearched.Remove(this.OnTechnologyResearched);
                    GameEvents.onVesselRecovered.Remove(this.OnVesselRecovered);
                }
                catch (Exception e)
                {
                    Log.Debug("Exception thrown in drawGUI(), catch 1, Exception: {0}", e.ToString());
                }
                if (showConnectionWindow)
                {
                    GUILayout.Window(
                        GUIUtility.GetControlID(999996, FocusType.Passive),
                        KMPConnectionDisplay.windowPos,
                        connectionWindow,
                        "Connection Settings",
                        KMPConnectionDisplay.layoutOptions
                    );
                }
            }
			
            if (!KMPConnectionDisplay.windowEnabled && KMPClientMain.handshakeCompleted && KMPClientMain.tcpClient != null)
            {
                if (KMPInfoDisplay.infoDisplayActive && !isGameHUDHidden && KMPToggleButtonState)
                {
                    KMPInfoDisplay.infoWindowPos = GUILayout.Window(
                        GUIUtility.GetControlID(999999, FocusType.Passive),
                        KMPInfoDisplay.infoWindowPos,
                        infoDisplayWindow,
                        KMPInfoDisplay.infoDisplayMinimized ? "KMP" : "KerbalMP v" + KMPCommon.PROGRAM_VERSION + " (" + KMPGlobalSettings.instance.guiToggleKey + ")",
                        KMPInfoDisplay.layoutOptions
                    );
					
                    if (isInFlight && !KMPInfoDisplay.infoDisplayMinimized)
                    {
                        GUILayout.Window(
                            GUIUtility.GetControlID(999995, FocusType.Passive),
                            KMPVesselLockDisplay.windowPos,
                            lockWindow,
                            syncing ? "Bailout" : "Lock",
                            KMPVesselLockDisplay.layoutOptions
                        );
                    }
                }	
            }

			
            if (!gameRunning)
            {
                //close the windows if not connected to a server 
                KMPScreenshotDisplay.windowEnabled = false;
                KMPGlobalSettings.instance.chatDXWindowEnabled = false;
            }
			
            if (KMPScreenshotDisplay.windowEnabled && !isGameHUDHidden && KMPToggleButtonState)
            {
                KMPScreenshotDisplay.windowPos = GUILayout.Window(
                    GUIUtility.GetControlID(999998, FocusType.Passive),
                    KMPScreenshotDisplay.windowPos,
                    screenshotWindow,
                    "KerbalMP Viewer (" + KMPGlobalSettings.instance.screenshotToggleKey + ")",
                    KMPScreenshotDisplay.layoutOptions
                );
            }

//			if (KMPGlobalSettings.instance.chatWindowEnabled)
//			{
//				KMPChatDisplay.windowPos = GUILayout.Window(
//					GUIUtility.GetControlID(999997, FocusType.Passive),
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
                GUILayout.BeginVertical();
                GUIStyle lockButtonStyle = new GUIStyle(GUI.skin.button);
                lockButtonStyle.fontSize = 10;
				
                if (!syncing)
                {
                    bool wasLocked;
                    if (!serverVessels_IsPrivate.ContainsKey(FlightGlobals.ActiveVessel.id) || !serverVessels_IsMine.ContainsKey(FlightGlobals.ActiveVessel.id))
                    {
                        //Must be ours
                        serverVessels_IsPrivate[FlightGlobals.ActiveVessel.id] = false;
                        serverVessels_IsMine[FlightGlobals.ActiveVessel.id] = true;
                        sendVesselMessage(FlightGlobals.ActiveVessel);
                        wasLocked = false;
                    }
                    else
                    {
                        //Get locked status
                        wasLocked = serverVessels_IsPrivate[FlightGlobals.ActiveVessel.id];
                    }
                    if (!wasLocked && (serverVessels_InUse.ContainsKey(FlightGlobals.ActiveVessel.id) ? !serverVessels_InUse[FlightGlobals.ActiveVessel.id] : true))
                    {
                        //Unlocked unoccupied vessel is now ours
                        serverVessels_IsMine[FlightGlobals.ActiveVessel.id] = true;
                    }
                    if (!serverVessels_IsMine[FlightGlobals.ActiveVessel.id])
                        GUI.enabled = false;
                    bool locked =
						GUILayout.Toggle(wasLocked,
                       wasLocked ? "Private" : "Public",
                       lockButtonStyle);
                    if (!serverVessels_IsMine[FlightGlobals.ActiveVessel.id])
                        GUI.enabled = true;
                    if (serverVessels_IsMine[FlightGlobals.ActiveVessel.id] && wasLocked != locked)
                    {
                        serverVessels_IsPrivate[FlightGlobals.ActiveVessel.id] = locked;
                        if (locked)
                            ScreenMessages.PostScreenMessage("Your vessel is now marked Private", 5, ScreenMessageStyle.UPPER_CENTER);
                        else
                            ScreenMessages.PostScreenMessage("Your vessel is now marked Public", 5, ScreenMessageStyle.UPPER_CENTER);
                        sendVesselMessage(FlightGlobals.ActiveVessel);
                    }
                }
                else
                {
                    //Offer bailout
                    bool quit = GUILayout.Button("Quit", lockButtonStyle);
                    if (quit)
                    {
                        if (KMPClientMain.tcpClient != null)
                        {
                            KMPClientMain.sendConnectionEndMessage("Requested quit during sync");
                        }
                        KMPClientMain.endSession = true;
                        forceQuit = true;
                    }
                }
                GUILayout.EndVertical();
            }
            catch (Exception e)
            {
                Log.Debug("Exception thrown in lockWindow(), catch 1, Exception: {0}", e.ToString());
            }
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
                GUIStyle screenshotButtonStyle = new GUIStyle(GUI.skin.button);
                int numScreenshotsWaiting = KMPClientMain.screenshotsWaiting.Count();
                if (numScreenshotsWaiting > 3)
                {
                    screenshotButtonStyle.normal.textColor = new Color(0.92f, 0.09f, 0.09f);
                }
                else if (numScreenshotsWaiting > 1)
                {
                    screenshotButtonStyle.normal.textColor = new Color(0.92f, 0.60f, 0.09f);
                }
                else if (numScreenshotsWaiting > 0)
                {
                    screenshotButtonStyle.normal.textColor = new Color(0.27f, 0.92f, 0.09f);
                }
                KMPGlobalSettings.instance.chatDXWindowEnabled = GUILayout.Toggle(KMPGlobalSettings.instance.chatDXWindowEnabled, "Chat (" + KMPGlobalSettings.instance.chatHideKey + ")", chatButtonStyle);
                KMPScreenshotDisplay.windowEnabled = GUILayout.Toggle(KMPScreenshotDisplay.windowEnabled, "Viewer (" + KMPGlobalSettings.instance.screenshotToggleKey + ")", screenshotButtonStyle);
                if (GUILayout.Button("Share Screen (" + KMPGlobalSettings.instance.screenshotKey + ")"))
                    StartCoroutine(shareScreenshot());
				
                GUIStyle syncButtonStyle = new GUIStyle(GUI.skin.button);
                string tooltip = showServerSync ? "Sync to the future" : "Already fully synced";
                if (showServerSync && isInFlight && FlightGlobals.ActiveVessel.ctrlState.mainThrottle == 0f && !isObserving)
                {
                    syncButtonStyle.normal.textColor = new Color(0.28f, 0.86f, 0.94f);
                    syncButtonStyle.hover.textColor = new Color(0.48f, 0.96f, 0.96f);
                    if (GUILayout.Button(new GUIContent("Sync", tooltip), syncButtonStyle))
                        StartCoroutine(sendSubspaceSyncRequest());
                }
                else
                {
                    syncButtonStyle.normal.textColor = new Color(0.5f, 0.5f, 0.5f);
                    GUI.enabled = false;
                    GUILayout.Button(new GUIContent("Sync", tooltip), syncButtonStyle);
                    GUI.enabled = true;
                }
                GUI.Label(new Rect(showServerSync ? 205 : 190, 298, 200, 10), GUI.tooltip);
                GUILayout.EndHorizontal();

                if (KMPInfoDisplay.infoDisplayOptions)
                {
                    //Connection
                    GUILayout.Label("Connection");

                    GUILayout.BeginHorizontal();
					
                    if (GUILayout.Button("Disconnect & Exit"))
                    {
                        disconnect();
                        KMPClientMain.sendConnectionEndMessage("Quit");
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

                    KMPGlobalSettings.instance.useNewUiSkin
						= GUILayout.Toggle(KMPGlobalSettings.instance.useNewUiSkin, "New GUI Skin", GUI.skin.toggle);

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
                    
                    GUILayout.BeginHorizontal();

                    mappingScreenshotToggleKey = GUILayout.Toggle(
                        mappingScreenshotToggleKey,
                        mappingScreenshotToggleKey ? "Press key" : "Screenshot Toggle: " + KMPGlobalSettings.instance.screenshotToggleKey,
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
            if (GUILayout.Button("<- Back"))
            {
                /* If the add menu is visible, turn that off first */
                if (addPressed)
                    addPressed = false;
                else
                {
                    showConnectionWindow = false;
                    MainMenu m = (MainMenu)FindObjectOfType(typeof(MainMenu));
                    m.envLogic.GoToStage(1);
                }
            }
            if (!configRead)
            {
                KMPClientMain.readConfigFile();
                configRead = true;
            }
            if (!isVerified)
            {
                KMPClientMain.verifyShipsDirectory();
                isVerified = true;
            }
            if (KMPClientMain.handshakeCompleted && KMPClientMain.tcpClient != null && !gameRunning && gameStart)
            {
                gameStart = false;
                gameRunning = true;

                Console.WriteLine("Game started.");
                //Clear dictionaries
                sentVessels_Situations.Clear();
		
                serverVessels_RemoteID.Clear();
                serverVessels_PartCounts.Clear();
                //serverVessels_Parts.Clear();
                serverVessels_ProtoVessels.Clear();
					
                serverVessels_InUse.Clear();
                serverVessels_IsPrivate.Clear();
                serverVessels_IsMine.Clear();
					
                serverVessels_LastUpdateDistanceTime.Clear();
                serverVessels_AddDelay.Clear();
                serverVessels_InPresent.Clear();

                isTimeSyncronized = false;
                listClientTimeSyncLatency.Clear();
                listClientTimeSyncOffset.Clear();
                listClientTimeWarp.Clear();
                //Request rate 1x subspace rate straight away.
                listClientTimeWarp.Add(1);
                listClientTimeWarpAverage = 1;
	
                newFlags.Clear();
					
                //Start MP game
                KMPConnectionDisplay.windowEnabled = false;
                KMPInfoDisplay.infoDisplayOptions = false;
                //This is to revert manually setting it to 1. Users won't know about this setting.
                //Let's remove this somewhere around July 2014.
                if (GameSettings.PHYSICS_FRAME_DT_LIMIT == 1.0f)
                {
                    GameSettings.PHYSICS_FRAME_DT_LIMIT = 0.04f;
                }
                HighLogic.SaveFolder = "KMP";
                HighLogic.CurrentGame = GamePersistence.LoadGame("start", HighLogic.SaveFolder, false, true);
                HighLogic.CurrentGame.Parameters.Flight.CanAutoSave = false;
                HighLogic.CurrentGame.Parameters.Flight.CanLeaveToEditor = false;
                HighLogic.CurrentGame.Parameters.Flight.CanLeaveToMainMenu = false;
                HighLogic.CurrentGame.Parameters.Flight.CanQuickLoad = false;
                HighLogic.CurrentGame.Parameters.Flight.CanRestart = false;
                HighLogic.CurrentGame.Parameters.Flight.CanSwitchVesselsFar = false;
                HighLogic.CurrentGame.Title = "KMP";
                HighLogic.CurrentGame.Description = "Kerbal Multi Player session";
                HighLogic.CurrentGame.flagURL = "KMP/Flags/default";
                vesselUpdatesHandled.Clear();

                if (gameMode == 1) //Career mode
                    HighLogic.CurrentGame.Mode = Game.Modes.CAREER;
					
                GamePersistence.SaveGame("persistent", HighLogic.SaveFolder, SaveMode.OVERWRITE);
                GameEvents.onFlightReady.Add(this.OnFirstFlightReady);
                syncing = true;
                HighLogic.CurrentGame.Start();

                if (HasModule("ResearchAndDevelopment"))
                {
                    Log.Debug("Erasing scenario modules");
                    HighLogic.CurrentGame.scenarios.Clear();
                    //This is done because scenarios is not cleared properly even when a new game is started, and it was causing bugs in KMP.
                    //Instead of clearing scenarios, KSP appears to set the moduleRefs of each module to null, which is what was causing KMP bugs #578, 
                    //and could be the cause of #579 (but closing KSP after disconnecting from a server, before connecting again, prevented it from happening, 
                    //at least for #578).
                }
					
                if (gameMode == 1)
                {
                    var proto = HighLogic.CurrentGame.AddProtoScenarioModule(typeof(ResearchAndDevelopment), GameScenes.SPACECENTER, GameScenes.EDITOR, GameScenes.FLIGHT, GameScenes.TRACKSTATION, GameScenes.SPH);
                    proto.Load(ScenarioRunner.fetch);
                    proto = HighLogic.CurrentGame.AddProtoScenarioModule(typeof(ProgressTracking), GameScenes.SPACECENTER, GameScenes.FLIGHT, GameScenes.TRACKSTATION);
                    proto.Load(ScenarioRunner.fetch);
                    clearEditorPartList = true;
                }
					
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
                GameEvents.OnProgressComplete.Add(this.OnProgressComplete);
                GameEvents.OnProgressReached.Add(this.OnProgressReached);
                GameEvents.onGUIRnDComplexDespawn.Add(this.OnGUIRnDComplexDespawn);
                GameEvents.OnTechnologyResearched.Add(this.OnTechnologyResearched);
                GameEvents.onVesselRecovered.Add(this.OnVesselRecovered);
                writePluginData();
                //Make sure user knows how to use new chat
                KMPChatDX.enqueueChatLine("Press Chat key (" + (KMPGlobalSettings.instance.chatTalkKey == KeyCode.BackQuote ? "~" : KMPGlobalSettings.instance.chatTalkKey.ToString()) + ") to send a message");
                KMPGlobalSettings.instance.chatDXWindowEnabled = true;
					
                return;
            }
			
            GUILayout.BeginHorizontal();
            if (addPressed)
            {
                GUILayout.BeginVertical();
                GUILayout.BeginHorizontal();
                GUILayoutOption[] name_options = new GUILayoutOption[1];
                name_options[0] = GUILayout.MaxWidth(240);
                GUILayout.Label("Server Name:");
                newFamiliar = GUILayout.TextField(newFamiliar, name_options).Trim();
						
						
                GUILayout.EndHorizontal();

                GUILayout.BeginHorizontal();
                GUILayoutOption[] field_options = new GUILayoutOption[1];
                field_options[0] = GUILayout.MaxWidth(120);
                GUILayout.Label("Address:");
                newHost = GUILayout.TextField(newHost);
                GUILayout.Label("Port:");
                newPort = GUILayout.TextField(newPort, field_options);

                GUILayout.EndHorizontal();
                GUILayout.BeginHorizontal();
                // Fetch favourites
                Dictionary<String, String[]> favorites = KMPClientMain.GetFavorites();

                bool favoriteItemExists = favorites.ContainsKey(newFamiliar);
                GUI.enabled = !favoriteItemExists;
                bool addHostPressed = GUILayout.Button("New", field_options);
                GUI.enabled = favoriteItemExists;
                bool editHostPressed = GUILayout.Button("Replace", field_options);
                GUI.enabled = true;
                bool cancelEdit = GUILayout.Button("Cancel", field_options);
                if (cancelEdit)
                {
                    addPressed = false; /* Return to previous screen */ 
                }
                else if (addHostPressed && !favoriteItemExists) // Probably don't need these extra checks, but there is no harm
                {
                    KMPClientMain.SetServer(newHost.Trim());
                    String[] sArr = {
                                newHost.Trim(),
                                newPort.Trim(),
                                KMPClientMain.GetUsername()
                            };

                            if (favorites.ContainsKey(newFamiliar))
                                {
                                    ScreenMessages.PostScreenMessage("Server name taken", 300f, ScreenMessageStyle.UPPER_CENTER);
                                } else if (favorites.ContainsValue(sArr))
                                {
                                    // Is this ever true? Arrays are compared by reference are they not ? - NC
                                    ScreenMessages.PostScreenMessage("This server already exists", 300f, ScreenMessageStyle.UPPER_CENTER);
                                } else
                                {
                                    favorites.Add(newFamiliar, sArr);

                                    //Close the add server bar after a server has been added and select the new server
                                    addPressed = false;
                                    // Personal preference, change back if you don't like, Gimp. - NC
                                    KMPConnectionDisplay.activeFamiliar = String.Empty;
                                    KMPConnectionDisplay.activeFamiliar = String.Empty;
                                    KMPClientMain.SetFavorites(favorites);
                                }
                        } else if (editHostPressed && favoriteItemExists)
                        {
                            KMPClientMain.SetServer(newHost.Trim());
                            String[] sArr = {
                                newHost.Trim(),
                                newPort.Trim(),
                                KMPClientMain.GetUsername()
                            };
                            favorites[newFamiliar] = sArr;
                            addPressed = false;
                            // Disable the active familar after this stage, because otherwise the controls feel sticky and confusing
                            KMPConnectionDisplay.activeFamiliar = String.Empty;
                            KMPConnectionDisplay.activeFamiliar = String.Empty;
                            KMPClientMain.SetFavorites(favorites); // I would love to have this as a seperate object in the manager, no more getting and setting. 
                        }
                    GUILayout.EndHorizontal();
                    GUILayout.EndVertical();
                }
            GUILayout.EndHorizontal();

            /* Add window now reoccupies the Connection Settings space */
            if (!addPressed)
                {
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
                    foreach (String familiar in KMPClientMain.GetFavorites().Keys)
                        {
                            if (!String.IsNullOrEmpty(familiar))
                                connectionButton(familiar);
                        }
                    GUILayout.EndScrollView();

                    GUILayout.EndVertical();

                    GUILayoutOption[] pane_options = new GUILayoutOption[1];
                    pane_options[0] = GUILayout.MaxWidth(50);

                    GUILayout.BeginVertical(pane_options);

                    bool allowConnect = true;
                    if (String.IsNullOrEmpty(KMPConnectionDisplay.activeFamiliar) || String.IsNullOrEmpty(KMPClientMain.GetUsername()))
                        allowConnect = false;

                    if (!allowConnect)
                        GUI.enabled = false;

                    bool connectPressed = GUILayout.Button("Connect");
                    GUI.enabled = true;

                    if (connectPressed && allowConnect)
                        {
                            KMPClientMain.SetMessage("");
                            KMPClientMain.SetServer(KMPConnectionDisplay.activeHostname);
                            KMPClientMain.Connect();
                        }

                    if (KMPClientMain.GetFavorites().Count < 1)
                        addPressed = true;

                    addPressed = GUILayout.Toggle(
                        addPressed,
                        (String.IsNullOrEmpty(KMPConnectionDisplay.activeFamiliar)) ?
                    "Add Server" : "Edit",
                        GUI.skin.button);
                
                    Dictionary<String, String[]> favorites = KMPClientMain.GetFavorites();


                    if (String.IsNullOrEmpty(KMPConnectionDisplay.activeFamiliar))
                        GUI.enabled = false;
                    bool deletePressed = GUILayout.Button("Remove");
                    if (deletePressed)
                        {
                            if (favorites.ContainsKey(KMPConnectionDisplay.activeFamiliar))
                                {
                                    favorites.Remove(KMPConnectionDisplay.activeFamiliar);
                                    KMPConnectionDisplay.activeHostname = "";
                                    KMPConnectionDisplay.activeFamiliar = "";
                                    KMPClientMain.SetFavorites(favorites);
                                }
                        }
                    GUI.enabled = true;

                    /* Add is a toggle after all */
                    if (addPressed && !deletePressed)
                        {
                            /* If add is pressed and a server is selected, apply it's values to the edit controls */
                            if (!String.IsNullOrEmpty(KMPConnectionDisplay.activeFamiliar) && favorites.ContainsKey(KMPConnectionDisplay.activeFamiliar))
                                {
                                    newFamiliar = KMPConnectionDisplay.activeFamiliar;
                                    if (KMPConnectionDisplay.activeHostname.Contains(":"))
                                        {
                                            var tokens = KMPConnectionDisplay.activeHostname.Split(':');
                                            newHost = tokens[0];
                                            newPort = tokens[1];
                                        }
                                } else //Defaults
                                {
                                    newHost = "localhost";
                                    newPort = "2076";
                                    newFamiliar = "Server";
                                }
                        }

                    GUILayout.EndVertical();

                    GUILayout.EndHorizontal();
                }
			
            GUILayout.BeginHorizontal();
            GUILayout.BeginVertical();
            GUILayoutOption[] status_options = new GUILayoutOption[1];
            status_options[0] = GUILayout.MaxWidth(310);

            if (String.IsNullOrEmpty(KMPClientMain.GetUsername()))
                GUILayout.Label("Please specify a username", status_options);
            else if (String.IsNullOrEmpty(KMPConnectionDisplay.activeFamiliar))
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
                } else
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
            entry_field_options[0] = GUILayout.MaxWidth(KMPChatDisplay.windowWidth - 58);

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
                        } else
                        {
                            var text = line.name + ": " + line.message;
                            if (line.isAdmin)
                                {
                                    text = "[" + KMPCommon.ADMIN_MARKER + "] " + text;
                                }
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


            if (KMPChatDX.showInput)
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
                            //ENABLE SHIP CONTROL
                            if (InputLockManager.GetControlLock("KMP_ChatActive") == (ControlTypes.All))
                                InputLockManager.RemoveControlLock("KMP_ChatActive");
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
                } else
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
				
                    if (status.currentSubspaceID > 0 && !syncing)
                        {
                            showSync = true;
                            showServerSync = true;
                        }
				
                    if (status.info != null && status.info.detail != null && status.info.detail.idle)
                        {
                            vessel_name = "(Idle) " + vessel_name;
                        }

                    name_pressed |= GUILayout.Button(vessel_name, vesselNameStyle);
                }
			
            GUIStyle syncButtonStyle = new GUIStyle(GUI.skin.button);
            syncButtonStyle.normal.textColor = new Color(0.28f, 0.86f, 0.94f);
            syncButtonStyle.hover.textColor = new Color(0.48f, 0.96f, 0.96f);
            syncButtonStyle.margin = new RectOffset(150, 10, 0, 0);
            syncButtonStyle.fixedHeight = 22f;
			
            if (big)
                {
                    GUILayout.EndHorizontal();
                    GUILayout.BeginHorizontal();
                }
            bool syncRequest = false;
            if (!isInFlight)
                GUI.enabled = false;
            if (showSync && FlightGlobals.ActiveVessel.ctrlState.mainThrottle == 0f && !isObserving)
                syncRequest |= GUILayout.Button("Sync", syncButtonStyle);
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
                        } else
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
                {
                    GUILayout.Label(sb.ToString(), stateTextStyle);
                }
			
            //If the name was pressed, then focus on that players' reference body
            if (name_pressed && HighLogic.LoadedSceneHasPlanetarium && planetariumCam != null && status.info != null && status.info.bodyName.Length > 0)
                {
                    if (!MapView.MapIsEnabled)
                        {
                            MapView.EnterMapView();
                        }

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
                {
                    StartCoroutine(sendSubspaceSyncRequest(status.currentSubspaceID));
                }
        }

        private void screenshotWatchButton(String name)
        {
		
            GUIStyle playerScreenshotButtonStyle = new GUIStyle(GUI.skin.button);
            bool playerNameInScreenshotsWaiting = false;
            foreach (string playerName in KMPClientMain.screenshotsWaiting)
                {
                    if (playerName == name)
                        {
                            playerNameInScreenshotsWaiting = true;
                        }
                }
            if (playerNameInScreenshotsWaiting)
                playerScreenshotButtonStyle.normal.textColor = new Color(0.92f, 0.60f, 0.09f);
            else
                playerScreenshotButtonStyle.normal.textColor = new Color(0.92f, 0.92f, 0.92f);
                        
            bool player_selected = GUILayout.Toggle(KMPScreenshotDisplay.watchPlayerName == name, name, playerScreenshotButtonStyle);
            if (player_selected != (KMPScreenshotDisplay.watchPlayerName == name))
                {
                    if (KMPScreenshotDisplay.watchPlayerName != name)
                        {
                            KMPScreenshotDisplay.watchPlayerName = name; //Set watch player name
                            bool listPlayerNameInScreenshotsWaiting = false;
                            foreach (string listPlayer in KMPClientMain.screenshotsWaiting)
                                {
                                    if (listPlayer == name)
                                        listPlayerNameInScreenshotsWaiting = true;
                                }
                            if (listPlayerNameInScreenshotsWaiting)
                                KMPClientMain.screenshotsWaiting.Remove(name);
                        } else
                        KMPScreenshotDisplay.watchPlayerName = String.Empty;

                    lastPluginDataWriteTime = 0.0f; //Force re-write of plugin data
                }
        }

        private void connectionButton(String name)
        {
            Dictionary<String, String[]> favorites = KMPClientMain.GetFavorites();
            String[] sArr = new String[favorites.Count];
            favorites.TryGetValue(name, out sArr);
            String hostname = sArr[0] + ":" + sArr[1];

            bool player_selected = GUILayout.Toggle(KMPConnectionDisplay.activeFamiliar == name, name, GUI.skin.button);
            if (player_selected != (KMPConnectionDisplay.activeHostname == hostname))
                {
                    if (KMPConnectionDisplay.activeHostname != hostname)
                        KMPConnectionDisplay.activeHostname = hostname;
                    else
                        KMPConnectionDisplay.activeHostname = String.Empty;
                }
            if (player_selected != (KMPConnectionDisplay.activeFamiliar == name))
                {
                    if (KMPConnectionDisplay.activeFamiliar != name)
                        KMPConnectionDisplay.activeFamiliar = name;
                    else
                        KMPConnectionDisplay.activeFamiliar = String.Empty;
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

        private bool isInSafetyBubble(Vector3d worldPosition)
        {
            //Get kerbin so we can transform to world co-ordinates
            CelestialBody kerbin = FlightGlobals.Bodies.Find(b => b.name == "Kerbin");

            //Two tiny bubbles.
            Vector3d landingPadPosition = kerbin.GetWorldSurfacePosition(-0.0971978130377757, 285.44237039111, 60);
            Vector3d runwayPosition = kerbin.GetWorldSurfacePosition(-0.0486001121594686, 285.275552559723, 60);

            //TODO: Old bubble
            return (Vector3d.Distance(worldPosition, landingPadPosition) < minSafetyBubbleRadius) || (Vector3d.Distance(worldPosition, runwayPosition) < minSafetyBubbleRadius) || (Vector3d.Distance(worldPosition, landingPadPosition) < safetyBubbleRadius);
        }

        private bool isProtoVesselInSafetyBubble(ProtoVessel protovessel)
        {
            //When vessels are landed, position is 0,0,0 - So we need to check lat/long

            //Create a config node
            ConfigNode protoVesselNode = new ConfigNode();
            protovessel.Save(protoVesselNode);

            int protoBodyIndex = Int32.Parse(protoVesselNode.GetNode("ORBIT").GetValue("REF"));
            CelestialBody protoBody = FlightGlobals.Bodies[protoBodyIndex];
            Vector3d protoVesselPosition;

            if (protovessel.landed)
                {
                    //Protovessels have position 0,0,0 if they are landed.
								
                    double protoVesselLat;
                    double protoVesselLong;
                    Double.TryParse(protoVesselNode.GetValue("lat"), out protoVesselLat);
                    Double.TryParse(protoVesselNode.GetValue("long"), out protoVesselLong);
								

                    //Get the protovessel position
                    protoVesselPosition = protoBody.GetWorldSurfacePosition(protoVesselLat, protoVesselLong, protovessel.altitude);
                } else
                {
                    protoVesselPosition = protoBody.transform.InverseTransformPoint(protovessel.position);
                }

            //Use the above check
            return isInSafetyBubble(protoVesselPosition);

        }

        public double horizontalDistanceToSafetyBubbleEdge()
        {
            CelestialBody body = FlightGlobals.ActiveVessel.mainBody;
            Vector3d pos = FlightGlobals.ship_position;
            double altitude = FlightGlobals.ActiveVessel.altitude;
			
            if (body == null)
                return -1d;
            //If KSC out of range, syncing, not at Kerbin, or past ceiling we're definitely clear
            if (syncing || body.name != "Kerbin" || altitude > SAFETY_BUBBLE_CEILING)
                return -1d;
			
            //Cylindrical safety bubble -- project vessel position to a plane positioned at KSC with normal pointed away from surface
            Vector3d kscNormal = body.GetSurfaceNVector(-0.102668048654, -74.5753856554);
            Vector3d kscPosition = body.GetWorldSurfacePosition(-0.102668048654, -74.5753856554, 60);
            double projectionDistance = Vector3d.Dot(kscNormal, (pos - kscPosition)) * -1;
            Vector3d projectedPos = pos + (Vector3d.Normalize(kscNormal) * projectionDistance);
			
            return safetyBubbleRadius - Vector3d.Distance(kscPosition, projectedPos);
        }

        public double verticalDistanceToSafetyBubbleEdge()
        {
            CelestialBody body = FlightGlobals.ActiveVessel.mainBody;
            double altitude = FlightGlobals.ActiveVessel.altitude;

            if (body == null)
                return -1d;
            //If KSC out of range, syncing, not at Kerbin, or past ceiling we're definitely clear
            if (syncing || body.name != "Kerbin" || altitude > SAFETY_BUBBLE_CEILING)
                return -1d;
			
			
            return SAFETY_BUBBLE_CEILING - altitude;
        }
        //This code adapted from Kerbal Engineer Redux source
        private void CheckEditorLock()
        {
            if (!gameRunning || !HighLogic.LoadedSceneIsEditor)
                {
                    //Only handle editor locks while in the editor
                    return;
                }
            EditorLogic editorObject = EditorLogic.fetch;
            if (editorObject == null)
                {
                    //If the editor isn't initialized, return early. This stops debug log spam.
                    return;
                }
            InputLockManager.RemoveControlLock("KMP_Occupied");
            InputLockManager.RemoveControlLock("KMP_Private");
            if (shouldDrawGUI)
                {
                    Vector2 mousePos = Input.mousePosition;
                    mousePos.y = Screen.height - mousePos.y;
	
                    bool should_lock = (KMPInfoDisplay.infoWindowPos.Contains(mousePos) || (KMPScreenshotDisplay.windowEnabled && KMPScreenshotDisplay.windowPos.Contains(mousePos)));
				
                    if (should_lock && !isEditorLocked)
                        {
                            EditorLogic.fetch.Lock(true, true, true, "KMP_lock");
                            isEditorLocked = true;
                        } else if (!should_lock)
                        {
                            if (!isEditorLocked)
                                editorObject.Lock(true, true, true, "KMP_lock");
                            editorObject.Unlock("KMP_lock");
                            isEditorLocked = false;
                        }
                }
            //Release the lock if the KMP window is hidden.
            if (!shouldDrawGUI && isEditorLocked)
                {
                    editorObject.Unlock("KMP_lock");
                    isEditorLocked = false;
                }
        }
    }
}
