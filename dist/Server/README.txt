KMP Server README
=================

**KMP Server requires .Net 4.0 or Mono 2.6+

Installation
------------
1. Unzip the contents of KMP_Server.zip to any folder.
2. Copy the following files from [KSP 0.23 folder]\KSP_Data\Managed\ to your KMP Server folder:
 * Assembly-CSharp.dll
 * Assembly-CSharp-firstpass.dll
 * UnityEngine.dll

FIREWALL NOTE: You must open your configured KMP port for both TCP & UDP traffic for players to be
  able to successfully connect.

LINUX USERS: The server needs mscorlib.dll to run. On Debian-derived systems (such as Ubuntu) you
  can do this with the command:
    sudo apt-get install mono-complete

LINUX USERS: The supplied libsqlite3.so.0, which is tested against Ubuntu 12.04, may not be fully
  compatible with your environment. If you are not using Ubuntu 12.04, it is recommended that you
  follow these additional steps:
    1. Remove the supplied libsqlite3.so.0 file from your KMP server directory.
        e.g. mv libsqlite3.so.0 /libsqlite3.so.0.bak
    2. If you haven't already, install your system's native SQLite libraries or compile them.
    3. Create a soft link in the KMP server directory linking libsqlite3.so.0 to your native SQLite
      library.
        e.g. ln -s /usr/local/lib/libsqlite3.so.0 libsqlite3.so.0

NOTE: KMP Server has been tested on Windows 7+ and Ubuntu 12.04 systems. Other platforms, such as
  OS X or other Linux distributions, are not "officially" supported but often still work correctly
  provided a suitable library for SQLite is supplied. If KMP Server doesn't work "out of the box"
  on your system and you are able to find a native SQLite package, you may be able to get things
  working by replacing the file "libsqlite3.so.0" with a native equivalent. You may also be able to
  work around this dependency by using a MySQL database instead of SQLite.


Starting the Server
-------------------
[On Windows]
Open a command prompt, switch to your KMP server directory, and run "KMPServer.exe"

[On Linux]
Open a terminal, switch to your KMP server directory, and run "mono KMPServer.exe"

After the server has been configured, you can start hosting the game with the "/start" command.


Configuration
-------------
ipBinding - The IP address the server should bind to. Defaults to binding to all available IPs.
port - The port used for connecting to the server.
httpPort - The port used for viewing server information from a web browser.
httpBroadcast - Enable simple http server for viewing server information from  a web browser.
maxClients - The maximum number of players that can be connected to the server simultaneously.
screenshotInterval - The minimum time a client must wait after sharing a screenshot before they can
  share another one.
autoRestart - If true, the server will attempt to restart after catching an unhandled exception.
autoHost - If true, the server will start hosting immediately rather than requiring the admin to
  enter the "/start" command.
saveScreenshots - If true, the server will save all screenshots to the KMPScreenshots folder.
hostIPv6 - If true, the server will attempt to use IPv6.
useMySQL - If true, the server will use the supplied MySQL connection string instead of the
  built-in SQLite database to store the universe.
mySQLConnString - The connection string used to connect to the MySQL database.
backupInterval - Time, in minutes, between universe database backups.
maxDirtyBackups - The maximum number of backups the server will perform before forcing database
  optimization (which otherwise happens only when the server is empty).
updatesPerSecond - CHANGING THIS VALUE IS NOT RECOMMENDED - The number of updates that will be
  received from all clients combined per second. The higher you set this number, the more
  frequently clients will send updates. As the number of active clients increases, the frequency of
  updates will decrease to not exceed this many updates per second.
    WARNING: If this value is set too high then players will be more likely to be disconnected due
      to lag, while if it is set too low the gameplay experience will degrade significantly.
totalInactiveShips - CHANGING THIS VALUE IS NOT RECOMMENDED - The number of secondary updates that
  are allowed between all players per primary update.
consoleScale - Changes the window size of the scale. Defaults to 1.0, requires restart.
LogLevel - Log verbosity. Choose from: Debug, Activity, Info, Notice, Warning, or Error.
maximumLogs - The number of log files to store.
screenshotHeight - The height of screenshots sent by players, in pixels. Can range from 135 to 540
  (recommended values: 135, 144, 180, 270, 315, 360, 540).
autoDekessler - Enable periodic clearing of debris from the server universe (useful for high-usage
  servers).
autoDekesslerTime - The interval, in minutes, between auto-dekessler cleanups.
profanityFilter - If true, enables the built-in profanity filter.
profanityWords - Words for the profanity filter.
whitelisted - If true, enables the player whitelist.
joinMessage - A message shown to players when they join the server.
serverInfo - A message displayed to anyone viewing server information in a browser.
serverMotd - A message displayed to users when they login to the server that can be changed while
  the server is running.
serverRules - A message displayed to users when they ask to view the server's rules.
safetyBubbleRadius - The radius of the "safety cylinder" which prevents collisions near KSC.
cheatsEnabled - If true, enable KSP's built-in debug cheats.
allowPriacy - If true, players can steal "Private" vessels if they can accomplish a manual docking.
freezeTimeWhenServerIsEmpty - If true, universe time is frozen when the server is empty (otherwise
  universe time runs continuously once a single player joins the server).

Use "/mode" to choose between Sandbox and Career mode.

You can also change these settings from the command line with this syntax:
  KMPServer.exe +port 2076


Server Config Commands (before /start)
----------------------------------------
/set <key> <value> - Modify a setting (use "/set help" for information about each setting).
/whitelist <add|del> <user> - Update player whitelist.
/admin <add|del> <user> - Update the list of server admins.
/mode <sandbox|career> - Change game-mode.
/modgen <blacklist|whitelist> <sha> - Generate a KMPModControl.txt from the 'Mods' directory. You
  can use blacklist or whitelist mode, defaulting to blacklist. You can optionally specify sha to
  force required versions.
/quit - Quit the server.
/start - Begin the server session.


Server Commands (after /start)
--------------------------------
/quit & stop - Quit the server cleanly, saving the universe database and disconnecting clients.
/listclients - List currently connected players.
/countclients - Display player counts.
/kick <username> - Kick the player with name <username>.
/ban <username> - Permanently ban player the player <username> and any currently known aliases.
/register <username> <token> - Add a new roster entry for player with <username> using
  authentication token <token> (BEWARE: this will delete any existing roster entries that match).
/update <username> <token> - Update an existing roster entry for player <username> or player with
  token <token> (one of these parameters must match an existing roster entry, and the other will be
  updated to match). This can be used to help a player who lost their original access token regain
  access to old vessels.
/unregister <username/token> - Remove any player that has a matching username or token from the
  roster.
/clearclients - Disconnect any invalid clients.
/countships - Lists number of ships in universe.
/listships - List all ships in universe along with their ID
/lockship <ID> <true/false> - Set ship as private or public.
/deleteship <ID> - Removes ship from universe.
/dekessler <mins> - Remove debris that has not been updated for at least <mins> minutes in-game
  time (default 30 mins).
/save - Backup the universe database to disk.
/reloadmodfile -  Reloads the KMPModControl.txt file. Note that this will not recheck any currently
  logged in clients, only those joining.
/setinfo <info> - Updates the server info seen on master server list.
/motd <message> - Set MOTD.
/rules <rules> - Set server rules.
/modgen <blacklist|whitelist> <sha> - Auto-generate a KMPModControl.txt file using what you have
  placed in the server's 'Mods' directory.
/say <-u username> <message> - Send a chat message as server (optional: to specified user).
/help - Display server commands.


Enabling Mods
-------------
Mods are now controlled from the server using the KMPModControl.txt file in the server directory.
  This file will be automatically created if it is missing. Further instructions for setting up mod
  control are detailed in KMPModControl.txt. You can auto-generate the mod-control file by placing
  all desired server mod files into the "Mods" folder (as they would appear in [KSP]/GameData) and
  using the "/modgen" server command.


Source available at: https://github.com/TehGimp/KerbalMultiPlayer
