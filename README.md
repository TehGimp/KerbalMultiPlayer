KMP Client README
===============

About
-----
KMP is a mod for v0.22 of Kerbal Space Program that adds a multiplayer game option. In a KMP game
you can freely interact with other players and do all the Kerbally things you'd normally do in KSP,
but with friends (or strangers) playing in the same universe, at the same time. See the FAQ for
more information.


Installation
------------
Simply copy the contents of KMP.zip to your KSP directory. Be sure you copy both the "GameData" &
"saves" folders as both are required for KMP to function correctly. It is recommended that you use
a fresh copy of KSP for playing KMP--as KMP is in an EARLY ALPHA state it could potentially cause
major issues even for non-KMP sessions, and KMP is not compatible with most other KSP mods.


Getting Connected
-----------------
To connect to a server, select it from the favourites list and click "Connect". Clicking
"Add Server" will show/hide the new server form. To add a new server, just enter the address & port
(default 2076) and click "Add".


Gameplay
--------
For the most part, playing with KMP is just like playing a single-player KSP game: you still build
and launch ships, and you can even use time warp whenever you need/want to (see "Warping & Relative
Time" below). There are several new windows added to the interface to allow you to communicate and
coordinate with other players, and you will see indicators for other players on all the in-game
maps of the solar system. Many features (such as reverting to launch, quick-loading, pausing, &
physics-warp) are disabled during a KMP session.

[Main KMP Window]
The main KMP window lists all the currently connected players with status information, and
  provides access to most KMP features. The buttons on the main window are:
"[X]" - Hides the main KMP window and the KMP privacy lock. Click the "[-]" button to restore the
  main KMP window.
"+"/"-" - Maximizes/minimizes the main KMP window size
"Detail" - Toggles whether detailed flight status information is displayed for other players
"Options" - Displays settings including keybindings, as well as the "Disconnect & Exit" button
"Chat" - Toggles whether the Chat window is displayed
"Viewer" - Toggles whether the Viewer window is displayed
"Share Screen" - Takes a screenshot to be shared with other players
"Sync" - Syncronize with the server's latest "subspace" (see "Warping & Relative Time")

[Chat]
The Chat window, unsurprisingly, allows you to chat with other players on the server. Press enter
  or click "Send" to send a message to other players.
"Wide" - Toggles wide chat display-mode
"Help" - Toggles display of chat commands

WARNING FOR LINUX USERS: A bug with Unity/KSP as of 0.21.1 causes keyboard input to be picked up by
  your vessel even when you a typing in a text box. Be sure you are on the Map View before
  participating in chat to prevent accidents!

[Viewer]
The Viewer allows you to view screenshots shared by other players. Just click on a player's name to
  see the most recent screenshot they've shared.

WARNING FOR LINUX USERS: A bug with Unity/KSP as of 0.21.1 causes issues with sending or viewing
  screenshots. While these features may work normally in some cases, it is recommended that you
  avoid using them as they often cause the game to crash without warning.

Hex-editing your game binary remedies this issue as discussed on the forums here:
http://forum.kerbalspaceprogram.com/threads/24529-The-Linux-compatibility-thread!?p=849510&viewfull=1#post849510

  echo "838077: 00" | xxd -r - KSP.x86\_64
  echo "83807c: 00" | xxd -r - KSP.x86\_64


Warping & Relative Time
-----------------------
KMP allows you to use regular "on-rails" warp on-demand, as much as you need. However, whenever you
use warp this desynchronizes you from other players putting you in the future relative to them. You
can still play your game normally when other players are in the past, but you won't be able to
interact with them directly and they'll only see the things you're doing if the "catch up" to you
in time. Other players that use warp may end up in the future relative to you, and it's not
uncommon to find yourself sitting between other groups of players, some in the future and some in
the past. You can sync with any player that's in the future relative to you--just click the "Sync"
button under their name and you'll be jumped ahead in time to the same 'subspace' as that player.
The "Sync" button at the bottom of the main KMP window always sychronizes you with the "latest"
subspace available in the current game, which is where (when?) newly connected players join the
game as well. Be sure that you're landed or in a stable orbit when you sync, as otherwise you may
find yourself somewhere unexpected!

You're still able to see other players and what they're doing in-game when they're in a different
subspace--you can chat, send screenshots, share designs, etc, but you won't be able to interact
with any in-game vessels that those players control. Vessels from the past or future turn
translucent so that you know that you won't be able to affect them. If the other player is in the
past, KMP tries to predict where they'll be in the future and shows their ship at that location.
Keep in mind, though, that since the ship is still being manipulated in the past, the predicted
future location can rapidly change. If another ship has been manipulated in the future, you're
effectively just watching a recording of events that have "already happened" play out.


Vessel Privacy
--------------
By default a vessel you launch will be in a "Public" status, which means that other players can
take control of it if you leave and they may freely dock with your ship as well. To keep a vessel
to yourself, use the "Lock" window on the right-side of the screen to set the vessel to "Private"
status. You can't take control of another vessel owned by another player that has marked it
"Private", nor can you dock with a "Private" vessel. Keep in mind, though, that if another player
is in control of your vessel they can choose to lock it to themselves--only share your ships with
players you trust. Your vessel locks are tied to your username & token (see "User Authentication"
below)--if you change your username or token, you will lose access to vessels that were marked
"Private" with those credentials. Players can always interact physically (i.e. collide) with
another vessel even when the owner is absent.


User Authentication
-------------------
KMP uses a simple username+token system for controlling your access to a server and to your
vessels. Your "token" is a file created by KMP which is stored at
[KSP folder]\GameData\KMP\Plugins\PluginData\KerbalMultiPlayer\KMPPlayerToken.txt--think of this
file as your password. Once you first join a server successfully, the username you are using is
immediately tied to your token *for that server*. Other players that join that server will not be
able to connect with your username as they won't have a matching token. If you want to access the
same server from multiple computers with a single username, be sure to copy the KMPPlayerToken.txt
file created on the first computer you use to the other(s) in order to make it possible for you to
connect. If your token file is missing, a new one will be generated for you but this will of course
also change your token. Your vessel locks (if any) are tied to your username & token--if either
changes you will lose access to any vessels that were set to "Private" until you restore the
original username and token or contact a server admin for assistance.


Mods
----

KMP allows server-side mod control (although many mods that change parts of the universe, like Kethane, still do not work). Add mods to your server at your own risk. The old (prior to v0.1.5.0) system, which uses the KMPModControl.txt file on the client's installation, is no longer in effect. Instead, server admins configure their servers' KMPModControl.txt file to their specifications, and clients that do not meet the mod requirements will not be allowed to connect.

The simplest way for an admin to configure mods is to use the "Mods/Required", "Mods/Required_Exact", "Mods/Optional", and "Mods/Optional_Exact" directories in their KMP Server directory. Simply copy any mods that you would like to use to the respective directories: mods that you place in the "Mods/Required" or "Mods/Required_Exact" directories will be mandatory for the client to have in order to connect, and mods that you place in the "Mods/Optional" or "Mods/Optional_Exact" directories will be allowed, but not necessary. The "Exact" variants are for mods where you want to force the client to have exactly the same file (does a SHA hash check, so use this if you want to force a specific version of a mod or make sure that certain files weren't modified by the client), and the non-Exact variants are for mods where you just want to make sure they have the right filenames installed (use this if you want to force the client to have a mod, but it doesn't have to match the server's file exactly, so the client could use a newer or older version of the mod). Mods that include parts (e.g. KWRocketry) or that change persistent data about the universe (e.g. RealSolarSystem) should be placed in a "Required" directory, in order to maintain synchronization with other players. Mods that only affect the client (e.g. EditorExtensions) can be placed in an "Optional" directory, so clients can use them if they want to.

The server's "checkAllModFiles" setting determines which mod files are checked. If this is set to false (default), it will only check ".cfg" and ".dll" files. If it's set to true, it will check every file in any of the "Mods/" directories.

Once you've configured your mods in the "Required" and "Optional" directories as you see fit, run the server program and use the "/modgen" command. This will automatically create a KMPModControl.txt file that matches the file structure you created in the Mods directories. You can now use the "resource-whitelist" section to specify files that are allowed, but DON'T need to exist or be SHA checked. This is useful for adding files that are changed on the client side by the mod itself (like KMP's "KMPPlayerToken.txt" file), which may or may not exist and will not match a SHA hash check. The effects of placing a filename in the whitelist would be the same as putting that file in the "Optional" directory and running /modgen, but this allows you to keep that mod together and not have to split it among different directories (e.g. putting most of KMP in the "Required_Exact" directory, but the "KMPPlayerToken.txt" file in the "Optional" directory). By default, the client can only connect if:
- they have ALL the files in both of the "Required" directories
- their copies of files match the ones on the server in the "Required_Exact" directory (using a SHA256 hash check)
- if they have a file that's in the "Optional_Exact" directory, it must match the one on the server (also using SHA256)
- if they have a file that's in neither the "Required" nor "Optional" directories, they are not allowed to connect

Example:
I would like to use KWRocketry parts on my server, and I'm OK with my clients using EditorExtensions but I don't want to force them to (but if they want to, it should be the same version that I put on my server). I'm also OK with them using Kerbal Alarm Clock, and I don't care which version they're using. I download the KWRocketry mod, and I place the directory that would normally go in KSP's "GameData" directory into "Mods/Required_Exact". I then download the EditorExtensions mod, and I place the directory that would normally go in KSP's "GameData" directory into "Mods/Optional_Exact".
I then download the KerbalAlarmClock mod, and place the directory that would normally go in KSP's "GameData" directory into "Mods/Optional". Now, I start the server and use the "/modgen" command. I will now have a running server that will force clients to have KWRocketry, let them have a specific version of EditorExtensions if they want, let them have any version of KerbalAlarmClock, and not allow them to have any other mods. I can use the /modgen command again if I like (e.g. if I added a new mod), and it will retain any filenames that I've added to the "resource-whitelist" section.

Other configuration options are available as well. If you would like to manually configure your KMPModControl.txt file, start the server once (with nothing in any of the "Mods/" directories) and it will auto-generate a basic file. Read the comments to understand exactly how each section works. You will probably only need this option if you want to run a "blacklist" server instead of a "whitelist" server, where any file is allowed except the ones specifically listed. Blacklist servers make it very easy for one client with different mods to mess up the whole system though, so be careful with this.
