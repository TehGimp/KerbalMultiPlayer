KMP Client README
=================

About
-----
KMP is a mod for v0.23.5 of Kerbal Space Program that adds a multiplayer game
option. In a KMP game you can freely interact with other players and do all the
Kerbally things you'd normally do in KSP, but with friends (or strangers)
playing in the same universe, at the same time. See the FAQ for more
information.


Installation
------------
Simply copy the contents of KMP.zip to your KSP directory. Be sure you copy both
the "GameData" & "saves" folders as both are required for KMP to function
correctly. It is recommended that you use a fresh copy of KSP for playing KMP.
As KMP is in an EARLY ALPHA state it could potentially cause major issues even
for non-KMP sessions, and KMP is not compatible with some other KSP mods.

LINUX USERS: The client needs mscorlib.dll to run. On Debian-derived systems
  (such as Ubuntu) you can do this with the command:
    sudo apt-get install mono-complete


Getting Connected
-----------------
To connect to a server: start KSP, click "Start Game", then "KerbalMultiPlayer",
and then add/select a server from the favourites list and click "Connect".
Clicking "Add Server" will show/hide the new server form. To add a new server,
just enter the address & port (default 2076) and click "New"/"Replace".


Gameplay
--------
For the most part, playing with KMP is just like playing a single-player KSP
game: you still build and launch ships, and you can even use time warp whenever
you need/want to (see "Warping & Relative Time" below). There are several new
windows added to the interface to allow you to communicate and coordinate with
other players, and you will see indicators for other players on all the in-game
maps of the solar system. Many features (such as reverting to launch,
quick-loading, pausing, & physics-warp) are disabled during a KMP session. When
the chat is visible, press the chat key (default "~") to send a chat message.

[Main KMP Window]
The main KMP window lists all the currently connected players with status
  information, and provides access to most KMP features. The buttons on the main
  window are:
"X" - Hides the main KMP window and the KMP privacy lock. Click the "[-]" button
  to restore the main KMP window.
"+"/"-" - Maximizes/minimizes the main KMP window size
"Detail" - Toggles whether detailed flight status information is displayed for
  other players
"Options" - Displays settings including keybindings, as well as the "Disconnect
  & Exit" button
"Chat" - Toggles whether the Chat window is displayed
"Viewer" - Toggles whether the Viewer window is displayed
"Share Screen" - Takes a screenshot to be shared with other players
"Sync" - Syncronize with the server's latest "subspace" (see "Warping &
  Relative Time")

WARNING FOR LINUX USERS: A bug with Unity/KSP as of 0.23 causes keyboard input
  to be picked up by your vessel even when you a typing in a text box. Be sure
  you are on the Map View before participating in chat to prevent accidents!

[Viewer]
The Viewer allows you to view screenshots shared by other players. Just click on
  a player's name to see the most recent screenshot they've shared.

WARNING FOR LINUX USERS: A bug with Unity/KSP as of 0.23 causes issues with sending or viewing
  screenshots. While these features may work normally in some cases, it is recommended that you
  avoid using them as they often cause the game to crash without warning.

Hex-editing your game binary remedies this issue as discussed on the forums here:
  http://forum.kerbalspaceprogram.com/threads/24529-The-Linux-compatibility-thread!?p=849510&viewfull=1#post849510

    echo "838077: 00" | xxd -r - KSP.x86_64 echo "83807c: 00" | xxd -r - KSP.x86_64


Warping & Relative Time
-----------------------
KMP allows you to use regular "on-rails" warp on-demand, as much as you need.
However, whenever you use warp this desynchronizes you from other players
putting you in the future relative to them. You can still play your game
normally when other players are in the past, but you won't be able to interact
with them directly and they'll only see the things you're doing if the "catch
up" to you in time. Other players that use warp may end up in the future
relative to you, and it's not uncommon to find yourself sitting between other
groups of players, some in the future and some in the past. You can sync with
any player that's in the future relative to you--just click the "Sync" button
under their name and you'll be jumped ahead in time to the same 'subspace' as
that player. The "Sync" button at the bottom of the main KMP window always
sychronizes you with the "latest" subspace available in the current game, which
is where (when?) newly connected players join the game as well. Be sure that
you're landed or in a stable orbit when you sync, as otherwise you may find
yourself somewhere unexpected!

You're still able to see other players and what they're doing in-game when
they're in a different subspace--you can chat, send screenshots, share designs,
etc, but you won't be able to interact with any in-game vessels that those
players control. Vessels from the past or future turn translucent so that you
know that you won't be able to affect them. If the other player is in the past,
KMP tries to predict where they'll be in the future and shows their ship at
that location. Keep in mind, though, that since the ship is still being
manipulated in the past, the predicted future location can rapidly change. If
another ship has been manipulated in the future, you're effectively just
watching a recording of events that have "already happened" play out.


Safety Bubble
-------------
To help prevent launch collisions and performance problems near the KSC, there
is a "safety bubble" surrounding the area where vessels from other players will
not be loaded or updated. The current "safety bubble" is cylinder shaped, with
the 'ceiling' at 35000m and the radius controlled on a per-server basis (the
default bubble radius is 2000m). Use the "!bubble" chat command for information
about your position relative to the bubble edge on the current server.


Vessel Privacy
--------------
By default and vessel you launch will be in a "Public" status, which means that
other players can take control of it if you leave and they may freely dock with
your ship as well. To keep a vessel to yourself, use the "Lock" window on the
right-side of the screen to set the vessel to "Private" status. You can't take
control of another vessel owned by another player that has marked it "Private",
nor can you dock with a "Private" vessel. Keep in mind, though, that if another
player is in control of your vessel they can choose to lock it to themselves--
only share your ships with players you trust. Your vessel locks are tied to your
username & token (see "User Authentication" below)--if you change your username
or token, you will lose access to vessels that were marked "Private" with those
credentials. Players can always interact physically (i.e. collide) with another
vessel even when the owner is absent.


User Authentication
-------------------
KMP uses a simple username+token system for controlling your access to a server
and to your vessels. Your "token" is a file created by KMP which is stored at
[KSP]\GameData\KMP\Plugins\PluginData\KerbalMultiPlayer\KMPPlayerToken.txt--
think of this file as your password. Once you first join a server successfully,
the username you are using is immediately tied to your token *for that server*.
Other players that join that server will not be able to connect with your
username as they won't have a matching token. If you want to access the same
server from multiple computers with a single username, be sure to copy the
KMPPlayerToken.txt file created on the first computer you use to the other(s) in
order to make it possible for you to connect. If your token file is missing, a
new one will be generated for you but this will of course also change your
token. Your vessel locks (if any) are tied to your username & token--if either
changes you will lose access to any vessels that were set to "Private" until you
restore the original username and token or contact a server admin for
assistance.


Mods
----
Some other KSP mods do not work well with KMP, but many are known to work
normally. Access to mods is controlled on a per-server basis. Please refer to
the server README.txt for more information about how to specify mod settings for
a server.

Source available at: https://github.com/TehGimp/KerbalMultiPlayer
