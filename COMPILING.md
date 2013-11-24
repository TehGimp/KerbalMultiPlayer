ALL PLATFORMS
=============

GIT
---
Cloning

    git clone https://github.com/TehGimp/KerbalMultiPlayer.git

Updating

    git fetch --all
    git merge

Contributing
First you will need to fork "KerbalMultiPlayer" on github, and set up your SSH key.

    git remote remove origin
    git remote add upstream https://github.com/TehGimp/KerbalMultiPlayer.git
    git remote add origin git@github.com:YOUR_USER_NAME_HERE/KerbalMultiPlayer.git
    git fetch --all
    git push --set-upstream origin master

Make a branch so you don't bork your master.

    git branch bugfix-number
    git checkout bugfix-number

Do your changes here with your favourite text editor or IDE.

    git add -A
    git commit -a
    git push --set-upstream origin bugfix-number

When you are happy with the code, open a pull request on github. After it is merged you can delete it and merge it in your master

    git checkout master
    git fetch -all
    git merge upstream/master
    git branch -D bugfix-number
    git push origin :bugfix-number


DEPENDANCIES - DON'T MISS THIS STEP!:
-------------------------------------

In order to compile KMP, You need to copy these 3 files from "KSP_ROOT/KSP_Data/Managed/" into the root directory of your KMP clone.
* Assembly-CSharp-firstpass.dll
* Assembly-CSharp.dll
* UnityEngine.dll


WINDOWS
=======

Visual Studio
-------------
To edit and compile, open up the KerbalMultiPlayer.sln file with Visual Studio. 
Upon opening, you may receive errors relating towards KMPServerList which can be safely ignored. If you have installed all dependencies, you should be able to build the solution.
Builds will be stored in either \bin\Release for the client, or \KLFServer\bin\Release for the server.

Debugging does not seem to work at this current point with the server or client. (?)

MAC
===

To be written by a mac developer.

LINUX
=====

MonoDevelop
-----------
To be written by a MonoDevelop user.

xbuild
------
xbuild is included on mono linux installs (mono-complete will install this on debian and ubuntu).
cd into your KMP root clone directory and type "xbuild/p:Configuration=Release". The builds are under bin/Release/ and KMPServer/bin/Release/
