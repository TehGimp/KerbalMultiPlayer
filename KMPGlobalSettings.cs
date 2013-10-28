using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using UnityEngine;
using System.Runtime.Serialization;

namespace KMP
{
	[Serializable]
	class KMPGlobalSettings
	{
		public float infoDisplayWindowX;
		public float infoDisplayWindowY;

		public float screenshotDisplayWindowX;
		public float screenshotDisplayWindowY;

		public float chatDisplayWindowX;
		public float chatDisplayWindowY;

		public bool infoDisplayBig = false;

		public bool chatWindowEnabled = false;
		public bool chatWindowWide = false;

		public KeyCode guiToggleKey = KeyCode.F7;
		public KeyCode screenshotKey = KeyCode.F8;

		[OptionalField(VersionAdded = 1)]
		public bool smoothScreens = true;

		[OptionalField(VersionAdded = 2)]
		public bool chatColors = true;

		[OptionalField(VersionAdded = 2)]
		public bool showInactiveShips = true;

		[OnDeserializing]
		private void SetDefault(StreamingContext sc)
		{
			smoothScreens = true;
			guiToggleKey = KeyCode.F7;
			screenshotKey = KeyCode.F8;
			chatColors = true;
			showInactiveShips = true;
		}

		public static KMPGlobalSettings instance = new KMPGlobalSettings();

	}
}
