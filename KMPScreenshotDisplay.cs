using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using UnityEngine;

namespace KMP
{
	class KMPScreenshotDisplay
	{
		public const float MIN_WINDOW_WIDTH = ScreenshotSettings.MIN_WIDTH + 100;
		public const float MIN_WINDOW_HEIGHT = ScreenshotSettings.MIN_HEIGHT + 10;

		public static ScreenshotSettings screenshotSettings = new ScreenshotSettings();
		public static bool windowEnabled = false;
		public static String watchPlayerName = String.Empty;
		public static Texture2D texture;
		public static String description = String.Empty;

		public static Rect windowPos = new Rect(
			Screen.width / 2 - MIN_WINDOW_WIDTH / 2, Screen.height / 2 - MIN_WINDOW_HEIGHT / 2,
			MIN_WINDOW_WIDTH, MIN_WINDOW_HEIGHT);
		public static Vector2 scrollPos = Vector2.zero;

		public static GUILayoutOption[] layoutOptions;
	}
}
