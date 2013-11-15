using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using UnityEngine;

namespace KMP
{
	class KMPConnectionDisplay
	{
		public const float MIN_WINDOW_WIDTH = 400;
		public const float MIN_WINDOW_HEIGHT = 260;

		public static bool windowEnabled = true;
		public static String activeHostname = String.Empty;

		public static Rect windowPos 
		{
			get
			{
				return new Rect(Screen.width - MIN_WINDOW_WIDTH * 1.5f, Screen.height / 2 - MIN_WINDOW_HEIGHT / 2, MIN_WINDOW_WIDTH, MIN_WINDOW_HEIGHT); 
			}
		}
		public static Vector2 scrollPos = Vector2.zero;

		public static GUILayoutOption[] layoutOptions;
	}
}
