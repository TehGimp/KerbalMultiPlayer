using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using UnityEngine;

namespace KMP
{
	class KMPVesselLockDisplay
	{
		public const float MIN_WINDOW_WIDTH = 60;
		public const float MIN_WINDOW_HEIGHT = 10;
		
		public static Rect windowPos 
		{
			get
			{
				return new Rect(Screen.width - MIN_WINDOW_WIDTH, Screen.height / 2 - MIN_WINDOW_HEIGHT / 2, MIN_WINDOW_WIDTH, MIN_WINDOW_HEIGHT); 
			}
		}

		public static GUILayoutOption[] layoutOptions;
	}
}
