using System;
using UnityEngine;

namespace KMP
{
	[Serializable()]
	public class KMPFlightCtrlState
	{
		public float fastThrottle, mainThrottle;
		public float pitch, pitchTrim, roll, rollTrim, wheelSteer, wheelSteerTrim, yaw, yawTrim;
		public bool gearDown, gearUp, headlight, killRot;
		public float X, Y, Z;
		
		public KMPFlightCtrlState(FlightCtrlState ctrlState)
		{
			updateFromFlightCtrlState(ctrlState);
		}
		
		public void updateFromFlightCtrlState(FlightCtrlState ctrlState)
		{
			fastThrottle = ctrlState.fastThrottle;
			gearDown = ctrlState.gearDown;
			gearUp = ctrlState.gearUp;
			headlight = ctrlState.headlight;
			killRot = false; //ctrlState.killRot;
			mainThrottle = ctrlState.mainThrottle;
			pitch = ctrlState.pitch;
			pitchTrim = ctrlState.pitchTrim;
			roll = ctrlState.roll;
			rollTrim = ctrlState.rollTrim;
			wheelSteer = ctrlState.wheelSteer;
			wheelSteerTrim = ctrlState.wheelSteerTrim;
			yaw = ctrlState.yaw;
			yawTrim = ctrlState.yawTrim;
			X = ctrlState.X;
			Y = ctrlState.Y;
			Z = ctrlState.Z;
		}
		
		public FlightCtrlState getAsFlightCtrlState()
		{
			return getAsFlightCtrlState(1f);
		}
		
		public FlightCtrlState getAsFlightCtrlState(float scale)
		{
			FlightCtrlState ctrlState = new FlightCtrlState();
			ctrlState.fastThrottle = fastThrottle * scale;
			ctrlState.gearDown = gearDown;
			ctrlState.gearUp = gearUp;
			ctrlState.headlight = headlight;
			ctrlState.killRot = killRot;
			ctrlState.mainThrottle = mainThrottle * scale;
			ctrlState.pitch = pitch * scale;
			ctrlState.pitchTrim = pitchTrim * scale;
			ctrlState.roll = roll * scale;
			ctrlState.rollTrim = rollTrim * scale;
			ctrlState.wheelSteer = wheelSteer * scale;
			ctrlState.wheelSteerTrim = wheelSteerTrim * scale;
			ctrlState.yaw = yaw * scale;
			ctrlState.yawTrim = yawTrim * scale;
			ctrlState.X = X * scale;
			ctrlState.Y = Y * scale;
			ctrlState.Z = Z * scale;
			return ctrlState;
		}
	}
}

