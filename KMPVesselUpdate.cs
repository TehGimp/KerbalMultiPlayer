using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;
using System.Runtime.Serialization;

namespace KMP
{

	public enum Activity
	{
		NONE,
		AEROBRAKING,
		PARACHUTING,
		DOCKING
	}

	public enum Situation
	{
		UNKNOWN,
		DESTROYED,
		LANDED,
		SPLASHED,
		PRELAUNCH,
		ORBITING,
		ENCOUNTERING,
		ESCAPING,
		ASCENDING,
		DESCENDING,
		FLYING,
		DOCKED
	}

	public enum State
	{
		ACTIVE,
		INACTIVE,
		DEAD
	}
	
	public enum RelativeTime
	{
		PAST,
		PRESENT,
		FUTURE
	}
	
	[Serializable()]
	public class KMPVesselDetail
	{
		/// <summary>
		/// The specific activity the vessel is performing in its situation
		/// </summary>
		public Activity activity;

		/// <summary>
		/// Whether or not the player controlling this vessel is idle
		/// </summary>
		public bool idle;

		/// <summary>
		/// The number of crew the vessel is holding. byte.Max signifies not applicable
		/// </summary>
		public byte numCrew;

		/// <summary>
		/// The percentage of fuel remaining in the vessel. byte.Max signifies no fuel capacity
		/// </summary>
		public byte percentFuel;

		/// <summary>
		/// The percentage of rcs fuel remaining in the vessel. byte.Max signifies no rcs capacity
		/// </summary>
		public byte percentRCS;

		/// <summary>
		/// The mass of the vessel
		/// </summary>
		public float mass;

		public KMPVesselDetail()
		{
			activity = Activity.NONE;
			numCrew = 0;
			percentFuel = 0;
			percentRCS = 0;
			mass = 0.0f;
			idle = false;
		}
		
	}

	[Serializable()]
	public class KMPVesselInfo
	{

		/// <summary>
		/// The vessel's KSP Vessel situation
		/// </summary>
		public Situation situation;

		/// <summary>
		/// The vessel's KSP vessel state
		/// </summary>
		public State state;

		/// <summary>
		/// The timescale at which the vessel is warping
		/// </summary>
		public float timeScale;

		/// <summary>
		/// The name of the body the vessel is orbiting
		/// </summary>
		public String bodyName;

		public KMPVesselDetail detail;
		
		public KMPVesselInfo()
		{
			situation = Situation.UNKNOWN;
			timeScale = 1.0f;
			detail = null;
		}
		
		public KMPVesselInfo(KMPVesselInfo copyFrom)
		{
			situation = copyFrom.situation;
			state = copyFrom.state;
			timeScale = copyFrom.timeScale;
			bodyName = copyFrom.bodyName;
			detail = copyFrom.detail;
		}
	}

    [Serializable()]
    public class KMPVesselUpdate : KMPVesselInfo
    {
		/// <summary>
		/// The vessel name
		/// </summary>
        public String name;

		/// <summary>
		/// The player who owns this ship
		/// </summary>
        public String player;

		/// <summary>
		/// The ID of the vessel
		/// </summary>
		public Guid id;

		/// <summary>
		/// The position of the vessel relative to its parent body transform
		/// </summary>
        public float[] pos;

		/// <summary>
		/// The direction of the vessel relative to its parent body transform
		/// </summary>
        public float[] dir;

		/// <summary>
		/// The velocity of the vessel relative to its parent body transform
		/// </summary>
        public float[] vel;
		
		/// <summary>
		/// Orbit and surface velocity vectors
		/// </summary>
		public double[] o_vel;
		public double[] s_vel;
		
		/// <summary>
		/// Used for transferring various context-dependent double values: relative vessel positions in docking mode, needed orbit properties
		/// </summary>
		public double[] w_pos;
		
		/// <summary>
		/// Rotation quat
		/// </summary>
		public float[] rot;
		
		public ConfigNode protoVesselNode = null;
		public Guid kmpID = Guid.Empty;
		public bool isPrivate = true;
		public bool isMine = false;
		public double tick = 0d;
		public float distance = 0f;
		public int crewCount = 0;
		public bool isSyncOnlyUpdate = false;
		public Guid relativeTo = Guid.Empty;
		public bool isDockUpdate = false;
		
		public RelativeTime relTime = RelativeTime.PRESENT;
		public KMPFlightCtrlState flightCtrlState = null;
		
		public KMPVesselUpdate(Vessel _vessel)
        {
           InitKMPVesselUpdate(_vessel, true);
        }
		
		public KMPVesselUpdate(Vessel _vessel, bool includeProtoVessel)
		{
			InitKMPVesselUpdate(_vessel, includeProtoVessel);
		}
		
		public KMPVesselUpdate(Guid gameGuid, ConfigNode protoVessel)
        {
           	pos = new float[3];
            dir = new float[3];
            vel = new float[3];
			o_vel = new double[3];
			s_vel = new double[3];
			w_pos = new double[3];
			rot = new float[4];
			id = gameGuid;
			flightCtrlState = new KMPFlightCtrlState(new FlightCtrlState());
			protoVesselNode = protoVessel;
        }
		
        private void InitKMPVesselUpdate(Vessel _vessel, bool includeProtoVessel)
        {
            pos = new float[3];
            dir = new float[3];
            vel = new float[3];
            o_vel = new double[3];
            s_vel = new double[3];
            w_pos = new double[3];
            rot = new float[4];
            id = _vessel.id;
            if (_vessel.packed)
            {
                flightCtrlState = new KMPFlightCtrlState(new FlightCtrlState());
            }
            else
            {
                flightCtrlState = new KMPFlightCtrlState(_vessel.ctrlState);
            }
            if (includeProtoVessel)
            {
                protoVesselNode = new ConfigNode();
                ProtoVessel proto;
                try
                {
                    proto = new ProtoVessel(_vessel);
                }
                catch (Exception e)
                {
                    Log.Debug("Exception thrown in InitKMPVesselUpdate(), catch 1, Exception: {0}", e.ToString());
                    proto = null;
                }
                if (proto != null)
                {
                    foreach (ProtoCrewMember crewMember in proto.GetVesselCrew())
                    {
                        crewMember.KerbalRef = null;
                    }
                    proto.Save(protoVesselNode);
                }
            }
        }
		
		public ConfigNode getProtoVesselNode()
		{
            return protoVesselNode;
		}
		
		public void clearProtoVessel()
		{
			protoVesselNode = null;
		}
		
		public void setProtoVessel(ConfigNode node)
		{
			protoVesselNode = node;
		}
    }

}
