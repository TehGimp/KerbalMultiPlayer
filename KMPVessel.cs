using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

namespace KMP
{
    public class KMPVessel
    {
        public const double PACK_CHECK_INTERVAL = 1;
        public const float PLAYER_PACK_DISTANCE = 2400;
        public const float PLAYER_UNPACK_DISTANCE = 2000;
        public const float NORMAL_PACK_DISTANCE = 600;
        public const float NORMAL_UNPACK_DISTANCE = 200;
        private double lastPackCheck = 0;
        //Properties
        public KMPVesselInfo info;
        public String ownerName;
        public String vesselName;
        public Guid id;
        public OrbitRenderer orbitRenderer;
        public GameObject gameObj;
        public LineRenderer line;
        public Color activeColor;
        #region Vessel state data
        public Orbit referenceOrbit;
        //public Quaternion referenceRotation;
        public Vector3 referenceForward;
        public Vector3 referenceUp;
        public Vector3d referenceSurfacePosition;
        public Vector3d referenceSurfaceVelocity;
        public Vector3 referenceAngularVelocity;
        public Vector3d referenceAcceleration;
        public double referenceUT;
        #endregion
        public Orbit currentOrbit
        {
            get
            {
                if (referenceOrbit == null)
                {
                    KMP.Log.Debug("Reference orbit is null!");
                    return null;
                }

                Orbit tempOrbit = new Orbit(referenceOrbit.inclination, referenceOrbit.eccentricity, referenceOrbit.semiMajorAxis, referenceOrbit.LAN, referenceOrbit.argumentOfPeriapsis, referenceOrbit.meanAnomalyAtEpoch, referenceOrbit.epoch, referenceOrbit.referenceBody);

                double timeNow = Planetarium.GetUniversalTime();

                //Sync orbit backwards
                if (tempOrbit.StartUT > timeNow)
                {
                    KMP.Log.Debug("Reference orbit begins at " + tempOrbit.StartUT + ", updating to " + timeNow);

                    while (tempOrbit.StartUT > timeNow || tempOrbit == null)
                    {
                        if (tempOrbit != null ? tempOrbit.previousPatch != null : false)
                        {
                            KMP.Log.Debug("Updating orbit from " + tempOrbit.referenceBody.bodyName + "(begins " + tempOrbit.StartUT + ") to " + tempOrbit.previousPatch.referenceBody.bodyName + " (starts " + tempOrbit.previousPatch.StartUT + ")");
                        }
                        else
                        {
                            KMP.Log.Debug("Updating orbit from " + tempOrbit.referenceBody.bodyName + " to null (no next patch available)");
                        }
                        tempOrbit = tempOrbit.previousPatch;
                    }
                }

                //Sync orbit forwards
                if ((tempOrbit.EndUT < timeNow) && (tempOrbit.EndUT != 0))
                {
                    KMP.Log.Debug("Reference orbit ends at " + tempOrbit.EndUT + ", updating to " + timeNow);
                    
                    while ((tempOrbit.EndUT < timeNow) && (tempOrbit.EndUT != 0) && tempOrbit != null)
                    {

                        if (tempOrbit != null ? tempOrbit.nextPatch != null : false)
                        {
                            KMP.Log.Debug("Updating orbit from " + tempOrbit.referenceBody.bodyName + "(ends " + tempOrbit.EndUT + ") to " + tempOrbit.nextPatch.referenceBody.bodyName + " (ends " + tempOrbit.nextPatch.EndUT + ")");
                        }
                        else
                        {
                            KMP.Log.Debug("Updating orbit from " + tempOrbit.referenceBody.bodyName + " to null (no next patch available)");
                        }
                        tempOrbit = tempOrbit.nextPatch;
                    }
                }

                //Update the orbit
                if (tempOrbit == null)
                {
                    KMP.Log.Debug("KMPVessel: New orbit is null!");
                }

                return tempOrbit;
            }
        }

        public bool useSurfacePositioning
        {
            get
            {
                return (situationIsGrounded(info.situation) || referenceSurfacePosition.z < 10000);
            }
        }
        #region Surface positioning
        public Vector3d surfaceModePosition
        {
            get
            {
                //surfaceMotionPrediciton returns Vector3d.zero if we aren't in a situation to use it.
                //Add 10cm to the height so we don't go popping wheels
                switch (currentVessel.vesselType)
                {
                    case VesselType.EVA:
                        return referenceOrbit.referenceBody.GetWorldSurfacePosition(referenceSurfacePosition.x, referenceSurfacePosition.y, referenceSurfacePosition.z) + surfacePositionPrediction;
                    case VesselType.Flag:
                        return referenceOrbit.referenceBody.GetWorldSurfacePosition(referenceSurfacePosition.x, referenceSurfacePosition.y, -1);
                    default:
                        return referenceOrbit.referenceBody.GetWorldSurfacePosition(referenceSurfacePosition.x, referenceSurfacePosition.y, (referenceSurfacePosition.z + .1)) + surfacePositionPrediction;
                }
            }
        }

        public Vector3d surfaceModeVelocity
        {
            get
            {
                //return vesselRef.rigidbody.transform.TransformDirection(referenceSurfaceVelocity);
                return referenceSurfaceVelocity + velocityPrediction;
            }
        }
        //Returns a vector in world co-ordinates of the estimated surface transform if we are well in-sync.
        private Vector3d surfacePositionPrediction
        {
            get
            {
                //This uses the most awesome algorithm of position = position + (surface_velocity * time_difference).
                Vector3d fudge = referenceSurfaceVelocity;
                float timeDelta = (float)(Planetarium.GetUniversalTime() - referenceUT);
                //These values should probably be constants somewhere.
                //Max prediction: 3 seconds.
                //Has to be grounded as this is intended for rover drag racing.
                if (Math.Abs(timeDelta) < 3)
                {
                    return (fudge * timeDelta);
                }
                return Vector3d.zero;
            }
        }

        private Vector3d velocityPrediction
        {
            get
            {
                Vector3d fudge = referenceAcceleration;
                float timeDelta = (float)(Planetarium.GetUniversalTime() - referenceUT);
                //These values should probably be constants somewhere.
                //Max prediction: 3 seconds.
                if (Math.Abs(timeDelta) < 3)
                {
                    return (fudge * timeDelta);
                }
                return Vector3d.zero;
            }
        }
        #endregion
        public bool orbitValid
        {
            get
            {
                if (referenceOrbit == null)
                {
                    return false;
                }
                return true;
            }
        }

        public bool shouldShowOrbit
        {
            get
            {
                if (currentVessel == null || !orbitValid || situationIsGrounded(info.situation))
                {
                    return false;
                }
                return true;
            }
        }

        private Vessel currentVessel
        {
            get
            {
                return FlightGlobals.fetch.vessels.Find(v => v.id == id);
            }
        }
        //Methods
        public KMPVessel(String vessel_name, String owner_name, Guid _id)
        {
            info = new KMPVesselInfo();
            ownerName = owner_name;
            id = _id;
            vesselName = vessel_name;

            System.Text.StringBuilder sb = new StringBuilder();
            sb.Append(vesselName);
            sb.Append(" <");
            sb.Append(ownerName);
            sb.Append('>');

            gameObj = new GameObject(sb.ToString());
            gameObj.layer = 9;
            generateActiveColor();

            line = gameObj.AddComponent<LineRenderer>();
            orbitRenderer = gameObj.AddComponent<OrbitRenderer>();
            orbitRenderer.driver = new OrbitDriver();

            line.transform.parent = gameObj.transform;
            line.transform.localPosition = Vector3.zero;
            line.transform.localEulerAngles = Vector3.zero;

            line.useWorldSpace = true;
            line.material = new Material(Shader.Find("Particles/Alpha Blended Premultiply"));
            line.SetVertexCount(2);
            line.enabled = false;
        }

        public void generateActiveColor()
        {
            //Generate a display color from the owner name
            activeColor = generateActiveColor(ownerName);
        }

        public static Color generateActiveColor(String str)
        {
            int val = 5381;

            foreach (char c in str)
            {
                val = ((val << 5) + val) + c;
            }

            return generateActiveColor(Math.Abs(val));
        }

        public static Color generateActiveColor(int val)
        {
            switch (val % 17)
            {
                case 0:
                    return Color.red;

                case 1:
                    return new Color(1, 0, 0.5f, 1); //Rosy pink
					
                case 2:
                    return new Color(0.6f, 0, 0.5f, 1); //OU Crimson
					
                case 3:
                    return new Color(1, 0.5f, 0, 1); //Orange
					
                case 4:
                    return Color.yellow;
					
                case 5:
                    return new Color(1, 0.84f, 0, 1); //Gold
					
                case 6:
                    return Color.green;
					
                case 7:
                    return new Color(0, 0.651f, 0.576f, 1); //Persian Green
					
                case 8:
                    return new Color(0, 0.651f, 0.576f, 1); //Persian Green
					
                case 9:
                    return new Color(0, 0.659f, 0.420f, 1); //Jade
					
                case 10:
                    return new Color(0.043f, 0.855f, 0.318f, 1); //Malachite
					
                case 11:
                    return Color.cyan;					

                case 12:
                    return new Color(0.537f, 0.812f, 0.883f, 1); //Baby blue;

                case 13:
                    return new Color(0, 0.529f, 0.741f, 1); //NCS blue
					
                case 14:
                    return new Color(0.255f, 0.412f, 0.882f, 1); //Royal Blue
					
                case 15:
                    return new Color(0.5f, 0, 1, 1); //Violet
					
                default:
                    return Color.magenta;
					
            }
        }

        public void updatePlayerMarker()
        {
            if (!orbitValid)
            {
                return;
            }
            if (currentVessel == null)
            {
                return;
            }
            if (gameObj == null)
            {
                return;
            }
            gameObj.transform.localPosition = currentVessel.transform.position;
            Vector3 scaled_pos = ScaledSpace.LocalToScaledSpace(currentVessel.transform.position);
            //Determine the scale of the line so its thickness is constant from the map camera view
            float apparent_size = 0.01f;
            bool pointed = true;
            switch (info.state)
            {
                case State.ACTIVE:
                    apparent_size = 0.015f;
                    pointed = true;
                    break;

                case State.INACTIVE:
                    apparent_size = 0.01f;
                    pointed = true;
                    break;

                case State.DEAD:
                    apparent_size = 0.01f;
                    pointed = false;
                    break;

            }
            float scale = (float)(apparent_size * Vector3.Distance(MapView.MapCamera.transform.position, scaled_pos));
            Vector3 worldDirection = currentVessel.mainBody.transform.TransformDirection(referenceForward);

            //Set line vertex positions
            Vector3 line_half_dir = worldDirection * (scale * ScaledSpace.ScaleFactor);
            if (pointed)
            {
                line.SetWidth(scale, 0);
            }
            else
            {
                line.SetWidth(scale, scale);
                line_half_dir *= 0.5f;
            }
            line.SetPosition(0, ScaledSpace.LocalToScaledSpace(currentVessel.transform.position - line_half_dir));
            line.SetPosition(1, ScaledSpace.LocalToScaledSpace(currentVessel.transform.position + line_half_dir));
        }

        public void updatePackDistance(bool forceNormal = false) {
            if (((Planetarium.GetUniversalTime() - lastPackCheck) > PACK_CHECK_INTERVAL) || forceNormal) {
                lastPackCheck = Planetarium.GetUniversalTime();
                Vessel vessel = currentVessel;
                if (vessel != null)
                {
                    if (info.state == State.ACTIVE && !forceNormal)
                    {
                        vessel.distanceLandedPackThreshold = PLAYER_PACK_DISTANCE;
                        vessel.distancePackThreshold = PLAYER_PACK_DISTANCE;
                        vessel.distanceUnpackThreshold = PLAYER_UNPACK_DISTANCE;
                        vessel.distanceLandedUnpackThreshold = PLAYER_UNPACK_DISTANCE;
                    }
                    else
                    {
                        vessel.distanceLandedPackThreshold = NORMAL_PACK_DISTANCE;
                        vessel.distancePackThreshold = NORMAL_PACK_DISTANCE;
                        vessel.distanceUnpackThreshold = NORMAL_UNPACK_DISTANCE;
                        vessel.distanceLandedUnpackThreshold = NORMAL_UNPACK_DISTANCE;
                    }
                }
            }
        }

        public void updateRenderProperties(bool force_hide = false)
        {
           try
           {
                if (orbitRenderer.driver.orbit == null) {
                    return;
                }

                if (gameObj == null || currentVessel == null)
                {
                    KMP.Log.Debug("KMPVessel - updateRendererProperties is null");
                    return;
                }
                line.enabled = !force_hide && MapView.MapIsEnabled;
                if (!force_hide && shouldShowOrbit)
                {
                    orbitRenderer.drawMode = OrbitRenderer.DrawMode.REDRAW_AND_RECALCULATE;
                    orbitRenderer.enabled = true;
                }
                else
                {
                    orbitRenderer.drawMode = OrbitRenderer.DrawMode.OFF;
                    orbitRenderer.enabled = false;
                }
                //Determine the color
                Color color = activeColor;
                if (orbitRenderer.mouseOver)
                {
                    color = Color.white; //Change line color when moused over
                }
                else
                {
				
                    switch (info.state)
                    {
                        case State.ACTIVE:
                            color = activeColor;
                            break;

                        case State.INACTIVE:
                            color = activeColor * 0.75f;
                            color.a = 1;
                            break;

                        case State.DEAD:
                            color = activeColor * 0.5f;
                            break;
                    }
				
                }
                line.SetColors(color, color);

                orbitRenderer.driver.orbitColor = color * 0.5f;
                if (force_hide || !orbitValid)
                {
                    orbitRenderer.drawIcons = OrbitRenderer.DrawIcons.NONE;
                }
                else
                {
                    if (info.state == State.ACTIVE && shouldShowOrbit)
                    {
                        orbitRenderer.drawIcons = OrbitRenderer.DrawIcons.OBJ_PE_AP;
                    }
                    else
                    {
                        orbitRenderer.drawIcons = OrbitRenderer.DrawIcons.OBJ;
                    }
                }
            }
            catch (Exception e)
            {
                KMP.Log.Debug("KMPVessel - updateRendererProperties failed: " + e.Message);
            }
        }

        public static bool situationIsGrounded(Situation situation)
        {

            switch (situation)
            {

                case Situation.LANDED:
                case Situation.SPLASHED:
                case Situation.PRELAUNCH:
                case Situation.DESTROYED:
                case Situation.UNKNOWN:
                    return true;

                default:
                    return false;
            }
        }

        public static bool situationIsOrbital(Situation situation)
        {

            switch (situation)
            {

                case Situation.ASCENDING:
                case Situation.DESCENDING:
                case Situation.ENCOUNTERING:
                case Situation.ESCAPING:
                case Situation.ORBITING:
                    return true;

                default:
                    return false;
            }
        }
    }
}
