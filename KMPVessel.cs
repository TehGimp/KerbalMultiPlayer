using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

namespace KMP
{
    public class KMPVessel
    {

        //Properties

		public KMPVesselInfo info;

		public String vesselName
		{
			private set;
			get;
		}

		public String ownerName
		{
			private set;
			get;
		}

		public Guid id
		{
			private set;
			get;
		}

        public Vector3 localDirection
        {
            private set;
            get;
        }

        public Vector3 localPosition
        {
            private set;
            get;
        }

        public Vector3 localVelocity
        {
            private set;
            get;
        }

        public Vector3 translationFromBody
        {
            private set;
            get;
        }

        public Vector3 worldDirection
        {
            private set;
            get;
        }

        public Vector3 worldPosition
        {
            get
            {
				if (!orbitValid)
					return Vector3.zero;

				if (mainBody != null)
				{
					if (situationIsGrounded(info.situation))
					{
						//Vessel is fixed in relation to body
						return mainBody.transform.TransformPoint(localPosition);
					}
					else
					{
						//Calculate vessel's position at the current (real-world) time
						double time = adjustedUT;

						if (mainBody.referenceBody != null && mainBody.referenceBody != mainBody && mainBody.orbit != null)
						{
							//Adjust for the movement of the vessel's parent body
							Vector3 body_pos_at_ref = mainBody.orbit.getTruePositionAtUT(time);
							Vector3 body_pos_now = mainBody.orbit.getTruePositionAtUT(Planetarium.GetUniversalTime());

							return body_pos_now + (orbitRenderer.driver.orbit.getTruePositionAtUT(time) - body_pos_at_ref);
						}
						else
						{
							//Vessel is probably orbiting the sun
							return orbitRenderer.driver.orbit.getTruePositionAtUT(time);
						}

					}
				}
				else
					return localPosition;
            }
        }

        public Vector3 worldVelocity
        {
            private set;
            get;
        }

        public CelestialBody mainBody
        {
           private set;
           get;
        }

        public GameObject gameObj
        {
            private set;
            get;
        }

        public LineRenderer line
        {
            private set;
            get;
        }
		
        public OrbitRenderer orbitRenderer
        {
            private set;
            get;
        }

		public Color activeColor
		{
			private set;
			get;
		}

		public bool orbitValid
		{
			private set;
			get;
		}

        public bool shouldShowOrbit
        {
            get
            {
				if (!orbitValid || situationIsGrounded(info.situation))
					return false;
				else
					return info.state == State.ACTIVE || orbitRenderer.mouseOver;
            }
        }

		public double referenceUT
		{
			private set;
			get;
		}

		public double referenceFixedTime
		{
			private set;
			get;
		}

		public double adjustedUT
		{
			get
			{
				return referenceUT + (UnityEngine.Time.fixedTime - referenceFixedTime) * info.timeScale;
			}
		}
		
		public Vessel vesselRef
		{
			set;
            get;
		}
		
        //Methods

        public KMPVessel(String vessel_name, String owner_name, Guid _id)
        {
			info = new KMPVesselInfo();

			vesselName = vessel_name;
			ownerName = owner_name;
			id = _id;

			//Build the name of the game object
			System.Text.StringBuilder sb = new StringBuilder();
			sb.Append(vesselName);
//			sb.Append(" (");
//			sb.Append(ownerName);
//			sb.Append(')');

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

            //orbitRenderer.forceDraw = true;

            mainBody = null;

            localDirection = Vector3.zero;
            localVelocity = Vector3.zero;
            localPosition = Vector3.zero;

            worldDirection = Vector3.zero;
            worldVelocity = Vector3.zero;

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

        public void setOrbitalData(CelestialBody body, Vector3 local_pos, Vector3 local_vel, Vector3 local_dir) {

            mainBody = body;

			if (mainBody != null)
            {
                localPosition = local_pos;
                translationFromBody = mainBody.transform.TransformPoint(localPosition) - mainBody.transform.position;
                localDirection = local_dir;
                localVelocity = local_vel;

				orbitValid = true;

				//Check for invalid values in the physics data
				if (!situationIsGrounded(info.situation)
					&& ((localPosition.x == 0.0f && localPosition.y == 0.0f && localPosition.z == 0.0f)
						|| (localVelocity.x == 0.0f && localVelocity.y == 0.0f && localVelocity.z == 0.0f)
						|| localPosition.magnitude > mainBody.sphereOfInfluence)
					)
				{
					orbitValid = false;
				}

				for (int i = 0; i < 3; i++)
				{
					if (float.IsNaN(localPosition[i]) || float.IsInfinity(localPosition[i]))
					{
						orbitValid = false;
						break;
					}

					if (float.IsNaN(localDirection[i]) || float.IsInfinity(localDirection[i]))
					{
						orbitValid = false;
						break;
					}

					if (float.IsNaN(localVelocity[i]) || float.IsInfinity(localVelocity[i]))
					{
						orbitValid = false;
						break;
					}
				}

				if (!orbitValid)
				{
                    //Log.Debug("Orbit invalid: " + vesselName);
					//Spoof some values so the game doesn't freak out
					localPosition = new Vector3(1000.0f, 1000.0f, 1000.0f);
					translationFromBody = localPosition;
					localDirection = new Vector3(1.0f, 0.0f, 0.0f);
					localVelocity = new Vector3(1.0f, 0.0f, 0.0f);
				}

				//Calculate world-space properties
				worldDirection = mainBody.transform.TransformDirection(localDirection);
				worldVelocity = mainBody.transform.TransformDirection(localVelocity);

				//Update game object transform
				updateOrbitProperties();
				updatePosition();

            }

        }

        public void updatePosition()
        {
			if (!orbitValid)
				return;

            gameObj.transform.localPosition = worldPosition;

            Vector3 scaled_pos = ScaledSpace.LocalToScaledSpace(worldPosition);

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

            line.SetPosition(0, ScaledSpace.LocalToScaledSpace(worldPosition - line_half_dir));
            line.SetPosition(1, ScaledSpace.LocalToScaledSpace(worldPosition + line_half_dir));

			if (!situationIsGrounded(info.situation))
				orbitRenderer.driver.orbit.UpdateFromUT(adjustedUT);
        }

        public void updateOrbitProperties()
        {

			if (mainBody != null)
            {
				
                Vector3 orbit_pos = translationFromBody;
                Vector3 orbit_vel = worldVelocity;
				
                //Swap the y and z values of the orbital position/velocities because that's the way it goes?
                float temp = orbit_pos.y;
                orbit_pos.y = orbit_pos.z;
                orbit_pos.z = temp;
				
                temp = orbit_vel.y;
                orbit_vel.y = orbit_vel.z;
                orbit_vel.z = temp;
				
                //Update orbit
                orbitRenderer.driver.orbit.UpdateFromStateVectors(orbit_pos, orbit_vel, mainBody, Planetarium.GetUniversalTime());
				
				referenceUT = Planetarium.GetUniversalTime();
				referenceFixedTime = UnityEngine.Time.fixedTime;
                
            }
        }

        public void updateRenderProperties(bool force_hide = false)
        {
			line.enabled = !force_hide && orbitValid && gameObj != null && MapView.MapIsEnabled;

			if (gameObj != null && !force_hide && shouldShowOrbit)
				orbitRenderer.drawMode = OrbitRenderer.DrawMode.REDRAW_AND_RECALCULATE;
			else
				orbitRenderer.drawMode = OrbitRenderer.DrawMode.OFF;

			//Determine the color
			Color color = activeColor;

			if (orbitRenderer.mouseOver)
				color = Color.white; //Change line color when moused over
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
			orbitRenderer.orbitColor = color * 0.5f;

			if (force_hide || !orbitValid)
				orbitRenderer.drawIcons = OrbitRenderer.DrawIcons.NONE;
			else if (info.state == State.ACTIVE && shouldShowOrbit)
				orbitRenderer.drawIcons = OrbitRenderer.DrawIcons.OBJ_PE_AP;
			else
				orbitRenderer.drawIcons = OrbitRenderer.DrawIcons.OBJ;


        }

		public static bool situationIsGrounded(Situation situation) {

			switch (situation) {

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
		
		
		public static bool situationIsOrbital(Situation situation) {

			switch (situation) {

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
