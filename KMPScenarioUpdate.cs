using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;
using System.Runtime.Serialization;

namespace KMP
{

    [Serializable()]
    public class KMPScenarioUpdate
    {
		/// <summary>
		/// The Scenario name
		/// </summary>
        public String name;

		public ConfigNode scenarioNode = null;
		public double tick = 0d;
		
		public KMPScenarioUpdate(string _name, ScenarioModule _module = null, double _tick = 0d)
        {
			scenarioNode = new ConfigNode();
			if (_module != null) _module.Save(scenarioNode);
			tick = _tick;
			name = _name;
        }
		
		public ConfigNode getScenarioNode()
		{
            return scenarioNode;
		}
		
		public void setScenarioNode(ConfigNode node)
		{
			scenarioNode = node;
		}
    }

}
