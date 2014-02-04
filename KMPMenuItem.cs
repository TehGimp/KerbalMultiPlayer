using System.Collections.Generic;
using UnityEngine;

namespace KMP {
	[KSPAddon(KSPAddon.Startup.MainMenu, false)] //Start at mainmenu, every time.
	public class KMPMenuItem : MonoBehaviour {
		private MainMenu mainMenu;
		public void Awake() {
			Log.Debug("MenuItemLoader is awake.");
			GameEvents.onGameSceneLoadRequested.Add(GameSceneLoadRequested);

			//Grab the MainMenu object.
			mainMenu = (MainMenu)FindObjectOfType(typeof(MainMenu));
			//Clone a menu item and change it for our needs.
			GameObject clone = (GameObject)GameObject.Instantiate(mainMenu.scenariosBtn.gameObject);
			clone.name = "KMP MenuItem";
			clone.transform.SetParent(mainMenu.scenariosBtn.transform.parent);
			clone.transform.localPosition = new Vector3(-0.2621014f, -0.1907118f, 1.004246f);
			clone.transform.localRotation = mainMenu.scenariosBtn.transform.localRotation;
			clone.transform.localScale = mainMenu.scenariosBtn.transform.localScale;
			clone.GetComponent<TextMesh>().text = "KerbalMultiPlayer";
			//Add our renderer to envLogic's list of renderers, for the fade in effect.
			List<MeshRenderer> renderers = new List<MeshRenderer>(mainMenu.envLogic.uiRenderers);
			renderers.Add(clone.GetComponent<MeshRenderer>());
			mainMenu.envLogic.uiRenderers = renderers.ToArray();
			//Get the TextButton3D component and set a onPressed callback
			TextButton3D multiPlayerBtn = clone.GetComponent<TextButton3D>();
			multiPlayerBtn.onPressed = new Callback(KMPButtonPressed);
			
			//Make a new MenuStage to move the camera.
			GameObject stage3 = new GameObject("stage3");
			stage3.transform.position = new Vector3(18, 0, 4);
			GameObject stage3camstart = new GameObject("stage3camstart");
			stage3camstart.transform.position = new Vector3(19, 0, 5);
			MainMenuEnvLogic.MenuStage stage = new MainMenuEnvLogic.MenuStage();
			stage.initialPoint = stage3camstart.transform;
			stage.targetPoint = stage3.transform;
			
			//Add our new stage to the menu stages
			List<MainMenuEnvLogic.MenuStage> stages = new List<MainMenuEnvLogic.MenuStage>(mainMenu.envLogic.camPivots);
			if (stages.Count < 3) {
				stages.Add(stage);
			}
			mainMenu.envLogic.camPivots = stages.ToArray();
		}
		private void GameSceneLoadRequested(GameScenes scene) {
			if (scene != GameScenes.MAINMENU) {
				GameEvents.onGameSceneLoadRequested.Remove(GameSceneLoadRequested);
				mainMenu.envLogic.GoToStage(1);
			}
		}
		private void Start() {
			if (KMPManager.showConnectionWindow) {
				mainMenu.envLogic.GoToStage(2);
			}
		}
		private void KMPButtonPressed() {
			mainMenu.envLogic.GoToStage(2); //our new stage.
			Invoke("ShowKMPWindow", 1f); //Show the window in 2 seconds to wait for the camera to move.
		}
		private void ShowKMPWindow() {
			KMPManager.showConnectionWindow = true;
		}
		
	}
}
