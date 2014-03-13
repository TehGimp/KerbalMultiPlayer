//
//  ToolbarWrapper.cs - Borrwed directly from https://github.com/pizzaoverhead/Chatterer/blob/master/Chatterer/ToolbarButtonWrapper.cs
//
//  Author:
//       toadicus <>
//
//  Copyright (c) 2013 toadicus
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.
using System;
using System.Linq;
using System.Reflection;
using UnityEngine;

namespace KMP
{
    /// <summary>
    /// Wraps a Toolbar clickable button, after fetching it from a foreign assembly.
    /// </summary>
    internal class ToolbarButtonWrapper
    {
        protected static System.Type ToolbarManager;
        protected static object TBManagerInstance;
        protected static MethodInfo TBManagerAdd;

        /// <summary>
        /// Wraps the ToolbarManager class, if present.
        /// </summary>
        /// <returns><c>true</c>, if ToolbarManager is wrapped, <c>false</c> otherwise.</returns>
        protected static bool TryWrapToolbarManager()
        {
            if (ToolbarManager == null)
            {
                Tools.PostDebugMessage(string.Format(
                    "{0}: Loading ToolbarManager.",
                    "ToolbarButtonWrapper"
                    ));

                ToolbarManager = AssemblyLoader.loadedAssemblies
                    .Select(a => a.assembly.GetExportedTypes())
                        .SelectMany(t => t)
                        .FirstOrDefault(t => t.FullName == "Toolbar.ToolbarManager");

                Tools.PostDebugMessage(string.Format(
                    "{0}: Loaded ToolbarManager.  Getting Instance.",
                    "ToolbarButtonWrapper"
                    ));

                if (ToolbarManager == null)
                {
                    return false;
                }

                TBManagerInstance = ToolbarManager.GetProperty(
                    "Instance",
                    System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static
                    )
                    .GetValue(null, null);

                Tools.PostDebugMessage(string.Format(
                    "{0}: Got ToolbarManager Instance '{1}'.  Getting 'add' method.",
                    "ToolbarButtonWrapper",
                    TBManagerInstance
                    ));

                TBManagerAdd = ToolbarManager.GetMethod("add");

                Tools.PostDebugMessage(string.Format(
                    "{0}: Got ToolbarManager Instance 'add' method.  Loading IButton.",
                    "ToolbarButtonWrapper"
                    ));
            }

            return true;
        }

        /// <summary>
        /// Gets a value indicating whether <see cref="Toolbar.ToolbarManager"/> is present.
        /// </summary>
        /// <value><c>true</c>, if ToolbarManager is wrapped, <c>false</c> otherwise.</value>
        public static bool ToolbarManagerPresent
        {
            get
            {
                return TryWrapToolbarManager();
            }
        }

        /// <summary>
        /// If ToolbarManager is present, initializes a new instance of the <see cref="VOID.ToolbarButtonWrapper"/> class.
        /// </summary>
        /// <param name="ns">Namespace, usually the plugin name.</param>
        /// <param name="id">Identifier, unique per namespace.</param>
        /// <returns>If ToolbarManager is present, a new <see cref="Toolbar.IButton"/> object, <c>null</c> otherwise.</returns>
        public static ToolbarButtonWrapper TryWrapToolbarButton(string ns, string id)
        {
            if (ToolbarManagerPresent)
            {
                object button = TBManagerAdd.Invoke(TBManagerInstance, new object[] { ns, id });

                Tools.PostDebugMessage(string.Format(
                    "{0}: Added Button '{1}' with ToolbarManager.  Getting 'Text' property",
                    "ToolbarButtonWrapper",
                    button.ToString()
                ));

                return new ToolbarButtonWrapper(button);
            }
            else
            {
                return null;
            }
        }

        protected System.Type IButton;
        protected object Button;
        protected PropertyInfo ButtonText;
        protected PropertyInfo ButtonTextColor;
        protected PropertyInfo ButtonTexturePath;
        protected PropertyInfo ButtonToolTip;
        protected PropertyInfo ButtonVisible;
        protected PropertyInfo ButtonVisibility;
        protected PropertyInfo ButtonEnalbed;
        protected PropertyInfo ButtonImportant;
        protected EventInfo ButtonOnClick;
        protected EventInfo ButtonOnMouseEnter;
        protected EventInfo ButtonOnMouseLeave;
        protected MethodInfo ButtonDestroy;
        protected System.Type GameScenesVisibilityType;

        /// <summary>
        /// The text displayed on the button. Set to null to hide text.
        /// </summary>
        /// <remarks>
        /// The text can be changed at any time to modify the button's appearance. Note that since this will also
        /// modify the button's size, this feature should be used sparingly, if at all.
        /// </remarks>
        /// <seealso cref="TexturePath"/>
        public string Text
        {
            get
            {
                return this.ButtonText.GetValue(this.Button, null) as String;
            }
            set
            {
                this.ButtonText.SetValue(this.Button, value, null);
            }
        }

        /// <summary>
        /// The color the button text is displayed with. Defaults to Color.white.
        /// </summary>
        /// <remarks>
        /// The text color can be changed at any time to modify the button's appearance.
        /// </remarks>
        public Color TextColor
        {
            get
            {
                return (Color)this.ButtonTextColor.GetValue(this.Button, null);
            }
            set
            {
                this.ButtonTextColor.SetValue(this.Button, value, null);
            }
        }

        /// <summary>
        /// The path of a texture file to display an icon on the button. Set to null to hide icon.
        /// </summary>
        /// <remarks>
        /// <para>
        /// A texture path on a button will have precedence over text. That is, if both text and texture path
        /// have been set on a button, the button will show the texture, not the text.
        /// </para>
        /// <para>
        /// The texture size must not exceed 24x24 pixels.
        /// </para>
        /// <para>
        /// The texture path must be relative to the "GameData" directory, and must not specify a file name suffix.
        /// Valid example: MyAddon/Textures/icon_mybutton
        /// </para>
        /// <para>
        /// The texture path can be changed at any time to modify the button's appearance.
        /// </para>
        /// </remarks>
        /// <seealso cref="Text"/>
        public string TexturePath
        {
            get
            {
                return this.ButtonTexturePath.GetValue(this.Button, null) as string;
            }
            set
            {
                this.ButtonTexturePath.SetValue(this.Button, value, null);
            }
        }

        /// <summary>
        /// The button's tool tip text. Set to null if no tool tip is desired.
        /// </summary>
        /// <remarks>
        /// Tool Tip Text Should Always Use Headline Style Like This.
        /// </remarks>
        public string ToolTip
        {
            get
            {
                return this.ButtonToolTip.GetValue(this.Button, null) as string;
            }
            set
            {
                this.ButtonToolTip.SetValue(this.Button, value, null);
            }
        }

        /// <summary>
        /// Whether this button is currently visible or not. Can be used in addition to or as a replacement for <see cref="Visibility"/>.
        /// </summary>
        public bool Visible
        {
            get
            {
                return (bool)this.ButtonVisible.GetValue(this.Button, null);
            }
            set
            {
                this.ButtonVisible.SetValue(this.Button, value, null);
            }
        }

        /// <summary>
        /// Whether this button is currently enabled (clickable) or not. This will not affect the player's ability to
        /// position the button on their screen.
        /// </summary>
        public bool Enabled
        {
            get
            {
                return (bool)this.ButtonEnalbed.GetValue(this.Button, null);
            }
            set
            {
                this.ButtonEnalbed.SetValue(this.Button, value, null);
            }
        }

        /// <summary>
        /// Whether this button is currently "important."  Set to false to return to normal button behaviour.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This can be used to temporarily force the button to be shown on the screen regardless of the toolbar being
        /// currently in auto-hidden mode.  For example, a button that signals the arrival of a private message in a
        /// chat room could mark itself as "important" as long as the message has not been read.
        /// </para>
        /// <para>
        /// Setting this property does not change the appearance of the button.  use <see cref="TexturePath"/>  to
        /// change the button's icon.
        /// </para>
        /// <para>
        /// This feature should be used only sparingly, if at all, since it forces the button to be displayed on screen
        /// even when it normally wouldn't.
        /// </para>
        /// </remarks>
        /// <value><c>true</c> if important; otherwise, <c>false</c>.</value>
        public bool Important
        {
            get
            {
                return (bool)this.ButtonImportant.GetValue(this.Button, null);
            }
            set
            {
                this.ButtonImportant.SetValue(this.Button, value, null);
            }
        }

        private ToolbarButtonWrapper()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="VOID.ToolbarButtonWrapper"/> class.
        /// </summary>
        /// <param name="ns">Namespace, usually the plugin name.</param>
        /// <param name="id">Identifier, unique per namespace.</param>
        protected ToolbarButtonWrapper(object button)
        {
            this.Button = button;

            this.IButton = AssemblyLoader.loadedAssemblies
                .Select(a => a.assembly.GetExportedTypes())
                .SelectMany(t => t)
                .FirstOrDefault(t => t.FullName == "Toolbar.IButton");

            Tools.PostDebugMessage(string.Format(
                "{0}: Loaded IButton.  Adding Button with ToolbarManager.",
                this.GetType().Name
            ));

            this.ButtonText = this.IButton.GetProperty("Text");

            Tools.PostDebugMessage(string.Format(
                "{0}: Got 'Text' property.  Getting 'TextColor' property.",
                this.GetType().Name
            ));

            this.ButtonTextColor = this.IButton.GetProperty("TextColor");

            Tools.PostDebugMessage(string.Format(
                "{0}: Got 'TextColor' property.  Getting 'TexturePath' property.",
                this.GetType().Name
            ));

            this.ButtonTexturePath = this.IButton.GetProperty("TexturePath");

            Tools.PostDebugMessage(string.Format(
                "{0}: Got 'TexturePath' property.  Getting 'ToolTip' property.",
                this.GetType().Name
            ));

            this.ButtonToolTip = this.IButton.GetProperty("ToolTip");

            Tools.PostDebugMessage(string.Format(
                "{0}: Got 'ToolTip' property.  Getting 'Visible' property.",
                this.GetType().Name
            ));

            this.ButtonVisible = this.IButton.GetProperty("Visible");

            Tools.PostDebugMessage(string.Format(
                "{0}: Got 'Visible' property.  Getting 'Visibility' property.",
                this.GetType().Name
            ));

            this.ButtonVisibility = this.IButton.GetProperty("Visibility");

            Tools.PostDebugMessage(string.Format(
                "{0}: Got 'Visibility' property.  Getting 'Enabled' property.",
                this.GetType().Name
            ));

            this.ButtonEnalbed = this.IButton.GetProperty("Enabled");

            Tools.PostDebugMessage(string.Format(
                "{0}: Got 'Enabled' property.  Getting 'OnClick' event.",
                this.GetType().Name
            ));

            this.ButtonImportant = this.IButton.GetProperty("Important");

            Tools.PostDebugMessage(string.Format(
                "{0}: Got 'Enabled' property.  Getting 'OnClick' event.",
                this.GetType().Name
            ));

            this.ButtonOnClick = this.IButton.GetEvent("OnClick");

            Tools.PostDebugMessage(string.Format(
                "{0}: Got 'OnClick' event '{1}'.  Getting 'OnMouseEnter' event.",
                this.GetType().Name,
                this.ButtonOnClick.ToString()
            ));

            this.ButtonOnMouseEnter = this.IButton.GetEvent("OnMouseEnter");

            Tools.PostDebugMessage(string.Format(
                "{0}: Got 'OnMouseEnter' event '{1}'.  Getting 'OnMouseLeave' event.",
                this.GetType().Name,
                this.ButtonOnClick.ToString()
                ));

            this.ButtonOnMouseLeave = this.IButton.GetEvent("OnMouseLeave");

            Tools.PostDebugMessage(string.Format(
                "{0}: Got 'OnMouseLeave' event '{1}'.  Getting 'Destroy' method.",
                this.GetType().Name,
                this.ButtonOnClick.ToString()
                ));

            this.ButtonDestroy = this.IButton.GetMethod("Destroy");

            Tools.PostDebugMessage(string.Format(
                "{0}: Got 'Destroy' property '{1}'.  Loading GameScenesVisibility class.",
                this.GetType().Name,
                this.ButtonDestroy.ToString()
            ));

            this.GameScenesVisibilityType = AssemblyLoader.loadedAssemblies
                .Select(a => a.assembly.GetExportedTypes())
                    .SelectMany(t => t)
                    .FirstOrDefault(t => t.FullName == "Toolbar.GameScenesVisibility");

            Tools.PostDebugMessage(string.Format(
                "{0}: Got 'GameScenesVisibility' class '{1}'.",
                this.GetType().Name,
                this.GameScenesVisibilityType.ToString()
            ));

            Tools.PostDebugMessage("ToolbarButtonWrapper built!");
        }

        /// <summary>
        /// Adds event handler to receive "on click" events.
        /// </summary>
        /// <example>
        /// <code>
        /// ToolbarButtonWrapper button = ...
        /// button.AddButtonClickHandler(
        /// 	(e) =>
        /// 	{
        /// 		Debug.Log("button clicked, mouseButton: " + e.Mousebutton");
        /// 	}
        /// );
        /// </code>
        /// </example>
        /// <param name="Handler">Delegate to handle "on click" events</param>
        public void AddButtonClickHandler(Action<object> Handler)
        {
            this.AddButtonEventHandler(this.ButtonOnClick, Handler);
        }

        /// <summary>
        /// Adds event handler that can be registered with to receive "on mouse enter" events.
        /// </summary>
        /// <example>
        /// <code>
        /// ToolbarWrapperButton button = ...
        /// button.AddButtonOnMouseEnterHandler(
        /// 	(e) =>
        /// 	{
        /// 		Debug.Log("mouse entered button");
        /// 	}
        /// );
        /// </code>
        /// </example>
        /// <param name="Handler">Delegate to handle "OnMouseEnter" events.</param>
        public void AddButtonOnMouseEnterHandler(Action<object> Handler)
        {
            this.AddButtonEventHandler(this.ButtonOnMouseEnter, Handler);
        }

        /// <summary>
        /// Adds event handler that can be registered with to receive "on mouse leave" events.
        /// </summary>
        /// <example>
        /// <code>
        /// ToolbarWrapperButton button = ...
        /// button.AddButtonOnMouseLeaveHandler(
        /// 	(e) =>
        /// 	{
        /// 		Debug.Log("mouse left button");
        /// 	}
        /// );
        /// </code>
        /// </example>
        /// <param name="Handler">Delegate to handle "OnMouseLeave" events.</param>
        public void AddButtonOnMouseLeaveHandler(Action<object> Handler)
        {
            this.AddButtonEventHandler(this.ButtonOnMouseLeave, Handler);
        }

        /// <summary>
        /// Sets this button's visibility. Can be used in addition to or as a replacement for <see cref="Visible"/>.
        /// </summary>
        /// <param name="gameScenes">Array of GameScene objects in which the button should be visible.</param>
        public void SetButtonVisibility(params GameScenes[] gameScenes)
        {
            object GameScenesVisibilityObj = Activator.CreateInstance(this.GameScenesVisibilityType, gameScenes);
            this.ButtonVisibility.SetValue(this.Button, GameScenesVisibilityObj, null);
        }

        /// <summary>
        /// Permanently destroys this button so that it is no longer displayed.
        /// Should be used when a plugin is stopped to remove leftover buttons.
        /// </summary>
        public void Destroy()
        {
            this.ButtonDestroy.Invoke(this.Button, null);
        }

        // Utility method for use with the AddButton<event>Handler API methods.
        protected void AddButtonEventHandler(EventInfo Event, Action<object> Handler)
        {
            Delegate d = Delegate.CreateDelegate(Event.EventHandlerType, Handler.Target, Handler.Method);
            MethodInfo addHandler = Event.GetAddMethod();
            addHandler.Invoke(this.Button, new object[] { d });
        }
    }

    public static class Tools
    {
        private static ScreenMessage debugmsg = new ScreenMessage("", 2f, ScreenMessageStyle.UPPER_RIGHT);

        [System.Diagnostics.Conditional("DEBUG")]
        public static void PostDebugMessage(string Msg)
        {
            if (HighLogic.LoadedScene > GameScenes.SPACECENTER)
            {
                debugmsg.message = Msg;
                ScreenMessages.PostScreenMessage(debugmsg, true);
            }

            KSPLog.print(Msg);
        }
    }
}