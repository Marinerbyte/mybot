import threading
import websocket
import json
import time
import logging
import queue
import traceback
import sys
import os # For os.getenv

# --- Logging Setup for bot_engine.py ---
engine_logger = logging.getLogger('bot_engine')
engine_logger.setLevel(logging.INFO) # Default level
# ----------------------------------------

class HowdiesBotEngine:
    def __init__(self, session_token, bot_id, default_room_name, master_admin_username, db_manager, ui_log_queue, bot_status_event):
        self._session_token = session_token
        self._bot_id = bot_id # Bot's own user ID, received from login confirmation
        self._bot_username = os.getenv("BOT_ID") # Bot's username from .env
        self._default_room_name = default_room_name
        self._master_admin_username = master_admin_username
        self._db_manager = db_manager # Database manager instance
        self._ui_log_queue = ui_log_queue # Queue for real-time UI logs
        self._bot_status_event = bot_status_event # Event to signal UI about bot status

        self._ws_app = None
        self._ws_connected = False
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5 # Example: Max reconnect attempts

        self._event_listeners = {} # { "event_name": [callback1, callback2] }
        # _user_map: { "username_lower": {"userid": uid, "avatar": "url", "username": "OriginalName"} }
        self._user_map = {} 
        self._joined_rooms = {} # { "room_id": {"name": "room_name", "type": "public"} }

        self._locks = {} # { "key": threading.Lock() }
        self._bot_running = True # Control bot's main loop

        self._log_to_ui("INFO", "engine", "Bot Engine initialized.")
        self._update_bot_status("Initialized")

    # --- Session & Identity Management ---
    def get_session(self):
        """Returns bot's session details."""
        return {
            "token": self._session_token,
            "bot_id": self._bot_id,
            "bot_username": self._bot_username,
        }

    def _get_gid(self):
        """Generates a short unique ID for payloads."""
        return str(uuid.uuid4())[:8]

    # --- Shared Access Layer ---
    def get_db(self):
        """Returns the database manager instance."""
        return self._db_manager

    def get_master_admin(self):
        """Returns the username of the master admin."""
        return self._master_admin_username

    def get_user_info(self, user_id=None, username=None):
        """
        Resolves a user's ID or username to their full info dict.
        Returns {"userid": id, "username": name, "avatar": url} or None.
        """
        self.lock("user_map")
        try:
            if username:
                return self._user_map.get(username.lower())
            elif user_id:
                # Iterate to find user by ID (less efficient, optimize if needed)
                for user_info in self._user_map.values():
                    if str(user_info.get("userid")) == str(user_id):
                        return user_info
            return None
        finally:
            self.unlock("user_map")
    
    def get_room_info(self, room_id=None):
        """
        Returns info for a specific room or the default room if room_id is None.
        Returns {"name": "room_name", "id": "room_id", ...} or None.
        """
        self.lock("room_map")
        try:
            if room_id:
                return self._joined_rooms.get(room_id)
            else: # Return default room info if available
                for r_id, r_info in self._joined_rooms.items():
                    if r_info.get("name").lower() == self._default_room_name.lower():
                        return r_info
                return None
        finally:
            self.unlock("room_map")

    # --- Thread Safety ---
    def lock(self, key):
        """Acquires a lock for a given key."""
        if key not in self._locks:
            with threading.Lock(): # Protect _locks dict modification
                if key not in self._locks: # Double-check after acquiring lock
                    self._locks[key] = threading.Lock()
        self._locks[key].acquire()

    def unlock(self, key):
        """Releases a lock for a given key."""
        if key in self._locks:
            self._locks[key].release()
        else:
            engine_logger.warning(f"Attempted to unlock non-existent lock: {key}")

    # --- Event Dispatcher ---
    def emit(self, event_name, *args, **kwargs):
        """Emits an event, triggering all registered callbacks."""
        # engine_logger.debug(f"Emitting event: {event_name}")
        self._log_to_ui("EVENT", "engine", {"event": event_name, "args": args, "kwargs": kwargs})

        for callback in self._event_listeners.get(event_name, []):
            # Execute plugins in a sandboxed manner in separate threads
            threading.Thread(target=self._execute_plugin_callback, args=(callback, event_name, args, kwargs), daemon=True).start()

    def on(self, event_name, callback):
        """Registers a callback for a specific event."""
        if event_name not in self._event_listeners:
            self._event_listeners[event_name] = []
        self._event_listeners[event_name].append(callback)
        engine_logger.debug(f"Registered callback for event: {event_name}")

    def _execute_plugin_callback(self, callback, event_name, args, kwargs):
        """Sandboxes plugin execution to prevent crashes."""
        try:
            callback(self, *args, **kwargs) # Pass engine instance as first arg
        except Exception as e:
            plugin_name = getattr(callback, '__module__', 'unknown_plugin') # Get plugin name
            error_message = f"Plugin '{plugin_name}' crashed during event '{event_name}': {e}\n{traceback.format_exc()}"
            engine_logger.error(error_message)
            # Log error to UI, ensuring full_json is serializable
            self._log_to_ui("ERROR", plugin_name, error_message.splitlines()[0], full_json={"error": str(e), "traceback": traceback.format_exc()})

    # --- Howdies API & WebSocket Communication ---
    def _upload_image(self, img_bytes, file_type='jpg'):
        """Uploads image bytes to Howdies server, returns URL. Used by plugins."""
        try:
            mime = 'image/gif' if file_type == 'gif' else 'image/jpeg'
            fname = f'file.{file_type}'
            files = {'file': (fname, img_bytes, mime)}
            payload = {'UserID': self._bot_id, 'token': self._session_token, 'uploadType': 'image'}
            r = requests.post("https://api.howdies.app/api/upload", files=files, data=payload).json()
            return r.get("url")
        except Exception as e:
            engine_logger.error(f"Error uploading image to Howdies: {e}")
            return None

    def send_payload(self, payload):
        """Sends a payload over the WebSocket connection."""
        if self._ws_connected and self._ws_app:
            try:
                self._ws_app.send(json.dumps(payload))
                self._log_to_ui("EVENT_OUT", "websocket", "Outgoing Payload", full_json=payload)
                time.sleep(0.1) # Small delay to prevent flooding
                return True
            except Exception as e:
                engine_logger.error(f"Failed to send WebSocket payload: {e}")
                self._log_to_ui("ERROR", "engine", "Failed to send payload", full_json=payload)
                return False
        else:
            engine_logger.warning("Attempted to send payload but WebSocket is not connected.")
            self._log_to_ui("WARNING", "engine", "Payload not sent, WS disconnected", full_json=payload)
            return False

    def send_text_message(self, target_id_or_username, text, is_dm=False, room_id=None):
        """
        Sends a text message.
        target_id_or_username can be a user ID (for DM) or a room ID/name (for room message).
        If is_dm=True, target_id_or_username must be recipient's username.
        If is_dm=False, it tries to send to room_id or default room.
        """
        payload = {"id": self._get_gid(), "type": "text", "text": text}
        if is_dm:
            payload["handler"] = "message"
            payload["to"] = target_id_or_username # Expecting username for DM
        else:
            payload["handler"] = "chatroommessage"
            room_to_send_to = room_id
            if not room_to_send_to: # Use default room if none specified
                default_room_info = self.get_room_info(room_id=None) # Gets default room by name
                if default_room_info:
                    room_to_send_to = default_room_info.get("id")
            
            if not room_to_send_to:
                engine_logger.error("Cannot send room message: No room_id provided and default room not found/joined.")
                self._log_to_ui("ERROR", "engine", "Failed to send room message: no target room.", full_json={"text":text})
                return False
            payload["roomid"] = room_to_send_to
        
        return self.send_payload(payload)

    def send_image_message(self, target_id_or_username, url, caption, is_dm=False, room_id=None):
        """
        Sends an image message.
        target_id_or_username can be a user ID (for DM) or a room ID/name (for room message).
        If is_dm=True, target_id_or_username must be recipient's username.
        If is_dm=False, it tries to send to room_id or default room.
        """
        payload = {"id": self._get_gid(), "type": "image", "text": caption, "url": url}
        if is_dm:
            payload["handler"] = "message"
            payload["to"] = target_id_or_username # Expecting username for DM
        else:
            payload["handler"] = "chatroommessage"
            room_to_send_to = room_id
            if not room_to_send_to: # Use default room if none specified
                default_room_info = self.get_room_info(room_id=None)
                if default_room_info:
                    room_to_send_to = default_room_info.get("id")
            
            if not room_to_send_to:
                engine_logger.error("Cannot send room image: No room_id provided and default room not found/joined.")
                self._log_to_ui("ERROR", "engine", "Failed to send room image: no target room.", full_json={"url":url, "caption":caption})
                return False
            payload["roomid"] = room_to_send_to
        
        return self.send_payload(payload)


    # --- WebSocket Event Handlers ---
    def _ws_connect(self):
        """Establishes WebSocket connection."""
        websocket_url = f"wss://app.howdies.app/howdies?token={self._session_token}"
        engine_logger.info(f"Connecting to WebSocket at {websocket_url[:30]}...")
        self._ws_app = websocket.WebSocketApp(
            websocket_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        self._ws_connected = False
        self._reconnect_attempts = 0
        self._update_bot_status("Connecting")
        # run_forever handles auto-reconnect logic itself
        self._ws_app.run_forever(ping_interval=20, ping_timeout=10, reconnect=True) 

    def _on_open(self, ws):
        engine_logger.info("WebSocket connection opened.")
        self._log_to_ui("INFO", "engine", "WebSocket connection opened.")
        self._ws_connected = True
        self._reconnect_attempts = 0
        self._update_bot_status("Connected")
        # Send initial login payload to Howdies WebSocket protocol
        self.send_payload({"handler": "login", "username": self._bot_username, "password": os.getenv("BOT_PASSWORD")})

    def _on_message(self, ws, message):
        try:
            payload = json.loads(message)
            handler = payload.get("handler")
            self._log_to_ui("EVENT_IN", "websocket", "Incoming Payload", full_json=payload)

            # Update internal user_map and room_map FIRST
            self._update_internal_state(payload)

            # Emit generic event for handler
            self.emit(f"event:{handler}", payload)

        except json.JSONDecodeError:
            engine_logger.error(f"Failed to decode WebSocket message as JSON: {message}")
            self._log_to_ui("ERROR", "websocket", "Failed to decode JSON", full_json=message)
        except Exception as e:
            engine_logger.error(f"Error processing WebSocket message: {e}\n{traceback.format_exc()}")
            self._log_to_ui("ERROR", "websocket", "Error processing message", full_json={"error": str(e), "message": message, "traceback": traceback.format_exc()})

    def _on_error(self, ws, error):
        engine_logger.error(f"WebSocket Error: {error}")
        self._log_to_ui("ERROR", "websocket", "WebSocket Error", full_json=str(error))

    def _on_close(self, ws, close_status_code, close_msg):
        engine_logger.warning(f"WebSocket connection closed. Code: {close_status_code}, Message: {close_msg}")
        self._log_to_ui("WARNING", "engine", f"WebSocket closed. Code: {close_status_code}", full_json={"code": close_status_code, "message": close_msg})
        self._ws_connected = False
        self._update_bot_status("Disconnected")
        
        # If run_forever is set to reconnect=True, it will handle reconnection itself.
        # This method is purely for logging the closure.

    def _update_internal_state(self, payload):
        """Updates user_map and joined_rooms based on incoming payloads."""
        handler = payload.get("handler")

        # Update bot's own ID from login confirmation
        if handler == "login" and payload.get("success"):
            received_bot_id = payload.get("userID") or payload.get("userid") or payload.get("id")
            if received_bot_id and not self._bot_id: # Only set if not already set from app.py
                self._bot_id = received_bot_id
                engine_logger.info(f"Bot's internal user ID set to: {self._bot_id}")
            self.send_payload({"handler": "joinchatroom", "id": self._get_gid(), "name": self._default_room_name, "roomPassword": ""})

        # Update user_map
        if handler in ["activeoccupants", "getusers", "userjoin", "profile"]:
            users_data = []
            if handler in ["activeoccupants", "getusers"] and "users" in payload:
                users_data = payload["users"]
            elif handler == "userjoin":
                users_data = [payload] # payload itself is the user data
            elif handler == "profile" and "user" in payload:
                users_data = [payload["user"]] # payload["user"] is the user data
            
            for user_info in users_data:
                uname = user_info.get("username")
                uid = user_info.get("userID") or user_info.get("userid") or user_info.get("id")
                avatar = user_info.get("avatar")
                if uname and uid:
                    self.lock("user_map")
                    self._user_map[uname.lower()] = {"userid": uid, "username": uname, "avatar": avatar}
                    self.unlock("user_map")
                    # engine_logger.debug(f"User map updated: {uname}={uid}")

        # Update joined_rooms
        if handler == "joinchatroom" and payload.get("success"):
            room_id = payload.get("roomid")
            room_name = payload.get("name") or self._default_room_name # Room name might not be in payload if it's a success ACK
            if room_id and room_name:
                self.lock("room_map")
                self._joined_rooms[room_id] = {"id": room_id, "name": room_name}
                self.unlock("room_map")
                engine_logger.info(f"Joined room: {room_name} ({room_id})")
        elif handler == "leavechatroom" and payload.get("success"):
            room_id = payload.get("roomid")
            if room_id in self._joined_rooms:
                self.lock("room_map")
                del self._joined_rooms[room_id]
                self.unlock("room_map")
                engine_logger.info(f"Left room: {room_id}")

    # --- Bot Lifecycle Control ---
    def run(self):
        """Starts the WebSocket connection and event processing loop."""
        engine_logger.info("Bot Engine main loop starting...")
        self._update_bot_status("Starting")

        while self._bot_running:
            try:
                self._ws_connect() # This blocks until connection closes or app stops
                # If ws_connect returns (e.g. max reconnects reached or explicitly closed)
                if not self._bot_running: 
                    engine_logger.info("Bot was stopped or max reconnects reached. Exiting run loop.")
                    break
                # If _ws_connect exited without stopping, it means a severe error or unexpected close.
                # We'll re-attempt connection after a delay.
                delay = 10 # Short delay before attempting main loop restart
                engine_logger.info(f"WebSocket loop ended unexpectedly. Retrying in {delay} seconds...")
                self._log_to_ui("WARNING", "engine", "WS loop ended unexpectedly. Retrying...", full_json=None)
                time.sleep(delay)
            except Exception as e:
                engine_logger.critical(f"Unhandled error in Bot Engine run loop: {e}\n{traceback.format_exc()}")
                self._log_to_ui("CRITICAL", "engine", "Unhandled error in run loop", full_json={"error": str(e), "traceback": traceback.format_exc()})
                if self._bot_running: # Only attempt reconnect if still supposed to be running
                    delay = 30 # Long delay before retrying outer loop
                    engine_logger.info(f"Retrying Bot Engine run loop in {delay} seconds...")
                    time.sleep(delay)
        engine_logger.info("Bot Engine main loop stopped.")
        self._update_bot_status("Stopped")


    def stop(self):
        """Stops the bot engine gracefully."""
        engine_logger.info("Stopping Bot Engine...")
        self._log_to_ui("INFO", "engine", "Stopping Bot Engine initiated.")
        if self._bot_running: # Only set to False if it's currently running
            self._bot_running = False
            if self._ws_app:
                try:
                    # Closing WebSocket will cause run_forever to exit
                    self._ws_app.close()
                except Exception as e:
                    engine_logger.error(f"Error closing WebSocket app: {e}")
            self._update_bot_status("Stopping")
        else:
            engine_logger.info("Bot Engine is already stopped or stopping.")
            self._log_to_ui("INFO", "engine", "Bot Engine is already stopped or stopping.", full_json=None)

    def clean_logout(self):
        """Performs a clean logout from Howdies API (if supported) and stops."""
        engine_logger.info("Performing clean logout...")
        self._log_to_ui("INFO", "engine", "Clean logout initiated.")
        self.stop()
        # Add actual Howdies API logout logic here if available
        # requests.post("https://api.howdies.app/api/logout", headers={"Authorization": f"Bearer {self._session_token}"})
        engine_logger.info("Bot Engine gracefully shut down.")

    # --- UI Logging Integration ---
    def _log_to_ui(self, log_type, source, message, full_json=None):
        """Sends log entries to the UI's log queue."""
        log_entry = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "source": source,
            "type": log_type,
            "message": message,
            "full_json": full_json # Ensure this is serializable
        }
        try:
            self._ui_log_queue.put_nowait(log_entry)
        except queue.Full:
            engine_logger.warning("UI log queue is full, dropping log entry.")
        
    def _update_bot_status(self, status_message):
        """Updates the bot's status and signals the UI
