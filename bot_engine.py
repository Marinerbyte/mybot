import threading
import websocket
import json
import time
import logging
import queue
import traceback
import sys

# --- Logging Setup for bot_engine.py ---
engine_logger = logging.getLogger('bot_engine')
engine_logger.setLevel(logging.INFO) # Default level
# ----------------------------------------

class HowdiesBotEngine:
    def __init__(self, session_token, bot_id, default_room_name, master_admin_username, db_manager, ui_log_queue, bot_status_event):
        self._session_token = session_token
        self._bot_id = bot_id # Bot's own user ID, received from login
        self._bot_username = os.getenv("BOT_ID") # Bot's username from .env
        self._default_room_name = default_room_name
        self._master_admin_username = master_admin_username
        self._db_manager = db_manager # Database manager instance
        self._ui_log_queue = ui_log_queue # Queue for real-time UI logs
        self._bot_status_event = bot_status_event # Event to signal UI about bot status

        self._ws_app = None
        self._ws_connected = False
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5 # Example

        self._event_listeners = {} # { "event_name": [callback1, callback2] }
        self._user_map = {} # { "username_lower": "user_id" }
        self._joined_rooms = {} # { "room_id": "room_name" }

        self._locks = {} # { "key": threading.Lock() }
        self._event_queue = queue.Queue() # For processing events sequentially if needed
        self._bot_running = True # Control bot's main loop

        self._log_to_ui("INFO", "engine", "Bot Engine initialized.")
        self._update_bot_status("Initialized")

    # --- Session Management ---
    def get_session(self):
        """Returns bot's session details."""
        return {
            "token": self._session_token,
            "bot_id": self._bot_id,
            "bot_username": self._bot_username,
            "default_room_name": self._default_room_name
        }

    # --- Shared Access Layer ---
    def get_db(self):
        """Returns the database manager instance."""
        return self._db_manager

    def get_master_admin(self):
        """Returns the username of the master admin."""
        return self._master_admin_username

    def resolve_user(self, username):
        """Resolves a username to a user ID from the internal map."""
        return self._user_map.get(username.lower())
    
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
            # Execute plugins in a sandboxed manner
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
            plugin_name = callback.__module__ # Get plugin name
            error_message = f"Plugin '{plugin_name}' crashed during event '{event_name}': {e}\n{traceback.format_exc()}"
            engine_logger.error(error_message)
            self._log_to_ui("ERROR", plugin_name, error_message, full_json=None)

    # --- WebSocket Management ---
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
        self._ws_app.run_forever(ping_interval=20, ping_timeout=10, reconnect=self._reconnect_on_error) # Auto-reconnect support

    def _reconnect_on_error(self, ws, exc):
        if not self._bot_running:
            engine_logger.info("Bot shutting down, not attempting to reconnect.")
            return False # Do not reconnect if bot is stopping
        
        self._reconnect_attempts += 1
        if self._reconnect_attempts <= self._max_reconnect_attempts:
            delay = min(30, 2 ** self._reconnect_attempts) # Exponential backoff
            engine_logger.warning(f"WebSocket connection error: {exc}. Attempting to reconnect in {delay} seconds (Attempt {self._reconnect_attempts}/{self._max_reconnect_attempts}).")
            self._log_to_ui("WARNING", "engine", f"WS error, reconnecting in {delay}s...", full_json=str(exc))
            time.sleep(delay)
            self._update_bot_status(f"Reconnecting (Attempt {self._reconnect_attempts})")
            return True # Attempt to reconnect
        else:
            engine_logger.critical("Max reconnect attempts reached. Stopping bot engine.")
            self._log_to_ui("CRITICAL", "engine", "Max reconnect attempts reached. Stopping bot.")
            self._bot_running = False # Stop the bot
            self._update_bot_status("Failed to Connect")
            return False # Do not reconnect

    def _on_open(self, ws):
        engine_logger.info("WebSocket connection opened.")
        self._log_to_ui("INFO", "engine", "WebSocket connection opened.")
        self._ws_connected = True
        self._reconnect_attempts = 0
        self._update_bot_status("Connected")
        # Send initial login payload (though app.py handles initial Howdies API login)
        # This is for Howdies WebSocket protocol to register the session
        self.send_payload({"handler": "login", "username": self._bot_username, "password": os.getenv("BOT_PASSWORD")})

    def _on_message(self, ws, message):
        try:
            payload = json.loads(message)
            handler = payload.get("handler")
            # engine_logger.debug(f"Received handler: {handler}")
            self._log_to_ui("EVENT_IN", "websocket", "Incoming Payload", full_json=payload)

            # Update internal user_map and room_map
            self._update_internal_state(payload)

            # Emit generic event for handler
            self.emit(f"event:{handler}", payload)

        except json.JSONDecodeError:
            engine_logger.error(f"Failed to decode WebSocket message as JSON: {message}")
            self._log_to_ui("ERROR", "websocket", "Failed to decode JSON", full_json=message)
        except Exception as e:
            engine_logger.error(f"Error processing WebSocket message: {e}\n{traceback.format_exc()}")
            self._log_to_ui("ERROR", "websocket", "Error processing message", full_json=message)

    def _on_error(self, ws, error):
        engine_logger.error(f"WebSocket Error: {error}")
        self._log_to_ui("ERROR", "websocket", "WebSocket Error", full_json=str(error))

    def _on_close(self, ws, close_status_code, close_msg):
        engine_logger.warning(f"WebSocket connection closed. Code: {close_status_code}, Message: {close_msg}")
        self._log_to_ui("WARNING", "engine", f"WebSocket closed. Code: {close_status_code}", full_json={"code": close_status_code, "message": close_msg})
        self._ws_connected = False
        self._update_bot_status("Disconnected")

        # The run_forever with reconnect=True should handle reconnections automatically.
        # If run_forever exits, it means max reconnect attempts were reached or _bot_running is False.

    def send_payload(self, payload):
        """Sends a payload over the WebSocket connection."""
        if self._ws_connected and self._ws_app:
            try:
                self._ws_app.send(json.dumps(payload))
                # engine_logger.debug(f"Sent handler: {payload.get('handler')}")
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

    def _update_internal_state(self, payload):
        """Updates user_map and joined_rooms based on incoming payloads."""
        handler = payload.get("handler")

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
                if uname and uid:
                    self.lock("user_map")
                    self._user_map[uname.lower()] = uid
                    self.unlock("user_map")
                    # engine_logger.debug(f"User map updated: {uname}={uid}")

        # Update joined_rooms
        if handler == "joinchatroom":
            room_id = payload.get("roomid")
            room_name = payload.get("name") # Often 'name' is in the initial join request
            if room_id and room_name:
                self.lock("room_map")
                self._joined_rooms[room_id] = room_name
                self.unlock("room_map")
                engine_logger.info(f"Joined room: {room_name} ({room_id})")
        
        # Store bot's own ID from login confirmation if not set already
        if handler == "login" and not self._bot_id:
            self._bot_id = payload.get("userID") or payload.get("userid") or payload.get("id")
            if self._bot_id:
                engine_logger.info(f"Bot's internal user ID set to: {self._bot_id}")

    # --- Bot Lifecycle Control ---
    def run(self):
        """Starts the WebSocket connection and event processing loop."""
        engine_logger.info("Bot Engine main loop starting...")
        self._update_bot_status("Starting")

        while self._bot_running:
            try:
                self._ws_connect() # This blocks until connection closes or app stops
                if not self._bot_running: # Check if bot was stopped during ws_connect loop
                    break
                engine_logger.info("WebSocket connection loop ended. Checking bot_running flag.")
            except Exception as e:
                engine_logger.critical(f"Unhandled error in Bot Engine run loop: {e}\n{traceback.format_exc()}")
                self._log_to_ui("CRITICAL", "engine", "Unhandled error in run loop", full_json=str(e))
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
        self._bot_running = False
        if self._ws_app:
            try:
                # This might not close immediately if run_forever is blocking,
                # but it ensures no more reconnects and eventually the loop will terminate.
                self._ws_app.close()
            except Exception as e:
                engine_logger.error(f"Error closing WebSocket app: {e}")
        self._update_bot_status("Stopping")

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
            "full_json": full_json
        }
        try:
            self._ui_log_queue.put_nowait(log_entry)
        except queue.Full:
            engine_logger.warning("UI log queue is full, dropping log entry.")
        
    def _update_bot_status(self, status_message):
        """Updates the bot's status and signals the UI."""
        self.lock("bot_status")
        self._current_status = status_message
        self.unlock("bot_status")
        self._bot_status_event.set() # Signal to UI that status might have changed
        self._bot_status_event.clear() # Clear it after signalling

    def get_current_status(self):
        """Returns the bot's current status for UI."""
        self.lock("bot_status")
        status = getattr(self, '_current_status', 'Unknown')
        self.unlock("bot_status")
        return status
