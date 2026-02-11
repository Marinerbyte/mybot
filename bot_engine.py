import threading
import websocket
import json
import time
import logging
import queue
import traceback
import sys
import os
import requests
import uuid

# --- Logging Setup ---
engine_logger = logging.getLogger('bot_engine')
engine_logger.setLevel(logging.INFO)

class HowdiesBotEngine:
    def __init__(self, session_token, bot_id, default_room_name, master_admin_username, db_manager, ui_log_queue, bot_status_event):
        self._session_token = session_token
        self._bot_id = bot_id
        self._bot_username = os.getenv("BOT_ID")
        self._default_room_name = default_room_name
        self._master_admin_username = master_admin_username
        self._db_manager = db_manager
        self._ui_log_queue = ui_log_queue
        self._bot_status_event = bot_status_event

        self._ws_app = None
        self._ws_connected = False
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 100

        self._event_listeners = {}
        self._user_map = {}
        self._joined_rooms = {}
        self._locks = {}
        self._bot_running = True

        self._log_to_ui("INFO", "engine", "Bot Engine initialized.")
        self._update_bot_status("Initialized")

    def get_session(self):
        return {
            "token": self._session_token,
            "bot_id": self._bot_id,
            "bot_username": self._bot_username,
        }

    def _get_gid(self):
        return str(uuid.uuid4())[:8]

    def get_db(self):
        return self._db_manager

    def get_master_admin(self):
        return self._master_admin_username

    def get_user_info(self, user_id=None, username=None):
        self.lock("user_map")
        try:
            if username:
                return self._user_map.get(username.lower())
            elif user_id:
                for user_info in self._user_map.values():
                    if str(user_info.get("userid")) == str(user_id):
                        return user_info
            return None
        finally:
            self.unlock("user_map")
    
    def get_room_info(self, room_id=None):
        self.lock("room_map")
        try:
            if room_id:
                return self._joined_rooms.get(room_id)
            else:
                for r_id, r_info in self._joined_rooms.items():
                    if r_info.get("name", "").lower() == self._default_room_name.lower():
                        return r_info
                if not room_id and self._joined_rooms:
                    return list(self._joined_rooms.values())[0]
                return None
        finally:
            self.unlock("room_map")

    def lock(self, key):
        if key not in self._locks:
            with threading.Lock():
                if key not in self._locks:
                    self._locks[key] = threading.Lock()
        self._locks[key].acquire()

    def unlock(self, key):
        if key in self._locks:
            self._locks[key].release()

    def emit(self, event_name, *args, **kwargs):
        self._log_to_ui("EVENT", "engine", {"event": event_name, "args": args, "kwargs": kwargs})
        for callback in self._event_listeners.get(event_name, []):
            threading.Thread(target=self._execute_plugin_callback, args=(callback, event_name, args, kwargs), daemon=True).start()

    def on(self, event_name, callback):
        if event_name not in self._event_listeners:
            self._event_listeners[event_name] = []
        self._event_listeners[event_name].append(callback)

    def _execute_plugin_callback(self, callback, event_name, args, kwargs):
        try:
            callback(self, *args, **kwargs)
        except Exception as e:
            plugin_name = getattr(callback, '__module__', 'unknown_plugin')
            error_message = f"Plugin '{plugin_name}' crashed: {e}"
            engine_logger.error(error_message)
            self._log_to_ui("ERROR", plugin_name, error_message, full_json={"error": str(e)})

    def _upload_image(self, img_bytes, file_type='jpg'):
        try:
            mime = 'image/gif' if file_type == 'gif' else 'image/jpeg'
            fname = f'file.{file_type}'
            files = {'file': (fname, img_bytes, mime)}
            payload = {'UserID': self._bot_id, 'token': self._session_token, 'uploadType': 'image'}
            r = requests.post("https://api.howdies.app/api/upload", files=files, data=payload).json()
            return r.get("url")
        except Exception as e:
            engine_logger.error(f"Error uploading image: {e}")
            return None

    def send_payload(self, payload):
        if self._ws_connected and self._ws_app:
            try:
                self._ws_app.send(json.dumps(payload))
                self._log_to_ui("EVENT_OUT", "websocket", "Outgoing Payload", full_json=payload)
                time.sleep(0.1)
                return True
            except Exception as e:
                engine_logger.error(f"Failed to send payload: {e}")
                self._log_to_ui("ERROR", "engine", "Failed to send payload", full_json=payload)
                return False
        else:
            self._log_to_ui("WARNING", "engine", "WS disconnected", full_json=payload)
            return False

    def send_text_message(self, target_id_or_username, text, is_dm=False, room_id=None):
        payload = {"id": self._get_gid(), "type": "text", "text": text}
        if is_dm:
            payload["handler"] = "message"
            payload["to"] = target_id_or_username
        else:
            payload["handler"] = "chatroommessage"
            room_to_send_to = room_id
            if not room_to_send_to:
                default_room_info = self.get_room_info(room_id=None)
                if default_room_info:
                    room_to_send_to = default_room_info.get("id")
            
            if not room_to_send_to:
                return False
            payload["roomid"] = room_to_send_to
        return self.send_payload(payload)

    def send_image_message(self, target_id_or_username, url, caption, is_dm=False, room_id=None):
        payload = {"id": self._get_gid(), "type": "image", "text": caption, "url": url}
        if is_dm:
            payload["handler"] = "message"
            payload["to"] = target_id_or_username
        else:
            payload["handler"] = "chatroommessage"
            room_to_send_to = room_id
            if not room_to_send_to:
                default_room_info = self.get_room_info(room_id=None)
                if default_room_info:
                    room_to_send_to = default_room_info.get("id")
            if not room_to_send_to:
                return False
            payload["roomid"] = room_to_send_to
        return self.send_payload(payload)

    def _ws_connect(self):
        websocket_url = f"wss://app.howdies.app/howdies?token={self._session_token}"
        engine_logger.info(f"Connecting to WS...")
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
        self._ws_app.run_forever(ping_interval=20, ping_timeout=10, reconnect=True) 

    def _on_open(self, ws):
        engine_logger.info("WS Opened.")
        self._log_to_ui("INFO", "engine", "WS Opened.")
        self._ws_connected = True
        self._reconnect_attempts = 0
        self._update_bot_status("Connected")
        self.send_payload({"handler": "login", "username": self._bot_username, "password": os.getenv("BOT_PASSWORD")})

    def _on_message(self, ws, message):
        try:
            payload = json.loads(message)
            handler = payload.get("handler")
            self._log_to_ui("EVENT_IN", "websocket", "Incoming Payload", full_json=payload)
            self._update_internal_state(payload)
            self.emit(f"event:{handler}", payload)
        except Exception as e:
            engine_logger.error(f"Error processing WS message: {e}")

    def _on_error(self, ws, error):
        engine_logger.error(f"WS Error: {error}")
        self._log_to_ui("ERROR", "websocket", "WS Error", full_json=str(error))

    def _on_close(self, ws, close_status_code, close_msg):
        engine_logger.warning(f"WS Closed: {close_status_code}")
        self._ws_connected = False
        self._update_bot_status("Disconnected")

    def _update_internal_state(self, payload):
        handler = payload.get("handler")

        if handler == "login" and payload.get("success"):
            received_bot_id = payload.get("userID") or payload.get("userid") or payload.get("id")
            if received_bot_id and not self._bot_id:
                self._bot_id = received_bot_id
            self.send_payload({"handler": "joinchatroom", "id": self._get_gid(), "name": self._default_room_name, "roomPassword": ""})

        if handler in ["activeoccupants", "getusers", "userjoin", "profile"]:
            users_data = []
            if handler in ["activeoccupants", "getusers"] and "users" in payload:
                users_data = payload["users"]
            elif handler == "userjoin":
                users_data = [payload]
            elif handler == "profile" and "user" in payload:
                users_data = [payload["user"]]
            
            for user_info in users_data:
                uname = user_info.get("username")
                uid = user_info.get("userID") or user_info.get("userid") or user_info.get("id")
                avatar = user_info.get("avatar")
                if uname and uid:
                    self.lock("user_map")
                    self._user_map[uname.lower()] = {"userid": uid, "username": uname, "avatar": avatar}
                    self.unlock("user_map")

        if handler == "joinchatroom" and payload.get("success"):
            room_id = payload.get("roomid")
            room_name = payload.get("name") or self._default_room_name
            if room_id:
                self.lock("room_map")
                self._joined_rooms[room_id] = {"id": room_id, "name": room_name}
                self.unlock("room_map")
        elif handler == "leavechatroom" and payload.get("success"):
            room_id = payload.get("roomid")
            if room_id in self._joined_rooms:
                self.lock("room_map")
                del self._joined_rooms[room_id]
                self.unlock("room_map")

    def run(self):
        engine_logger.info("Bot Engine Starting...")
        self._update_bot_status("Starting")
        while self._bot_running:
            try:
                self._ws_connect()
                if not self._bot_running: 
                    break
                time.sleep(10)
            except Exception as e:
                engine_logger.critical(f"Run loop error: {e}")
                time.sleep(30)
        self._update_bot_status("Stopped")

    def stop(self):
        engine_logger.info("Stopping Bot Engine...")
        self._bot_running = False
        if self._ws_app:
            try:
                self._ws_app.close()
            except: pass
        self._update_bot_status("Stopping")

    def clean_logout(self):
        self.stop()

    def _log_to_ui(self, log_type, source, message, full_json=None):
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
            pass
        
    def _update_bot_status(self, status_message):
        self.lock("bot_status")
        self._current_status = status_message
        self.unlock("bot_status")
        self._bot_status_event.set()

    def get_current_status(self):
        self.lock("bot_status")
        status = getattr(self, '_current_status', 'Unknown')
        self.unlock("bot_status")
        return status