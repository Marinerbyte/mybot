import threading
import requests
import json
import traceback
import logging

# --- Logging Setup for this plugin ---
plugin_logger = logging.getLogger('plugin.example')
plugin_logger.setLevel(logging.INFO)
# ------------------------------------

# Store active command handlers specific to this plugin
_command_handlers = {}

def _parse_command(text):
    """Parses command text into cmd, arg1, arg2."""
    if not text.startswith(("!", ".")):
        return None, None, None, None
    
    parts = text[1:].split(maxsplit=2)
    cmd = parts[0].lower()
    arg1 = parts[1].strip() if len(parts) > 1 else ""
    arg2 = parts[2].strip() if len(parts) > 2 else ""
    return cmd, arg1, arg2, text # Return original text too for full context

def _register_command(cmd_name, handler_func):
    """Registers a command handler for this plugin."""
    _command_handlers[cmd_name.lower()] = handler_func

# ================================
# Command Implementations (from dm.py)
# ================================

def _handle_profile_info(engine_instance, payload, cmd, arg1, arg2):
    """Handles !info [username] command."""
    sender_username = payload.get("username", "Unknown")
    reply_target = sender_username if payload.get("handler") == "message" else None # For DM replies

    if not arg1:
        engine_instance.send_text_message(sender_username, "Usage: !info [username]", is_dm=bool(reply_target))
        return

    target_user_info = engine_instance.get_user_info(username=arg1)
    if not target_user_info:
        engine_instance.send_text_message(sender_username, f"User @{arg1} found nahi.", is_dm=bool(reply_target))
        return

    # To get full profile details, Howdies needs a specific 'profile' handler request.
    # The engine emits 'event:profile' when a profile response comes back.
    # So, we'll request it and let a separate listener handle the response.
    request_payload = {
        "handler": "profile", 
        "id": engine_instance._get_gid(), # Use engine's internal gid
        "username": arg1
    }
    engine_instance.send_payload(request_payload)
    engine_instance.send_text_message(sender_username, f"Profile info ke liye request bhej di @{arg1} ki.", is_dm=bool(reply_target))

def _on_profile_response(engine_instance, payload):
    """Listens for 'event:profile' to display detailed profile info."""
    u = payload.get("user") or payload.get("profile")
    if u:
        username = u.get('username')
        user_id = u.get('id')
        level = u.get('level')
        
        # Determine where to send the reply. This is tricky for async responses.
        # A robust solution might involve the initiating _handle_profile_info storing a "pending request ID"
        # and its origin, then _on_profile_response checks if it's for an active request.
        # For simplicity, we'll just send to the default room for now, or to the master admin.
        default_room_info = engine_instance.get_room_info()
        target_room_id = default_room_info["id"] if default_room_info else None
        
        if target_room_id:
            engine_instance.send_text_message(target_room_id, f"👤 {username} | ID: {user_id} | Lvl: {level}")
        else:
            plugin_logger.warning(f"Profile response for {username} received, but no default room to reply to.")

def _handle_display_picture(engine_instance, payload, cmd, arg1, arg2):
    """Handles !dp [username] command."""
    sender_username = payload.get("username", "Unknown")
    reply_target = sender_username if payload.get("handler") == "message" else None

    if not arg1:
        engine_instance.send_text_message(sender_username, "Usage: !dp [username]", is_dm=bool(reply_target))
        return

    target_user_info = engine_instance.get_user_info(username=arg1)
    if target_user_info and target_user_info.get("avatar"):
        engine_instance.send_image_message(sender_username, target_user_info["avatar"], f"DP of @{arg1}", is_dm=bool(reply_target))
    else:
        engine_instance.send_text_message(sender_username, f"User @{arg1} mila nahi ya uski DP nahi hai.", is_dm=bool(reply_target))

def _handle_list_users(engine_instance, payload, cmd, arg1, arg2):
    """Handles !l command to list active users."""
    sender_username = payload.get("username", "Unknown")
    reply_target = sender_username if payload.get("handler") == "message" else None

    engine_instance.lock("user_map") # Lock user_map before reading
    try:
        users = sorted([u['username'] for u in engine_instance._user_map.values()])
    finally:
        engine_instance.unlock("user_map") # Unlock after reading

    if users:
        user_list_msg = "👥 Users:\n" + "\n".join([f"{i}. {n}" for i, n in enumerate(users, 1)])
        engine_instance.send_text_message(sender_username, user_list_msg, is_dm=bool(reply_target))
    else:
        engine_instance.send_text_message(sender_username, "Abhi room mein koi user nahi hai.", is_dm=bool(reply_target))

# ================================
# Main Plugin Listener
# ================================

def _on_message_received(engine_instance, payload):
    """
    Listens for all incoming chatroom and DM text messages,
    parses commands, and dispatches them to registered handlers.
    """
    handler_type = payload.get("handler")
    text = payload.get("text", "").strip()
    sender_id = payload.get("userid") or payload.get("userID")
    sender_username = payload.get("username", "Unknown")
    
    # Ignore bot's own messages
    if str(sender_id) == str(engine_instance.get_session()["bot_id"]):
        return

    cmd, arg1, arg2, original_text = _parse_command(text)

    if cmd and cmd in _command_handlers:
        plugin_logger.info(f"Command '{cmd}' received from @{sender_username}: '{original_text}'")
        try:
            # Execute the command handler in a separate thread for non-blocking execution
            # The engine already does this via _execute_plugin_callback, but if a plugin
            # wants to do additional long-running sub-tasks for a command, it can use threading here.
            _command_handlers[cmd](engine_instance, payload, cmd, arg1, arg2)
        except Exception as e:
            plugin_logger.error(f"Error handling command '{cmd}' by plugin: {e}\n{traceback.format_exc()}")
            reply_target = sender_username if handler_type == "message" else None
            engine_instance.send_text_message(sender_username, f"Oops! '{cmd}' command chalate waqt error ho gayi. Please try again.", is_dm=bool(reply_target))


# ================================
# Plugin Setup Function
# ================================

def setup(engine):
    """
    This function is called by the PluginLoader when the plugin is loaded.
    It registers event listeners and internal command handlers.
    """
    plugin_logger.info("Example Plugin: Setting up...")

    # Register internal command handlers
    _register_command("info", _handle_profile_info)
    _register_command("dp", _handle_display_picture)
    _register_command("l", _handle_list_users)
    _register_command("list", _handle_list_users) # Alias

    # Register event listeners with the bot engine
    # Listen for all chatroom messages
    engine.on("event:chatroommessage", _on_message_received)
    # Listen for all direct messages
    engine.on("event:message", _on_message_received)
    # Listen for profile responses (for !info command)
    engine.on("event:profile", _on_profile_response)
    
    plugin_logger.info("Example Plugin: Setup complete. Commands registered: !info, !dp, !l")
