import os
import requests
import time
import threading
import logging
from dotenv import load_dotenv

# Import other modules as per project structure
from bot_engine import HowdiesBotEngine
from plugins_loader import PluginLoader # Import PluginLoader
from db import DatabaseManager
from ui import start_ui_server, ui_log_queue, bot_status_event, plugins_status_event

# --- Logging Setup for app.py ---
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s')
app_logger = logging.getLogger('app')
# ---------------------------------

def enforce_single_session_policy():
    """
    Placeholder for strict single session enforcement.
    In a real production environment, this might involve:
    - A centralized redis/database check for active sessions.
    - A lock file system.
    - Howdies API specific checks (if available).
    For this skeleton, we'll assume a simple flag or process check.
    """
    app_logger.info("Enforcing single session policy...")
    pass

def main():
    load_dotenv() # Load environment variables from .env file

    bot_id = os.getenv("BOT_ID")
    bot_password = os.getenv("BOT_PASSWORD")
    default_room = os.getenv("DEFAULT_ROOM")
    master_admin_username = os.getenv("MASTER_ADMIN_USERNAME")
    database_url = os.getenv("DATABASE_URL")
    ui_port = int(os.getenv("PORT", 8000)) # Default to 8000 if not set

    if not all([bot_id, bot_password, default_room, master_admin_username, database_url]):
        app_logger.error("Missing one or more required environment variables. Check your .env file.")
        return

    enforce_single_session_policy()

    app_logger.info(f"Attempting to log in bot: {bot_id}")
    session_token = None
    bot_user_id = None # Initialize bot_user_id here
    try:
        # Authenticate with Howdies API
        login_url = "https://api.howdies.app/api/login"
        payload = {"username": bot_id, "password": bot_password}
        response = requests.post(login_url, json=payload)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        login_data = response.json()
        session_token = login_data.get("token")
        
        # Capture bot's own ID from login response if available
        bot_user_id = login_data.get("userID") or login_data.get("userid") or login_data.get("id")

        if not session_token:
            app_logger.critical("Login failed: No session token received.")
            return
        
        app_logger.info("Bot successfully authenticated with Howdies API.")

    except requests.exceptions.RequestException as e:
        app_logger.critical(f"Howdies API login request failed: {e}")
        return
    except json.JSONDecodeError:
        app_logger.critical("Howdies API login response was not valid JSON.")
        return

    # Initialize Database Manager
    db_manager = DatabaseManager(database_url)
    try:
        db_manager.init_db() # Ensure table exists
        app_logger.info("Database initialized successfully.")
    except Exception as e:
        app_logger.critical(f"Failed to initialize database: {e}")
        return

    # Initialize Bot Engine
    # Pass bot_user_id to the engine, it will be updated by 'login' event handler as well
    bot_engine = HowdiesBotEngine(
        session_token=session_token,
        bot_id=bot_user_id, 
        default_room_name=default_room,
        master_admin_username=master_admin_username,
        db_manager=db_manager,
        ui_log_queue=ui_log_queue,
        bot_status_event=bot_status_event
    )

    # Initialize Plugin Loader
    plugin_loader = PluginLoader()
    plugin_loader.load_plugins(bot_engine, plugins_status_event) # Pass plugin_loader_instance

    # Start Bot Engine in a separate thread
    bot_thread = threading.Thread(target=bot_engine.run, daemon=True)
    bot_thread.start()
    app_logger.info("Bot Engine started in a separate thread.")

    # Start UI in the main thread (passing plugin_loader_instance to UI)
    app_logger.info(f"Starting UI server on port {ui_port}...")
    start_ui_server(bot_engine, plugin_loader, ui_port) # UI needs bot_engine AND plugin_loader

    app_logger.info("Bot system shutting down.")
    bot_engine.clean_logout()

if __name__ == "__main__":
    main()
