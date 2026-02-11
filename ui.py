import threading
import queue
import json
import time
import logging
from flask import Flask, render_template_string, jsonify, request, Response

# --- Logging Setup for ui.py ---
ui_logger = logging.getLogger('ui')
ui_logger.setLevel(logging.INFO)
# -------------------------------

# Global queue for logs (populated by bot_engine)
ui_log_queue = queue.Queue(maxsize=1000)
# Event to signal UI about bot status changes (set by bot_engine)
bot_status_event = threading.Event()
# Event to signal UI about plugin status changes (set by plugins_loader)
plugins_status_event = threading.Event()


# --- Flask App Initialization ---
app = Flask(__name__)
_bot_engine_ref = None # Reference to the bot_engine instance, set by start_ui_server
_plugin_loader_ref = None # Reference to the plugin_loader instance, set by start_ui_server


# --- HTML Template (Embedded with CSS and JS) ---
# ... (HTML_TEMPLATE remains unchanged for brevity, but it's the full template from before) ...
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Howdies Bot Control Panel</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; background-color: #1a1a2e; color: #e0e0e0; }
        .header { background-color: #0f3460; padding: 20px; text-align: center; border-bottom: 3px solid #e94560; }
        .header h1 { margin: 0; color: #e0e0e0; }
        .nav-tabs { display: flex; justify-content: center; background-color: #16213e; padding: 10px 0; }
        .nav-tabs button {
            background-color: #0f3460; color: #e0e0e0; border: none; padding: 10px 20px;
            cursor: pointer; font-size: 16px; transition: background-color 0.3s, color 0.3s;
            margin: 0 5px; border-radius: 5px;
        }
        .nav-tabs button:hover { background-color: #e94560; color: #1a1a2e; }
        .nav-tabs button.active { background-color: #e94560; color: #1a1a2e; font-weight: bold; }
        .tab-content { padding: 20px; max-width: 1200px; margin: 20px auto; background-color: #0f3460; border-radius: 8px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.4); }
        .section-header { color: #e94560; border-bottom: 2px solid #1a1a2e; padding-bottom: 10px; margin-bottom: 20px; font-size: 1.5em; }
        .status-box { background-color: #16213e; padding: 15px; border-radius: 5px; margin-bottom: 15px; display: flex; justify-content: space-between; align-items: center; }
        .status-box strong { color: #e0e0e0; }
        .status-indicator { display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin-left: 10px; }
        .status-connected { background-color: #4CAF50; } /* Green */
        .status-disconnected { background-color: #F44336; } /* Red */
        .status-connecting { background-color: #FFC107; } /* Yellow */
        .status-stopped { background-color: #9E9E9E; } /* Grey */
        .status-error { background-color: #e94560; } /* Pink */

        .log-container { max-height: 500px; overflow-y: scroll; background-color: #16213e; border-radius: 5px; padding: 10px; }
        .log-entry { border-bottom: 1px dashed #2e4a86; padding: 8px 0; font-family: 'Consolas', 'Monaco', monospace; font-size: 0.9em; }
        .log-entry:last-child { border-bottom: none; }
        .log-timestamp { color: #94d82d; margin-right: 10px; } /* Greenish */
        .log-source { color: #5cb8e4; margin-right: 10px; } /* Light blue */
        .log-type-INFO { color: #e0e0e0; font-weight: bold; }
        .log-type-EVENT { color: #ffee58; } /* Yellow */
        .log-type-EVENT_IN, .log-type-EVENT_OUT { color: #81c784; } /* Lighter green */
        .log-type-WARNING { color: #ff9800; } /* Orange */
        .log-type-ERROR, .log-type-CRITICAL { color: #ef5350; font-weight: bold; } /* Red */
        .log-message { display: inline-block; max-width: calc(100% - 250px); overflow-x: auto; vertical-align: middle; }
        .log-details { display: none; margin-top: 5px; background-color: #0f3460; padding: 5px; border-radius: 3px; white-space: pre-wrap; word-break: break-all; font-size: 0.8em; }
        .log-entry.expanded .log-details { display: block; }
        .log-toggle { cursor: pointer; color: #e94560; font-size: 0.8em; margin-left: 10px; }

        .plugin-list { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 15px; }
        .plugin-card { background-color: #16213e; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2); }
        .plugin-card h3 { margin-top: 0; color: #e94560; }
        .plugin-status { font-weight: bold; }
        .plugin-status.Active { color: #4CAF50; }
        .plugin-status.Error { color: #F44336; }
        .plugin-status.Loading { color: #FFC107; }
        .plugin-status.Loaded { color: #5cb8e4; } /* Blue for loaded but not active fully */

        .control-buttons button { background-color: #4CAF50; color: white; border: none; padding: 12px 25px; margin: 5px; border-radius: 5px; cursor: pointer; font-size: 1.1em; transition: background-color 0.3s; }
        .control-buttons button:hover { opacity: 0.9; }
        .control-buttons button.stop { background-color: #F44336; }
        .control-buttons button:disabled { background-color: #616161; cursor: not-allowed; }

        .copy-button { background-color: #e94560; color: white; border: none; padding: 8px 15px; border-radius: 5px; cursor: pointer; font-size: 0.9em; margin-top: 20px; float: right; }
        .copy-button:hover { opacity: 0.9; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Howdies Bot Control Panel</h1>
    </div>

    <div class="nav-tabs">
        <button class="tab-button active" onclick="openTab('control')">Control</button>
        <button class="tab-button" onclick="openTab('logs')">Live Logs</button>
        <button class="tab-button" onclick="openTab('plugins')">Plugins</button>
    </div>

    <div id="control" class="tab-content">
        <h2 class="section-header">Bot Control</h2>
        <div class="status-box">
            <strong>Bot Status:</strong> <span id="botStatus">Loading...</span>
            <span id="statusIndicator" class="status-indicator status-stopped"></span>
        </div>
        <div class="control-buttons">
            <button id="startButton" onclick="controlBot('start')" disabled>Start Bot</button>
            <button id="stopButton" class="stop" onclick="controlBot('stop')" disabled>Stop Bot</button>
        </div>
        <h2 class="section-header" style="margin-top: 30px;">System Status</h2>
        <div class="status-box">
            <strong>Current Time:</strong> <span id="currentTime">Loading...</span>
        </div>
        <!-- Add more system metrics here if needed -->
    </div>

    <div id="logs" class="tab-content" style="display:none;">
        <h2 class="section-header">Live Logs</h2>
        <div class="log-container" id="logContainer">
            <!-- Log entries will be inserted here by JavaScript -->
        </div>
    </div>

    <div id="plugins" class="tab-content" style="display:none;">
        <h2 class="section-header">Plugins Overview</h2>
        <div class="plugin-list" id="pluginList">
            <!-- Plugin cards will be inserted here by JavaScript -->
            <p>Loading plugin data...</p>
        </div>
        <button class="copy-button" onclick="copyPrompt()">Copy System Prompt</button>
    </div>

    <script>
        const botStatusElem = document.getElementById('botStatus');
        const statusIndicator = document.getElementById('statusIndicator');
        const startButton = document.getElementById('startButton');
        const stopButton = document.getElementById('stopButton');
        const logContainer = document.getElementById('logContainer');
        const pluginList = document.getElementById('pluginList');
        const currentTimeElem = document.getElementById('currentTime');

        let currentBotStatus = 'Loading...';
        let botIsRunning = false;

        function updateCurrentTime() {
            const now = new Date();
            currentTimeElem.textContent = now.toLocaleString();
        }
        setInterval(updateCurrentTime, 1000); // Update time every second
        updateCurrentTime(); // Initial call


        function openTab(tabName) {
            const tabs = document.querySelectorAll('.tab-content');
            tabs.forEach(tab => tab.style.display = 'none');
            document.getElementById(tabName).style.display = 'block';

            const buttons = document.querySelectorAll('.tab-button');
            buttons.forEach(button => button.classList.remove('active'));
            document.querySelector(`.tab-button[onclick="openTab('${tabName}')"]`).classList.add('active');
        }

        async function fetchBotStatus() {
            try {
                const response = await fetch('/api/status');
                const data = await response.json();
                currentBotStatus = data.status;
                botStatusElem.textContent = currentBotStatus;
                
                statusIndicator.className = 'status-indicator';
                startButton.disabled = true;
                stopButton.disabled = true;

                if (currentBotStatus === 'Connected' || currentBotStatus.startsWith('Reconnecting')) {
                    statusIndicator.classList.add('status-connected');
                    botIsRunning = true;
                    stopButton.disabled = false;
                } else if (currentBotStatus === 'Disconnected' || currentBotStatus === 'Stopped' || currentBotStatus === 'Failed to Connect') {
                    statusIndicator.classList.add('status-disconnected');
                    botIsRunning = false;
                    startButton.disabled = false;
                } else if (currentBotStatus.startsWith('Connecting') || currentBotStatus.startsWith('Starting') || currentBotStatus.startsWith('Stopping')) {
                    statusIndicator.classList.add('status-connecting');
                    botIsRunning = false; // Cannot start/stop during transition
                } else if (currentBotStatus.includes('Error')) {
                    statusIndicator.classList.add('status-error');
                    botIsRunning = false;
                    startButton.disabled = false;
                }
                
            } catch (error) {
                console.error('Error fetching bot status:', error);
                botStatusElem.textContent = 'API Error';
                statusIndicator.className = 'status-indicator status-error';
                startButton.disabled = false;
                stopButton.disabled = false;
            }
        }

        async function controlBot(action) {
            startButton.disabled = true;
            stopButton.disabled = true;
            botStatusElem.textContent = (action === 'start' ? 'Starting...' : 'Stopping...');
            statusIndicator.className = 'status-indicator status-connecting'; // Temporarily set to connecting

            try {
                const response = await fetch(`/api/control/${action}`, { method: 'POST' });
                const data = await response.json();
                console.log(data.message);
                // Status will be updated by SSE or periodic fetch after action
            } catch (error) {
                console.error(`Error sending ${action} command:`, error);
                alert(`Failed to ${action} bot. Check logs.`);
                fetchBotStatus(); // Revert button state
            }
        }

        // --- Live Log Stream (Server-Sent Events) ---
        function setupLogStream() {
            const eventSource = new EventSource('/api/logs/stream');
            eventSource.onmessage = function(event) {
                const logEntry = JSON.parse(event.data);
                appendLogEntry(logEntry);
            };
            eventSource.onerror = function(err) {
                console.error("EventSource failed:", err);
                eventSource.close();
                // Optional: Reconnect after a delay
                setTimeout(setupLogStream, 3000); 
            };
        }

        function appendLogEntry(logEntry) {
            const entryDiv = document.createElement('div');
            entryDiv.className = 'log-entry';
            
            let fullJsonContent = '';
            if (logEntry.full_json) {
                try {
                    // Try to pretty print if it's a JSON string
                    // Ensure full_json is treated as a string before parsing
                    const parsedJson = JSON.parse(JSON.stringify(logEntry.full_json)); 
                    fullJsonContent = JSON.stringify(parsedJson, null, 2);
                } catch (e) {
                    fullJsonContent = JSON.stringify(logEntry.full_json, null, 2); // Fallback to stringify directly
                }
            }
            
            entryDiv.innerHTML = `
                <span class="log-timestamp">${logEntry.timestamp}</span>
                <span class="log-source">[${logEntry.source}]</span>
                <span class="log-type-${logEntry.type}">${logEntry.type}</span>
                <span class="log-message">${logEntry.message}</span>
                ${logEntry.full_json ? `<span class="log-toggle" onclick="toggleLogDetails(this)">[Details]</span>` : ''}
                <pre class="log-details">${fullJsonContent}</pre>
            `;
            logContainer.prepend(entryDiv); // Add new logs to the top

            // Limit log entries to keep performance
            while (logContainer.children.length > 200) {
                logContainer.removeChild(logContainer.lastChild);
            }
        }

        function toggleLogDetails(toggleElement) {
            const entryDiv = toggleElement.closest('.log-entry');
            entryDiv.classList.toggle('expanded');
            toggleElement.textContent = entryDiv.classList.contains('expanded') ? '[Hide]' : '[Details]';
        }

        async function fetchPluginStatuses() {
            try {
                const response = await fetch('/api/plugins/status');
                const data = await response.json();
                pluginList.innerHTML = ''; // Clear existing list
                
                if (Object.keys(data).length === 0) {
                    pluginList.innerHTML = '<p>No plugins loaded or status unavailable.</p>';
                    return;
                }

                for (const pluginName in data) {
                    const plugin = data[pluginName];
                    const cardDiv = document.createElement('div');
                    cardDiv.className = 'plugin-card';
                    cardDiv.innerHTML = `
                        <h3>${pluginName}</h3>
                        <p>Status: <span class="plugin-status ${plugin.status}">${plugin.status}</span></p>
                        ${plugin.error_info ? `<p style="color:#ef5350;">Error: ${plugin.error_info}</p>` : ''}
                    `;
                    pluginList.appendChild(cardDiv);
                }
            } catch (error) {
                console.error('Error fetching plugin statuses:', error);
                pluginList.innerHTML = '<p style="color:#ef5350;">Failed to load plugin statuses. API error.</p>';
            }
        }

        // --- Periodically update status and plugins ---
        setInterval(fetchBotStatus, 3000); // Update bot status every 3 seconds
        setInterval(fetchPluginStatuses, 5000); // Update plugin status every 5 seconds

        // Initial calls
        fetchBotStatus();
        fetchPluginStatuses();
        setupLogStream();
        openTab('control'); // Open control tab by default

        // Copy button for system prompt
        function copyPrompt() {
            const promptText = `
YOU ARE BUILDING A ROBUST, SCALABLE, EXTENSIBLE, PRODUCTION-GRADE
PLUGIN-BASED BOT SYSTEM IN PYTHON FOR THE HOWDIES PLATFORM.

THIS DOCUMENT IS FINAL.
DO NOT SKIP ANY REQUIREMENT.
DO NOT ADD EXTRA FILES.
DO NOT MODIFY CORE ARCHITECTURE.
DO NOT HARD-CODE FEATURES.
ALL FEATURES MUST BE PLUGINS.

==================================================
OBJECTIVE
==================================================

Build a multi-room, thread-safe, production-grade bot system.

Core must be IMMUTABLE.
All functionality must be implemented through plugins.

System must support:
- Real-time WebSocket communication
- Multi-room operation
- Economy system
- Score tracking
- Ranking system
- Concurrency safety
- Database consistency
- Plugin isolation
- Future extensibility

Core must never change after initial build.

==================================================
PROJECT STRUCTURE (STRICT)
==================================================

mybot/
│
├── plugins/                <-- ALL FEATURES go here
│   ├── example_plugin.py
│   └── ... (future feature files)
│
├── .env
├── app.py
├── bot_engine.py
├── db.py
├── plugins_loader.py
├── ui.py
└── requirements.txt

NO EXTRA FILES.
NO STRUCTURE CHANGES.

==================================================
1. .env (CONFIGURATION)
==================================================

All sensitive configuration stored here:

BOT_ID=
BOT_PASSWORD=
DEFAULT_ROOM=
MASTER_ADMIN_USERNAME=
DATABASE_URL=
PORT=

Nothing must be hardcoded inside code.

==================================================
2. app.py (LOGIN / SESSION CONTROLLER)
==================================================

Responsibilities:

- Read credentials from .env
- Authenticate with Howdies API
- Retrieve session token
- Initialize bot_engine with token
- Start ui.py
- Enforce STRICT single session policy:
  - Only one active instance
  - No duplicate logins
  - No ghost sessions
- Handle clean logout

app.py must NOT:
- Contain feature logic
- Contain economy logic
- Contain command logic
- Contain plugin logic

It only controls lifecycle.

==================================================
3. bot_engine.py (IMMUTABLE CORE ENGINE)
==================================================

THIS FILE IS IMMUTABLE AFTER CREATION.

This is the heart of the system.

Must be production-grade.

Responsibilities:

1. WebSocket Management
   - Connect to Howdies server
   - Send and receive payloads
   - Auto-reconnect support
   - Heartbeat/ping handling

2. Event Dispatcher
   - Convert each incoming handler to:
     engine.emit("event:<handler>")
   - Allow plugins to register using:
     engine.on("event:<handler>", callback)

3. Plugin Sandboxing
   - Wrap plugin execution in try/except
   - One plugin failure must NOT crash system
   - Log errors with:
     - plugin name
     - traceback
     - event source

4. Session Management
   - Store bot user ID
   - Store token
   - Provide engine.get_session()

5. Multi-Room Support
   - Track joined rooms
   - Route events correctly

6. User Resolver
   - Maintain user_map {username: user_id}
   - Update on join/leave/active events
   - Provide engine.resolve_user(username)

7. Thread Safety
   - Provide lock system:
     engine.lock(key)
     engine.unlock(key)
   - Use threading.Lock internally
   - Prevent race conditions in high-load logic

8. Shared Access Layer
   - engine.get_db()
   - engine.get_master_admin()
   - engine.get_session()
   - engine.resolve_user()

bot_engine.py MUST NOT contain:
- Feature logic
- Command logic
- Economy logic
- Hardcoded behaviors

==================================================
4. db.py (DATABASE LAYER – CRITICAL)
==================================================

Database: PostgreSQL (Neon DB)
Use psycopg2 or asyncpg.

Table: user_stats

Structure:

user_id VARCHAR PRIMARY KEY
username VARCHAR NOT NULL
permanent_score BIGINT DEFAULT 0
currency BIGINT DEFAULT 500
feature_data JSONB DEFAULT '{}'::jsonb

feature_data is CRITICAL.

It stores dynamic data for all plugins.

Example structu
