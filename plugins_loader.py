# db.py (DATABASE LAYER – CRITICAL)

import psycopg2
import psycopg2.extras
import os
import json
import logging
import threading

# --- Logging Setup for db.py ---
db_logger = logging.getLogger('db')
db_logger.setLevel(logging.INFO)
# -------------------------------

class DatabaseManager:
    def __init__(self, database_url):
        self._database_url = database_url
        self._local = threading.local() # To store connection per thread if needed, or use a pool

        # Basic check to ensure required extensions are present if using JSONB
        # This can be handled during init_db or as a separate check.
        
    def _get_connection(self):
        """Establishes and returns a new database connection."""
        # For a production system, use a connection pool (e.g., psycopg2.pool.SimpleConnectionPool)
        # For this skeleton, we'll open a new connection or reuse one per thread for simplicity.
        if not hasattr(self._local, "conn") or self._local.conn.closed:
            try:
                self._local.conn = psycopg2.connect(self._database_url)
                self._local.conn.autocommit = False # Ensure transactions are used explicitly
                db_logger.debug("New database connection established for thread.")
            except Exception as e:
                db_logger.critical(f"Failed to connect to database: {e}")
                raise
        return self._local.conn

    def init_db(self):
        """Initializes the user_stats table, creating it if it doesn't exist."""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_stats (
                    user_id VARCHAR PRIMARY KEY,
                    username VARCHAR NOT NULL,
                    permanent_score BIGINT DEFAULT 0,
                    currency BIGINT DEFAULT 500,
                    feature_data JSONB DEFAULT '{}'::jsonb
                );
                -- Ensure the username can be updated if a user changes their name on Howdies
                CREATE UNIQUE INDEX IF NOT EXISTS idx_user_username ON user_stats (username); 
                -- Or, if username is not unique in DB but just an identifier for logs
                -- This index depends on your exact user handling strategy.
            """)
            conn.commit()
            db_logger.info("Database 'user_stats' table ensured/initialized.")
        except Exception as e:
            db_logger.critical(f"Error initializing database table: {e}")
            conn.rollback() if conn else None
            raise
        finally:
            conn.close() if conn and not conn.closed else None # Close if not using pool

    def query(self, sql, params=None, fetch_one=False):
        """
        Executes a SQL query.
        Returns a list of dictionaries (rows) or a single dictionary if fetch_one is True.
        """
        conn = None
        result = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) # For dict results
            cursor.execute(sql, params)
            conn.commit() # For DDL/DML, ensure commit. For SELECT, it's harmless.
            
            if cursor.description: # If it's a SELECT query
                if fetch_one:
                    result = cursor.fetchone()
                else:
                    result = cursor.fetchall()
            else: # DDL/DML query without returning data
                result = {"status": "success", "rows_affected": cursor.rowcount}
            
            return result
        except Exception as e:
            db_logger.error(f"Database query failed: {sql} with params {params}. Error: {e}")
            conn.rollback() if conn else None
            raise # Re-raise to let calling plugin/engine handle specific errors
        finally:
            conn.close() if conn and not conn.closed else None

    def update_user_stats(self, user_id, username, stats, feature_key=None):
        """
        Updates user statistics.
        If feature_key is provided, safely merges into feature_data JSONB.
        Updates permanent_score and currency if specified in 'stats'.
        Ensures user exists, or creates them.
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Ensure user exists (UPSERT pattern)
            cursor.execute("""
                INSERT INTO user_stats (user_id, username)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username;
            """, (user_id, username))

            update_parts = []
            update_params = []

            # Update permanent_score if present in stats
            if 'permanent_score' in stats and isinstance(stats['permanent_score'], int):
                update_parts.append("permanent_score = permanent_score + %s")
                update_params.append(stats['permanent_score']) # Assuming we want to add to score

            # Update currency if present in stats (use adjust_currency for this to be atomic)
            # For simplicity here, we'll add it to an update, but adjust_currency is preferred for atomic
            if 'currency' in stats and isinstance(stats['currency'], int):
                # We'll call adjust_currency separately, as it needs specific transaction logic
                pass 
            
            # Merge into feature_data JSONB if feature_key is provided
            if feature_key:
                feature_update_dict = {feature_key: stats.get(feature_key, {})} # Extract plugin-specific data
                update_parts.append("feature_data = jsonb_merge(feature_data, %s::jsonb)")
                # jsonb_merge is a common custom function in Postgres.
                # If not available, use feature_data || %s::jsonb (for top-level merge)
                # or build more complex jsonb_set logic.
                # For basic merge, we can do: feature_data = feature_data || jsonb_build_object('featureA', featureA_data)
                # Let's assume a simpler update for now, or require specific JSONB operators.
                # For `jsonb_merge` to work like deep merge, you often need an extension or complex query.
                # A simpler || operator merges at top level.
                
                # For deep merge, a common pattern is:
                # jsonb_set(feature_data, '{feature_key}', (feature_data->'feature_key' || %s::jsonb))
                # Let's use `||` for top-level merge for simplicity in skeleton.
                update_parts.append(f"feature_data = jsonb_set(COALESCE(feature_data, '{{}}'::jsonb), %s, COALESCE(feature_data->%s, '{{}}'::jsonb) || %s::jsonb, true)")
                update_params.extend([
                    [feature_key],              # Path for jsonb_set
                    feature_key,                # Key for COALESCE(feature_data->%s)
                    json.dumps(stats.get(feature_key, {})) # JSON object to merge
                ])

            if update_parts:
                sql = f"UPDATE user_stats SET {', '.join(update_parts)} WHERE user_id = %s;"
                update_params.append(user_id)
                cursor.execute(sql, tuple(update_params))
            
            conn.commit()
            db_logger.info(f"User stats updated for {username} (ID: {user_id}).")

            # Handle currency separately if it was in 'stats'
            if 'currency' in stats and isinstance(stats['currency'], int):
                self.adjust_currency(user_id, stats['currency'])

        except Exception as e:
            db_logger.error(f"Error updating user stats for {username} (ID: {user_id}): {e}")
            conn.rollback() if conn else None
            raise
        finally:
            conn.close() if conn and not conn.closed else None

    def adjust_currency(self, user_id, amount):
        """
        Atomically adjusts user's currency.
        Uses a transaction and prevents negative balances.
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Acquire advisory lock for the user_id to prevent race conditions on balance
            # This is a PostgreSQL-specific feature for fine-grained concurrency control
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (int(user_id),)) # Use integer for advisory lock

            # Fetch current balance
            cursor.execute("SELECT currency FROM user_stats WHERE user_id = %s FOR UPDATE;", (user_id,))
            user_data = cursor.fetchone()

            if not user_data:
                db_logger.warning(f"Attempted to adjust currency for non-existent user_id: {user_id}. Creating user with default.")
                # This should ideally be handled before this point by update_user_stats or a user creation event
                # For now, let's create a default entry for a new user if not found (though less ideal here)
                # It's better if `update_user_stats` ensures user exists first.
                raise ValueError(f"User {user_id} not found in database for currency adjustment.")

            current_currency = user_data['currency']
            new_currency = current_currency + amount

            if new_currency < 0:
                raise ValueError(f"Insufficient funds for user {user_id}. Attempted to go negative.")

            cursor.execute("UPDATE user_stats SET currency = %s WHERE user_id = %s;", (new_currency, user_id))
            conn.commit()
            db_logger.info(f"Adjusted currency for {user_id} by {amount}. New balance: {new_currency}")
            return new_currency
        except ValueError as ve:
            db_logger.warning(f"Currency adjustment failed for {user_id}: {ve}")
            conn.rollback() if conn else None
            raise # Re-raise for plugin to handle
        except Exception as e:
            db_logger.error(f"Critical error during atomic currency adjustment for {user_id}: {e}")
            conn.rollback() if conn else None
            raise
        finally:
            conn.close() if conn and not conn.closed else None

# Helper function for JSONB merging
# PostgreSQL's `||` operator merges top-level JSON objects.
# For deep merge, you might need a custom function or more complex `jsonb_set` logic.
# Example for deep merge (requires PL/pgSQL function or similar):
# CREATE OR REPLACE FUNCTION jsonb_merge(jsonb, jsonb) RETURNS jsonb LANGUAGE SQL AS $$
#     SELECT jsonb_strip_nulls($1) || jsonb_strip_nulls($2)
# $$;
# For feature_data, using `jsonb_set` with `||` on the target key is a common robust approach for nested merging.
