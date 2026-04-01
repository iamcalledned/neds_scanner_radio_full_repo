import sqlite3
import os
import json
import logging

DB_PATH = os.path.join(os.path.dirname(__file__), 'push_subs.sqlite3')
logger = logging.getLogger("scanner_web.push_db")

DB_PATH_login = os.environ.get("LOGIN_DB_PATH", "/home/ned/data/login/login.sqlite3")

def ensure_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        endpoint TEXT UNIQUE,
        subscription_json TEXT,
        created_at INTEGER,
        feed_prefs TEXT
    )
    ''')
    # Add feed_prefs column to existing DBs that predate this migration
    try:
        cur.execute('ALTER TABLE subscriptions ADD COLUMN feed_prefs TEXT')
    except Exception:
        pass  # Column already exists
    conn.commit()
    conn.close()


def save_prefs(endpoint, feeds):
    """Persist an ordered list of feed IDs the subscriber wants notifications for.
    An empty list means all feeds.
    """
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        'UPDATE subscriptions SET feed_prefs = ? WHERE endpoint = ?',
        (json.dumps(feeds), endpoint)
    )
    conn.commit()
    conn.close()


def get_prefs(endpoint):
    """Return the list of feed IDs for this endpoint, or [] meaning all feeds."""
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT feed_prefs FROM subscriptions WHERE endpoint = ?', (endpoint,))
    row = cur.fetchone()
    conn.close()
    if row and row[0]:
        try:
            return json.loads(row[0])
        except Exception:
            return []
    return []


def list_subscriptions_with_prefs():
    """Return list of (subscription_json_dict, [feed_ids]) tuples."""
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT subscription_json, feed_prefs FROM subscriptions')
    rows = cur.fetchall()
    conn.close()
    result = []
    for sub_json, prefs_json in rows:
        try:
            sub = json.loads(sub_json)
        except Exception:
            continue
        try:
            prefs = json.loads(prefs_json) if prefs_json else []
        except Exception:
            prefs = []
        result.append((sub, prefs))
    return result


def save_subscription(subscription_json):
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        '''
        INSERT INTO subscriptions (endpoint, subscription_json, created_at)
        VALUES (?, ?, strftime("%s","now"))
        ON CONFLICT(endpoint) DO UPDATE SET
            subscription_json = excluded.subscription_json,
            created_at = excluded.created_at
        ''',
        (subscription_json.get('endpoint'), json.dumps(subscription_json))
    )
    conn.commit()
    conn.close()


def list_subscriptions():
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT subscription_json FROM subscriptions')
    rows = [json.loads(r[0]) for r in cur.fetchall()]
    conn.close()
    return rows


def remove_subscription(endpoint):
    ensure_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('DELETE FROM subscriptions WHERE endpoint = ?', (endpoint,))
    conn.commit()
    conn.close()


def list_loggedin_users():
    """
    Counts the number of users with a non-null current_session_id,
    indicating they are currently logged in.
    Returns a list of dictionaries, each representing a logged-in user.
    """
    conn = sqlite3.connect(DB_PATH_login)
    conn.row_factory = sqlite3.Row # This allows accessing columns by name
    cur = conn.cursor()
    
    # Select userID, username, and current_session_id for users where session is active
    cur.execute('SELECT user_ID, username, current_session_id FROM user_data WHERE current_session_id IS NOT NULL')
    
    # Convert rows to a list of dictionaries
    logged_in_users = [dict(row) for row in cur.fetchall()]
    # get the count
    count = len(logged_in_users)
    
    
    conn.close()
    return count

def get_loggedin_users_count():
    """Returns just the count of logged-in users."""
    count = list_loggedin_users()
    logger.debug("logged_in_users.count=%s", count)
    return count

# You would also need a function to ensure the user_data table exists,
# similar to push_db.ensure_db(), if it's not already handled elsewhere.
# For example:
def ensure_user_data_table():
    conn = sqlite3.connect(DB_PATH_login)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_data (
            userID INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            email TEXT,
            name TEXT,
            setup_date TEXT,
            last_login_date TEXT,
            current_session_id TEXT
        );
    """)
    conn.commit()
    conn.close()

