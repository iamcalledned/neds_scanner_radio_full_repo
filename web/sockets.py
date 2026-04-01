# /home/ned/Documents/neds_scanner_radio_d102925/scanner_web/sockets.py
import logging
import redis
import json
import datetime
import pytz
from dateutil import parser
from flask import request
from flask_socketio import SocketIO, emit

import push_db
import push_utils



from client_tracker import init_client_table, log_client_connection, fetch_client_geo

init_client_table()  # Run once on startup

# Will be initialized in app_socket.py via init_sockets
socketio = SocketIO()
r = None
ALL_FEEDS = []
ALL_DEPARTMENT_IDS = []
LOCAL_TIMEZONE = None
logger = logging.getLogger('scanner_web')


def _format_iso_time(iso_string):
    """
    Parses an ISO timestamp string and returns a friendly
    12-hour time (e.g., "2:30 PM") in the local timezone.
    Returns an empty string if the input is invalid or None.
    """
    if not iso_string:
        return ""
    try:
        # Parse the ISO string (from UTC/Z)
        utc_time = parser.isoparse(iso_string)
        
        # Convert to our local timezone
        local_time = utc_time.astimezone(LOCAL_TIMEZONE)
        
        # Format as "2:30 PM"
        return local_time.strftime('%-l:%M %p').strip()
    except Exception as e:
        print(f"Error parsing time '{iso_string}': {e}")
        return ""

# -------------------------
# WebSocket Event Handlers
# -------------------------
@socketio.on('connect')
def handle_connect():
    sid = request.sid
    headers = request.headers

    # --- Capture key fields ---
    client_ip = (
        headers.get('CF-Connecting-IP') or
        headers.get('X-Forwarded-For', request.remote_addr)
    )
    user_agent = headers.get('User-Agent', 'Unknown')
    origin = headers.get('Origin', 'Unknown')
    referrer = headers.get('Referer', 'Unknown')
    language = headers.get('Accept-Language', 'Unknown')
    client_id = request.cookies.get('client_id')  # Optional JS-side identifier

    # --- Optional: Geo lookup ---
    geo = fetch_client_geo(client_ip)

    # --- Log to SQLite ---
    log_client_connection(
        client_id=client_id,
        ip=client_ip,
        user_agent=user_agent,
        origin=origin,
        referrer=referrer,
        language=language,
        geo_json=geo
    )

    # --- Standard Socket.IO response ---
    emit('connection_response', {
        'status': 'connected',
        'timestamp': datetime.datetime.now().isoformat(),
        'sessionId': sid
    }, to=sid)

    logger.info(f"[CONNECT] SID={sid} | IP={client_ip} | Agent={user_agent} | Origin={origin}")


@socketio.on('disconnect')
def handle_disconnect():
    # request.sid is also available here if needed
    print(f'Client disconnected: {request.sid}')

@socketio.on('client_message')
def handle_client_message(json_data):
    print(f"Received 'client_message' from {request.sid}: {json_data.get('data', 'No data')}")
    
    # This broadcasts to EVERYONE. 
    # If you only want to reply to the sender, add 'to=request.sid'
    socketio.emit('server_response', {'data': f"Server received: {json_data.get('data', 'No data')}"})

# -------------------------
# Background Workers
# -------------------------
def push_worker():
    worker_logger = logging.getLogger('scanner_web.push_worker')
    worker_logger.info("Push notification worker initializing...")
    
    try:
        vapid_pub, vapid_priv = push_utils.load_vapid_keys()
        vapid_claims = {'sub': 'mailto:admin@iamcalledned.ai'}
        worker_logger.info("VAPID keys loaded successfully")
    except Exception as e:
        worker_logger.critical(f"Failed to load VAPID keys | Error: {str(e)}")
        return
        
    while True:
        try:
            item = r.brpop('push_queue', timeout=10)
            if not item:
                socketio.sleep(0.1)
                continue
                
            _, payload_raw = item
            # r uses decode_responses=True so values are already str; guard for bytes just in case
            payload_str = payload_raw.decode('utf-8') if isinstance(payload_raw, (bytes, bytearray)) else payload_raw
            
            try:
                job = json.loads(payload_str)
                message_content = job.get('message', 'New scanner call')
                title = job.get('title', 'Scanner Activity')
                feed = job.get('feed', '')
                targeted_endpoints = job.get('targeted_endpoints')  # None = send to all

                if targeted_endpoints is not None:
                    # Pre-filtered by new_call_watcher; look up full sub objects
                    endpoint_set = set(targeted_endpoints)
                    all_subs = push_db.list_subscriptions()
                    subs = [s for s in all_subs if s.get('endpoint') in endpoint_set]
                else:
                    subs = push_db.list_subscriptions()

                worker_logger.info(f"Processing push notification | Message: '{message_content}' | Recipients: {len(subs)}")

                push_payload = {'title': title, 'message': message_content}
                if feed:
                    push_payload['feed'] = feed

                success_count = 0
                failed_count = 0
                for sub in subs:
                    endpoint = sub.get('endpoint', 'unknown')
                    try:
                        ok, err = push_utils.send_push(sub, push_payload, vapid_priv, vapid_claims)
                        if ok:
                            success_count += 1
                        else:
                            failed_count += 1
                            if err and ('410' in err or '404' in err or 'unsubscribed' in err.lower() or 'expired' in err.lower()):
                                push_db.remove_subscription(endpoint)
                                worker_logger.info(f"Removed stale push subscription | Subscriber: {endpoint}")
                            worker_logger.warning(f"Push delivery failed | Subscriber: {endpoint} | Error: {err}")
                    except Exception as push_err:
                        failed_count += 1
                        worker_logger.error(f"Push delivery failed | Subscriber: {endpoint} | Error: {str(push_err)}")

                worker_logger.info(f"Push notification complete | Successful: {success_count}/{len(subs)} | Failed: {failed_count}")
                
            except json.JSONDecodeError:
                worker_logger.error(f"Invalid JSON payload received | Raw: {payload_str}")
            except Exception as e:
                worker_logger.error(f"Push processing error | Error: {str(e)}")
                
        except redis.RedisError as redis_err:
            worker_logger.error(f"Redis connection error | Error: {str(redis_err)} | Action: Reconnecting in 5s")
            socketio.sleep(5)
        except Exception as e:
            worker_logger.error(f"Unexpected worker error | Error: {str(e)} | Action: Retry in 5s")
            socketio.sleep(5)

def transmitting_worker():
    worker_logger = logging.getLogger('scanner_web.transmitting_worker')
    worker_logger.info("Transmitting status monitor initializing...")
    
    last_status = {}
    status_check_count = 0
    last_active_time = None
    
    while True:
        current_status = {}
        active_found = False
        status_check_count += 1
        
        try:
            # Fetch all transmitting status keys
            keys = r.keys('scanner:*:transmitting')
            active_departments = []
            
            if keys:
                worker_logger.debug(f"Status check #{status_check_count} | Found {len(keys)} status keys")
                
                # Pipeline Redis gets for efficiency
                pipe = r.pipeline()
                for key in keys:
                    pipe.get(key)
                values = pipe.execute()
                
                # Process each key-value pair
                for key, value in zip(keys, values):
                    try:
                        parts = key.split(':')
                        if len(parts) == 3:
                            dept_id = parts[1]
                            current_status[dept_id] = value or 'N'
                            if value == 'Y':
                                active_found = True
                                active_departments.append(dept_id)
                    except IndexError:
                        worker_logger.warning(f"Malformed Redis key detected | Key: {key}")
            
            # Set default 'N' status for departments not found in Redis
            for dept_id in ALL_DEPARTMENT_IDS:
                if dept_id not in current_status:
                    current_status[dept_id] = 'N'
            
            # Track status changes
            changed_statuses = {
                dept_id: status
                for dept_id, status in current_status.items()
                if last_status.get(dept_id) != status
            }
            
            if changed_statuses:
                changes_str = " | ".join(f"{dept}: {status}" for dept, status in changed_statuses.items())
                #worker_logger.info(f"Department status changes detected | {changes_str}")
                socketio.emit('transmitting_update', changed_statuses)
            
            # Update activity tracking
            if active_found:
                last_active_time = datetime.datetime.now()
                #worker_logger.info(f"Active transmission detected | Departments: {', '.join(active_departments)}")
            
            last_status = current_status.copy()
            
        except redis.RedisError as e:
            worker_logger.error(f"Redis connection error | Error: {str(e)} | Action: Reconnecting in 5s")
            last_status = {}
            socketio.sleep(5)
        except Exception as e:
            worker_logger.error(f"Unexpected monitoring error | Error: {str(e)} | Action: Retry in 1s")
            socketio.sleep(1)
        
        # Adaptive sleep duration based on activity
        sleep_duration = 0.25 if active_found else 1.0
        socketio.sleep(sleep_duration)

def new_call_watcher():
    """Watches scanner:*:latest_time Redis keys for changes.
    When a feed's latest_time advances, enqueues a push notification.
    """
    worker_logger = logging.getLogger('scanner_web.new_call_watcher')
    worker_logger.info("New call watcher initializing...")

    # Human-readable feed names for notification titles
    FEED_NAMES = {
        'pd': 'Hopedale PD', 'fd': 'Hopedale FD',
        'mpd': 'Milford PD', 'mfd': 'Milford FD',
        'bpd': 'Bellingham PD', 'bfd': 'Bellingham FD',
        'mndpd': 'Mendon PD', 'mndfd': 'Mendon FD',
        'uptpd': 'Upton PD', 'uptfd': 'Upton FD',
        'blkpd': 'Blackstone PD', 'blkfd': 'Blackstone FD',
        'frkpd': 'Franklin PD', 'frkfd': 'Franklin FD',
        'milpd': 'Millis PD', 'milfd': 'Millis FD',
        'medpd': 'Medway PD', 'medfd': 'Medway FD',
        'foxpd': 'Foxborough PD', 'sfd': 'Southborough FD',
    }

    # Seed last-seen timestamps so we don't fire on startup
    last_times = {}
    try:
        for key in r.scan_iter(match='scanner:*:latest_time'):
            last_times[key] = r.get(key) or ''
        worker_logger.info(f"Seeded {len(last_times)} feed timestamps")
    except Exception as e:
        worker_logger.warning(f"Could not seed timestamps: {e}")

    while True:
        try:
            for key in r.scan_iter(match='scanner:*:latest_time'):
                try:
                    current = r.get(key) or ''
                    prev = last_times.get(key, '')
                    if current and current != prev:
                        last_times[key] = current
                        feed = key.split(':')[1]
                        feed_name = FEED_NAMES.get(feed, feed.upper())
                        # Filter to subscribers who opted in to this feed
                        # (empty prefs list = receive all feeds)
                        subs_with_prefs = push_db.list_subscriptions_with_prefs()
                        targeted = [
                            sub for sub, prefs in subs_with_prefs
                            if not prefs or feed in prefs
                        ]
                        if targeted:
                            job = json.dumps({
                                'title': f'📻 {feed_name}',
                                'message': 'New call recorded',
                                'feed': feed,
                                'targeted_endpoints': [s.get('endpoint') for s in targeted],
                            })
                            r.lpush('push_queue', job)
                            worker_logger.info(f"New call on {feed} — queued push for {len(targeted)} subscriber(s)")
                except Exception as key_err:
                    worker_logger.warning(f"Error processing key {key}: {key_err}")
        except redis.RedisError as e:
            worker_logger.error(f"Redis error in watcher: {e}")
        except Exception as e:
            worker_logger.error(f"Unexpected watcher error: {e}")

        socketio.sleep(5)  # Poll every 5 seconds


def init_sockets(app, redis_client, all_feeds_list, all_department_ids_list, local_timezone):
    """Initializes the SocketIO extension and starts background workers."""
    global r, ALL_FEEDS, ALL_DEPARTMENT_IDS, LOCAL_TIMEZONE
    r = redis_client
    ALL_FEEDS = all_feeds_list
    ALL_DEPARTMENT_IDS = all_department_ids_list
    LOCAL_TIMEZONE = local_timezone
    
    socketio.init_app(app, async_mode='eventlet', cors_allowed_origins="*")
    
    # Start background workers
    logger.info("Starting socket background workers...")
    socketio.start_background_task(target=push_worker)
    socketio.start_background_task(target=transmitting_worker)
    socketio.start_background_task(target=new_call_watcher)
