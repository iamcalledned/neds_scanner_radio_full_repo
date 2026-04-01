from flask import Blueprint, request, jsonify, send_file
import os
import json
import logging
from . import routes_scanner as scanner_routes
import push_db
import push_utils
import redis

push_bp = Blueprint('push', __name__)
logger = logging.getLogger("scanner_web.routes_push")

REDIS_URL = os.environ.get('REDIS_URL', 'redis://127.0.0.1:6379/0')
redis_client = redis.from_url(REDIS_URL)

VAPID_PUBLIC_FILE = os.path.join(os.path.dirname(__file__), '..', 'vapid_public.key')
VAPID_PRIVATE_FILE = os.path.join(os.path.dirname(__file__), '..', 'vapid_private.key')


@push_bp.route('/scanner/push/vapid_public')
def get_vapid_public():
    if os.path.exists(VAPID_PUBLIC_FILE):
        # return the raw base64url public key as plain text so the client can consume it directly
        with open(VAPID_PUBLIC_FILE, 'r') as f:
            key = f.read().strip()
        logger.debug("push.vapid_public_served")
        return (key, 200, {'Content-Type': 'text/plain; charset=utf-8'})
    return jsonify({'error': 'no key'}), 404


@push_bp.route('/scanner/push/subscribe', methods=['POST'])
def subscribe():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'invalid json'}), 400
    push_db.save_subscription(data)
    logger.info("push.subscribe endpoint=%s", str(data.get('endpoint', ''))[:120])
    return jsonify({'success': True})


@push_bp.route('/scanner/push/unsubscribe', methods=['POST'])
def unsubscribe():
    data = request.get_json()
    endpoint = data.get('endpoint')
    push_db.remove_subscription(endpoint)
    logger.info("push.unsubscribe endpoint=%s", str(endpoint or '')[:120])
    return jsonify({'success': True})


# Canonical channel list — single source of truth for the whole push system
CHANNELS = [
    {'id': 'pd',    'label': 'Hopedale PD',    'town': 'Hopedale',    'type': 'police'},
    {'id': 'fd',    'label': 'Hopedale FD',    'town': 'Hopedale',    'type': 'fire'},
    {'id': 'mpd',   'label': 'Milford PD',     'town': 'Milford',     'type': 'police'},
    {'id': 'mfd',   'label': 'Milford FD',     'town': 'Milford',     'type': 'fire'},
    {'id': 'bpd',   'label': 'Bellingham PD',  'town': 'Bellingham',  'type': 'police'},
    {'id': 'bfd',   'label': 'Bellingham FD',  'town': 'Bellingham',  'type': 'fire'},
    {'id': 'mndpd', 'label': 'Mendon PD',      'town': 'Mendon',      'type': 'police'},
    {'id': 'mndfd', 'label': 'Mendon FD',      'town': 'Mendon',      'type': 'fire'},
    {'id': 'uptpd', 'label': 'Upton PD',       'town': 'Upton',       'type': 'police'},
    {'id': 'uptfd', 'label': 'Upton FD',       'town': 'Upton',       'type': 'fire'},
    {'id': 'blkpd', 'label': 'Blackstone PD',  'town': 'Blackstone',  'type': 'police'},
    {'id': 'blkfd', 'label': 'Blackstone FD',  'town': 'Blackstone',  'type': 'fire'},
    {'id': 'frkpd', 'label': 'Franklin PD',    'town': 'Franklin',    'type': 'police'},
    {'id': 'frkfd', 'label': 'Franklin FD',    'town': 'Franklin',    'type': 'fire'},
    {'id': 'milpd', 'label': 'Millis PD',      'town': 'Millis',      'type': 'police'},
    {'id': 'milfd', 'label': 'Millis FD',      'town': 'Millis',      'type': 'fire'},
    {'id': 'medpd', 'label': 'Medway PD',      'town': 'Medway',      'type': 'police'},
    {'id': 'medfd', 'label': 'Medway FD',      'town': 'Medway',      'type': 'fire'},
    {'id': 'foxpd', 'label': 'Foxborough PD',  'town': 'Foxborough',  'type': 'police'},
    {'id': 'sfd',   'label': 'Southborough FD','town': 'Southborough','type': 'fire'},
]


@push_bp.route('/scanner/push/channels')
def get_channels():
    """Return the list of available notification channels grouped by town."""
    # Group by town
    towns = {}
    for ch in CHANNELS:
        t = ch['town']
        if t not in towns:
            towns[t] = []
        towns[t].append({'id': ch['id'], 'label': ch['label'], 'type': ch['type']})
    logger.debug("push.channels_requested count=%s", len(CHANNELS))
    return jsonify({'channels': CHANNELS, 'by_town': towns})


@push_bp.route('/scanner/push/prefs', methods=['GET'])
def get_prefs():
    """Return saved feed prefs for the given endpoint."""
    endpoint = request.args.get('endpoint', '')
    if not endpoint:
        return jsonify({'error': 'endpoint required'}), 400
    prefs = push_db.get_prefs(endpoint)
    logger.debug("push.prefs_get endpoint=%s feeds=%s", endpoint[:120], len(prefs))
    return jsonify({'feeds': prefs})


@push_bp.route('/scanner/push/prefs', methods=['POST'])
def save_prefs():
    """Save feed prefs (list of feed IDs) for the given endpoint."""
    data = request.get_json() or {}
    endpoint = data.get('endpoint', '')
    feeds = data.get('feeds', [])
    if not endpoint:
        return jsonify({'error': 'endpoint required'}), 400
    if not isinstance(feeds, list):
        return jsonify({'error': 'feeds must be a list'}), 400
    push_db.save_prefs(endpoint, feeds)
    logger.info("push.prefs_saved endpoint=%s feeds=%s", endpoint[:120], len(feeds))
    return jsonify({'success': True})


@push_bp.route('/scanner/push/send', methods=['POST'])
def send_push():
    data = request.get_json() or {}
    message = data.get('message', 'Test push')
    # push job to redis list
    redis_client.lpush('push_queue', json.dumps({'message': message}))
    logger.info("push.send_queued")
    return jsonify({'queued': True})


@push_bp.route('/scanner/push/send_now', methods=['POST'])
def send_push_now():
    """Send a push to all stored subscriptions immediately (useful for testing).

    WARNING: this will attempt to send to every subscription in the DB and will
    perform network calls synchronously. Intended for local testing only.
    """
    data = request.get_json() or {}
    message = data.get('message', 'Test push')
    vapid_pub, vapid_priv = push_utils.load_vapid_keys()
    if not vapid_priv:
        return jsonify({'error': 'VAPID private key not configured'}), 500
    vapid_claims = {'sub': 'mailto:admin@iamcalledned.ai'}
    results = []
    subs = push_db.list_subscriptions()
    logger.info("push.send_now recipients=%s", len(subs))
    for s in subs:
        try:
            ok, err = push_utils.send_push(s, {'message': message}, vapid_priv, vapid_claims)
            entry = {'endpoint': s.get('endpoint'), 'ok': bool(ok)}
            if err:
                entry['error'] = str(err)
            results.append(entry)
        except Exception as e:
            results.append({'endpoint': s.get('endpoint'), 'ok': False, 'error': str(e)})
    return jsonify({'sent': sum(1 for r in results if r.get('ok')), 'results': results})

