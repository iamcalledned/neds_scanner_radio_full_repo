import os
import json
import logging
from base64 import urlsafe_b64encode, urlsafe_b64decode
from pywebpush import webpush, WebPushException
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
import base64

VAPID_PUBLIC_FILE = os.path.join(os.path.dirname(__file__), 'vapid_public.key')
VAPID_PRIVATE_FILE = os.path.join(os.path.dirname(__file__), 'vapid_private.key')
logger = logging.getLogger("scanner_web.push")

# Helper to load VAPID keys if present
def load_vapid_keys():
    # Return the public key (base64url string) and private key (PEM) as text.
    if os.path.exists(VAPID_PUBLIC_FILE) and os.path.exists(VAPID_PRIVATE_FILE):
        with open(VAPID_PUBLIC_FILE, 'r', encoding='utf-8') as f:
            public = f.read().strip()
        with open(VAPID_PRIVATE_FILE, 'r', encoding='utf-8') as f:
            private = f.read()
        return public, private
    return None, None


def send_push(subscription_info, payload, vapid_private_key, vapid_claims):
    endpoint = subscription_info.get('endpoint') if isinstance(subscription_info, dict) else 'unknown'

    try:
        webpush(
            subscription_info=subscription_info,
            data=json.dumps(payload),
            # pywebpush expects the private key as a PEM string
            vapid_private_key=(vapid_private_key.decode('utf-8') if isinstance(vapid_private_key, (bytes, bytearray)) else vapid_private_key),
            vapid_claims=vapid_claims,
            ttl=60
        )
        return True, None
    except Exception as ex:
        try:
            err_text = ex.response.text if hasattr(ex, 'response') and ex.response is not None else str(ex)
        except Exception:
            err_text = str(ex)
        logger.warning("WebPush failed (initial attempt) for %s: %s", endpoint[:120], err_text)
        try:
            if isinstance(vapid_private_key, (bytes, bytearray)):
                pem = vapid_private_key
            else:
                pem = vapid_private_key.encode('utf-8')
            priv = serialization.load_pem_private_key(pem, password=None)
            priv_nums = priv.private_numbers().private_value
            raw = priv_nums.to_bytes(32, 'big')
            raw_b64 = base64.urlsafe_b64encode(raw).rstrip(b'=').decode('ascii')
            logger.info("Retrying WebPush with raw VAPID scalar for %s", endpoint[:120])
            webpush(
                subscription_info=subscription_info,
                data=json.dumps(payload),
                vapid_private_key=raw_b64,
                vapid_claims=vapid_claims,
                ttl=60
            )
            return True, None
        except Exception as ex2:
            try:
                err2 = ex2.response.text if hasattr(ex2, 'response') and ex2.response is not None else str(ex2)
            except Exception:
                err2 = str(ex2)
            logger.warning("WebPush failed (raw scalar attempt) for %s: %s", endpoint[:120], err2)
            return False, err_text + ' || ' + err2
    except Exception as e:
        logger.exception("send_push unexpected error for %s", endpoint[:120])
        return False, str(e)
