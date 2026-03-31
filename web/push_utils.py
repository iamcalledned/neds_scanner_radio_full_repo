import os
import json
from base64 import urlsafe_b64encode, urlsafe_b64decode
from pywebpush import webpush, WebPushException
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
import base64

VAPID_PUBLIC_FILE = os.path.join(os.path.dirname(__file__), 'vapid_public.key')
VAPID_PRIVATE_FILE = os.path.join(os.path.dirname(__file__), 'vapid_private.key')

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
    # Debug: log input shapes (do not log secrets in production)
    try:
        pk_type = type(vapid_private_key)
        print('send_push: vapid_private_key type=', pk_type)
        if isinstance(vapid_private_key, (bytes, bytearray)):
            print('send_push: vapid_private_key bytes len=', len(vapid_private_key))
        else:
            print('send_push: vapid_private_key str len=', len(vapid_private_key) if vapid_private_key else 0)
    except Exception:
        pass

    try:
        endpoint = subscription_info.get('endpoint') if isinstance(subscription_info, dict) else 'unknown'
        print('send_push: endpoint=', endpoint[:120])
        keys = subscription_info.get('keys') if isinstance(subscription_info, dict) else None
        if keys:
            p256 = keys.get('p256dh')
            auth = keys.get('auth')
            print('send_push: p256dh len=', len(p256) if p256 else None, ' auth len=', len(auth) if auth else None)
    except Exception:
        pass

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
        # capture initial error text
        try:
            err_text = ex.response.text if hasattr(ex, 'response') and ex.response is not None else str(ex)
        except Exception:
            err_text = str(ex)
        print('WebPush failed (initial attempt):', err_text)
        # Try a fallback: some versions of the underlying crypto expect the private
        # key as a base64url-encoded raw private scalar (32 bytes). Extract the
        # scalar from the PEM and try again.
        try:
            if isinstance(vapid_private_key, (bytes, bytearray)):
                pem = vapid_private_key
            else:
                pem = vapid_private_key.encode('utf-8')
            priv = serialization.load_pem_private_key(pem, password=None)
            priv_nums = priv.private_numbers().private_value
            raw = priv_nums.to_bytes(32, 'big')
            raw_b64 = base64.urlsafe_b64encode(raw).rstrip(b'=').decode('ascii')
            print('Retrying webpush with raw base64url private scalar (len=', len(raw), ')')
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
            print('WebPush failed (raw scalar attempt):', err2)
            return False, err_text + ' || ' + err2
    except Exception as e:
        print('send_push unexpected error', e)
        return False, str(e)
