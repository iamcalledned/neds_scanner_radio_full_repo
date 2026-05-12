import os
import redis
import glob
from datetime import datetime, timedelta

# Config
REDIS_URL = "redis://127.0.0.1:6379/0"
STREAM_KEY = "scanner:stream:new_call"
ARCHIVE_ROOT = "/home/ned/data/scanner_calls/scanner_archive/raw/"

# Time window: last 4 hours
since = datetime.now() - timedelta(hours=4)

# Helper to extract timestamp from filename
def extract_timestamp(fname):
    # Example: rec_2026-04-30_11-51-56_*.wav
    try:
        base = os.path.basename(fname)
        parts = base.split('_')
        if len(parts) < 3:
            return None
        date_str = parts[1]  # 2026-04-30
        time_str = parts[2]  # 11-51-56
        dt = datetime.strptime(f"{date_str}_{time_str}", "%Y-%m-%d_%H-%M-%S")
        return dt
    except Exception:
        return None

# Find all .wav files in archive
files = glob.glob(os.path.join(ARCHIVE_ROOT, "**", "rec_*.wav"), recursive=True)

# Filter files by timestamp in last 4 hours
selected = []
for f in files:
    ts = extract_timestamp(f)
    if ts and ts >= since:
        selected.append((f, ts))

# Connect to Redis
r = redis.from_url(REDIS_URL)

# Push to Redis stream
for fpath, ts in selected:
    r.xadd(
        STREAM_KEY,
        {
            "file": fpath,
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        }
    )
    print(f"Zapped: {fpath} at {ts}")

print(f"Done. Zapped {len(selected)} .wav files from the last 4 hours.")