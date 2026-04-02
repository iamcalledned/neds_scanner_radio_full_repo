import subprocess
from pathlib import Path
import os

RMS_THRESHOLD = float(os.environ.get("RMS_THRESHOLD", "0.001"))



def preprocess_audio(inp: Path, outp: Path, profile: str = "default") -> None:
    """
    Preprocess using ffmpeg. Profiles let you tune filters without changing code.
    """
    profiles = {
        "default": "highpass=f=100,volume=5dB",
        "radio": "highpass=f=120,lowpass=f=3500,afftdn=nf=-25,volume=6dB",
        "static_fix": "highpass=f=150,lowpass=f=3200,afftdn=nf=-30,volume=7dB",
        "aggressive": "highpass=f=200,lowpass=f=3000,afftdn=nf=-35,acompressor=threshold=-20dB:ratio=4,volume=8dB",
    }
    af = profiles.get(profile, profiles["default"])

    subprocess.run(
        ["ffmpeg", "-y", "-i", str(inp), "-ac", "1", "-ar", "16000", "-af", af, str(outp)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True
    )

# ==========================
# Audio helpers
# ==========================
def get_duration(path: Path) -> float:
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=False
        )
        return float((r.stdout or "").strip() or "0")
    except Exception:
        return 0.0


def get_rms(path: Path) -> float:
    """Compute RMS amplitude using SoX."""
    try:
        r = subprocess.run(
            ["sox", "-t", "wav", str(path), "-n", "stat"],
            stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True, check=False
        )
        for line in r.stderr.splitlines():
            if "RMS" in line and "amplitude" in line:
                return float(line.split(":")[1].strip())
    except Exception:
        pass
    return 0.0


def is_static(path: Path) -> bool:
    """Skip only true dead-air."""
    try:
        return get_rms(path) < RMS_THRESHOLD
    except Exception:
        return False
