#!/usr/bin/env python3
"""
iq_scan.py - RTL-TCP IQ spectrum scanner / LO offset finder.

Run this BEFORE ned_sdr_receiver.py to see exactly where your signal sits
relative to DC in the captured IQ bandwidth.  It tunes the hardware directly
to the target frequency (no LO offset), captures ~0.5 s of IQ, and prints:
  - a text waterfall of the ±480 kHz band
  - the top signal peaks with SNR
  - the recommended LO_OFFSET_HZ to paste into ned_sdr_receiver.py

Usage:
    python3 iq_scan.py
"""

from __future__ import annotations

import math
import socket
import struct
import sys
import time

import numpy as np

# ── config ────────────────────────────────────────────────────────────────────
RTL_HOST        = "127.0.0.1"
RTL_PORT        = 12004
FREQUENCY_HZ    = 162_475_000   # target frequency – NO offset applied here
SAMPLE_RATE     = 960_000       # wide enough to show ±480 kHz
GAIN_TENTHS_DB  = 496           # 49.6 dB
CAPTURE_BLOCKS  = 30            # ~30 × 16384 / 960000 ≈ 0.51 s of IQ
IQ_BLOCK_SAMPLES = 16384
FFT_SIZE        = 8192
DISPLAY_STEP    = 16            # print every Nth FFT bin (text width ~80 cols)
TOP_N_PEAKS     = 12
# ──────────────────────────────────────────────────────────────────────────────


def send_cmd(sock: socket.socket, cmd: int, value: int) -> None:
    sock.sendall(struct.pack(">BI", cmd, value & 0xFFFF_FFFF))


def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise RuntimeError("rtl_tcp socket closed unexpectedly")
        buf.extend(chunk)
    return bytes(buf)


def u8_to_complex(raw: bytes) -> np.ndarray:
    iq = np.frombuffer(raw, dtype=np.uint8).astype(np.float32)
    iq = (iq - 127.5) / 128.0
    return (iq[0::2] + 1j * iq[1::2]).astype(np.complex64)


def ascii_bar(norm: float, width: int = 36) -> str:
    filled = min(width, max(0, int(round(norm * width))))
    return "█" * filled + "░" * (width - filled)


def main() -> int:
    print(f"[iq_scan] Connecting to rtl_tcp at {RTL_HOST}:{RTL_PORT} …")
    try:
        sock = socket.create_connection((RTL_HOST, RTL_PORT), timeout=10)
    except OSError as exc:
        print(f"[iq_scan] ERROR: cannot connect – {exc}")
        return 1
    sock.settimeout(8.0)

    header = recv_exact(sock, 12)
    magic = header[:4]
    tuner_type, gain_count = struct.unpack(">II", header[4:])
    print(f"[iq_scan] Header: magic={magic!r}  tuner_type={tuner_type}  gains={gain_count}")

    # Configure hardware - tune directly to target, NO LO offset
    send_cmd(sock, 0x01, FREQUENCY_HZ)      # set freq
    send_cmd(sock, 0x02, SAMPLE_RATE)        # set sample rate
    send_cmd(sock, 0x05, 0)                  # PPM = 0
    send_cmd(sock, 0x08, 0)                  # AGC off
    send_cmd(sock, 0x03, 1)                  # manual gain mode
    send_cmd(sock, 0x04, GAIN_TENTHS_DB)     # gain

    print(f"[iq_scan] Tuned to {FREQUENCY_HZ/1e6:.6f} MHz  SR={SAMPLE_RATE} Hz  bw=±{SAMPLE_RATE//2} Hz")
    print(f"[iq_scan] Capturing {CAPTURE_BLOCKS} blocks ({CAPTURE_BLOCKS*IQ_BLOCK_SAMPLES/SAMPLE_RATE:.2f} s) …")

    time.sleep(0.15)   # let AGC/gain settle

    iq_list = []
    for i in range(CAPTURE_BLOCKS):
        raw = recv_exact(sock, IQ_BLOCK_SAMPLES * 2)
        iq_list.append(u8_to_complex(raw))
    sock.close()

    iq = np.concatenate(iq_list)
    # Remove DC bias spike
    iq -= iq.mean()

    # ── average power spectrum ────────────────────────────────────────────────
    window = np.hanning(FFT_SIZE).astype(np.float32)
    n_chunks = len(iq) // FFT_SIZE
    power = np.zeros(FFT_SIZE, dtype=np.float64)
    for k in range(n_chunks):
        seg = iq[k * FFT_SIZE:(k + 1) * FFT_SIZE] * window
        power += np.abs(np.fft.fft(seg)) ** 2
    power /= max(1, n_chunks)
    power_db = 10.0 * np.log10(power + 1e-30)
    power_db = np.fft.fftshift(power_db)

    freqs = np.fft.fftshift(np.fft.fftfreq(FFT_SIZE, d=1.0 / SAMPLE_RATE))

    noise_db  = float(np.percentile(power_db, 15))
    peak_db   = float(power_db.max())
    span_db   = max(1.0, peak_db - noise_db)

    # ── text waterfall ────────────────────────────────────────────────────────
    print()
    print("=" * 72)
    print(f"  SPECTRUM  centre={FREQUENCY_HZ/1e6:.3f} MHz  noise≈{noise_db:.1f} dB  peak={peak_db:.1f} dB")
    print("=" * 72)
    print(f"  {'offset Hz':>10}  {'dBrel':>6}  signal")
    print(f"  {'-'*10}  {'-'*6}  {'-'*36}")
    for i in range(0, FFT_SIZE, DISPLAY_STEP):
        f  = freqs[i]
        db = power_db[i]
        rel = db - noise_db
        bar = ascii_bar(rel / span_db)
        print(f"  {f:+10.0f}  {rel:+6.1f}  {bar}")
    print("=" * 72)

    # ── top peaks ─────────────────────────────────────────────────────────────
    # Simple peak finder: ignore bins within ±5 of an already-found peak
    found: list[tuple[float, float]] = []
    remaining = list(range(FFT_SIZE))
    pw = power_db.copy()
    for _ in range(TOP_N_PEAKS):
        if not remaining:
            break
        idx = int(np.argmax(pw))
        snr = pw[idx] - noise_db
        found.append((freqs[idx], snr))
        # suppress neighbourhood
        lo = max(0, idx - 5)
        hi = min(FFT_SIZE, idx + 6)
        pw[lo:hi] = noise_db

    print()
    print(f"  Top {TOP_N_PEAKS} peaks (after DC removal):")
    print(f"  {'offset Hz':>10}  {'SNR dB':>7}  note")
    print(f"  {'-'*10}  {'-'*7}  {'-'*30}")
    for freq_off, snr in sorted(found, key=lambda x: -x[1]):
        note = ""
        if abs(freq_off) < SAMPLE_RATE * 0.02:
            note = "<-- near DC (probably residual DC spike)"
        elif snr > 6:
            note = "<-- possible signal"
        print(f"  {freq_off:+10.0f}  {snr:7.1f}  {note}")

    # ── recommendation ────────────────────────────────────────────────────────
    # Best non-DC peak
    best_freq: float = 0.0
    best_snr: float  = 0.0
    for freq_off, snr in found:
        if abs(freq_off) > SAMPLE_RATE * 0.02 and snr > best_snr:
            best_snr  = snr
            best_freq = freq_off

    print()
    if best_snr > 3:
        lo_rec = -int(round(best_freq / 1000.0) * 1000)
        print(f"  >>> Best non-DC peak at {best_freq:+.0f} Hz  SNR={best_snr:.1f} dB <<<")
        print(f"  >>> Recommended setting in ned_sdr_receiver.py: <<<")
        print(f"  >>>   LO_OFFSET_HZ = {lo_rec}   (tunes hw {lo_rec:+d} Hz from target) <<<")
        print(f"  >>>   This puts the signal at {-lo_rec:+d} Hz in IQ, away from DC.  <<<")
    else:
        print("  >>> No clear signal found above noise floor. <<<")
        print("  >>> Check that rtl_tcp is running, gain is set, and antenna is connected. <<<")

    return 0


if __name__ == "__main__":
    sys.exit(main())
