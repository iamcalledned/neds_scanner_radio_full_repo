#!/usr/bin/env python3
"""
multi_channel_rtl_tcp_receiver.py

One rtl_tcp source -> many digital NFM channels.

What this does
- Connects to rtl_tcp once
- Tunes one hardware center frequency
- Captures one wide IQ stream
- Demodulates multiple NFM channels inside that RF window
- Sends each channel to its own Pulse sink
- Records each channel continuously to its own WAV file
- Closes WAV files cleanly on Ctrl-C

This is the architectural equivalent of what SDR++ is doing with multiple "radio"
modules on one wide source, except headless and less interested in wasting your time.

Requirements
- Python 3.10+
- pip install numpy scipy
- pactl, pacat available in PATH
- rtl_tcp already running on the desired port

Notes
- This version records continuously for each configured channel.
- It does NOT do squelch gating yet. That can be layered on later per channel.
- All channels must fit inside the sample-rate bandwidth around CENTER_FREQ_HZ.
"""

from __future__ import annotations

import math
import os
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import wave
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
from scipy.signal import butter, lfilter, lfilter_zi


# ============================================================================
# CONFIG
# ============================================================================

RTL_TCP_HOST = "127.0.0.1"
RTL_TCP_PORT = 12004

# One wide center frequency that covers all desired channels.
CENTER_FREQ_HZ = 471_800_000
SAMPLE_RATE = 2_400_000
AUDIO_RATE = 24_000
GAIN_TENTHS_DB = 496
PPM = 0

ARCHIVE_ROOT = Path.home() / "scanner_archive" / "raw" / "multi"

# DSP
AUDIO_LOWPASS_HZ = 3500.0
DEEMPHASIS_US = 75.0
CHANNEL_FILTER_HZ = 16_000.0
IQ_LIMITER_ENABLED = True

# rtl_tcp protocol commands
CMD_SET_FREQ = 0x01
CMD_SET_SAMPLE_RATE = 0x02
CMD_SET_GAIN_MODE = 0x03
CMD_SET_GAIN = 0x04
CMD_SET_FREQ_CORRECTION = 0x05
CMD_SET_AGC_MODE = 0x08

# Read size
IQ_BLOCK_SAMPLES = 32768
IQ_BLOCK_BYTES = IQ_BLOCK_SAMPLES * 2  # uint8 interleaved IQ

# Channels inside the capture window.
# Adjust these as needed.
CHANNELS = [
    {
        "name": "milford_police",
        "freq_hz": 472_600_000,
        "sink": "sdr_sink_milford_police",
        "record": True,
        "play": True,
    },
    {
        "name": "bellingham_pd",
        "freq_hz": 471_025_000,
        "sink": "sdr_sink_bellingham_pd",
        "record": True,
        "play": True,
    },
]

STOP_EVENT = threading.Event()


# ============================================================================
# UTILS
# ============================================================================


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)



def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)



def shutil_which(name: str) -> Optional[str]:
    for path in os.environ.get("PATH", "").split(os.pathsep):
        full = Path(path) / name
        if full.exists() and os.access(full, os.X_OK):
            return str(full)
    return None



def run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=check)



def float_to_pcm16(audio: np.ndarray) -> bytes:
    audio = np.clip(audio, -1.0, 1.0)
    pcm = (audio * 32767.0).astype(np.int16)
    return pcm.tobytes()



def dbfs_rms(samples_f32: np.ndarray) -> float:
    if samples_f32.size == 0:
        return 0.0
    return float(np.sqrt(np.mean(samples_f32 * samples_f32)))


# ============================================================================
# PULSE HELPERS
# ============================================================================


def ensure_null_sink(sink_name: str, description: Optional[str] = None) -> None:
    description = description or sink_name
    try:
        result = run_cmd(["pactl", "list", "short", "sinks"], check=True)
        if sink_name in result.stdout:
            return
    except Exception:
        pass

    run_cmd([
        "pactl", "load-module", "module-null-sink",
        f"sink_name={sink_name}",
        f"sink_properties=device.description={description}",
    ], check=True)
    log(f"Created Pulse sink: {sink_name}")



def start_pacat_playback(device_name: str, rate: int) -> subprocess.Popen:
    cmd = [
        "pacat",
        "--playback",
        "--raw",
        "--format=s16le",
        "--channels=1",
        f"--rate={rate}",
        f"--device={device_name}",
    ]
    log(f"Starting pacat playback -> {device_name}")
    return subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        bufsize=0,
    )


# ============================================================================
# RTL_TCP CLIENT
# ============================================================================


class RTLTCPClient:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None

    def connect(self) -> None:
        log(f"Connecting to rtl_tcp at {self.host}:{self.port}")
        self.sock = socket.create_connection((self.host, self.port), timeout=10)
        self.sock.settimeout(5.0)

        header = self._recv_exact(12)
        if len(header) != 12:
            raise RuntimeError("Failed to read rtl_tcp header")

        magic = header[:4]
        if magic != b"RTL0":
            log(f"WARNING: unexpected rtl_tcp header magic: {magic!r}")
        else:
            tuner_type, tuner_gain_count = struct.unpack(">II", header[4:])
            log(f"rtl_tcp header OK - tuner_type={tuner_type}, gain_count={tuner_gain_count}")

    def close(self) -> None:
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def _send_cmd(self, cmd: int, value: int) -> None:
        if not self.sock:
            raise RuntimeError("rtl_tcp socket not connected")
        payload = struct.pack(">BI", cmd, value & 0xFFFFFFFF)
        self.sock.sendall(payload)

    def configure(
        self,
        frequency_hz: int,
        sample_rate: int,
        gain_tenths_db: int,
        ppm: int = 0,
        manual_gain: bool = True,
        agc: bool = False,
    ) -> None:
        self._send_cmd(CMD_SET_FREQ, frequency_hz)
        self._send_cmd(CMD_SET_SAMPLE_RATE, sample_rate)
        self._send_cmd(CMD_SET_FREQ_CORRECTION, ppm)
        self._send_cmd(CMD_SET_AGC_MODE, 1 if agc else 0)

        if manual_gain:
            self._send_cmd(CMD_SET_GAIN_MODE, 1)
            self._send_cmd(CMD_SET_GAIN, gain_tenths_db)
        else:
            self._send_cmd(CMD_SET_GAIN_MODE, 0)

    def recv_iq_bytes(self, nbytes: int) -> bytes:
        return self._recv_exact(nbytes)

    def _recv_exact(self, nbytes: int) -> bytes:
        if not self.sock:
            raise RuntimeError("rtl_tcp socket not connected")

        chunks: list[bytes] = []
        remaining = nbytes
        while remaining > 0 and not STOP_EVENT.is_set():
            try:
                data = self.sock.recv(remaining)
            except socket.timeout:
                continue
            if not data:
                raise RuntimeError("rtl_tcp socket closed")
            chunks.append(data)
            remaining -= len(data)
        return b"".join(chunks)


# ============================================================================
# DSP
# ============================================================================


class NFMProcessor:
    def __init__(
        self,
        sample_rate: int,
        audio_rate: int,
        channel_offset_hz: float,
        channel_lowpass_hz: float = CHANNEL_FILTER_HZ,
        iq_limiter_enabled: bool = IQ_LIMITER_ENABLED,
        audio_lowpass_hz: float = AUDIO_LOWPASS_HZ,
        deemphasis_us: float = DEEMPHASIS_US,
    ) -> None:
        self.sample_rate = sample_rate
        self.audio_rate = audio_rate
        self.channel_offset_hz = float(channel_offset_hz)
        self.iq_limiter_enabled = iq_limiter_enabled
        self.decim = sample_rate // audio_rate
        if sample_rate % audio_rate != 0:
            raise ValueError("SAMPLE_RATE must be an integer multiple of AUDIO_RATE")

        # RF channel filter
        rf_nyq = sample_rate / 2.0
        rf_cutoff = min(channel_lowpass_hz / rf_nyq, 0.99)
        self.b_rf, self.a_rf = butter(4, rf_cutoff, btype="low")
        self.zi_rf = lfilter_zi(self.b_rf, self.a_rf).astype(np.complex64)

        # NCO for channel shift
        self.nco_phase = 0.0
        self.nco_step = 2.0 * math.pi * self.channel_offset_hz / float(sample_rate)

        # Audio LPF
        nyq = audio_rate / 2.0
        cutoff = min(audio_lowpass_hz / nyq, 0.99)
        self.b_audio, self.a_audio = butter(4, cutoff, btype="low")
        self.zi_audio = lfilter_zi(self.b_audio, self.a_audio).astype(np.float32)

        # De-emphasis
        tau = deemphasis_us * 1e-6
        dt = 1.0 / audio_rate
        self.deemph_alpha = dt / (tau + dt)
        self.deemph_prev = 0.0

        self.prev_iq: Optional[np.complex64] = None
        self.last_residual_hz = 0.0

    def u8_iq_to_complex(self, raw: bytes) -> np.ndarray:
        iq_u8 = np.frombuffer(raw, dtype=np.uint8)
        iq = iq_u8.astype(np.float32)
        iq = (iq - 127.5) / 128.0
        i = iq[0::2]
        q = iq[1::2]
        return (i + 1j * q).astype(np.complex64)

    def fm_demod(self, iq: np.ndarray) -> np.ndarray:
        if iq.size == 0:
            return np.array([], dtype=np.float32)

        if self.prev_iq is None:
            prev = iq[:-1]
            curr = iq[1:]
        else:
            iq2 = np.concatenate(([self.prev_iq], iq))
            prev = iq2[:-1]
            curr = iq2[1:]

        self.prev_iq = iq[-1]
        demod = np.angle(curr * np.conj(prev)).astype(np.float32)
        demod *= 0.8
        return demod

    def process_complex_block(self, iq: np.ndarray) -> np.ndarray:
        if iq.size == 0:
            return np.array([], dtype=np.float32)

        # remove DC
        iq = iq - iq.mean()

        # mix target channel to DC
        n = iq.size
        phase = self.nco_phase + self.nco_step * np.arange(n, dtype=np.float32)
        iq = iq * np.exp(-1j * phase).astype(np.complex64)
        self.nco_phase = (self.nco_phase + self.nco_step * n) % (2.0 * math.pi)

        # narrow RF filter before demod
        iq, self.zi_rf = lfilter(self.b_rf, self.a_rf, iq, zi=self.zi_rf)

        if iq.size > 1:
            ph = np.angle(iq[1:] * np.conj(iq[:-1]))
            self.last_residual_hz = float(np.mean(ph) * self.sample_rate / (2.0 * math.pi))

        if self.iq_limiter_enabled:
            mag = np.abs(iq)
            iq = iq / np.maximum(mag, 1e-6)

        demod = self.fm_demod(iq)
        if demod.size == 0:
            return np.array([], dtype=np.float32)

        audio = demod[::self.decim]
        audio, self.zi_audio = lfilter(self.b_audio, self.a_audio, audio, zi=self.zi_audio)

        out = np.empty_like(audio, dtype=np.float32)
        prev = self.deemph_prev
        a = self.deemph_alpha
        for idx, x in enumerate(audio):
            prev = prev + a * (float(x) - prev)
            out[idx] = prev
        self.deemph_prev = prev

        out *= 0.90
        return np.clip(out, -1.0, 1.0).astype(np.float32)


# ============================================================================
# CHANNEL OUTPUT
# ============================================================================


@dataclass
class ChannelConfig:
    name: str
    freq_hz: int
    sink: str
    record: bool = True
    play: bool = True


class ChannelOutput:
    def __init__(self, cfg: ChannelConfig, center_freq_hz: int, sample_rate: int, audio_rate: int) -> None:
        self.cfg = cfg
        self.center_freq_hz = center_freq_hz
        self.sample_rate = sample_rate
        self.audio_rate = audio_rate
        self.offset_hz = cfg.freq_hz - center_freq_hz

        half_bw = sample_rate / 2.0
        if abs(self.offset_hz) >= half_bw:
            raise ValueError(
                f"Channel {cfg.name} at offset {self.offset_hz:+,} Hz does not fit inside ±{half_bw:,.0f} Hz"
            )

        self.processor = NFMProcessor(
            sample_rate=sample_rate,
            audio_rate=audio_rate,
            channel_offset_hz=float(self.offset_hz),
        )

        self.play_proc: Optional[subprocess.Popen] = None
        self.wave_fp: Optional[wave.Wave_write] = None
        self.file_path: Optional[Path] = None
        self.last_diag_log = time.monotonic()

    def start(self) -> None:
        ensure_null_sink(self.cfg.sink, description=self.cfg.name)

        if self.cfg.play:
            self.play_proc = start_pacat_playback(self.cfg.sink, self.audio_rate)
            if not self.play_proc.stdin:
                raise RuntimeError(f"Sink stdin unavailable for {self.cfg.name}")

        if self.cfg.record:
            ensure_dir(ARCHIVE_ROOT / self.cfg.name)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.file_path = ARCHIVE_ROOT / self.cfg.name / f"{self.cfg.name}_{ts}.wav"
            wf = wave.open(str(self.file_path), "wb")
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(self.audio_rate)
            self.wave_fp = wf
            log(f"Recording {self.cfg.name} -> {self.file_path}")

        log(
            f"Channel ready: {self.cfg.name} freq={self.cfg.freq_hz/1e6:.6f} MHz "
            f"offset={self.offset_hz:+,} Hz sink={self.cfg.sink}"
        )

    def process_and_output(self, iq: np.ndarray) -> None:
        audio = self.processor.process_complex_block(iq)
        if audio.size == 0:
            return

        pcm = float_to_pcm16(audio)

        if self.wave_fp:
            self.wave_fp.writeframes(pcm)

        if self.play_proc and self.play_proc.stdin:
            try:
                self.play_proc.stdin.write(pcm)
                self.play_proc.stdin.flush()
            except BrokenPipeError as exc:
                raise RuntimeError(f"Playback pipe broke for {self.cfg.name}") from exc

        now = time.monotonic()
        if now - self.last_diag_log >= 2.0:
            self.last_diag_log = now
            rms = dbfs_rms(audio)
            print(
                f"CH {self.cfg.name}: rms={rms:.4f} residual={self.processor.last_residual_hz:+8.1f} Hz",
                flush=True,
            )

    def stop(self) -> None:
        if self.wave_fp:
            try:
                self.wave_fp.close()
                if self.file_path:
                    log(f"Closed file for {self.cfg.name} -> {self.file_path}")
            except Exception:
                pass
            self.wave_fp = None

        if self.play_proc:
            try:
                if self.play_proc.stdin:
                    self.play_proc.stdin.close()
            except Exception:
                pass
            try:
                self.play_proc.terminate()
            except Exception:
                pass
            self.play_proc = None


# ============================================================================
# MAIN RECEIVER
# ============================================================================


class MultiChannelReceiver:
    def __init__(self) -> None:
        self.client = RTLTCPClient(RTL_TCP_HOST, RTL_TCP_PORT)
        self.channels = [
            ChannelOutput(
                cfg=ChannelConfig(**ch),
                center_freq_hz=CENTER_FREQ_HZ,
                sample_rate=SAMPLE_RATE,
                audio_rate=AUDIO_RATE,
            )
            for ch in CHANNELS
        ]

    def start(self) -> None:
        for ch in self.channels:
            ch.start()

        self.client.connect()
        self.client.configure(
            frequency_hz=CENTER_FREQ_HZ,
            sample_rate=SAMPLE_RATE,
            gain_tenths_db=GAIN_TENTHS_DB,
            ppm=PPM,
            manual_gain=True,
            agc=False,
        )

        log(
            f"Receiver running: center={CENTER_FREQ_HZ/1e6:.6f} MHz "
            f"bw=±{SAMPLE_RATE//2:,} Hz sample_rate={SAMPLE_RATE} audio_rate={AUDIO_RATE}"
        )

    def run(self) -> None:
        self.start()
        try:
            while not STOP_EVENT.is_set():
                raw = self.client.recv_iq_bytes(IQ_BLOCK_BYTES)
                iq = self.channels[0].processor.u8_iq_to_complex(raw)
                for ch in self.channels:
                    ch.process_and_output(iq)
        finally:
            self.stop()

    def stop(self) -> None:
        for ch in self.channels:
            ch.stop()
        self.client.close()


# ============================================================================
# ENTRYPOINT
# ============================================================================


def signal_handler(signum, frame) -> None:
    log(f"Received signal {signum}, shutting down")
    STOP_EVENT.set()



def sanity_checks() -> None:
    for exe in ("pacat", "pactl"):
        if not shutil_which(exe):
            raise RuntimeError(f"Required executable not found in PATH: {exe}")

    if SAMPLE_RATE % AUDIO_RATE != 0:
        raise RuntimeError("SAMPLE_RATE must be an integer multiple of AUDIO_RATE")

    if not CHANNELS:
        raise RuntimeError("No channels configured")

    ensure_dir(ARCHIVE_ROOT)



def main() -> int:
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    sanity_checks()

    rx = MultiChannelReceiver()
    try:
        rx.run()
    except KeyboardInterrupt:
        STOP_EVENT.set()
    except Exception as exc:
        if not STOP_EVENT.is_set():
            log(f"FATAL: {exc}")
        STOP_EVENT.set()
        return 1
    finally:
        rx.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
