#!/usr/bin/env python3
import contextlib
import wave
import webrtcvad
import collections
import numpy as np
import json
import shutil
import torch
import requests
from pathlib import Path
from resemblyzer import VoiceEncoder, preprocess_wav
from sklearn.cluster import KMeans
from textwrap import fill

# ====== CONFIG ======
TMP_DIR = Path("/home/ned/data/town_hall_streams")
TEXT_WRAP_WIDTH = 100
FRAME_MS = 30
VAD_AGGRESSIVENESS = 2
VAD_PADDING_MS = 700
MERGE_GAP_MS = 600
MAX_SPEAKERS = 4
MIN_SEG_SEC = 0.25

MCP_URL = "http://127.0.0.1:8000/interactive/transcribe-batch"
MCP_TIMEOUT = 300
MCP_TOOL_NAME = "interactive_transcribe_batch"
DEFAULT_MODEL_CATALOG = Path(
    "/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/transcriber/model_catalog.json"
)

MCP_PROFILE = "default"
MCP_LANGUAGE = "en"
MCP_WRITE_ARTIFACTS = False
MCP_INSERT_DB = False
MCP_DELETE_SOURCE_RAW = False
MCP_SKIP_WAV_COPY = True
MCP_AUTO_ROUTE = False


# ====== MODEL SELECTION ======
def load_model_catalog(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Model catalog not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) or "models" not in data or not isinstance(data["models"], dict):
        raise ValueError("Model catalog must contain a top-level 'models' object.")

    return data


def choose_model(catalog: dict):
    models = catalog.get("models", {})
    default_key = catalog.get("default_model")
    keys = []
    for key, cfg in models.items():
        if not isinstance(cfg, dict):
            continue
        if str(cfg.get("kind") or "").strip().lower() == "chat":
            continue
        keys.append(key)

    if not keys:
        raise ValueError("Model catalog contains no models.")

    print("\nAvailable models:")
    for i, key in enumerate(keys, start=1):
        model_value = models[key].get("model", "<missing model>")
        marker = " (default)" if key == default_key else ""
        print(f"  {i}. {key} -> {model_value}{marker}")

    while True:
        choice = input("Select model number (Enter for default): ").strip()

        if not choice:
            selected_key = default_key if default_key in models else keys[0]
            break

        if not choice.isdigit():
            print("[!] Please enter a valid number.")
            continue

        idx = int(choice) - 1
        if idx < 0 or idx >= len(keys):
            print("[!] Selection out of range.")
            continue

        selected_key = keys[idx]
        break

    selected_cfg = models[selected_key]
    selected_model_value = selected_cfg.get("model")
    if not selected_model_value:
        raise ValueError(f"Selected model '{selected_key}' is missing its 'model' value.")

    print(f"[*] Selected model key: {selected_key}")
    print(f"[*] MCP model value: {selected_model_value}")
    return selected_key, selected_model_value, selected_cfg


# ====== FILE / AUDIO HELPERS ======
def reset_tmp_dir(tmp_dir: Path):
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)


def read_wave(path: Path):
    with contextlib.closing(wave.open(str(path), "rb")) as wf:
        assert wf.getnchannels() == 1, "Audio must be mono"
        assert wf.getsampwidth() == 2, "Audio must be 16-bit PCM"
        assert wf.getframerate() == 16000, "Audio must be 16kHz"
        pcm_data = wf.readframes(wf.getnframes())
        return pcm_data, wf.getframerate()


def frame_generator(wav_bytes, rate, frame_duration_ms):
    frame_bytes = int(rate * frame_duration_ms / 1000) * 2
    offset = 0
    timestamp = 0.0
    duration = frame_duration_ms / 1000.0

    while offset + frame_bytes <= len(wav_bytes):
        yield {
            "bytes": wav_bytes[offset: offset + frame_bytes],
            "timestamp": timestamp,
            "duration": duration,
        }
        offset += frame_bytes
        timestamp += duration


def format_timestamp(seconds: float) -> str:
    total_ms = int(round(seconds * 1000))
    hours, rem = divmod(total_ms, 3_600_000)
    minutes, rem = divmod(rem, 60_000)
    secs, ms = divmod(rem, 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}.{ms:03d}"


# ====== VAD ======
def vad_collector(rate, frame_duration_ms, padding_ms, vad, frames):
    num_padding_frames = padding_ms // frame_duration_ms
    ring_buffer = collections.deque(maxlen=num_padding_frames)
    triggered = False
    voiced_segments = []
    current_frames = []
    segment_start = None

    for frame in frames:
        is_speech = vad.is_speech(frame["bytes"], rate)

        if not triggered:
            ring_buffer.append((frame, is_speech))
            num_voiced = sum(1 for _, speech in ring_buffer if speech)

            if ring_buffer.maxlen and num_voiced > 0.9 * ring_buffer.maxlen:
                triggered = True
                first_frame = ring_buffer[0][0]
                segment_start = first_frame["timestamp"]
                current_frames = [f["bytes"] for f, _ in ring_buffer]
                ring_buffer.clear()
        else:
            current_frames.append(frame["bytes"])
            ring_buffer.append((frame, is_speech))
            num_unvoiced = sum(1 for _, speech in ring_buffer if not speech)

            if ring_buffer.maxlen and num_unvoiced > 0.9 * ring_buffer.maxlen:
                segment_end = frame["timestamp"] + frame["duration"]
                voiced_segments.append({
                    "start": segment_start,
                    "end": segment_end,
                    "audio": b"".join(current_frames),
                })
                triggered = False
                ring_buffer.clear()
                current_frames = []
                segment_start = None

    if current_frames:
        last_frame_end = frames[-1]["timestamp"] + frames[-1]["duration"] if frames else 0.0
        voiced_segments.append({
            "start": segment_start if segment_start is not None else 0.0,
            "end": last_frame_end,
            "audio": b"".join(current_frames),
        })

    return voiced_segments


def merge_close_segments(segments, gap_ms):
    if not segments:
        return []

    merged = [segments[0].copy()]
    max_gap_sec = gap_ms / 1000.0

    for seg in segments[1:]:
        prev = merged[-1]
        gap = seg["start"] - prev["end"]

        if gap <= max_gap_sec:
            prev["audio"] += seg["audio"]
            prev["end"] = seg["end"]
        else:
            merged.append(seg.copy())

    return merged


def save_wav(segment_bytes, rate, out_path: Path):
    with wave.open(str(out_path), "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(rate)
        wf.writeframes(segment_bytes)


# ====== MCP HELPERS ======
def extract_text_from_mcp_response(payload):
    if not isinstance(payload, dict):
        return str(payload)

    for key in ("text", "transcript", "result", "output"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("text", "transcript", "result", "output"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    content = payload.get("content")
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text") or item.get("content")
                if isinstance(text, str) and text.strip():
                    parts.append(text.strip())
        if parts:
            return "\n".join(parts)

    return ""


def extract_ok_from_mcp_response(payload):
    if isinstance(payload, dict):
        if "ok" in payload:
            return bool(payload.get("ok"))
        data = payload.get("data")
        if isinstance(data, dict) and "ok" in data:
            return bool(data.get("ok"))
    return None


def call_mcp_transcriber_batch(segment_items: list[dict], source_audio: Path, model_key: str):
    payload = {
        "model_key": model_key,
        "profile": MCP_PROFILE,
        "language": MCP_LANGUAGE,
        "write_artifacts": MCP_WRITE_ARTIFACTS,
        "custom_output_dir": "",
        "segments": segment_items,
        "source_audio": str(source_audio.resolve()),
    }

    response = requests.post(MCP_URL, json=payload, timeout=MCP_TIMEOUT)

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        try:
            body = response.json()
        except ValueError:
            raw = response.text.strip()
            return {
                "ok": False,
                "raw_response": raw,
                "error": str(exc),
                "model_key": model_key,
                "model_value": None,
                "elapsed_s": None,
                "count": len(segment_items),
                "success_count": 0,
                "results": [],
            }

        return {
            "ok": bool(body.get("ok")) if isinstance(body, dict) else False,
            "raw_response": body,
            "error": body.get("error") if isinstance(body, dict) else str(exc),
            "model_key": body.get("model_key") if isinstance(body, dict) else model_key,
            "model_value": body.get("model_value") if isinstance(body, dict) else None,
            "elapsed_s": body.get("elapsed_s") if isinstance(body, dict) else None,
            "count": body.get("count") if isinstance(body, dict) else len(segment_items),
            "success_count": body.get("success_count") if isinstance(body, dict) else 0,
            "results": body.get("results") if isinstance(body, dict) else [],
        }

    try:
        body = response.json()
    except ValueError:
        raw = response.text.strip()
        return {
            "ok": False,
            "raw_response": raw,
            "error": "non_json_response",
            "model_key": model_key,
            "model_value": None,
            "elapsed_s": None,
            "count": len(segment_items),
            "success_count": 0,
            "results": [],
        }

    return {
        "ok": bool(body.get("ok")) if isinstance(body, dict) else False,
        "raw_response": body,
        "error": body.get("error") if isinstance(body, dict) else None,
        "model_key": body.get("model_key"),
        "model_value": body.get("model_value"),
        "elapsed_s": body.get("elapsed_s"),
        "count": body.get("count") if isinstance(body, dict) else len(segment_items),
        "success_count": body.get("success_count") if isinstance(body, dict) else 0,
        "results": body.get("results") if isinstance(body, dict) else [],
    }


# ====== MAIN ======
def main(audio_file: Path, model_catalog_path: Path, selected_model_key: str, selected_model_value: str):
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[*] Torch CUDA available: {torch.cuda.is_available()}")
    print(f"[*] Selected device for speaker embeddings: {device}")
    print(f"[*] MCP endpoint: {MCP_URL}")
    print(f"[*] Model catalog: {model_catalog_path}")
    print(f"[*] Model key passed to MCP: {selected_model_key}")
    print(f"[*] Model value from catalog: {selected_model_value}")

    if not audio_file.exists():
        raise FileNotFoundError(f"Audio file not found: {audio_file}")

    reset_tmp_dir(TMP_DIR)

    print("[*] Running VAD across full file...")
    pcm_data, rate = read_wave(audio_file)
    vad = webrtcvad.Vad(VAD_AGGRESSIVENESS)
    frames = list(frame_generator(pcm_data, rate, FRAME_MS))

    raw_segments = vad_collector(
        rate,
        FRAME_MS,
        padding_ms=VAD_PADDING_MS,
        vad=vad,
        frames=frames,
    )
    merged_segments = merge_close_segments(raw_segments, MERGE_GAP_MS)

    print(f"[*] VAD found {len(raw_segments)} raw voiced segments.")
    print(f"[*] After merging close gaps: {len(merged_segments)} conversational segments.")

    kept_segments = []
    for i, seg in enumerate(merged_segments):
        duration = seg["end"] - seg["start"]
        if duration < MIN_SEG_SEC:
            continue

        out_path = TMP_DIR / f"seg_{i:04d}.wav"
        save_wav(seg["audio"], rate, out_path)
        kept_segments.append({
            "index": i,
            "path": out_path,
            "start": seg["start"],
            "end": seg["end"],
            "duration": duration,
        })

    if not kept_segments:
        raise RuntimeError("No valid speech segments remained after VAD and filtering.")

    # ====== EMBEDDING & CLUSTERING ======
    print("[*] Encoding and clustering speakers...")
    encoder = VoiceEncoder(device=device)
    print(f"[*] VoiceEncoder now running on {device}")

    embeddings = []
    seg_meta = []

    for item in kept_segments:
        path = item["path"]
        try:
            wav = preprocess_wav(path)
            embed = encoder.embed_utterance(wav)
            embeddings.append(embed)
            seg_meta.append(item)
        except Exception as e:
            print(f"[!] Skipped {path.name}: {e}")

    if not embeddings:
        raise RuntimeError("No valid audio segments to embed.")

    X = np.vstack(embeddings)
    n_clusters = min(MAX_SPEAKERS, len(embeddings))
    kmeans = KMeans(n_clusters=n_clusters, random_state=0, n_init=10).fit(X)
    labels = kmeans.labels_

    # ====== MCP-BASED TRANSCRIPTION ======
    transcript = []
    print(f"[*] Sending {len(seg_meta)} segments to MCP in one batch request...")

    batch_items = []
    for item in seg_meta:
        batch_items.append({
            "path": str(item["path"].resolve()),
            "segment_meta": {
                "index": item["index"],
                "start": float(item["start"]),
                "end": float(item["end"]),
                "duration": float(item["duration"]),
                "start_ts": format_timestamp(item["start"]),
                "end_ts": format_timestamp(item["end"]),
            },
        })

    batch_result = call_mcp_transcriber_batch(batch_items, audio_file, selected_model_key)
    batch_results = batch_result.get("results") if isinstance(batch_result.get("results"), list) else []
    print(
        f"[*] MCP batch completed: "
        f"{batch_result.get('success_count', 0)}/{batch_result.get('count', len(seg_meta))} segments succeeded "
        f"in {batch_result.get('elapsed_s')}s"
    )

    for idx, item in enumerate(seg_meta):
        if idx < len(batch_results) and isinstance(batch_results[idx], dict):
            mcp_result = batch_results[idx]
        else:
            mcp_result = {
                "ok": False,
                "text": "",
                "error": "missing_batch_result",
                "model_key": selected_model_key,
                "model_value": selected_model_value,
            }
        text = (mcp_result.get("text") or "").strip()
        speaker_id = f"Speaker {labels[idx] + 1}"

        transcript.append({
            "index": item["index"],
            "segment": item["path"].name,
            "speaker_id": speaker_id,
            "speaker_label": "Unknown",
            "start": round(item["start"], 3),
            "end": round(item["end"], 3),
            "start_ts": format_timestamp(item["start"]),
            "end_ts": format_timestamp(item["end"]),
            "duration": round(item["duration"], 3),
            "text": text,
            "mcp": {
                "ok": mcp_result.get("ok"),
                "error": mcp_result.get("error"),
                "model_key": mcp_result.get("model_key") or selected_model_key,
                "model_value": mcp_result.get("model_value") or selected_model_value,
                "tool": MCP_TOOL_NAME,
            },
        })

    transcript.sort(key=lambda x: x["start"])

    output = {
        "source_audio": str(audio_file.resolve()),
        "model_catalog_path": str(model_catalog_path.resolve()),
        "selected_model_key": selected_model_key,
        "selected_model_value": selected_model_value,
        "config": {
            "frame_ms": FRAME_MS,
            "vad_aggressiveness": VAD_AGGRESSIVENESS,
            "vad_padding_ms": VAD_PADDING_MS,
            "merge_gap_ms": MERGE_GAP_MS,
            "min_seg_sec": MIN_SEG_SEC,
            "max_speakers": MAX_SPEAKERS,
            "mcp_url": MCP_URL,
            "mcp_tool_name": MCP_TOOL_NAME,
            "mcp_profile": MCP_PROFILE,
            "mcp_language": MCP_LANGUAGE,
            "mcp_auto_route": MCP_AUTO_ROUTE,
            "mcp_write_artifacts": MCP_WRITE_ARTIFACTS,
            "mcp_insert_db": MCP_INSERT_DB,
            "mcp_delete_source_raw": MCP_DELETE_SOURCE_RAW,
            "mcp_skip_wav_copy": MCP_SKIP_WAV_COPY,
        },
        "speaker_map": {f"Speaker {i + 1}": "Unknown" for i in range(n_clusters)},
        "transcript": transcript,
    }

    transcript_json_path = Path("transcript.json")
    with open(transcript_json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print(f"[✓] {transcript_json_path} written ({len(transcript)} segments).")

    text_out = []
    text_out.append("──────────────────────────────────────────────")
    text_out.append(f"TRANSCRIPT SUMMARY - {audio_file.name}")
    text_out.append(f"Segments processed: {len(transcript)} | Speakers: {n_clusters}")
    text_out.append(
        f"Config: frame={FRAME_MS}ms | vad={VAD_AGGRESSIVENESS} | padding={VAD_PADDING_MS}ms | merge_gap={MERGE_GAP_MS}ms"
    )
    text_out.append(f"Model catalog: {model_catalog_path}")
    text_out.append(f"Selected model: {selected_model_key} -> {selected_model_value}")
    text_out.append(f"Transcriber: MCP -> {MCP_URL} ({MCP_TOOL_NAME})")
    text_out.append("──────────────────────────────────────────────\n")

    for t in transcript:
        body = fill(t["text"], width=TEXT_WRAP_WIDTH) if t["text"] else "[NO TRANSCRIPT RETURNED]"
        text_out.append(
            f"[{t['speaker_id']}] ({t['segment']}) {t['start_ts']} -> {t['end_ts']}\n{body}\n"
        )

    transcript_txt_path = Path("transcript.txt")
    with open(transcript_txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(text_out))
    print(f"[✓] {transcript_txt_path} written — ordered in natural progression.")


if __name__ == "__main__":
    audio_file = Path(input("Enter the path to the audio file: ").strip())

    catalog_input = input(
        f"Enter model catalog path (Enter for default: {DEFAULT_MODEL_CATALOG}): "
    ).strip()
    model_catalog_path = Path(catalog_input) if catalog_input else DEFAULT_MODEL_CATALOG

    catalog = load_model_catalog(model_catalog_path)
    selected_model_key, selected_model_value, _selected_cfg = choose_model(catalog)

    main(audio_file, model_catalog_path, selected_model_key, selected_model_value)
