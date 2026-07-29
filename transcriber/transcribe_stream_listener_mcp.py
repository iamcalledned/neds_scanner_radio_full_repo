#!/usr/bin/env python3
"""
transcribe_stream_listener_mcp_client.py

Redis Stream listener that calls a warm MCP server using the MCP client
(streamable HTTP). No subprocess Whisper loads. No raw HTTP hacks.

Deps:
  pip install redis mcp anyio

Run:
  export MCP_URL="http://127.0.0.1:8000/mcp"
  python3 transcribe_stream_listener_mcp_client.py
"""

import os
import time
import logging
import logging.handlers
import redis
import asyncio
import json
import sqlite3
from pathlib import Path
from datetime import datetime, UTC

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client

# ==========================
# Logging
# ==========================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_DIR = Path(os.environ.get("LOG_DIR", "/home/ned/data/scanner_calls/logs/transcriber_logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

_log_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_log_fmt)

_file_handler = logging.handlers.RotatingFileHandler(
    LOG_DIR / "stream_listener_mcp.log", maxBytes=10_000_000, backupCount=5
)
_file_handler.setFormatter(_log_fmt)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    handlers=[_console_handler, _file_handler],
)
log = logging.getLogger("stream-listener-mcp")

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
STREAM_KEY = os.environ.get("STREAM_KEY", "scanner:stream:new_call")
RETRANSCRIBE_STREAM_KEY = os.environ.get(
    "RETRANSCRIBE_STREAM_KEY",
    "scanner:stream:retranscribe",
)
MCP_URL = os.environ.get("MCP_URL", "http://127.0.0.1:8000/mcp")

PROCESSED_FILE = Path(os.environ.get("PROCESSED_FILE", "/tmp/transcribe_processed.txt"))
LAST_ID_FILE = Path(os.environ.get("LAST_ID_FILE", "/tmp/last_stream_id.txt"))

DEFAULT_PROFILE = os.environ.get("DEFAULT_PROFILE", "default")
DEFAULT_LANGUAGE = os.environ.get("DEFAULT_LANGUAGE", "en")

LAST_ID_KEY = os.environ.get("LAST_ID_KEY", "scanner:transcriber:last_id")
RETRANSCRIBE_LAST_ID_KEY = os.environ.get(
    "RETRANSCRIBE_LAST_ID_KEY",
    "scanner:transcriber:retranscribe:last_id",
)
SCANNER_DB_PATH = os.environ.get("SCANNER_DB_PATH", "/home/ned/data/scanner_calls/scanner_calls.db")
SECONDARY_MODELS = [m.strip() for m in os.environ.get("SECONDARY_MODELS", "").split(",") if m.strip()]

# State
processed = set()
if PROCESSED_FILE.exists():
    processed.update(PROCESSED_FILE.read_text().splitlines())

r = redis.from_url(REDIS_URL)

# last_id: resume from Redis if present; otherwise start at "$" (new messages only)
saved = r.get(LAST_ID_KEY)
if saved:
    last_id = saved.decode()
else:
    last_id = "$"
saved_retranscribe_id = r.get(RETRANSCRIBE_LAST_ID_KEY)
if saved_retranscribe_id:
    retranscribe_last_id = saved_retranscribe_id.decode()
else:
    retranscribe_last_id = "0-0"

processed_count = 0
failed_count = 0
start_time = datetime.now(UTC)
last_summary_time = time.time()


def mark_processed(path: str):
    processed.add(path)
    PROCESSED_FILE.write_text("\n".join(sorted(processed)))


def extract_tool_payload(result) -> dict | None:
    """
    Accept both common MCP tool response shapes:
    - structuredContent={"result": {...}}
    - structuredContent={...}
    """
    structured = getattr(result, "structuredContent", None)
    if isinstance(structured, dict):
        wrapped = structured.get("result")
        if isinstance(wrapped, dict):
            return wrapped
        if "ok" in structured or "error" in structured or "text" in structured:
            return structured

    content = getattr(result, "content", None)
    if isinstance(content, list):
        for item in content:
            text = getattr(item, "text", None)
            if not isinstance(text, str):
                continue
            try:
                parsed = json.loads(text)
            except Exception:
                continue
            if isinstance(parsed, dict):
                wrapped = parsed.get("result")
                if isinstance(wrapped, dict):
                    return wrapped
                if "ok" in parsed or "error" in parsed or "text" in parsed:
                    return parsed

    return None


def summarize():
    uptime = (datetime.now(UTC) - start_time).total_seconds() / 60
    log.info("")
    log.info("┌──────────────────────────────────────────────┐")
    log.info("│   Redis Stream Listener Status (MCP client)  │")
    log.info("├──────────────────────────────────────────────┤")
    log.info(f"│  Uptime:          {uptime:.1f} min")
    log.info(f"│  Files processed: {processed_count}")
    log.info(f"│  Failures:        {failed_count}")
    log.info(f"│  Last ID:         {last_id}")
    log.info("└──────────────────────────────────────────────┘")
    log.info("")


async def call_transcribe(session: ClientSession, wav_path: Path):
    """
    Call MCP tool transcribe_file on the warm server.
    """
    return await session.call_tool(
        "transcribe_file",
        {
            "path": str(wav_path),
            "profile": DEFAULT_PROFILE,
            "language": DEFAULT_LANGUAGE,
            "write_artifacts": True,
            "insert_db": True,
            "delete_source_raw": False,
        },
    )

async def call_sec_transcribe(session: ClientSession, wav_path: Path, model_key: str):
    """
    Call MCP tool route_and_transcribe for secondary model test runs.
    We don't want the server writing artifacts or overwriting the DB row natively.
    """
    return await session.call_tool(
        "route_and_transcribe",
        {
            "path": str(wav_path),
            "profile": DEFAULT_PROFILE,
            "language": DEFAULT_LANGUAGE,
            "auto_route": False,
            "model_key": model_key,
            "write_artifacts": False,
            "insert_db": False,
            "delete_source_raw": False,
        },
    )


async def call_retranscribe_candidate(
    session: ClientSession,
    wav_path: Path,
    profile: str,
):
    """Generate a comparison transcript without overwriting artifacts or DB."""
    return await session.call_tool(
        "transcribe_file",
        {
            "path": str(wav_path),
            "profile": profile or "aggressive",
            "language": DEFAULT_LANGUAGE,
            "write_artifacts": False,
            "insert_db": False,
            "delete_source_raw": False,
        },
    )


def _complete_retranscription_request(
    request_id: int,
    status: str,
    result: dict,
) -> None:
    try:
        from scanner_intelligence import complete_retranscription_request

        complete_retranscription_request(
            request_id=request_id,
            status=status,
            result=result,
            db_path=SCANNER_DB_PATH,
        )
    except Exception as exc:
        log.error(
            "Failed to update retranscription request %s: %s",
            request_id,
            exc,
        )


async def handle_retranscription_request(
    session: ClientSession,
    fields: dict,
) -> None:
    """Create, score, and conditionally promote a comparison transcript."""
    request_id = int(fields.get(b"request_id", b"0").decode() or 0)
    call_id = int(fields.get(b"call_id", b"0").decode() or 0)
    wav_path = Path(fields.get(b"file", b"").decode())
    profile = fields.get(b"profile", b"aggressive").decode() or "aggressive"
    if not request_id or not call_id or not wav_path.exists():
        result = {
            "error": "invalid_request_or_missing_audio",
            "call_id": call_id,
            "path": str(wav_path),
        }
        _complete_retranscription_request(request_id, "failed", result)
        log.warning("Retranscription request invalid: %s", result)
        return

    with sqlite3.connect(SCANNER_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT transcript, raw_transcript, duration, rms,
                   transcription_score, needs_retry, extra
            FROM calls
            WHERE id = ?
            """,
            (call_id,),
        ).fetchone()
    if not row:
        result = {"error": "call_not_found", "call_id": call_id}
        _complete_retranscription_request(request_id, "failed", result)
        return

    try:
        response = await call_retranscribe_candidate(
            session,
            wav_path,
            profile,
        )
        payload = extract_tool_payload(response)
        if not payload or not payload.get("ok"):
            raise RuntimeError(f"MCP retranscription failed: {payload!r}")
        candidate_text = (payload.get("text") or "").strip()
        if not candidate_text:
            raise RuntimeError("MCP retranscription returned empty text")

        from mcp_tools.scoring import score_transcript

        duration = float(row["duration"] or payload.get("duration") or 0)
        rms = float(row["rms"] or payload.get("rms") or 0)
        original_text = (row["transcript"] or row["raw_transcript"] or "").strip()
        original_quality = score_transcript(original_text, duration, rms)
        candidate_quality = score_transcript(candidate_text, duration, rms)
        original_score = float(
            row["transcription_score"]
            if row["transcription_score"] is not None
            else original_quality["score"]
        )
        candidate_score = float(candidate_quality["score"])
        accepted = bool(
            candidate_text != original_text
            and bool(row["needs_retry"])
            and candidate_score >= 0.6
            and candidate_score >= original_score + 0.1
        )
        try:
            extra = json.loads(row["extra"] or "{}")
            if not isinstance(extra, dict):
                extra = {}
        except (TypeError, ValueError):
            extra = {}
        attempts = extra.get("ai_retranscriptions")
        if not isinstance(attempts, list):
            attempts = []
        attempts.append(
            {
                "request_id": request_id,
                "timestamp": datetime.now(UTC).isoformat(),
                "profile": profile,
                "candidate_transcript": candidate_text,
                "candidate_score": candidate_score,
                "original_score": original_score,
                "accepted": accepted,
            }
        )
        extra["ai_retranscriptions"] = attempts[-5:]

        with sqlite3.connect(SCANNER_DB_PATH) as conn:
            if accepted:
                conn.execute(
                    """
                    UPDATE calls
                    SET transcript = ?, normalized_transcript = ?,
                        transcription_score = ?, needs_retry = ?,
                        needs_review = ?, quality_reasons = ?, extra = ?
                    WHERE id = ?
                    """,
                    (
                        candidate_text,
                        candidate_text,
                        candidate_score,
                        int(candidate_quality["needs_retry"]),
                        int(candidate_quality["needs_review"]),
                        json.dumps(candidate_quality["reasons"]),
                        json.dumps(extra),
                        call_id,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE calls
                    SET needs_review = 1, extra = ?
                    WHERE id = ?
                    """,
                    (json.dumps(extra), call_id),
                )
        result = {
            "call_id": call_id,
            "accepted": accepted,
            "profile": profile,
            "original_score": original_score,
            "candidate_score": candidate_score,
            "candidate_transcript": candidate_text,
        }
        _complete_retranscription_request(request_id, "complete", result)
        log.info(
            "Retranscription complete call_id=%s accepted=%s score=%.3f->%.3f",
            call_id,
            accepted,
            original_score,
            candidate_score,
        )
    except Exception as exc:
        result = {"error": str(exc), "call_id": call_id}
        _complete_retranscription_request(request_id, "failed", result)
        log.error("Retranscription failed call_id=%s error=%s", call_id, exc)


async def main():
    global last_id, retranscribe_last_id
    global last_summary_time, processed_count, failed_count

    log.info(f"Connected to Redis stream: {STREAM_KEY}")
    log.info(f"Using MCP endpoint: {MCP_URL}")
    log.info(f"Starting from stream ID: {last_id}")

    async with streamablehttp_client(MCP_URL) as streams:
        read, write, *_ = streams
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            log.info(f"MCP tools: {[t.name for t in tools.tools]}")

            while True:
                try:
                    retry_messages = r.xread(
                        {RETRANSCRIBE_STREAM_KEY: retranscribe_last_id},
                        block=1,
                        count=1,
                    )
                    if retry_messages:
                        for _, retry_entries in retry_messages:
                            for retry_msg_id, retry_fields in retry_entries:
                                await handle_retranscription_request(
                                    session,
                                    retry_fields,
                                )
                                retranscribe_last_id = (
                                    retry_msg_id.decode()
                                    if isinstance(retry_msg_id, bytes)
                                    else str(retry_msg_id)
                                )
                                r.set(
                                    RETRANSCRIBE_LAST_ID_KEY,
                                    retranscribe_last_id,
                                )
                        continue

                    messages = r.xread({STREAM_KEY: last_id}, block=5000, count=1)
                    now = time.time()

                    if now - last_summary_time >= 60:
                        summarize()
                        last_summary_time = now

                    if not messages:
                        continue

                    for _, entries in messages:
                        for msg_id, fields in entries:
                            tag = fields.get(b"tag", b"?").decode()
                            file_path = Path(fields.get(b"file", b"").decode())
                            ts = fields.get(b"time", b"").decode()
                            log.info(f"New call ({tag}) at {ts}: {file_path.name}")

                            if not file_path.exists():
                                log.warning(f"Missing file: {file_path}")
                                failed_count += 1
                            elif str(file_path) in processed:
                                log.info(f"Already processed: {file_path.name}")
                            else:
                                log.info(f"MCP Transcribing: {file_path}")
                                try:
                                    result = await call_transcribe(session, file_path)
                                    payload = extract_tool_payload(result)

                                    if not payload or not payload.get("ok"):
                                        log.error(
                                            "MCP transcription failed: %s | structured=%r | content=%r",
                                            payload,
                                            getattr(result, "structuredContent", None),
                                            getattr(result, "content", None),
                                        )
                                        failed_count += 1
                                    else:
                                        mark_processed(str(file_path))
                                        processed_count += 1
                                        text = (payload.get("text") or "").strip()
                                        snippet = (text[:140] + "…") if len(text) > 140 else text
                                        log.info(f"Done: {file_path.name} | {snippet}")
                                        
                                        # -------------
                                        # Secondary Models Logic
                                        # -------------
                                        if SECONDARY_MODELS and "artifacts" in payload and payload["artifacts"].get("json"):
                                            json_path = Path(payload["artifacts"]["json"])
                                            
                                            try:
                                                with open(json_path, "r") as f:
                                                    meta = json.load(f)
                                                    
                                                if "secondary_transcripts" not in meta:
                                                    meta["secondary_transcripts"] = []
                                                    
                                                for sec_model in SECONDARY_MODELS:
                                                    log.info(f"Running secondary transcript using '{sec_model}'...")
                                                    sec_res = await call_sec_transcribe(session, file_path, sec_model)
                                                    sec_payload = extract_tool_payload(sec_res)
                                                    
                                                    if sec_payload and sec_payload.get("ok"):
                                                        sec_text = sec_payload.get("text", "").strip()
                                                        meta["secondary_transcripts"].append({
                                                            "model": sec_model,
                                                            "transcript": sec_text
                                                        })
                                                        log.info(f"Secondary ('{sec_model}') done: {sec_text[:60]}...")
                                                    else:
                                                        log.warning(f"Secondary model '{sec_model}' failed/skipped.")
                                                        
                                                # Save updated JSON 
                                                with open(json_path, "w") as f:
                                                    json.dump(meta, f, indent=2)
                                                
                                                # Update DB logic via direct SQLite update to exactly apply the JSON string
                                                # to the "extra" JSON column natively.
                                                if Path(SCANNER_DB_PATH).exists():
                                                    try:
                                                        with sqlite3.connect(SCANNER_DB_PATH) as conn:
                                                            conn.execute(
                                                                "UPDATE calls SET extra = ? WHERE filename = ?",
                                                                (json.dumps(meta), file_path.name)
                                                            )
                                                        log.info(f"Appended secondary transcripts to DB config and JSON for {file_path.name}")
                                                    except Exception as dbe:
                                                        log.error(f"Failed to update secondary transcripts into DB: {dbe}")
                                                        
                                            except Exception as je:
                                                log.error(f"Failed processing secondary transcripts JSON sidecar: {je}")

                                except Exception as e:
                                    log.error(f"MCP error for {file_path.name}: {e}")
                                    failed_count += 1

                            # advance stream id even if transcription failed, or you'll loop forever
                            last_id = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)
                            r.set(LAST_ID_KEY, last_id)
                            # tiny cooldown
                            await asyncio.sleep(0.05)

                except KeyboardInterrupt:
                    log.info("Stopping listener...")
                    summarize()
                    return
                except redis.ConnectionError as e:
                    log.error(f"Redis connection error: {e}. Retrying in 3s...")
                    await asyncio.sleep(3)
                except Exception as e:
                    log.error(f"Unexpected error: {e}. Retrying in 2s...")
                    await asyncio.sleep(2)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        summarize()
        log.info("Clean exit.")
