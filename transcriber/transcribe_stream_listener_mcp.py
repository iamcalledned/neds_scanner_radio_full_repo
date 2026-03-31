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
MCP_URL = os.environ.get("MCP_URL", "http://127.0.0.1:8000/mcp")

PROCESSED_FILE = Path(os.environ.get("PROCESSED_FILE", "/tmp/transcribe_processed.txt"))
LAST_ID_FILE = Path(os.environ.get("LAST_ID_FILE", "/tmp/last_stream_id.txt"))

DEFAULT_PROFILE = os.environ.get("DEFAULT_PROFILE", "default")
DEFAULT_LANGUAGE = os.environ.get("DEFAULT_LANGUAGE", "en")

LAST_ID_KEY = os.environ.get("LAST_ID_KEY", "scanner:transcriber:last_id")


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

processed_count = 0
failed_count = 0
start_time = datetime.now(UTC)
last_summary_time = time.time()


def mark_processed(path: str):
    processed.add(path)
    PROCESSED_FILE.write_text("\n".join(sorted(processed)))


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


async def main():
    global last_id, last_summary_time, processed_count, failed_count

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
                                    # MCP returns a content wrapper; the structured result is usually in structuredContent
                                    structured = getattr(result, "structuredContent", None) or {}
                                    payload = structured.get("result") if isinstance(structured, dict) else None

                                    if not payload or not payload.get("ok"):
                                        log.error(f"MCP transcription failed: {payload}")
                                        failed_count += 1
                                    else:
                                        mark_processed(str(file_path))
                                        processed_count += 1
                                        text = (payload.get("text") or "").strip()
                                        snippet = (text[:140] + "…") if len(text) > 140 else text
                                        log.info(f"Done: {file_path.name} | {snippet}")
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