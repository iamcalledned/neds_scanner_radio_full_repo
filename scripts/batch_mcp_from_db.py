#!/usr/bin/env python3
import os
import sys
import sqlite3
import asyncio
from pathlib import Path

# Load environment variables (DB_PATH, etc.)
from dotenv import load_dotenv
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(_project_root, ".env"))

# Import MCP SDK (must be in venv_whisper52)
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client

DB_PATH = os.environ.get("SCANNER_DB_PATH", "/home/ned/data/scanner_calls/scanner_calls.db")
MCP_URL = os.environ.get("MCP_URL", "http://127.0.0.1:8000/mcp")

# ==============================================================================
# EDIT YOUR BATCH QUERY HERE
# We just need it to return the absolute 'wav_path' for the files you want.
# ==============================================================================
QUERY = """
    SELECT wav_path 
    FROM calls 
    WHERE timestamp >= date('now', '-1 day')
      AND wav_path IS NOT NULL
    ORDER BY timestamp DESC 
    LIMIT 50
"""

async def process_file(session, file_path, output_dir):
    p = Path(file_path).expanduser().resolve()
    if not p.exists():
        print(f"⚠️ Missing file: {p}")
        return

    print(f"Transcribing: {p.name}")
    try:
        # Fire off to our GPU server
        result = await session.call_tool(
            "transcribe_file",
            {
                "path": str(p),
                "insert_db": False,          # Keep DB untouched
                "write_artifacts": True,     # We want the .json and .txt
                "skip_wav_copy": True,       # Do not duplicate/move the .wav
                "custom_output_dir": output_dir, # Dump jsons exactly here
                "profile": "default"
            }
        )
        
        structured = getattr(result, "structuredContent", None) or {}
        payload = structured.get("result") if isinstance(structured, dict) else None
        
        if payload and payload.get("ok"):
            artifacts = payload.get('artifacts', {})
            print(f"  ✅ Saved JSON: {artifacts.get('json')}")
        else:
            print(f"  ❌ MCP Failed: {payload}")
            
    except Exception as e:
        print(f"  ❌ Exception: {e}")

async def main():
    if len(sys.argv) < 2:
        print(f"Usage: python {os.path.basename(__file__)} <output_directory>")
        print("Note: Edit the QUERY string inside this script to change which files run.")
        sys.exit(1)

    output_dir = sys.argv[1]
    Path(output_dir).expanduser().resolve().mkdir(parents=True, exist_ok=True)

    print(f"Connecting to DB: {DB_PATH}")
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(QUERY)
        rows = cur.fetchall()
        conn.close()
    except Exception as e:
        print(f"DB Error: {e}")
        sys.exit(1)

    files_to_process = [r[0] for r in rows if r[0]]
    print(f"\n🚀 Found {len(files_to_process)} files to process based on query.")
    if not files_to_process:
        return

    print(f"Connecting to MCP server at {MCP_URL} ...\n")
    async with streamablehttp_client(MCP_URL) as streams:
        read, write, *_ = streams
        async with ClientSession(read, write) as session:
            await session.initialize()
            
            for f in files_to_process:
                await process_file(session, f, output_dir)
                # Small breather so we don't spam the MCP event loop queue instantly
                await asyncio.sleep(0.05)

    print("\n🎉 Batch processing complete.")

if __name__ == "__main__":
    asyncio.run(main())
