#!/usr/bin/env python3
"""
send_test_to_mcp.py

Simple test client to call the MCP server's analyze_audio tool.
Run with your venv active:
  python transcriber/send_test_to_mcp.py
"""

import asyncio
import os
from pathlib import Path

from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client  # returns (send, recv, meta)

MCP_URL = os.getenv("MCP_URL", "http://127.0.0.1:8000/mcp")
WAV = Path(
    os.getenv(
        "TEST_WAV",
        "/home/ned/data/scanner_calls/scanner_archive/clean/pd/rec_2026-04-01_20-14-06_pd.wav",
    )
)


async def main() -> None:
    try:
        async with streamablehttp_client(MCP_URL) as streams:
            read, write, *_ = streams
            async with ClientSession(read, write) as session:
                await session.initialize()
                resp = await session.call_tool("analyze_audio", {"path": str(WAV)})
                print(resp)
    except* Exception as eg:
        print(f"Request failed against {MCP_URL}: {eg}")
        for i, exc in enumerate(eg.exceptions, 1):
            print(f"─ sub-exception {i}: {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
