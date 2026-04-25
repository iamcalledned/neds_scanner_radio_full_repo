#!/usr/bin/env python3
"""
First-stage local town hall transcript summarizer.

What it does:
- Reads a transcript.json file produced by your meeting transcription pipeline
- Chunks transcript segments into manageable batches
- Sends each chunk to a local OpenAI-compatible chat endpoint
- Produces:
    1. per-chunk structured summaries
    2. a final combined structured meeting summary JSON
    3. a readable markdown summary

This is intentionally stage 1:
- no speaker role inference
- no database insert
- no fancy RAG
- no UI
- just a practical local summarizer that turns transcript JSON into usable notes
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


# ====== CONFIG DEFAULTS ======
DEFAULT_API_BASE = os.environ.get("LOCAL_LLM_API_BASE", "http://127.0.0.1:30000/v1")
DEFAULT_API_KEY = os.environ.get("LOCAL_LLM_API_KEY", "dummy")
DEFAULT_MODEL = os.environ.get("LOCAL_LLM_MODEL", "Qwen/Qwen2.5-Coder-14B-Instruct-AWQ")
DEFAULT_TIMEOUT = int(os.environ.get("LOCAL_LLM_TIMEOUT", "300"))
DEFAULT_MAX_CHARS_PER_CHUNK = int(os.environ.get("SUMMARY_MAX_CHARS_PER_CHUNK", "12000"))
DEFAULT_MAX_SEGMENTS_PER_CHUNK = int(os.environ.get("SUMMARY_MAX_SEGMENTS_PER_CHUNK", "18"))
DEFAULT_OUTPUT_DIR = Path(os.environ.get("SUMMARY_OUTPUT_DIR", "/home/ned/data/town_hall_transcripts"))
DEFAULT_MCP_CHAT_URL = os.environ.get("LOCAL_LLM_MCP_CHAT_URL", "http://127.0.0.1:8000/interactive/chat-completion")
DEFAULT_MODEL_CATALOG = Path(
    os.environ.get(
        "SUMMARY_MODEL_CATALOG",
        "/home/ned/Documents/neds_scanner_radio_full_pipeline_with_git/transcriber/model_catalog.json",
    )
)


# ====== DATA CLASSES ======
@dataclass
class TranscriptSegment:
    index: int
    segment: str
    speaker_id: str
    start: float
    end: float
    start_ts: str
    end_ts: str
    duration: float
    text: str


# ====== FILE / JSON HELPERS ======
def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError("Input JSON must be a top-level object.")

    return data


def load_model_catalog(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Model catalog not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) or "models" not in data or not isinstance(data["models"], dict):
        raise ValueError("Model catalog must contain a top-level 'models' object.")

    return data


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def slugify(value: str) -> str:
    value = value.strip()
    value = re.sub(r"[^A-Za-z0-9._ -]+", "", value)
    value = re.sub(r"\s+", "_", value)
    value = value.strip("._-")
    return value or "meeting"


def derive_summary_folder_name(input_path: Path) -> str:
    stem = input_path.stem
    month_names = r"January|February|March|April|May|June|July|August|September|October|November|December"
    match = re.match(rf"^(.*?)(?:_({month_names})_.*)$", stem, flags=re.IGNORECASE)
    folder_name = match.group(1) if match else stem
    return slugify(folder_name)


def prompt_for_base_output_dir(default_dir: Path) -> Path:
    entered = input(
        f"Enter base save directory for meeting summaries (Enter for default: {default_dir}): "
    ).strip()
    return Path(entered).expanduser().resolve() if entered else default_dir


def choose_model(catalog: dict[str, Any], *, kind: str | None = None) -> tuple[str, str, dict[str, Any]]:
    models = catalog.get("models", {})
    default_key = catalog.get("default_model")
    keys = []
    for key, cfg in models.items():
        if not isinstance(cfg, dict):
            continue
        entry_kind = str(cfg.get("kind") or "").strip().lower()
        if kind and entry_kind != kind:
            continue
        keys.append(key)

    if not keys:
        raise ValueError(f"Model catalog contains no models for kind='{kind}'.")

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
    print(f"[*] Summarizer model value: {selected_model_value}")
    return selected_key, selected_model_value, selected_cfg


# ====== TRANSCRIPT PARSING ======
def parse_segments(data: dict[str, Any]) -> list[TranscriptSegment]:
    raw_segments = data.get("transcript")
    if not isinstance(raw_segments, list):
        raise ValueError("Input JSON does not contain a valid 'transcript' list.")

    parsed: list[TranscriptSegment] = []
    for item in raw_segments:
        if not isinstance(item, dict):
            continue

        text = str(item.get("text", "") or "").strip()
        if not text:
            continue

        parsed.append(
            TranscriptSegment(
                index=int(item.get("index", 0)),
                segment=str(item.get("segment", "")),
                speaker_id=str(item.get("speaker_id", "Unknown")),
                start=float(item.get("start", 0.0)),
                end=float(item.get("end", 0.0)),
                start_ts=str(item.get("start_ts", "00:00:00.000")),
                end_ts=str(item.get("end_ts", "00:00:00.000")),
                duration=float(item.get("duration", 0.0)),
                text=text,
            )
        )

    if not parsed:
        raise ValueError("No usable transcript segments were found.")

    parsed.sort(key=lambda x: x.start)
    return parsed


# ====== PRE-CLEANUP ======
def normalize_transcript_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_probably_junk_segment(seg: TranscriptSegment) -> bool:
    txt = seg.text.strip().lower()

    junk_phrases = {
        "thanks for watching!",
        "thank you for watching!",
        "thanks for watching",
        "thank you for watching",
    }

    if txt in junk_phrases:
        return True

    if len(txt) <= 2:
        return True

    return False


def filter_segments(segments: list[TranscriptSegment]) -> list[TranscriptSegment]:
    cleaned: list[TranscriptSegment] = []
    for seg in segments:
        seg.text = normalize_transcript_text(seg.text)
        if is_probably_junk_segment(seg):
            continue
        cleaned.append(seg)
    return cleaned


# ====== CHUNKING ======
def segment_to_line(seg: TranscriptSegment) -> str:
    return f"[{seg.speaker_id}] {seg.start_ts} -> {seg.end_ts}: {seg.text}"


def chunk_segments(
    segments: list[TranscriptSegment],
    max_chars_per_chunk: int,
    max_segments_per_chunk: int,
) -> list[list[TranscriptSegment]]:
    chunks: list[list[TranscriptSegment]] = []
    current: list[TranscriptSegment] = []
    current_chars = 0

    for seg in segments:
        line = segment_to_line(seg)
        line_len = len(line) + 1

        would_exceed_chars = current and (current_chars + line_len > max_chars_per_chunk)
        would_exceed_count = current and (len(current) >= max_segments_per_chunk)

        if would_exceed_chars or would_exceed_count:
            chunks.append(current)
            current = []
            current_chars = 0

        current.append(seg)
        current_chars += line_len

    if current:
        chunks.append(current)

    return chunks


# ====== LOCAL LLM CLIENT ======
def call_local_chat_completion(
    api_base: str,
    api_key: str,
    model: str,
    messages: list[dict[str, str]],
    timeout: int,
    temperature: float = 0.1,
    mcp_chat_url: str = "",
    model_key: str = "",
    session_id: str = "",
    close_session: bool = False,
) -> str:
    if model_key and mcp_chat_url:
        response = requests.post(
            mcp_chat_url,
            json={
                "model_key": model_key,
                "messages": messages,
                "temperature": temperature,
                "timeout": timeout,
                "session_id": session_id or None,
                "close_session": close_session,
            },
            timeout=timeout + 60,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict) or not data.get("ok"):
            raise RuntimeError(f"Managed MCP chat request failed: {data}")
        text = data.get("text")
        if not isinstance(text, str):
            raise RuntimeError(f"Unexpected MCP chat response format: {json.dumps(data)[:2000]}")
        return text.strip()

    url = api_base.rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }

    response = requests.post(url, headers=headers, json=payload, timeout=timeout)
    response.raise_for_status()
    data = response.json()

    try:
        return data["choices"][0]["message"]["content"].strip()
    except Exception as exc:
        raise RuntimeError(f"Unexpected LLM response format: {exc}\nResponse: {json.dumps(data)[:2000]}")


# ====== PROMPTS ======
CHUNK_SYSTEM_PROMPT = """You summarize municipal and town meeting transcripts.

Return ONLY valid JSON.
Do not include markdown fences.
Do not invent facts.
If something is unclear, mark it as uncertain.
Be concrete, procedural, and concise.
Prioritize the most important issues over minor housekeeping.
"""


FINAL_SYSTEM_PROMPT = """You combine partial meeting summaries into one final structured meeting summary.

Return ONLY valid JSON.
Do not include markdown fences.
Do not invent facts.
Prefer specific outcomes, motions, votes, risks, disputes, and public concerns.
If a field is unknown, use an empty string, false, or an empty list as appropriate.
Prioritize what actually drove the meeting, not just every topic that appeared.
"""


def build_chunk_user_prompt(meeting_title: str, chunk_number: int, total_chunks: int, chunk_text: str) -> str:
    schema = {
        "chunk_number": chunk_number,
        "top_issues": [""],
        "main_topics": [""],
        "motions": [""],
        "votes": [""],
        "public_comments": [""],
        "decisions": [""],
        "follow_up_items": [""],
        "important_quotes": [""],
        "summary": "",
    }

    return (
        f"Meeting title: {meeting_title}\n"
        f"Chunk {chunk_number} of {total_chunks}\n\n"
        "Analyze the transcript chunk below and return JSON matching this schema exactly in spirit:\n"
        f"{json.dumps(schema, indent=2)}\n\n"
        "Rules:\n"
        "- Keep lists concise and factual\n"
        "- top_issues must contain only the 3-5 most important issues in this chunk\n"
        "- Extract procedural actions when present\n"
        "- Focus on what actually mattered, not every passing mention\n"
        "- Do not invent speaker names\n"
        "- important_quotes should be short paraphrases or brief direct snippets if clearly present\n"
        "- summary should explain what happened in this chunk in plain language\n\n"
        "Transcript chunk:\n"
        f"{chunk_text}"
    )


def build_final_user_prompt(meeting_title: str, meeting_date: str, chunk_summaries: list[dict[str, Any]]) -> str:
    schema = {
        "meeting_title": "",
        "meeting_date": "",
        "overall_summary": "",
        "top_issues": [""],
        "main_topics": [""],
        "key_decisions": [""],
        "motions": [""],
        "votes": [""],
        "public_concerns": [""],
        "follow_up_items": [""],
        "controversies_or_risks": [""],
        "plain_english_takeaway": "",
    }

    return (
        f"Meeting title: {meeting_title}\n"
        f"Meeting date: {meeting_date}\n\n"
        "Combine the partial chunk summaries below into one final meeting summary.\n"
        "Return JSON matching this schema exactly in spirit:\n"
        f"{json.dumps(schema, indent=2)}\n\n"
        "Rules:\n"
        "- Deduplicate repeated points\n"
        "- Preserve actual outcomes and votes\n"
        "- Prefer concrete specifics over generic phrasing\n"
        "- top_issues must contain only the 3-5 most important issues discussed in the full meeting\n"
        "- Before writing the summary, determine what the 1-3 biggest issues were and center the summary around them\n"
        "- overall_summary should explain what actually happened, not just list agenda items\n"
        "- plain_english_takeaway should explain the meeting in normal language\n"
        "- Do not add sections for addresses or cases unless they are central to the meeting, and if so fold them into the relevant topics instead\n\n"
        "Chunk summaries:\n"
        f"{json.dumps(chunk_summaries, indent=2)}"
    )


# ====== JSON PARSING ======
def try_parse_json_object(text: str) -> dict[str, Any]:
    text = text.strip()

    # Direct parse first
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        pass

    # Extract first {...} block if model wrapped it in extra text
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        candidate = match.group(0)
        obj = json.loads(candidate)
        if isinstance(obj, dict):
            return obj

    raise ValueError(f"Could not parse JSON from model response:\n{text[:2000]}")


# ====== SUMMARIZATION ======
def summarize_chunks(
    meeting_title: str,
    chunks: list[list[TranscriptSegment]],
    api_base: str,
    api_key: str,
    model: str,
    timeout: int,
    mcp_chat_url: str = "",
    model_key: str = "",
    session_id: str = "",
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []

    for i, chunk in enumerate(chunks, start=1):
        chunk_text = "\n".join(segment_to_line(seg) for seg in chunk)

        messages = [
            {"role": "system", "content": CHUNK_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": build_chunk_user_prompt(
                    meeting_title=meeting_title,
                    chunk_number=i,
                    total_chunks=len(chunks),
                    chunk_text=chunk_text,
                ),
            },
        ]

        print(f"[*] Summarizing chunk {i}/{len(chunks)}...")
        raw = call_local_chat_completion(
            api_base=api_base,
            api_key=api_key,
            model=model,
            messages=messages,
            timeout=timeout,
            temperature=0.1,
            mcp_chat_url=mcp_chat_url,
            model_key=model_key,
            session_id=session_id,
            close_session=False,
        )
        parsed = try_parse_json_object(raw)
        summaries.append(parsed)

    return summaries


def summarize_final(
    meeting_title: str,
    meeting_date: str,
    chunk_summaries: list[dict[str, Any]],
    api_base: str,
    api_key: str,
    model: str,
    timeout: int,
    mcp_chat_url: str = "",
    model_key: str = "",
    session_id: str = "",
) -> dict[str, Any]:
    messages = [
        {"role": "system", "content": FINAL_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": build_final_user_prompt(
                meeting_title=meeting_title,
                meeting_date=meeting_date,
                chunk_summaries=chunk_summaries,
            ),
        },
    ]

    print("[*] Generating final meeting summary...")
    raw = call_local_chat_completion(
        api_base=api_base,
        api_key=api_key,
        model=model,
        messages=messages,
        timeout=timeout,
        temperature=0.1,
        mcp_chat_url=mcp_chat_url,
        model_key=model_key,
        session_id=session_id,
        close_session=bool(session_id),
    )
    return try_parse_json_object(raw)


# ====== MARKDOWN OUTPUT ======
def list_to_markdown(items: list[Any]) -> str:
    cleaned = [str(x).strip() for x in items if str(x).strip()]
    if not cleaned:
        return "- None"
    return "\n".join(f"- {item}" for item in cleaned)


def build_markdown(final_summary: dict[str, Any]) -> str:
    title = str(final_summary.get("meeting_title", "Meeting Summary")).strip() or "Meeting Summary"
    meeting_date = str(final_summary.get("meeting_date", "")).strip()
    overall_summary = str(final_summary.get("overall_summary", "")).strip()
    takeaway = str(final_summary.get("plain_english_takeaway", "")).strip()

    md = []
    md.append(f"# {title}")
    if meeting_date:
        md.append("")
        md.append(f"**Date:** {meeting_date}")

    if overall_summary:
        md.append("")
        md.append("## Overall Summary")
        md.append(overall_summary)

    md.append("")
    md.append("## Top Issues")
    md.append(list_to_markdown(final_summary.get("top_issues", [])))

    md.append("")
    md.append("## Main Topics")
    md.append(list_to_markdown(final_summary.get("main_topics", [])))

    md.append("")
    md.append("## Key Decisions")
    md.append(list_to_markdown(final_summary.get("key_decisions", [])))

    md.append("")
    md.append("## Motions")
    md.append(list_to_markdown(final_summary.get("motions", [])))

    md.append("")
    md.append("## Votes")
    md.append(list_to_markdown(final_summary.get("votes", [])))

    md.append("")
    md.append("## Public Concerns")
    md.append(list_to_markdown(final_summary.get("public_concerns", [])))

    md.append("")
    md.append("## Follow-up Items")
    md.append(list_to_markdown(final_summary.get("follow_up_items", [])))

    md.append("")
    md.append("## Controversies / Risks")
    md.append(list_to_markdown(final_summary.get("controversies_or_risks", [])))

    if takeaway:
        md.append("")
        md.append("## Plain-English Takeaway")
        md.append(takeaway)

    md.append("")
    return "\n".join(md)


# ====== TITLE / DATE EXTRACTION ======
def derive_meeting_title(input_path: Path, transcript_data: dict[str, Any]) -> str:
    source_audio = str(transcript_data.get("source_audio", "")).strip()
    if source_audio:
        name = Path(source_audio).stem
        if name:
            return name.replace("_", " ")
    return input_path.stem.replace("_", " ")


def derive_meeting_date(transcript_data: dict[str, Any]) -> str:
    # Stage 1: leave blank unless upstream provides one cleanly.
    # You can improve this later from transcript text or source metadata.
    return str(transcript_data.get("meeting_date", "")).strip()


def prompt_for_input_json(initial_value: str) -> Path:
    entered = initial_value.strip()
    if not entered:
        entered = input("Enter the path to the transcript JSON file: ").strip()
    if not entered:
        raise ValueError("A transcript JSON path is required.")
    return Path(entered).expanduser().resolve()


# ====== MAIN ======
def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize a town hall transcript JSON using a local LLM.")
    parser.add_argument("input_json", nargs="?", default="", help="Path to transcript.json")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE, help=f"OpenAI-compatible API base (default: {DEFAULT_API_BASE})")
    parser.add_argument("--api-key", default=DEFAULT_API_KEY, help="API key (default: env LOCAL_LLM_API_KEY or dummy)")
    parser.add_argument("--model", default="", help="Model name to use. If omitted, prompt from model catalog.")
    parser.add_argument(
        "--model-catalog",
        default=str(DEFAULT_MODEL_CATALOG),
        help=f"Model catalog path for interactive model selection (default: {DEFAULT_MODEL_CATALOG})",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help=f"Request timeout in seconds (default: {DEFAULT_TIMEOUT})")
    parser.add_argument("--max-chars-per-chunk", type=int, default=DEFAULT_MAX_CHARS_PER_CHUNK, help=f"Max text chars per chunk (default: {DEFAULT_MAX_CHARS_PER_CHUNK})")
    parser.add_argument("--max-segments-per-chunk", type=int, default=DEFAULT_MAX_SEGMENTS_PER_CHUNK, help=f"Max transcript segments per chunk (default: {DEFAULT_MAX_SEGMENTS_PER_CHUNK})")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})")
    args = parser.parse_args()

    input_path = prompt_for_input_json(args.input_json)
    if args.output_dir and str(args.output_dir).strip() != str(DEFAULT_OUTPUT_DIR):
        base_output_dir = Path(args.output_dir).expanduser().resolve()
    else:
        base_output_dir = prompt_for_base_output_dir(DEFAULT_OUTPUT_DIR)

    folder_name = derive_summary_folder_name(input_path)
    output_dir = base_output_dir / folder_name
    ensure_dir(output_dir)

    selected_model_key = ""
    selected_model_value = args.model.strip()
    selected_cfg: dict[str, Any] = {}
    model_catalog_path = Path(args.model_catalog).expanduser().resolve()
    if not selected_model_value:
        catalog = load_model_catalog(model_catalog_path)
        selected_model_key, selected_model_value, selected_cfg = choose_model(catalog, kind="chat")
    else:
        print(f"[*] Using model from --model: {selected_model_value}")

    transcript_data = load_json(input_path)
    segments = parse_segments(transcript_data)
    segments = filter_segments(segments)
    if not segments:
        raise RuntimeError("No usable transcript segments remained after cleanup.")

    meeting_title = derive_meeting_title(input_path, transcript_data)
    meeting_date = derive_meeting_date(transcript_data)
    safe_title = slugify(meeting_title)

    chunks = chunk_segments(
        segments=segments,
        max_chars_per_chunk=args.max_chars_per_chunk,
        max_segments_per_chunk=args.max_segments_per_chunk,
    )

    print(f"[*] Meeting title: {meeting_title}")
    print(f"[*] Input transcript: {input_path}")
    print(f"[*] Clean segments: {len(segments)}")
    print(f"[*] Chunks to summarize: {len(chunks)}")
    if selected_model_key:
        print(f"[*] Model key: {selected_model_key}")
        print(f"[*] Model value: {selected_model_value}")
    else:
        print(f"[*] Model: {selected_model_value}")
    print(f"[*] API base: {args.api_base}")

    use_managed_mcp_chat = bool(selected_model_key) and str(selected_cfg.get("kind") or "").strip().lower() == "chat"
    mcp_session_id = f"summarize-{uuid.uuid4().hex}" if use_managed_mcp_chat else ""
    if use_managed_mcp_chat:
        print(f"[*] MCP chat route: {DEFAULT_MCP_CHAT_URL}")
        print(f"[*] MCP chat session: {mcp_session_id}")

    chunk_summaries = summarize_chunks(
        meeting_title=meeting_title,
        chunks=chunks,
        api_base=args.api_base,
        api_key=args.api_key,
        model=selected_model_value,
        timeout=args.timeout,
        mcp_chat_url=DEFAULT_MCP_CHAT_URL if use_managed_mcp_chat else "",
        model_key=selected_model_key if use_managed_mcp_chat else "",
        session_id=mcp_session_id,
    )

    final_summary = summarize_final(
        meeting_title=meeting_title,
        meeting_date=meeting_date,
        chunk_summaries=chunk_summaries,
        api_base=args.api_base,
        api_key=args.api_key,
        model=selected_model_value,
        timeout=args.timeout,
        mcp_chat_url=DEFAULT_MCP_CHAT_URL if use_managed_mcp_chat else "",
        model_key=selected_model_key if use_managed_mcp_chat else "",
        session_id=mcp_session_id,
    )

    if not final_summary.get("meeting_title"):
        final_summary["meeting_title"] = meeting_title
    if not final_summary.get("meeting_date"):
        final_summary["meeting_date"] = meeting_date

    markdown = build_markdown(final_summary)

    input_base_name = slugify(input_path.stem)
    chunk_json_path = output_dir / f"{input_base_name}_chunk_summaries.json"
    final_json_path = output_dir / f"{input_base_name}_summary.json"
    final_md_path = output_dir / f"{input_base_name}_summary.md"

    with open(chunk_json_path, "w", encoding="utf-8") as f:
        json.dump(chunk_summaries, f, indent=2)

    with open(final_json_path, "w", encoding="utf-8") as f:
        json.dump(final_summary, f, indent=2)

    with open(final_md_path, "w", encoding="utf-8") as f:
        f.write(markdown)

    print(f"[✓] Chunk summaries written: {chunk_json_path}")
    print(f"[✓] Final summary JSON written: {final_json_path}")
    print(f"[✓] Final summary markdown written: {final_md_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[!] Cancelled by user.")
        sys.exit(130)
    except Exception as exc:
        print(f"[!] Error: {exc}")
        sys.exit(1)
