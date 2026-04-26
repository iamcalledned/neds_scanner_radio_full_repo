import json
import logging
from typing import Any, Dict, List

import requests
from flask import Blueprint, jsonify, request

import chatbot.app as chatbot_app
from scanner_config import build_chat_preset_tool_call, get_chat_preset_catalog


chat_bp = Blueprint("scanner_chat", __name__)
logger = logging.getLogger("scanner_web.chat")


def _compact_items(items: List[Dict[str, Any]], label: str) -> List[str]:
    lines = []
    for item in items[:5]:
        call_id = item.get("id") or item.get("call_id")
        timestamp = item.get("timestamp") or "unknown time"
        town = item.get("town") or item.get("derived_town") or "unknown town"
        excerpt = item.get("best_transcript") or item.get("excerpt") or ""
        prefix = f"- {label}"
        if call_id:
            prefix += f" #{call_id}"
        lines.append(f"{prefix}: {timestamp}, {town}. {excerpt}".strip())
    return lines


def _answer_from_tool_result(tool_result: Dict[str, Any]) -> str:
    if not tool_result.get("ok"):
        return tool_result.get("error") or "I could not complete that request."

    if "total_calls" in tool_result:
        filters = tool_result.get("filters", {})
        scope = ", ".join(str(v) for v in filters.values() if v) or "all scanner calls"
        return (
            f"For {scope}: {tool_result.get('total_calls', 0)} total calls, "
            f"{tool_result.get('calls_with_transcript', 0)} with transcripts."
        )

    if "ticket_breakdown" in tool_result:
        breakdown = tool_result["ticket_breakdown"]
        lines = [
            "Likely ticket-related outcomes:",
            f"- Warnings: {breakdown.get('warnings', 0)}",
            f"- Citations: {breakdown.get('citations', 0)}",
            f"- Total likely outcomes: {breakdown.get('total_likely_enforcement_outcomes', 0)}",
        ]
        evidence = _compact_items(tool_result.get("citations", []), "Call")
        if evidence:
            lines.extend(["", "Evidence:", *evidence])
        note = tool_result.get("note")
        if note:
            lines.extend(["", note])
        return "\n".join(lines)

    for key, label in (("warning_count", "likely warnings"), ("citation_count", "likely citations")):
        if key in tool_result:
            lines = [f"I found {tool_result.get(key, 0)} {label}."]
            evidence = _compact_items(tool_result.get("citations", []), "Call")
            if evidence:
                lines.extend(["", "Evidence:", *evidence])
            note = tool_result.get("note")
            if note:
                lines.extend(["", note])
            return "\n".join(lines)

    if "type_breakdown" in tool_result:
        count = tool_result.get("count", 0)
        breakdown = tool_result.get("type_breakdown", {})
        lines = [f"I found {count} fire announcement matches."]
        if breakdown:
            lines.append(
                "Breakdown: "
                + ", ".join(f"{name}: {value}" for name, value in breakdown.items())
            )
        evidence = _compact_items(tool_result.get("results", []), "Call")
        if evidence:
            lines.extend(["", "Evidence:", *evidence])
        note = tool_result.get("note")
        if note:
            lines.extend(["", note])
        return "\n".join(lines)

    if "results" in tool_result:
        count = tool_result.get("count", len(tool_result.get("results", [])))
        lines = [f"I found {count} matching calls."]
        matches = _compact_items(tool_result.get("results", []), "Call")
        if matches:
            lines.extend(["", *matches])
        return "\n".join(lines)

    if "call" in tool_result:
        call = tool_result.get("call") or {}
        if not tool_result.get("found", True):
            return "I could not find that call."
        return "\n".join(
            [
                f"Call #{call.get('id')}",
                f"Time: {call.get('timestamp') or 'unknown'}",
                f"Town: {call.get('town') or call.get('derived_town') or 'unknown'}",
                f"Department: {call.get('dept') or 'unknown'}",
                "",
                call.get("best_transcript") or "No transcript available.",
            ]
        )

    return json.dumps(tool_result, indent=2, default=str)


@chat_bp.route("/scanner/api/chat/local", methods=["POST"])
def api_chat_local():
    payload = request.get_json(silent=True) or {}
    preset_id = (payload.get("preset_id") or "").strip()
    town_slug = (payload.get("town_slug") or "").strip().lower()
    user_messages = payload.get("messages")

    if preset_id:
        preset = build_chat_preset_tool_call(preset_id, town_slug)
        if not preset:
            return jsonify({"ok": False, "error": "Unknown preset or town."}), 400

        try:
            tool_result = chatbot_app.execute_tool_call_from_dict(preset["tool_call"])
            answer = _answer_from_tool_result(tool_result)
            status = 200 if tool_result.get("ok") else 500
            return jsonify(
                {
                    "ok": tool_result.get("ok", False),
                    "answer": answer,
                    "citations": tool_result.get("citations", []),
                    "tool_result": tool_result,
                    "preset_id": preset["preset_id"],
                    "preset_label": preset["preset_label"],
                    "prompt": preset["prompt"],
                    "town_slug": preset["town_slug"],
                    "town_name": preset["town_name"],
                }
            ), status
        except Exception as exc:
            logger.exception("chat.local.preset_failed preset=%s town=%s", preset_id, town_slug)
            return jsonify({"ok": False, "error": f"Preset request failed: {str(exc)}"}), 500

    if not isinstance(user_messages, list) or not user_messages:
        return jsonify({"ok": False, "error": "Body must include a non-empty 'messages' list."}), 400

    try:
        result = chatbot_app.run_tool_loop(user_messages)
        if result.get("ok") and not result.get("answer") and result.get("tool_result"):
            result["answer"] = _answer_from_tool_result(result["tool_result"])
        status = 200 if result.get("ok") else 500
        return jsonify(result), status
    except requests.HTTPError as exc:
        return jsonify(
            {
                "ok": False,
                "error": f"vLLM HTTP error: {str(exc)}",
                "details": getattr(exc.response, "text", None),
            }
        ), 502
    except Exception as exc:
        logger.exception("chat.local.failed")
        return jsonify({"ok": False, "error": f"Unhandled server error: {str(exc)}"}), 500


@chat_bp.route("/scanner/api/chat/local/health", methods=["GET"])
def api_chat_local_health():
    return jsonify(
        {
            "ok": True,
            "vllm_base_url": chatbot_app.VLLM_BASE_URL,
            "model": chatbot_app.VLLM_MODEL,
            "tools": [tool["function"]["name"] for tool in chatbot_app.TOOLS],
        }
    )


@chat_bp.route("/scanner/api/chat/local/tools", methods=["GET"])
def api_chat_local_tools():
    return jsonify({"ok": True, "tools": chatbot_app.TOOLS})


@chat_bp.route("/scanner/api/chat/local/presets", methods=["GET"])
def api_chat_local_presets():
    catalog = get_chat_preset_catalog()
    return jsonify({"ok": True, **catalog})
