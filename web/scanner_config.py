from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional


TOWNS: Dict[str, Dict[str, Any]] = {
    "hopedale": {"name": "Hopedale", "departments": ["pd", "fd"]},
    "milford": {"name": "Milford", "departments": ["mpd", "mfd"]},
    "bellingham": {"name": "Bellingham", "departments": ["bpd", "bfd"]},
    "mendon": {"name": "Mendon", "departments": ["mndpd", "mndfd"]},
    "upton": {"name": "Upton", "departments": ["uptpd", "uptfd"]},
    "blackstone": {"name": "Blackstone", "departments": ["blkpd", "blkfd"]},
    "franklin": {"name": "Franklin", "departments": ["frkpd", "frkfd"]},
}


CHAT_PRESET_TEMPLATES: List[Dict[str, Any]] = [
    {
        "id": "calls_today",
        "label": "Calls Today",
        "teaser": "Quick call volume check.",
        "prompt_template": "How many calls happened in {town} today?",
        "tool_name": "get_stats",
        "arguments": {"date": "today"},
    },
    {
        "id": "police_recent",
        "label": "Recent Police",
        "teaser": "Latest police traffic in town.",
        "prompt_template": "Show the latest police calls in {town}.",
        "tool_name": "search_calls",
        "arguments": {"department": "police", "start_time": "today", "end_time": "today", "limit": 5},
    },
    {
        "id": "citations_today",
        "label": "Citations",
        "teaser": "Likely ticket and citation outcomes.",
        "prompt_template": "How many likely citations were issued in {town} today?",
        "tool_name": "count_citations",
        "arguments": {"department": "police", "date": "today", "limit_examples": 4},
    },
    {
        "id": "fire_announcements",
        "label": "Fire Broadcasts",
        "teaser": "Recall and coverage-style fire traffic.",
        "prompt_template": "Find recent fire recall or coverage announcements in {town} today.",
        "tool_name": "find_fire_announcements",
        "arguments": {"department": "fire", "date": "today", "limit": 5},
    },
]


def get_configured_towns() -> List[Dict[str, Any]]:
    return [
        {
            "slug": slug,
            "name": town["name"],
            "departments": list(town.get("departments", [])),
        }
        for slug, town in TOWNS.items()
    ]


def get_chat_preset_catalog() -> Dict[str, Any]:
    return {
        "towns": get_configured_towns(),
        "presets": [
            {
                "id": item["id"],
                "label": item["label"],
                "teaser": item["teaser"],
                "prompt_template": item["prompt_template"],
            }
            for item in CHAT_PRESET_TEMPLATES
        ],
    }


def build_chat_preset_tool_call(preset_id: str, town_slug: str) -> Optional[Dict[str, Any]]:
    template = next((item for item in CHAT_PRESET_TEMPLATES if item["id"] == preset_id), None)
    town = TOWNS.get((town_slug or "").strip().lower())
    if not template or not town:
        return None

    town_name = town["name"]
    arguments = deepcopy(template.get("arguments", {}))
    arguments["town"] = town_name

    return {
        "preset_id": template["id"],
        "preset_label": template["label"],
        "town_slug": town_slug,
        "town_name": town_name,
        "prompt": template["prompt_template"].format(town=town_name),
        "tool_call": {
            "name": template["tool_name"],
            "arguments": arguments,
        },
    }
