from __future__ import annotations

import re
from typing import Any, Callable


def build_routing_rules(
    *,
    json_from_env_fn: Callable[[str, str], Any],
    model_routing_rules: str,
    model_routing_file: str,
    routing_rule_cls: type,
) -> list[Any]:
    raw = json_from_env_fn(model_routing_rules, model_routing_file) or []
    rules: list[Any] = []
    if not isinstance(raw, list):
        return rules

    for item in raw:
        if not isinstance(item, dict):
            continue
        model_key = item.get("model") or item.get("model_key")
        if not model_key:
            continue
        match = item.get("match", item)
        feed_re = match.get("feed_regex") if isinstance(match, dict) else None
        path_re = match.get("path_regex") if isinstance(match, dict) else None
        min_dur = match.get("min_duration") if isinstance(match, dict) else None
        max_dur = match.get("max_duration") if isinstance(match, dict) else None

        try:
            feed_regex = re.compile(feed_re, re.IGNORECASE) if feed_re else None
        except re.error:
            feed_regex = None
        try:
            path_regex = re.compile(path_re, re.IGNORECASE) if path_re else None
        except re.error:
            path_regex = None

        rule = routing_rule_cls(
            model_key=model_key,
            feed_regex=feed_regex,
            path_regex=path_regex,
            min_duration=float(min_dur) if min_dur is not None else None,
            max_duration=float(max_dur) if max_dur is not None else None,
        )
        rules.append(rule)
    return rules


def build_router(
    *,
    build_model_catalog_fn: Callable[[], tuple[dict[str, Any], str]],
    build_routing_rules_fn: Callable[[], list[Any]],
    default_model_key_env: str | None,
    default_model_key: str,
    model_router_factory: Callable[[dict[str, Any], list[Any], str], Any],
) -> Any:
    catalog, catalog_default_key = build_model_catalog_fn()
    rules = build_routing_rules_fn()
    if default_model_key_env:
        env_default = default_model_key.strip()
        default_key = env_default if env_default in catalog else "default"
    else:
        default_key = catalog_default_key
    return model_router_factory(catalog, rules, default_key)


def ensure_runtime(
    *,
    runtime: dict[str, Any],
    runtime_lock: Any,
    build_router_fn: Callable[[], Any],
    warm_default_model: bool,
    log: Any,
) -> dict[str, Any]:
    if runtime.get("router") is not None:
        return runtime

    with runtime_lock:
        if runtime.get("router") is not None:
            return runtime

        router = build_router_fn()
        runtime["router"] = router
        runtime["state"] = None

        if warm_default_model:
            log.info(f"[runtime] Warming default model '{router.default_key}'")
            runtime["state"] = router.get_state(router.default_key)
        else:
            log.info("[runtime] WARM_DEFAULT_MODEL=0; deferring model load until first request")

    return runtime
