"""
gpu_model_manager/core/endpoint_monitor.py

HTTP reachability checks for known runtime endpoints.
For MCP endpoints: basic reachability only — never calls expensive tools.
For OpenAI-compatible endpoints: checks /v1/models.
"""
from __future__ import annotations

import time

import httpx

from .config import LOCAL_LLM_BASE_URL, SCANNER_MCP_URL
from .logging_config import get_logger
from .schemas import EndpointStatus

log = get_logger("endpoint_monitor")

DEFAULT_TIMEOUT = 4.0


def check_http_endpoint(url: str, timeout: float = DEFAULT_TIMEOUT) -> EndpointStatus:
    t0 = time.monotonic()
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            r = client.get(url)
        elapsed_ms = (time.monotonic() - t0) * 1000
        return EndpointStatus(
            url=url,
            reachable=True,
            status_code=r.status_code,
            response_ms=round(elapsed_ms, 1),
        )
    except httpx.ConnectError:
        return EndpointStatus(url=url, reachable=False, error="connection_refused")
    except httpx.TimeoutException:
        return EndpointStatus(url=url, reachable=False, error="timeout")
    except Exception as exc:
        log.debug("Endpoint check failed for %s: %s", url, exc)
        return EndpointStatus(url=url, reachable=False, error=str(exc)[:200])


def check_scanner_mcp() -> EndpointStatus:
    """Basic reachability only — does not invoke any MCP tools."""
    return check_http_endpoint(SCANNER_MCP_URL)


def check_local_llm() -> EndpointStatus:
    return check_openai_models(LOCAL_LLM_BASE_URL)


def check_openai_models(base_url: str) -> EndpointStatus:
    url = base_url.rstrip("/") + "/v1/models"
    return check_http_endpoint(url)


def check_runtime_endpoint(runtime_key: str) -> EndpointStatus:
    from .runtime_registry import get_runtime_definition

    defn = get_runtime_definition(runtime_key)
    if defn is None or not defn.endpoint:
        return EndpointStatus(
            url="",
            reachable=False,
            error="no_endpoint_configured",
        )
    if defn.kind == "llm":
        return check_openai_models(defn.endpoint)
    return check_http_endpoint(defn.endpoint)


def check_all_runtime_endpoints() -> dict[str, EndpointStatus]:
    from .runtime_registry import get_runtime_registry

    results: dict[str, EndpointStatus] = {}
    for key, defn in get_runtime_registry().items():
        if defn.endpoint:
            results[key] = check_runtime_endpoint(key)
    return results
