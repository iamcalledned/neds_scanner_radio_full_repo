from tools.scanner_dashboard_health import (
    format_gatekeeper_leases,
    format_gatekeeper_services,
    gatekeeper_is_healthy,
)


def test_current_gatekeeper_status_shape_is_recognized_as_healthy() -> None:
    status = {
        "gpu": {"available": True},
        "registry": {"valid": True},
    }

    assert gatekeeper_is_healthy(status)
    assert not gatekeeper_is_healthy(
        {"gpu": {"available": True}, "registry": {"valid": False}}
    )


def test_explicit_legacy_status_takes_precedence() -> None:
    assert not gatekeeper_is_healthy(
        {
            "ok": False,
            "gpu": {"available": True},
            "registry": {"valid": True},
        }
    )


def test_duplicate_leases_are_grouped_and_markup_is_safe() -> None:
    leases = [
        {
            "owner": "scanner[mcp]",
            "runtime_key": "scanner_whisper",
            "estimated_vram_mb": 7000,
            "status": "active",
        },
        {
            "owner": "scanner[mcp]",
            "runtime_key": "scanner_whisper",
            "estimated_vram_mb": 7000,
            "status": "active",
        },
    ]

    rendered = format_gatekeeper_leases(leases)

    assert rendered.count(r"scanner\[mcp]") == 1
    assert "× 2 leases" in rendered
    assert r"scanner\[mcp]" in rendered
    assert "[green]active[/green]" in rendered


def test_dynamic_service_fields_are_escaped() -> None:
    rendered = format_gatekeeper_services(
        {"service[one]": {"active": False, "substate": "dead[tag]"}}
    )

    assert r"service\[one]" in rendered
    assert r"dead\[tag]" in rendered
