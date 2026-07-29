from mcp_functions.gatekeeper_registration import (
    ensure_scanner_reservation,
    matching_scanner_leases,
)


class StubLog:
    def warning(self, *_args) -> None:
        pass


class StubClient:
    def __init__(self, leases):
        self.leases = leases
        self.released = []
        self.ensure_calls = []

    def status(self):
        return {"leases": {"leases": self.leases}}

    def release_runtime(self, lease_id):
        self.released.append(lease_id)
        return {"lease_released": True}

    def ensure_runtime(self, **kwargs):
        self.ensure_calls.append(kwargs)
        return {"ok": True, "lease": {"lease_id": "new"}}


def lease(lease_id, created_at, status="active"):
    return {
        "lease_id": lease_id,
        "owner": "scanner-mcp",
        "runtime_key": "scanner_whisper",
        "status": status,
        "created_at": created_at,
    }


def test_matching_scanner_leases_ignores_other_and_inactive_leases() -> None:
    status = {
        "leases": {
            "leases": [
                lease("old", "2026-01-01T00:00:00Z", status="released"),
                {**lease("other", "2026-03-01T00:00:00Z"), "owner": "other"},
                lease("current", "2026-02-01T00:00:00Z"),
            ]
        }
    }

    assert [item["lease_id"] for item in matching_scanner_leases(status)] == [
        "current"
    ]


def test_existing_lease_is_reused_and_duplicates_are_released() -> None:
    client = StubClient(
        [
            lease("oldest", "2026-01-01T00:00:00Z"),
            lease("newest", "2026-03-01T00:00:00Z"),
            lease("middle", "2026-02-01T00:00:00Z"),
        ]
    )

    result = ensure_scanner_reservation(client, StubLog())

    assert result["action"] == "reused_existing_lease"
    assert result["lease"]["lease_id"] == "newest"
    assert client.released == ["middle", "oldest"]
    assert client.ensure_calls == []


def test_new_lease_is_created_only_when_no_reusable_lease_exists() -> None:
    client = StubClient([])

    result = ensure_scanner_reservation(client, StubLog())

    assert result["ok"]
    assert len(client.ensure_calls) == 1
    assert client.ensure_calls[0]["runtime_key"] == "scanner_whisper"
