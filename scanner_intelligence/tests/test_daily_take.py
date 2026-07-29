import json
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from scanner_intelligence.daily_take import (
    build_daily_fact_pack,
    compose_daily_take,
    find_incident_key_for_call,
    get_incident_detail,
    get_or_generate_daily_take,
    parse_day,
)


def _create_calls_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE calls (
                id INTEGER PRIMARY KEY,
                timestamp TEXT,
                town TEXT,
                derived_town TEXT,
                dept TEXT,
                category TEXT,
                filename TEXT,
                wav_path TEXT,
                duration REAL,
                rms REAL DEFAULT 0,
                classification TEXT,
                derived_address TEXT,
                needs_review INTEGER DEFAULT 0,
                needs_retry INTEGER DEFAULT 0,
                transcription_score REAL,
                quality_reasons TEXT,
                profile_used TEXT,
                play_count INTEGER DEFAULT 0,
                hook_request INTEGER DEFAULT 0,
                edited_transcript TEXT,
                transcript TEXT,
                normalized_transcript TEXT,
                raw_transcript TEXT
            )
            """
        )


def _insert_call(
    path: Path,
    call_id: int,
    timestamp: str,
    town: str,
    dept: str,
    category: str,
    call_type: str,
    transcript: str,
    address: str = "",
    units=None,
    tone: bool = False,
) -> None:
    classification = {
        "call_type": call_type,
        "units": units or [],
        "tone_detected": tone,
    }
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            INSERT INTO calls (
                id, timestamp, town, derived_town, dept, category, filename,
                duration, classification, derived_address, transcript
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                call_id,
                timestamp,
                town,
                town,
                dept,
                category,
                f"rec_2026-07-29_{call_id:06d}_{category}.wav",
                30,
                json.dumps(classification),
                address,
                transcript,
            ),
        )


class DailyTakeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.calls_db = Path(self.temp_dir.name) / "calls.db"
        _create_calls_db(self.calls_db)
        _insert_call(
            self.calls_db,
            1,
            "2026-07-29T10:00:00",
            "Bellingham",
            "fire",
            "bfd",
            "Alarm",
            "Food in the toaster, homeowner contacting the alarm company.",
            "10 Main Street",
            ["Engine 4"],
            tone=True,
        )
        _insert_call(
            self.calls_db,
            2,
            "2026-07-29T10:00:35",
            "Bellingham",
            "fire",
            "bfd",
            "Alarm",
            "Received, Engine 4 is clear.",
            "10 Main Street",
            ["Engine 4"],
        )
        _insert_call(
            self.calls_db,
            3,
            "2026-07-29T11:00:00",
            "Milford",
            "police",
            "mpd",
            "Animal",
            "Assisting animal control with a raccoon in a storm drain.",
        )
        _insert_call(
            self.calls_db,
            4,
            "2026-07-29T12:00:00",
            "Milford",
            "fire",
            "mfd",
            "Medical",
            "Medical response for an injured resident, transporting to hospital.",
        )
        _insert_call(
            self.calls_db,
            5,
            "2026-07-29T13:00:00",
            "Milford",
            "police",
            "mpd",
            "Parking",
            "911 hang up call near a parking lot.",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_groups_transmissions_without_subject_filtering(self) -> None:
        fact_pack = build_daily_fact_pack(
            db_path=str(self.calls_db),
            day="2026-07-29",
            now=datetime(2026, 7, 29, 14, 0),
        )

        self.assertEqual(fact_pack["totals"]["transmissions"], 5)
        self.assertEqual(fact_pack["totals"]["estimated_incidents"], 4)
        self.assertEqual(
            fact_pack["breakdowns"]["towns"],
            {"Milford": 3, "Bellingham": 2},
        )

        summaries = " ".join(item["summary"] for item in fact_pack["highlights"])
        notes = " ".join(item["ned_note"] for item in fact_pack["highlights"])
        highlighted_types = {
            item["call_type"] for item in fact_pack["highlights"]
        }
        self.assertEqual(
            highlighted_types,
            {"Alarm", "Animal", "Medical", "Parking"},
        )
        self.assertIn("toaster", summaries.lower())
        self.assertIn("raccoon", summaries.lower())
        self.assertIn("Breakfast", notes)
        self.assertEqual(
            fact_pack["method"]["highlight_filtering"],
            "none by call type or transcript subject",
        )
        self.assertEqual(
            [town["scope"]["town"] for town in fact_pack["towns"]],
            ["Milford", "Bellingham"],
        )
        bellingham = fact_pack["towns"][1]
        self.assertEqual(bellingham["totals"]["transmissions"], 2)
        self.assertEqual(
            [section["scope"]["department"] for section in bellingham["departments"]],
            ["fire"],
        )
        self.assertEqual(
            {
                section["scope"]["department"]
                for section in fact_pack["departments"]
            },
            {"police", "fire"},
        )

    def test_hopedale_order_timing_outcomes_and_comedic_review(self) -> None:
        hopedale_calls = [
            (6, "2026-07-29T09:00:00", "73 responding to a traffic stop.", "73"),
            (7, "2026-07-29T09:01:00", "73 is on scene.", "73"),
            (
                8,
                "2026-07-29T09:05:00",
                "73 to control. Owner operator, verbal warning speed, I'm clear.",
                "73",
            ),
            (9, "2026-07-29T10:00:00", "74 responding to a traffic stop.", "74"),
            (10, "2026-07-29T10:01:30", "74 is on scene.", "74"),
            (
                11,
                "2026-07-29T10:04:00",
                "74 to control. Citation issued for speed, I'm clear.",
                "74",
            ),
        ]
        for call_id, timestamp, transcript, unit in hopedale_calls:
            _insert_call(
                self.calls_db,
                call_id,
                timestamp,
                "Hopedale",
                "police",
                "pd",
                "Traffic Stop",
                transcript,
                units=[unit],
            )

        fact_pack = build_daily_fact_pack(
            db_path=str(self.calls_db),
            day="2026-07-29",
            persist=False,
        )
        take = compose_daily_take(fact_pack)

        self.assertEqual(
            [town["scope"]["town"] for town in fact_pack["towns"]],
            ["Hopedale", "Milford", "Bellingham"],
        )
        hopedale = fact_pack["towns"][0]
        self.assertEqual(
            hopedale["breakdowns"]["enforcement"],
            {"warnings": 1, "citations": 1, "arrests": 0},
        )
        police = hopedale["departments"][0]
        by_outcome = {
            highlight["outcome_type"]: highlight
            for highlight in police["highlights"]
        }
        self.assertEqual(by_outcome["warning"]["response_time_seconds"], 60)
        self.assertEqual(by_outcome["warning"]["incident_span_seconds"], 300)
        self.assertEqual(by_outcome["warning"]["recorded_audio_seconds"], 90)
        self.assertTrue(by_outcome["citation"]["citation_issued"])

        hopedale_take = take["towns"][0]
        self.assertIn(
            "Holy shit, they gave a citation!!!",
            hopedale_take["ned_take"],
        )

    def test_take_persists_and_returns_grounded_citations(self) -> None:
        now = datetime(2026, 7, 29, 14, 0)
        first = get_or_generate_daily_take(
            db_path=str(self.calls_db),
            day="today",
            now=now,
            force=True,
        )
        second = get_or_generate_daily_take(
            db_path=str(self.calls_db),
            day="today",
            now=now,
        )

        self.assertFalse(first["cached"])
        self.assertTrue(second["cached"])
        self.assertTrue(first["source_call_ids"])
        for highlight in first["take"]["highlights"]:
            for citation in highlight["citations"]:
                self.assertEqual(
                    citation["archive_url"],
                    f"/scanner/call/{citation['call_id']}",
                )
                self.assertTrue(citation["audio_url"].startswith("/scanner/audio/"))
                self.assertTrue(citation["excerpt"])

        with sqlite3.connect(self.calls_db) as conn:
            incident_count = conn.execute(
                "SELECT COUNT(*) FROM scanner_incidents WHERE day = '2026-07-29'"
            ).fetchone()[0]
            take_count = conn.execute(
                "SELECT COUNT(*) FROM scanner_daily_takes WHERE day = '2026-07-29'"
            ).fetchone()[0]
        self.assertEqual(incident_count, 4)
        self.assertEqual(take_count, 1)
        self.assertEqual(len(first["take"]["towns"]), 2)
        self.assertTrue(first["take"]["departments"])

        incident_key = first["take"]["highlights"][0]["incident_key"]
        incident = get_incident_detail(
            incident_key,
            db_path=str(self.calls_db),
        )
        self.assertIsNotNone(incident)
        self.assertEqual(incident["incident_key"], incident_key)
        self.assertTrue(incident["calls"])
        self.assertEqual(
            incident["source_call_ids"],
            [call["call_id"] for call in incident["calls"]],
        )
        self.assertTrue(incident["calls"][0]["audio_url"].startswith("/scanner/audio/"))
        self.assertTrue(incident["calls"][0]["archive_url"].startswith("/scanner/call/"))
        self.assertEqual(
            find_incident_key_for_call(
                incident["calls"][0]["call_id"],
                db_path=str(self.calls_db),
            ),
            incident_key,
        )

    def test_rolling_cache_uses_source_watermark(self) -> None:
        now = datetime(2026, 7, 29, 14, 0)
        first = get_or_generate_daily_take(
            db_path=str(self.calls_db),
            day="today",
            now=now,
        )
        unchanged = get_or_generate_daily_take(
            db_path=str(self.calls_db),
            day="today",
            now=datetime(2026, 7, 29, 15, 0),
        )
        self.assertFalse(first["cached"])
        self.assertTrue(unchanged["cached"])
        self.assertEqual(first["source_watermark"]["max_call_id"], 5)

        _insert_call(
            self.calls_db,
            6,
            "2026-07-29T15:01:00",
            "Milford",
            "police",
            "mpd",
            "Traffic",
            "208 responding to a traffic complaint.",
            units=["208"],
        )
        changed = get_or_generate_daily_take(
            db_path=str(self.calls_db),
            day="today",
            now=datetime(2026, 7, 29, 15, 2),
        )
        self.assertFalse(changed["cached"])
        self.assertEqual(changed["source_watermark"]["call_count"], 6)
        self.assertEqual(changed["source_watermark"]["max_call_id"], 6)
        self.assertEqual(changed["fact_pack"]["totals"]["transmissions"], 6)

    def test_final_edition_is_preserved_after_source_changes(self) -> None:
        final = get_or_generate_daily_take(
            db_path=str(self.calls_db),
            day="2026-07-29",
            edition_type="final",
            now=datetime(2026, 7, 30, 0, 10),
            force=True,
        )
        _insert_call(
            self.calls_db,
            6,
            "2026-07-29T23:59:00",
            "Milford",
            "fire",
            "mfd",
            "Alarm",
            "Engine 2 responding to a late alarm.",
            units=["Engine 2"],
        )
        preserved = get_or_generate_daily_take(
            db_path=str(self.calls_db),
            day="2026-07-29",
            edition_type="final",
            now=datetime(2026, 7, 30, 0, 20),
            force=True,
        )
        self.assertFalse(final["cached"])
        self.assertTrue(preserved["cached"])
        self.assertEqual(preserved["generated_at"], final["generated_at"])
        self.assertEqual(preserved["source_watermark"]["max_call_id"], 5)
        self.assertEqual(preserved["fact_pack"]["totals"]["transmissions"], 5)

    def test_rolling_commentary_is_regenerated_but_final_is_frozen(self) -> None:
        generated = []

        def commentary_generator(result):
            generated.append(result["edition_type"])
            take = json.loads(json.dumps(result["take"]))
            take["ned_take"] = f"Variable take {len(generated)}"
            take["highlights"][0]["ned_note"] = (
                f"Variable incident take {len(generated)}"
            )
            return take

        first = get_or_generate_daily_take(
            db_path=str(self.calls_db),
            day="today",
            now=datetime(2026, 7, 29, 14, 0),
            commentary_generator=commentary_generator,
        )
        second = get_or_generate_daily_take(
            db_path=str(self.calls_db),
            day="today",
            now=datetime(2026, 7, 29, 14, 1),
            commentary_generator=commentary_generator,
        )
        self.assertEqual(first["take"]["ned_take"], "Variable take 1")
        self.assertEqual(second["take"]["ned_take"], "Variable take 2")
        self.assertEqual(generated, ["rolling", "rolling"])
        self.assertEqual(second["commentary_generator"], "local-llm")
        prepared = get_or_generate_daily_take(
            db_path=str(self.calls_db),
            day="today",
            now=datetime(2026, 7, 29, 14, 2),
        )
        self.assertEqual(prepared["take"]["ned_take"], "Variable take 2")
        self.assertEqual(
            prepared["commentary_generator"],
            "stored-local-llm",
        )
        incident = get_incident_detail(
            second["take"]["highlights"][0]["incident_key"],
            db_path=str(self.calls_db),
        )
        self.assertEqual(incident["commentary"], "Variable incident take 2")
        self.assertEqual(
            incident["commentary_generator"],
            "stored-local-llm",
        )

        generated.clear()
        final = get_or_generate_daily_take(
            db_path=str(self.calls_db),
            day="2026-07-29",
            edition_type="final",
            now=datetime(2026, 7, 30, 0, 10),
            commentary_generator=commentary_generator,
        )
        preserved = get_or_generate_daily_take(
            db_path=str(self.calls_db),
            day="2026-07-29",
            edition_type="final",
            now=datetime(2026, 7, 30, 0, 20),
            force=True,
            commentary_generator=commentary_generator,
        )
        self.assertEqual(generated, ["final"])
        self.assertEqual(final["take"]["ned_take"], "Variable take 1")
        self.assertEqual(preserved["take"]["ned_take"], "Variable take 1")
        self.assertEqual(
            preserved["commentary_generator"],
            "stored-local-llm",
        )

    def test_commentary_failure_uses_grounded_fallback(self) -> None:
        def failed_generator(_result):
            raise RuntimeError("model offline")

        result = get_or_generate_daily_take(
            db_path=str(self.calls_db),
            day="today",
            now=datetime(2026, 7, 29, 14, 0),
            commentary_generator=failed_generator,
        )
        self.assertEqual(result["commentary_generator"], "deterministic-fallback")
        self.assertTrue(result["take"]["ned_take"])
        self.assertNotIn("Variable take", result["take"]["ned_take"])

    def test_town_scope_and_day_validation(self) -> None:
        fact_pack = build_daily_fact_pack(
            db_path=str(self.calls_db),
            day="2026-07-29",
            town="Bellingham",
            persist=False,
        )
        self.assertEqual(fact_pack["totals"]["transmissions"], 2)
        self.assertEqual(fact_pack["scope"]["town"], "Bellingham")
        self.assertEqual(
            parse_day("yesterday", datetime(2026, 7, 29)),
            "2026-07-28",
        )
        with self.assertRaises(ValueError):
            parse_day("last fortnight", datetime(2026, 7, 29))

    def test_fire_and_police_lifecycle_closure(self) -> None:
        lifecycle_db = Path(self.temp_dir.name) / "lifecycle.db"
        _create_calls_db(lifecycle_db)
        fire_calls = [
            (
                101,
                "2026-07-29T10:00:00",
                "TONE!!! Engine-4, fire alarm at 10 Main Street.",
                "10 Main Street",
            ),
            (
                102,
                "2026-07-29T10:01:00",
                "Engine-4 responding to 10 Main Street.",
                "10 Main Street",
            ),
            (
                103,
                "2026-07-29T10:20:00",
                "Engine-4 clear returning.",
                "",
            ),
            (
                104,
                "2026-07-29T10:35:00",
                "Engine-4 back in quarters.",
                "",
            ),
            (
                105,
                "2026-07-29T11:00:00",
                "TONE!!! Engine-4, another fire alarm at 10 Main Street.",
                "10 Main Street",
            ),
        ]
        for call_id, timestamp, transcript, address in fire_calls:
            _insert_call(
                lifecycle_db,
                call_id,
                timestamp,
                "Bellingham",
                "fire",
                "bfd",
                "Fire",
                transcript,
                address,
                ["Engine-4"],
                tone=call_id in {101, 105},
            )

        police_calls = [
            (201, "2026-07-29T12:00:00", "208 responding to the call."),
            (202, "2026-07-29T12:05:00", "208 is on scene."),
            (203, "2026-07-29T12:20:00", "208 to control. I'll be clear."),
            (204, "2026-07-29T12:21:00", "208 responding to another call."),
        ]
        for call_id, timestamp, transcript in police_calls:
            _insert_call(
                lifecycle_db,
                call_id,
                timestamp,
                "Milford",
                "police",
                "mpd",
                "Welfare Check",
                transcript,
                units=["208"],
            )

        build_daily_fact_pack(
            db_path=str(lifecycle_db),
            day="2026-07-29",
        )
        with sqlite3.connect(lifecycle_db) as conn:
            conn.row_factory = sqlite3.Row
            incidents = conn.execute(
                """
                SELECT source_call_ids_json, lifecycle_stages_json, closed
                FROM scanner_incidents
                ORDER BY start_timestamp
                """
            ).fetchall()

        by_ids = {
            tuple(json.loads(row["source_call_ids_json"])): row
            for row in incidents
        }
        self.assertIn((101, 102, 103, 104), by_ids)
        self.assertEqual(by_ids[(101, 102, 103, 104)]["closed"], 1)
        self.assertEqual(
            json.loads(by_ids[(101, 102, 103, 104)]["lifecycle_stages_json"]),
            ["dispatched", "responding", "returning", "in_quarters"],
        )
        self.assertIn((105,), by_ids)

        self.assertIn((201, 202, 203), by_ids)
        self.assertEqual(by_ids[(201, 202, 203)]["closed"], 1)
        self.assertEqual(
            json.loads(by_ids[(201, 202, 203)]["lifecycle_stages_json"])[-1],
            "cleared",
        )
        self.assertIn((204,), by_ids)


if __name__ == "__main__":
    unittest.main()
