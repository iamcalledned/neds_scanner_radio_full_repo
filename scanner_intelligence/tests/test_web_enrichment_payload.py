import importlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
WEB_ROOT = REPO_ROOT / "web"
for path in (str(REPO_ROOT), str(WEB_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)


class WebIntelligenceVisibilityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.original_log_dir = os.environ.get("LOG_DIR")
        os.environ["LOG_DIR"] = cls.temp_dir.name
        import shared.scanner_db as scanner_db

        with patch.object(scanner_db, "ensure_columns", return_value=None):
            cls.routes = importlib.import_module("routes.routes_scanner")

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.original_log_dir is None:
            os.environ.pop("LOG_DIR", None)
        else:
            os.environ["LOG_DIR"] = cls.original_log_dir
        cls.temp_dir.cleanup()

    def test_normal_call_payload_exposes_only_source_transcripts(self) -> None:
        row = {
            "id": 409818,
            "filename": "rec_2026-07-29_18-06-06_mpd.wav",
            "category": "mpd",
            "timestamp": "2026-07-29T18:06:07",
            "duration": 12.5,
            "transcript": "unit clear warning issued",
            "edited_transcript": None,
            "classification": "{}",
            "extra": json.dumps(
                {
                    "enhanced_transcript": "Unit clear. Warning issued.",
                    "secondary_transcripts": [{"model": "test", "transcript": "source"}],
                    "waveform": {
                        "version": 1,
                        "encoding": "uint8-base64",
                        "points": 3,
                        "peaks": "AP+A",
                    },
                }
            ),
            "save_for_eval": 0,
            "freeze_for_testing": 0,
            "derived_address": None,
        }

        payload = self.routes._row_to_call_payload(row)

        self.assertEqual(payload["transcript"], "unit clear warning issued")
        self.assertNotIn("enhanced_transcript", payload)
        self.assertNotIn("call_enrichment", payload)
        self.assertNotIn("enhanced_transcript", payload["metadata"])
        self.assertNotIn(
            "enhanced_transcript",
            payload["metadata"]["extra"],
        )
        self.assertIn("secondary_transcripts", payload["metadata"]["extra"])
        self.assertEqual(payload["waveform"]["peaks"], "AP+A")

    def test_normal_scanner_views_do_not_render_intelligence(self) -> None:
        normal_view_files = (
            WEB_ROOT / "templates" / "scanner_view.html",
            WEB_ROOT / "templates" / "scanner_call.html",
            WEB_ROOT / "templates" / "scanner_incident.html",
            WEB_ROOT / "static" / "js" / "scanner_view.js",
            WEB_ROOT / "static" / "js" / "scanner_incident.js",
        )
        combined = "\n".join(path.read_text() for path in normal_view_files)

        self.assertNotIn("AI enhanced call", combined)
        self.assertNotIn("call_enrichment", combined)
        self.assertNotIn("ai_enrichment", combined)
        self.assertNotIn("incident-commentary", combined)

    def test_commentary_remains_on_dedicated_neds_take_page(self) -> None:
        script = (WEB_ROOT / "static" / "js" / "scanner_neds_take.js").read_text()
        template = (WEB_ROOT / "templates" / "scanner_neds_take.html").read_text()

        self.assertIn("town.ned_take", script)
        self.assertIn("department.ned_take", script)
        self.assertIn("Ned’s Take", template)


if __name__ == "__main__":
    unittest.main()
