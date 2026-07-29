import asyncio
import time
from pathlib import Path

from textual.app import App, ComposeResult
from textual.containers import Vertical
from textual.widgets import Label, Static, TabbedContent, TabPane

from tools.scanner_dashboard import ModelMaintenancePanel, ScannerControlRoom


class DownloadStatusApp(App):
    CSS = ScannerControlRoom.CSS

    def compose(self) -> ComposeResult:
        with Vertical(id="llm_page"):
            with TabbedContent(
                initial="llm_download_subtab",
                id="llm_workspace",
            ):
                with TabPane("Download", id="llm_download_subtab"):
                    yield ModelMaintenancePanel(id="models_panel")
            yield Static("[b]INFO[/b] Ready.", id="llm_global_info")


def test_download_status_timer_updates_the_visible_label(tmp_path: Path) -> None:
    async def check() -> None:
        app = DownloadStatusApp()
        async with app.run_test(size=(140, 50)) as pilot:
            panel = app.query_one("#models_panel", ModelMaintenancePanel)
            now = time.monotonic()
            panel._dl_active = True
            panel._dl_repo = "test/model"
            panel._dl_local_dir = str(tmp_path)
            panel._dl_initial_size = 0
            panel._dl_started_at = now
            panel._dl_last_size = 0
            panel._dl_last_sample = now

            await pilot.pause(1.2)

            rendered = str(app.query_one("#mm_dl_status", Label).render())
            assert "Downloading test/model" in rendered
            assert "received" in rendered
            global_info = str(app.query_one("#llm_global_info", Static).render())
            assert "Downloading test/model" in global_info
            assert app.query_one("#llm_global_info").region.height == 3

    asyncio.run(check())
