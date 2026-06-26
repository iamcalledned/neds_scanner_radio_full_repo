"""
desktop/style.py — Shared dark-theme constants and style helpers.
"""
from __future__ import annotations

# ── Palette ────────────────────────────────────────────────────────────────
BG_DEEP       = "#0f172a"   # main background
BG_CARD       = "#1e293b"   # card / group background
BG_CARD_DARK  = "#162032"   # alternate row / raised section
BORDER_DIM    = "#334155"   # inactive borders
BORDER_MED    = "#475569"   # active card borders
TEXT_PRIMARY  = "#e2e8f0"
TEXT_MUTED    = "#94a3b8"
TEXT_DIM      = "#64748b"
ACCENT_BLUE   = "#38bdf8"

# ── Status colours ──────────────────────────────────────────────────────────
STATUS_HEALTHY  = "#22c55e"
STATUS_WARNING  = "#f59e0b"
STATUS_CRITICAL = "#ef4444"
STATUS_UNKNOWN  = "#94a3b8"
STATUS_INACTIVE = "#94a3b8"
STATUS_ACTIVE   = "#22c55e"
STATUS_FAILED   = "#ef4444"
STATUS_MISSING  = "#f97316"
STATUS_OPTIONAL = "#60a5fa"

# ── Button styles ───────────────────────────────────────────────────────────
BTN_DEFAULT = f"""
    QPushButton {{
        background: {BG_CARD};
        color: {TEXT_PRIMARY};
        border: 1px solid {BORDER_MED};
        border-radius: 5px;
        padding: 5px 16px;
        font-size: 12px;
        min-height: 28px;
    }}
    QPushButton:hover  {{ background: #3d5168; }}
    QPushButton:pressed {{ background: #283548; }}
    QPushButton:disabled {{ color: {TEXT_DIM}; background: {BG_CARD_DARK}; }}
"""

BTN_START = f"""
    QPushButton {{
        background: #14532d;
        color: #86efac;
        border: 1px solid #22c55e;
        border-radius: 5px;
        padding: 5px 16px;
        font-size: 12px;
        min-height: 28px;
    }}
    QPushButton:hover  {{ background: #166534; }}
    QPushButton:pressed {{ background: #0f3d20; }}
"""

BTN_STOP = f"""
    QPushButton {{
        background: #450a0a;
        color: #fca5a5;
        border: 1px solid #ef4444;
        border-radius: 5px;
        padding: 5px 16px;
        font-size: 12px;
        min-height: 28px;
    }}
    QPushButton:hover  {{ background: #5a0f0f; }}
    QPushButton:pressed {{ background: #350808; }}
"""

BTN_RESTART = f"""
    QPushButton {{
        background: #431407;
        color: #fdba74;
        border: 1px solid #f59e0b;
        border-radius: 5px;
        padding: 5px 16px;
        font-size: 12px;
        min-height: 28px;
    }}
    QPushButton:hover  {{ background: #57190a; }}
    QPushButton:pressed {{ background: #351005; }}
"""

BTN_DANGER = f"""
    QPushButton {{
        background: #7f1d1d;
        color: #fecaca;
        border: 1px solid #ef4444;
        border-radius: 5px;
        padding: 6px 20px;
        font-size: 13px;
        font-weight: bold;
        min-height: 32px;
    }}
    QPushButton:hover  {{ background: #991b1b; }}
    QPushButton:pressed {{ background: #6b1212; }}
"""

BTN_BLUE = f"""
    QPushButton {{
        background: #1e3a5f;
        color: #93c5fd;
        border: 1px solid #3b82f6;
        border-radius: 5px;
        padding: 5px 16px;
        font-size: 12px;
        min-height: 28px;
    }}
    QPushButton:hover  {{ background: #1e4070; }}
    QPushButton:pressed {{ background: #162d4a; }}
"""

# ── Status colour lookup ────────────────────────────────────────────────────
def status_color(status: str) -> str:
    return {
        "healthy":     STATUS_HEALTHY,
        "warning":     STATUS_WARNING,
        "critical":    STATUS_CRITICAL,
        "unknown":     STATUS_UNKNOWN,
        "active":      STATUS_ACTIVE,
        "inactive":    STATUS_INACTIVE,
        "failed":      STATUS_FAILED,
        "missing":     STATUS_MISSING,
        "activating":  STATUS_WARNING,
        "deactivating":STATUS_WARNING,
        "optional":    STATUS_OPTIONAL,
    }.get(status.lower(), STATUS_UNKNOWN)


# ── Global dark stylesheet ──────────────────────────────────────────────────
GLOBAL_STYLESHEET = f"""
    QMainWindow, QWidget {{
        background-color: {BG_DEEP};
        color: {TEXT_PRIMARY};
        font-family: 'Segoe UI', 'Inter', sans-serif;
        font-size: 13px;
    }}
    QTabWidget::pane {{
        border: none;
        background: {BG_DEEP};
    }}
    QTabBar::tab {{
        background: {BG_CARD};
        color: {TEXT_MUTED};
        padding: 9px 22px;
        border: none;
        border-bottom: 2px solid transparent;
        font-size: 13px;
        min-width: 90px;
    }}
    QTabBar::tab:selected {{
        color: {ACCENT_BLUE};
        border-bottom: 2px solid {ACCENT_BLUE};
        background: {BG_DEEP};
    }}
    QTabBar::tab:hover {{
        color: {TEXT_PRIMARY};
        background: #283548;
    }}
    QPushButton {{
        background: {BG_CARD};
        color: {TEXT_PRIMARY};
        border: 1px solid {BORDER_MED};
        border-radius: 5px;
        padding: 5px 16px;
        font-size: 12px;
        min-height: 28px;
    }}
    QPushButton:hover  {{ background: #3d5168; }}
    QPushButton:pressed {{ background: #283548; }}
    QPushButton:disabled {{ color: {TEXT_DIM}; background: {BG_CARD_DARK}; }}
    QGroupBox {{
        color: {TEXT_MUTED};
        font-weight: bold;
        font-size: 12px;
        border: 1px solid {BORDER_DIM};
        border-radius: 6px;
        margin-top: 10px;
        padding-top: 8px;
    }}
    QGroupBox::title {{
        subcontrol-origin: margin;
        left: 12px;
        padding: 0 6px;
        color: {TEXT_MUTED};
    }}
    QTableWidget {{
        background: {BG_CARD};
        alternate-background-color: {BG_CARD_DARK};
        color: {TEXT_PRIMARY};
        gridline-color: {BORDER_DIM};
        border: 1px solid {BORDER_DIM};
        border-radius: 4px;
        selection-background-color: #1d3a5e;
    }}
    QHeaderView::section {{
        background: {BORDER_DIM};
        color: {TEXT_MUTED};
        padding: 5px 10px;
        border: none;
        font-weight: bold;
        font-size: 12px;
    }}
    QScrollArea {{ background: transparent; border: none; }}
    QScrollBar:vertical {{
        background: {BG_CARD};
        width: 10px;
        border-radius: 5px;
    }}
    QScrollBar::handle:vertical {{
        background: {BORDER_MED};
        border-radius: 5px;
        min-height: 20px;
    }}
    QTextEdit {{
        background: {BG_CARD};
        color: {TEXT_PRIMARY};
        border: 1px solid {BORDER_DIM};
        border-radius: 4px;
    }}
    QLineEdit {{
        background: {BG_CARD};
        color: {TEXT_PRIMARY};
        border: 1px solid {BORDER_MED};
        border-radius: 4px;
        padding: 4px 8px;
        font-size: 12px;
    }}
    QCheckBox {{
        color: {TEXT_MUTED};
        font-size: 12px;
        spacing: 6px;
    }}
    QLabel {{ color: {TEXT_PRIMARY}; }}
    QStatusBar {{
        background: {BG_CARD};
        color: {TEXT_DIM};
        font-size: 11px;
    }}
    QSplitter::handle {{ background: {BORDER_DIM}; }}
"""
