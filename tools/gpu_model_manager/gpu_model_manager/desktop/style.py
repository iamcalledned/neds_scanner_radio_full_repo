"""
gpu_model_manager/desktop/style.py

Qt dark stylesheet for the desktop app.
"""

DARK_STYLESHEET = """
QWidget {
    background-color: #0f1117;
    color: #e0e0e0;
    font-family: "Segoe UI", "Ubuntu", sans-serif;
    font-size: 13px;
}

QMainWindow, QDialog {
    background-color: #0f1117;
}

QTabWidget::pane {
    border: 1px solid #2d3141;
    background: #1a1d27;
    border-radius: 4px;
}

QTabBar::tab {
    background: #1a1d27;
    color: #9e9e9e;
    padding: 8px 18px;
    border: 1px solid #2d3141;
    border-bottom: none;
    border-radius: 4px 4px 0 0;
    margin-right: 2px;
}

QTabBar::tab:selected {
    background: #252836;
    color: #4fc3f7;
}

QTabBar::tab:hover {
    background: #252836;
    color: #e0e0e0;
}

QTableWidget {
    background: #1a1d27;
    alternate-background-color: #1e2130;
    border: 1px solid #2d3141;
    border-radius: 4px;
    gridline-color: #2d3141;
}

QTableWidget::item {
    padding: 4px 8px;
}

QTableWidget::item:selected {
    background: #0d2a47;
    color: #4fc3f7;
}

QHeaderView::section {
    background: #252836;
    color: #9e9e9e;
    padding: 6px 8px;
    border: none;
    border-bottom: 1px solid #2d3141;
    border-right: 1px solid #2d3141;
    font-size: 11px;
    font-weight: bold;
    text-transform: uppercase;
}

QPushButton {
    background: #252836;
    color: #e0e0e0;
    border: 1px solid #2d3141;
    border-radius: 4px;
    padding: 5px 14px;
    font-weight: 600;
}

QPushButton:hover {
    background: #2d3141;
    color: #4fc3f7;
}

QPushButton:pressed {
    background: #1a1d27;
}

QPushButton:disabled {
    color: #546e7a;
    background: #1a1d27;
}

QPushButton#btn_green {
    background: #1b5e20;
    color: #a5d6a7;
    border-color: #2e7d32;
}

QPushButton#btn_red {
    background: #4a0000;
    color: #ef9a9a;
    border-color: #b71c1c;
}

QPushButton#btn_yellow {
    background: #4a3000;
    color: #ffe082;
    border-color: #f57f17;
}

QPushButton#btn_accent {
    background: #4fc3f7;
    color: #0f1117;
    border: none;
}

QPushButton#btn_accent:hover {
    background: #81d4fa;
}

QLabel {
    color: #e0e0e0;
}

QLabel#label_muted {
    color: #9e9e9e;
    font-size: 12px;
}

QLabel#label_title {
    color: #9e9e9e;
    font-size: 11px;
    text-transform: uppercase;
    font-weight: bold;
    letter-spacing: 1px;
}

QLabel#label_value {
    color: #e0e0e0;
    font-size: 22px;
    font-weight: bold;
}

QLabel#label_green  { color: #4caf50; }
QLabel#label_red    { color: #f44336; }
QLabel#label_yellow { color: #ffc107; }
QLabel#label_accent { color: #4fc3f7; }
QLabel#label_protected { color: #ff8f00; font-weight: bold; }

QProgressBar {
    background: #2d3141;
    border: none;
    border-radius: 4px;
    height: 10px;
    text-align: center;
}

QProgressBar::chunk {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
        stop:0 #4fc3f7, stop:1 #0288d1);
    border-radius: 4px;
}

QTextEdit, QPlainTextEdit {
    background: #0a0c10;
    color: #9e9e9e;
    border: 1px solid #2d3141;
    border-radius: 4px;
    font-family: "Consolas", "Courier New", monospace;
    font-size: 12px;
}

QLineEdit {
    background: #252836;
    color: #e0e0e0;
    border: 1px solid #2d3141;
    border-radius: 4px;
    padding: 4px 8px;
}

QLineEdit:focus {
    border-color: #4fc3f7;
}

QComboBox {
    background: #252836;
    color: #e0e0e0;
    border: 1px solid #2d3141;
    border-radius: 4px;
    padding: 4px 8px;
}

QComboBox::drop-down { border: none; }
QComboBox:hover { border-color: #4fc3f7; }

QScrollBar:vertical {
    background: #1a1d27;
    width: 8px;
    border-radius: 4px;
}
QScrollBar::handle:vertical {
    background: #2d3141;
    border-radius: 4px;
    min-height: 20px;
}
QScrollBar::handle:vertical:hover { background: #546e7a; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }

QGroupBox {
    border: 1px solid #2d3141;
    border-radius: 4px;
    margin-top: 0.8em;
    padding-top: 0.6em;
    font-weight: bold;
    color: #9e9e9e;
}

QGroupBox::title {
    subcontrol-origin: margin;
    left: 8px;
    padding: 0 4px;
    color: #9e9e9e;
    font-size: 11px;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}

QCheckBox { color: #e0e0e0; }
QCheckBox::indicator {
    width: 14px;
    height: 14px;
    border: 1px solid #2d3141;
    border-radius: 2px;
    background: #252836;
}
QCheckBox::indicator:checked {
    background: #4fc3f7;
    border-color: #4fc3f7;
}

QSplitter::handle {
    background: #2d3141;
}

QStatusBar {
    background: #1a1d27;
    color: #9e9e9e;
    border-top: 1px solid #2d3141;
    font-size: 12px;
}
"""
