
"""
collector_gui.py
----------------
PySide6 GUI wrapper around DataCollector.

Run:
    pip install PySide6 requests beautifulsoup4 urllib3
    python collector_gui.py
"""
from __future__ import annotations
import sys
import traceback
from typing import List

from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtGui import QFont, QTextOption
from PySide6.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLineEdit,
    QPushButton,
    QTextEdit,
    QLabel,
    QComboBox,
    QMessageBox,
)

from data_collector import DataCollector


class FetchThread(QThread):
    finished = Signal(str)
    error = Signal(str)

    def __init__(self, institution: str, sources: List[str], parent=None):
        super().__init__(parent)
        self.institution = institution
        self.sources = sources

    def run(self):
        try:
            text = DataCollector(sources=self.sources).collect(self.institution)
            self.finished.emit(text or "(no text retrieved)")
        except Exception as exc:
            tb = traceback.format_exc()
            self.error.emit(f"{exc}\n{tb}")


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Institution Context Collector")
        self.setMinimumSize(700, 480)

        # Widgets ------------------------------------------------------------
        self.input_line = QLineEdit()
        self.input_line.setPlaceholderText("Type institution name…")
        self.input_line.returnPressed.connect(self._on_fetch)

        self.sources_box = QComboBox()
        self.sources_box.addItems(
            ["wikipedia + homepage + news", "wikipedia only", "custom list"]
        )
        self.sources_box.currentIndexChanged.connect(self._on_mode_changed)

        self.custom_sources_line = QLineEdit()
        self.custom_sources_line.setPlaceholderText("Comma separated: wikipedia,news")
        self.custom_sources_line.setVisible(False)

        self.fetch_btn = QPushButton("Fetch")
        self.fetch_btn.clicked.connect(self._on_fetch)

        self.status_label = QLabel("Ready.")
        self.status_label.setStyleSheet("color: gray;")

        self.output_box = QTextEdit(readOnly=True)
        self.output_box.setFont(QFont("Consolas", 10))
        self.output_box.setWordWrapMode(QTextOption.WrapAnywhere)

        # Layout -------------------------------------------------------------
        top = QHBoxLayout()
        top.addWidget(self.input_line, 1)
        top.addWidget(self.sources_box)
        top.addWidget(self.custom_sources_line, 1)
        top.addWidget(self.fetch_btn)

        layout = QVBoxLayout(self)
        layout.addLayout(top)
        layout.addWidget(self.output_box, 1)
        layout.addWidget(self.status_label)

    # ---------------------------------------------------------------------
    def _on_mode_changed(self, idx: int):
        self.custom_sources_line.setVisible(idx == 2)

    def _on_fetch(self):
        inst = self.input_line.text().strip()
        if not inst:
            QMessageBox.warning(self, "Missing input", "Please enter an institution name.")
            return

        mode = self.sources_box.currentIndex()
        if mode == 0:
            sources = ["wikipedia", "homepage", "news"]
        elif mode == 1:
            sources = ["wikipedia"]
        else:
            raw = self.custom_sources_line.text()
            sources = [s.strip() for s in raw.split(",") if s.strip()]
            if not sources:
                QMessageBox.warning(
                    self,
                    "Missing sources",
                    "Specify at least one source or choose preset.",
                )
                return

        self._toggle_ui(False)
        self.output_box.clear()
        self.status_label.setText("Fetching…")
        self.status_label.setStyleSheet("color: blue;")

        self.worker = FetchThread(inst, sources, self)
        self.worker.finished.connect(self._on_done)
        self.worker.error.connect(self._on_error)
        self.worker.start()

    def _on_done(self, text: str):
        self._toggle_ui(True)
        self.output_box.setPlainText(text)
        self.status_label.setText("Done.")
        self.status_label.setStyleSheet("color: green;")

    def _on_error(self, msg: str):
        self._toggle_ui(True)
        self.status_label.setText("Error.")
        self.status_label.setStyleSheet("color: red;")
        QMessageBox.critical(self, "Fetch error", msg)

    def _toggle_ui(self, enable: bool):
        self.fetch_btn.setEnabled(enable)
        self.input_line.setEnabled(enable)
        self.sources_box.setEnabled(enable)
        self.custom_sources_line.setEnabled(enable)

# ---------------------------------------------------------------------------

def main():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
