from __future__ import annotations

from pathlib import Path

from ledger.refinery.config import ConfigLoader


class LayoutExtractor:
    def __init__(self, rules_path: str | Path = "rubric/extraction_rules.yaml") -> None:
        self.config_loader = ConfigLoader(rules_path)

    def extract(self, pdf_path: Path, page_numbers: list[int]):
        return []

    def _text_clarity_score(self, text: str) -> float:
        if not text:
            return 0.0
        return min(1.0, len(text.split()) / 100.0)

    def calculate_ocr_confidence(self, layout_data: dict) -> float:
        parsed_ok = bool(layout_data.get("parsed_ok", False))
        return 0.8 if parsed_ok else 0.2