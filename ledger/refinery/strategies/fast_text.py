from __future__ import annotations

from pathlib import Path

from ledger.refinery.config import ConfigLoader


class FastTextExtractor:
    def __init__(self, rules_path: str | Path = "rubric/extraction_rules.yaml") -> None:
        self.config_loader = ConfigLoader(rules_path)

    def extract(self, pdf_path: Path, page_numbers: list[int]):
        return []

    def calculate_confidence(self, page_data: dict) -> float:
        density = float(page_data.get("char_density", 0.0) or 0.0)
        return min(1.0, max(0.0, density * 1000))