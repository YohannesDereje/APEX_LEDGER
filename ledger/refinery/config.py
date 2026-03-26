from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


class ConfigLoader:
    def __init__(self, rules_path: str | Path = "rubric/extraction_rules.yaml") -> None:
        self.rules_path = Path(rules_path)
        self.config: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.rules_path.exists():
            return {}
        data = yaml.safe_load(self.rules_path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}

    def get(self, dotted_key: str, default: Any = None) -> Any:
        current: Any = self.config
        for part in dotted_key.split("."):
            if not isinstance(current, dict) or part not in current:
                return default
            current = current[part]
        return current