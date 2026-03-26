from __future__ import annotations

from datetime import datetime, timezone
import logging
import os
from pathlib import Path
from typing import Any

import pdfplumber

from ledger.refinery.config import ConfigLoader
from ledger.refinery.models import BBox, LDU, ProvenanceChain, StrategyName

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore[assignment]


class VisionExtractor:
    def __init__(
        self,
        api_key: str | None = None,
        api_base: str | None = None,
        model_name: str = "openai/gpt-4o-mini",
        max_budget: float = 30.0,
        dpi: int = 140,
        rules_path: str | Path = "rubric/extraction_rules.yaml",
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.api_key = api_key
        self.api_base = api_base
        self.model_name = model_name
        self.fallback_model_name = os.getenv("OPENROUTER_FALLBACK_MODEL", "openai/gpt-4o-mini")
        self.max_budget = max_budget
        self.dpi = dpi
        self.config_loader = ConfigLoader(rules_path)
        self.total_spend = 0.0
        self.last_call_cost_usd = 0.0
        self.last_usage = {
            "prompt_tokens": 0,
            "image_tokens": 0,
            "completion_tokens": 0,
        }
        self.client: Any = None
        if self.api_key and OpenAI is not None:
            try:
                kwargs = {"api_key": self.api_key}
                if self.api_base:
                    kwargs["base_url"] = self.api_base
                self.client = OpenAI(**kwargs)
            except Exception as exc:  # pragma: no cover
                self.logger.warning("Failed to initialize vision client: %s", exc)

    def _page_text(self, pdf_path: Path, page_number: int) -> str:
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[page_number - 1]
            return (page.extract_text() or "").strip()

    def _call_model(self, model_name: str, prompt: str) -> str:
        response = self.client.chat.completions.create(
            model=model_name,
            messages=[
                {
                    "role": "system",
                    "content": "You convert PDF page content into clean financial markdown for extraction.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0,
            max_tokens=1200,
        )
        usage = getattr(response, "usage", None)
        self.last_usage = {
            "prompt_tokens": int(getattr(usage, "prompt_tokens", 0) or 0),
            "image_tokens": 0,
            "completion_tokens": int(getattr(usage, "completion_tokens", 0) or 0),
        }
        self.last_call_cost_usd = 0.0
        self.total_spend += self.last_call_cost_usd
        return (response.choices[0].message.content or "").strip()

    def _llm_extract(self, pdf_path: Path, page_number: int, page_text: str) -> str:
        if self.client is None:
            return page_text

        prompt = (
            "You are extracting financial document text for downstream structuring. "
            "Return a concise markdown rendering of the important financial content from this page. "
            "Preserve figures, headings, and table-like relationships when possible.\n\n"
            f"File: {pdf_path.name}\n"
            f"Page: {page_number}\n"
            f"Page text:\n{page_text or '[NO_TEXT_EXTRACTED]'}"
        )

        try:
            text = self._call_model(self.model_name, prompt)
            return text or page_text
        except Exception as exc:
            self.logger.warning(
                "Vision model call failed for %s page %s via %s: %s",
                pdf_path.name,
                page_number,
                self.model_name,
                exc,
            )
            if self.fallback_model_name and self.fallback_model_name != self.model_name:
                try:
                    self.logger.info(
                        "Retrying %s page %s with fallback model %s",
                        pdf_path.name,
                        page_number,
                        self.fallback_model_name,
                    )
                    text = self._call_model(self.fallback_model_name, prompt)
                    return text or page_text
                except Exception as fallback_exc:
                    self.logger.warning(
                        "Fallback vision model call failed for %s page %s via %s: %s",
                        pdf_path.name,
                        page_number,
                        self.fallback_model_name,
                        fallback_exc,
                    )
            return page_text

    def extract(self, pdf_path: Path, page_numbers: list[int]):
        units: list[LDU] = []

        for page_number in page_numbers:
            page_text = self._page_text(pdf_path, page_number)
            llm_markdown = self._llm_extract(pdf_path, page_number, page_text)
            content_raw = page_text or llm_markdown or f"Scanned page {page_number} from {pdf_path.name}"
            content_markdown = llm_markdown or content_raw

            units.append(
                LDU(
                    uid=f"{pdf_path.stem}-p{page_number}-vision",
                    content_type="text",
                    content_raw=content_raw,
                    content_markdown=content_markdown,
                    bbox=BBox(x1=0.0, y1=0.0, x2=1.0, y2=1.0),
                    provenance=ProvenanceChain(
                        source_file=pdf_path.name,
                        page_number=page_number,
                        strategy_used=StrategyName.STRATEGY_C,
                        timestamp=datetime.now(timezone.utc).isoformat(),
                        strategy_escalation_path=[StrategyName.STRATEGY_C],
                    ),
                )
            )

        return units

    def calculate_confidence(self, payload: dict) -> float:
        uncertainty = bool(payload.get("uncertainty", False))
        return 0.65 if uncertainty else 0.95