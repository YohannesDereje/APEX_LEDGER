from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import List

from ledger.refinery.agents.chunker import ChunkingEngine
from ledger.refinery.agents.extractor import ExtractionRouter
from ledger.refinery.agents.indexer import PageIndexBuilder
from ledger.refinery.agents.triage import TriageAgent
from ledger.refinery.models import ApplicantProfile, Document, ExtractedDocument, FinalOutput, FinancialFacts, QualityAssessment


class Orchestrator:
    def __init__(
        self,
        rules_path: str | Path = "rubric/extraction_rules.yaml",
        output_root: str | Path = ".refinery",
        max_pages_per_doc: int = 21,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.rules_path = Path(rules_path)
        self.output_root = Path(output_root)
        self.max_pages_per_doc = int(max_pages_per_doc)

        self.output_dir = self.output_root / "profiles"
        self.pageindex_dir = self.output_root / "pageindex"
        self.ledger_path = self.output_root / "extraction_ledger.jsonl"

    @staticmethod
    def _safe_pageindex_stem(file_name: str, fallback_stem: str) -> str:
        raw = Path(str(file_name).strip()).stem if str(file_name).strip() else fallback_stem
        sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", raw).strip("._-")
        return sanitized or fallback_stem

    def _ensure_output_dirs(self) -> None:
        os.makedirs(self.output_root, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.pageindex_dir, exist_ok=True)
        if not self.ledger_path.exists():
            self.ledger_path.write_text("", encoding="utf-8")

    async def run_pipeline(self, applicant: ApplicantProfile, documents: list[Document]) -> FinalOutput:
        try:
            from dotenv import load_dotenv

            load_dotenv()
        except Exception:
            pass

        self._ensure_output_dirs()

        triage_agent = TriageAgent(rules_path=self.rules_path, max_pages_per_doc=self.max_pages_per_doc)

        api_key = (
            os.getenv("VISION_API_KEY")
            or os.getenv("OPENAI_API_KEY")
            or os.getenv("OPENROUTER_API_KEY")
        )

        extraction_router = ExtractionRouter(
            api_key=api_key,
            rules_path=self.rules_path,
            max_pages_per_doc=self.max_pages_per_doc,
        )
        chunking_engine = ChunkingEngine(rules_path=self.rules_path, max_pages_per_doc=self.max_pages_per_doc)

        extracted_documents: List[ExtractedDocument] = []
        total_pages = 0
        failures: list[str] = []

        for document in documents:
            pdf_path = document.path
            output_path = self.output_dir / f"{pdf_path.stem}_refined.json"

            try:
                profile = triage_agent.process_pdf(pdf_path)
                limited_pages = profile.pages[: self.max_pages_per_doc]
                profile = profile.model_copy(
                    update={
                        "pages": limited_pages,
                        "total_pages": len(limited_pages),
                    }
                )

                refined = extraction_router.process_document(pdf_path=pdf_path, profile=profile)
                output_path.write_text(refined.model_dump_json(indent=4), encoding="utf-8")

                semantic_chunks = chunking_engine.chunk_document(refined)
                pageindex_stem = self._safe_pageindex_stem(profile.file_name, pdf_path.stem)
                pageindex_path = self.pageindex_dir / f"{pageindex_stem}.json"

                page_index_builder = PageIndexBuilder(
                    rules_path=self.rules_path,
                    persistence_path=pageindex_path,
                )
                page_index_builder.build_tree(chunks=semantic_chunks, persist=False)
                page_index_builder.serialize(pageindex_path)

                extracted_documents.append(refined)
                total_pages += profile.total_pages
            except Exception as exc:
                self.logger.exception("Failed processing %s", pdf_path.name)
                failures.append(f"{pdf_path.name}: {exc}")

        total_cost = float(getattr(extraction_router, "total_cost_usd", extraction_router.vision_extractor.total_spend))
        quality_notes: list[str] = []

        if failures:
            quality_notes.append(f"{len(failures)} document(s) failed during processing.")
        if not extracted_documents:
            quality_notes.append("No documents were successfully extracted.")

        quality_assessment = QualityAssessment(
            is_coherent=bool(extracted_documents) and not failures,
            reextraction_recommended=bool(failures) or not bool(extracted_documents),
            notes=quality_notes,
        )

        financial_facts = FinancialFacts(
            applicant_id=applicant.applicant_id,
            extracted_documents=extracted_documents,
            total_documents=len(extracted_documents),
            total_pages=total_pages,
            total_cost_usd=total_cost,
        )

        return FinalOutput(
            financial_facts=financial_facts,
            quality_assessment=quality_assessment,
            failures=failures,
        )
