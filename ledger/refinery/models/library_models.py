from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .schemas import ExtractedDocument


class ApplicantProfile(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)

    applicant_id: str = "unknown"
    full_name: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class Document(BaseModel):
    model_config = ConfigDict(extra="allow", validate_assignment=True)

    pdf_path: str
    document_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("pdf_path")
    @classmethod
    def validate_pdf_path(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("pdf_path cannot be empty.")
        return value

    @property
    def path(self) -> Path:
        return Path(self.pdf_path)


class FinancialFacts(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    applicant_id: str
    extracted_documents: List[ExtractedDocument] = Field(default_factory=list)
    total_documents: int = 0
    total_pages: int = 0
    total_cost_usd: float = 0.0
    generated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class QualityAssessment(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    is_coherent: bool
    reextraction_recommended: bool
    notes: List[str] = Field(default_factory=list)


class FinalOutput(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    financial_facts: FinancialFacts
    quality_assessment: QualityAssessment
    failures: List[str] = Field(default_factory=list)
