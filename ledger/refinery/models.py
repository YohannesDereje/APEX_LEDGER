from pydantic import BaseModel

from ledger.schema.events import FinancialFacts


class ApplicantProfile(BaseModel):
    applicant_id: str


class Document(BaseModel):
    document_id: str
    content: str
    filename: str | None = None
    document_type: str | None = None


__all__ = ["ApplicantProfile", "Document", "FinancialFacts"]