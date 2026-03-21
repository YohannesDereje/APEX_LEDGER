import uuid
from enum import Enum
from typing import Optional

from ledger.event_store import EventStore
from ledger.schema.events import (
	ApplicationDeclined,
	ApplicationApproved,
	ApplicationSubmitted,
	ComplianceCheckRequested,
	ComplianceCheckCompleted,
	CreditAnalysisCompleted,
	CreditAnalysisRequested,
	DecisionGenerated,
	DecisionRequested,
	DomainError,
	StoredEvent,
)


class ApplicationStatus(str, Enum):
	SUBMITTED = "SUBMITTED"
	AWAITING_ANALYSIS = "AWAITING_ANALYSIS"
	ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
	COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
	PENDING_DECISION = "PENDING_DECISION"
	APPROVED_PENDING_HUMAN = "APPROVED_PENDING_HUMAN"
	DECLINED_PENDING_HUMAN = "DECLINED_PENDING_HUMAN"
	FINAL_APPROVED = "FINAL_APPROVED"
	FINAL_DECLINED = "FINAL_DECLINED"


class LoanApplicationAggregate:
	def __init__(self, application_id: uuid.UUID):
		self.application_id: uuid.UUID = application_id
		self.version: int = 0
		self.status: Optional[ApplicationStatus] = None
		self.applicant_id: Optional[str] = None
		self.requested_amount: Optional[float] = None
		self.approved_amount: Optional[float] = None

	@classmethod
	async def load(cls, store: EventStore, application_id: uuid.UUID) -> "LoanApplicationAggregate":
		agg = cls(application_id=application_id)
		stream_id = f"loan-{application_id}"
		events = await store.load_stream(stream_id)

		for event in events:
			agg._apply(event)

		return agg

	def _apply(self, event: StoredEvent) -> None:
		handler_name = f"_on_{event.event_type}"
		handler = getattr(self, handler_name, None)
		if handler:
			handler(event)

		self.version = event.stream_position

	def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
		self.status = ApplicationStatus.SUBMITTED
		self.applicant_id = event.payload["applicant_id"]
		self.requested_amount = event.payload["requested_amount_usd"]

	def _on_ApplicationApproved(self, event: StoredEvent) -> None:
		self.status = ApplicationStatus.FINAL_APPROVED
		self.approved_amount = event.payload["approved_amount_usd"]

	def _on_CreditAnalysisRequested(self, event: StoredEvent) -> None:
		self.status = ApplicationStatus.AWAITING_ANALYSIS

	def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
		self.status = ApplicationStatus.ANALYSIS_COMPLETE

	def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
		self.status = ApplicationStatus.COMPLIANCE_REVIEW

	def _on_ComplianceCheckCompleted(self, event: StoredEvent) -> None:
		self.status = ApplicationStatus.PENDING_DECISION

	def _on_DecisionRequested(self, event: StoredEvent) -> None:
		self.status = ApplicationStatus.PENDING_DECISION

	def _on_DecisionGenerated(self, event: StoredEvent) -> None:
		recommendation = event.payload.get("recommendation")
		if recommendation == "APPROVE":
			self.status = ApplicationStatus.APPROVED_PENDING_HUMAN
		elif recommendation == "DECLINE":
			self.status = ApplicationStatus.DECLINED_PENDING_HUMAN
		else:
			self.status = ApplicationStatus.PENDING_DECISION

	def _on_ApplicationDeclined(self, event: StoredEvent) -> None:
		self.status = ApplicationStatus.FINAL_DECLINED

	def _ensure_status(self, expected_status: ApplicationStatus) -> None:
		if self.status != expected_status:
			raise DomainError(
				f"Application is not in {expected_status.value} state."
			)

	def assert_is_submitted(self) -> None:
		self._ensure_status(ApplicationStatus.SUBMITTED)

	def assert_awaiting_credit_analysis(self) -> None:
		self._ensure_status(ApplicationStatus.AWAITING_ANALYSIS)

