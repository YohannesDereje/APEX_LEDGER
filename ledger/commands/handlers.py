import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
from ledger.event_store import EventStore
from ledger.schema.events import (
	ApplicationSubmitted,
	CreditAnalysisCompleted,
	CreditDecision,
	LoanPurpose,
)


class SubmitApplicationCommand(BaseModel):
	application_id: str
	applicant_id: str
	requested_amount_usd: Decimal
	loan_purpose: LoanPurpose
	loan_term_months: int
	submission_channel: str
	contact_email: str
	contact_name: str
	submitted_at: datetime = Field(default_factory=datetime.utcnow)
	application_reference: str


class CreditAnalysisCompletedCommand(BaseModel):
	application_id: str
	agent_id: str
	session_id: str
	model_version: str
	decision: CreditDecision
	model_deployment_id: str
	analysis_duration_ms: int
	completed_at: datetime = Field(default_factory=datetime.utcnow)

	model_config = ConfigDict(extra="ignore")


def _hash_input(data: dict) -> str:
	return hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode("utf-8")).hexdigest()


async def handle_submit_application(
	cmd: SubmitApplicationCommand,
	store: EventStore,
	correlation_id: Optional[str] = None,
	causation_id: Optional[str] = None,
) -> None:
	event = ApplicationSubmitted(
		application_id=cmd.application_id,
		applicant_id=cmd.applicant_id,
		requested_amount_usd=cmd.requested_amount_usd,
		loan_purpose=cmd.loan_purpose,
		loan_term_months=cmd.loan_term_months,
		submission_channel=cmd.submission_channel,
		contact_email=cmd.contact_email,
		contact_name=cmd.contact_name,
		submitted_at=cmd.submitted_at,
		application_reference=cmd.application_reference,
	)

	await store.append(
		stream_id=f"loan-{cmd.application_id}",
		events=[event],
		expected_version=0,
		aggregate_type="LoanApplication",
		correlation_id=correlation_id,
		causation_id=causation_id,
	)


async def handle_credit_analysis_completed(
	cmd: CreditAnalysisCompletedCommand,
	store: EventStore,
	correlation_id: Optional[str] = None,
	causation_id: Optional[str] = None,
) -> None:
	app = await LoanApplicationAggregate.load(store, cmd.application_id)
	session = await AgentSessionAggregate.load(store, cmd.session_id)

	app.assert_awaiting_credit_analysis()
	session.assert_session_started()

	event = CreditAnalysisCompleted(
		application_id=cmd.application_id,
		session_id=cmd.session_id,
		decision=cmd.decision,
		model_version=cmd.model_version,
		model_deployment_id=cmd.model_deployment_id,
		input_data_hash=_hash_input(
			{
				"application_id": cmd.application_id,
				"agent_id": cmd.agent_id,
				"session_id": cmd.session_id,
				"model_version": cmd.model_version,
				"decision": cmd.decision.model_dump(mode="json"),
			}
		),
		analysis_duration_ms=cmd.analysis_duration_ms,
		completed_at=cmd.completed_at,
	)

	await store.append(
		stream_id=f"loan-{cmd.application_id}",
		events=[event],
		expected_version=app.version,
		aggregate_type="LoanApplication",
		correlation_id=correlation_id,
		causation_id=causation_id,
	)
