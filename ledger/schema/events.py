import json
import uuid
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, field_validator

# ==============================================================================
# Custom Exceptions
# ==============================================================================

class OptimisticConcurrencyError(Exception):
	"""
	Raised when an event store operation fails due to a stream version mismatch.
	This is an expected error in concurrent systems and should be handled by
	reloading the aggregate state and retrying the operation.
	"""
	def __init__(self, expected_version: int, actual_version: int, stream_id: str):
		self.expected_version = expected_version
		self.actual_version = actual_version
		self.stream_id = stream_id
		super().__init__(
			f"Optimistic concurrency conflict on stream '{stream_id}'. "
			f"Expected version {expected_version}, but found {actual_version}."
		)

class DomainError(Exception):
	"""
	Base exception for domain logic violations (e.g., business rule errors).
	"""
	pass

# ==============================================================================
# Core Event Models
# ==============================================================================

class BaseEvent(BaseModel):
	"""
	An abstract base class for all domain events.
	All specific events (e.g., ApplicationSubmitted) will inherit from this.
	"""
	model_config = ConfigDict(extra="ignore")

	@property
	def event_type(self) -> str:
		return self.__class__.__name__

	def to_payload(self) -> dict[str, Any]:
		return self.model_dump(mode="json")

	def to_store_dict(self) -> dict[str, Any]:
		return {
			"event_type": self.event_type,
			**self.to_payload(),
		}


class LoanPurpose(str, Enum):
	WORKING_CAPITAL = "WORKING_CAPITAL"
	EQUIPMENT = "EQUIPMENT"
	EXPANSION = "EXPANSION"
	INVENTORY = "INVENTORY"
	REFINANCING = "REFINANCING"
	REAL_ESTATE = "REAL_ESTATE"
	OTHER = "OTHER"


class DocumentType(str, Enum):
	APPLICATION_PROPOSAL = "APPLICATION_PROPOSAL"
	INCOME_STATEMENT = "INCOME_STATEMENT"
	BALANCE_SHEET = "BALANCE_SHEET"


class DocumentFormat(str, Enum):
	PDF = "PDF"
	XLSX = "XLSX"


class AgentType(str, Enum):
	DOCUMENT_PROCESSING = "DOCUMENT_PROCESSING"
	CREDIT_ANALYSIS = "CREDIT_ANALYSIS"
	FRAUD_DETECTION = "FRAUD_DETECTION"
	COMPLIANCE = "COMPLIANCE"
	DECISION_ORCHESTRATOR = "DECISION_ORCHESTRATOR"


class RiskTier(str, Enum):
	LOW = "LOW"
	MEDIUM = "MEDIUM"
	HIGH = "HIGH"


class FinancialFacts(BaseModel):
	model_config = ConfigDict(extra="ignore")

	total_revenue: Decimal
	gross_profit: Decimal
	operating_income: Decimal
	net_income: Decimal
	ebitda: Optional[Decimal]
	interest_expense: Decimal
	depreciation_amortization: Decimal
	total_assets: Decimal
	total_liabilities: Decimal
	total_equity: Decimal
	cash_and_equivalents: Decimal
	current_assets: Decimal
	current_liabilities: Decimal
	long_term_debt: Decimal
	accounts_receivable: Decimal
	inventory: Decimal
	operating_cash_flow: Decimal
	debt_to_equity: float
	current_ratio: float
	debt_to_ebitda: float
	interest_coverage: float
	gross_margin: float
	net_margin: float
	fiscal_year_end: date
	field_confidence: dict[str, float]
	page_references: dict[str, str]
	balance_sheet_balances: bool


class CreditDecision(BaseModel):
	model_config = ConfigDict(extra="ignore")

	risk_tier: RiskTier
	recommended_limit_usd: Decimal
	confidence: float
	rationale: str
	key_concerns: list[str]
	data_quality_caveats: list[str]


class EventWriteReference(BaseModel):
	model_config = ConfigDict(extra="ignore")

	stream_id: str
	event_type: str
	stream_position: int


class ApplicationSubmitted(BaseEvent):
	application_id: str
	applicant_id: str
	requested_amount_usd: Decimal
	loan_purpose: LoanPurpose
	loan_term_months: int
	submission_channel: str
	contact_email: str
	contact_name: str
	submitted_at: datetime
	application_reference: str


class DocumentUploadRequested(BaseEvent):
	application_id: str
	required_document_types: list[DocumentType]
	deadline: datetime
	requested_by: str


class PackageCreated(BaseEvent):
	package_id: str
	application_id: str
	required_documents: list[DocumentType]
	created_at: datetime


class DocumentUploaded(BaseEvent):
	application_id: str
	document_id: str
	document_type: DocumentType
	document_format: DocumentFormat
	filename: str
	file_path: str
	file_size_bytes: int
	file_hash: str
	fiscal_year: Optional[int]
	uploaded_at: datetime
	uploaded_by: str


class DocumentAdded(BaseEvent):
	package_id: str
	document_id: str
	document_type: DocumentType
	document_format: DocumentFormat
	file_hash: str
	added_at: datetime


class AgentSessionStarted(BaseEvent):
	session_id: str
	agent_type: AgentType
	agent_id: str
	application_id: str
	model_version: str
	langgraph_graph_version: str
	context_source: str
	context_token_count: int
	started_at: datetime


class AgentInputValidated(BaseEvent):
	session_id: str
	agent_type: AgentType
	application_id: str
	inputs_validated: list[str]
	validation_duration_ms: int
	validated_at: datetime


class AgentNodeExecuted(BaseEvent):
	session_id: str
	agent_type: AgentType
	node_name: str
	node_sequence: int
	input_keys: list[str]
	output_keys: list[str]
	llm_called: bool
	llm_tokens_input: Optional[int]
	llm_tokens_output: Optional[int]
	llm_cost_usd: Optional[float]
	duration_ms: int
	executed_at: datetime


class DocumentFormatValidated(BaseEvent):
	package_id: str
	document_id: str
	document_type: DocumentType
	page_count: int
	detected_format: str
	validated_at: datetime


class ExtractionStarted(BaseEvent):
	package_id: str
	document_id: str
	document_type: DocumentType
	pipeline_version: str
	extraction_model: str
	started_at: datetime


class AgentToolCalled(BaseEvent):
	session_id: str
	agent_type: AgentType
	tool_name: str
	tool_input_summary: str
	tool_output_summary: str
	tool_duration_ms: int
	called_at: datetime


class ExtractionCompleted(BaseEvent):
	package_id: str
	document_id: str
	document_type: DocumentType
	facts: FinancialFacts
	raw_text_length: int
	tables_extracted: int
	processing_ms: int
	completed_at: datetime


class QualityAssessmentCompleted(BaseEvent):
	package_id: str
	document_id: str
	overall_confidence: float
	is_coherent: bool
	anomalies: list[str]
	critical_missing_fields: list[str]
	reextraction_recommended: bool
	auditor_notes: str
	assessed_at: datetime


class PackageReadyForAnalysis(BaseEvent):
	package_id: str
	application_id: str
	documents_processed: int
	has_quality_flags: bool
	quality_flag_count: int
	ready_at: datetime


class AgentOutputWritten(BaseEvent):
	session_id: str
	agent_type: AgentType
	application_id: str
	events_written: list[EventWriteReference]
	output_summary: str
	written_at: datetime


class AgentSessionCompleted(BaseEvent):
	session_id: str
	agent_type: AgentType
	application_id: str
	total_nodes_executed: int
	total_llm_calls: int
	total_tokens_used: int
	total_cost_usd: float
	total_duration_ms: int
	next_agent_triggered: Optional[str]
	completed_at: datetime


class CreditAnalysisRequested(BaseEvent):
	application_id: str
	requested_at: datetime
	requested_by: str
	priority: str


class CreditRecordOpened(BaseEvent):
	application_id: str
	applicant_id: str
	opened_at: datetime


class HistoricalProfileConsumed(BaseEvent):
	application_id: str
	session_id: str
	fiscal_years_loaded: list[int]
	has_prior_loans: bool
	has_defaults: bool
	revenue_trajectory: str
	data_hash: str
	consumed_at: datetime


class ExtractedFactsConsumed(BaseEvent):
	application_id: str
	session_id: str
	document_ids_consumed: list[str]
	facts_summary: str
	quality_flags_present: bool
	consumed_at: datetime


class CreditAnalysisCompleted(BaseEvent):
	application_id: str
	session_id: str
	decision: CreditDecision
	model_version: str
	model_deployment_id: str
	input_data_hash: str
	analysis_duration_ms: int
	completed_at: datetime


class FraudScreeningRequested(BaseEvent):
	application_id: str
	requested_at: datetime
	triggered_by_event_id: str


class FraudScreeningInitiated(BaseEvent):
	application_id: str
	session_id: str
	screening_model_version: str
	initiated_at: datetime


class FraudScreeningCompleted(BaseEvent):
	application_id: str
	session_id: str
	fraud_score: float
	risk_level: str
	anomalies_found: int
	recommendation: str
	screening_model_version: str
	input_data_hash: str
	completed_at: datetime


class ComplianceCheckRequested(BaseEvent):
	application_id: str
	requested_at: datetime
	triggered_by_event_id: str
	regulation_set_version: str
	rules_to_evaluate: list[str]


class ComplianceCheckInitiated(BaseEvent):
	application_id: str
	session_id: str
	regulation_set_version: str
	rules_to_evaluate: list[str]
	initiated_at: datetime


class ComplianceRuleNoted(BaseEvent):
	application_id: str
	session_id: str
	rule_id: str
	rule_name: str
	note_type: str
	note_text: str
	evaluated_at: datetime


class ComplianceRulePassed(BaseEvent):
	application_id: str
	session_id: str
	rule_id: str
	rule_name: str
	rule_version: str
	evidence_hash: str
	evaluation_notes: str
	evaluated_at: datetime


class ComplianceRuleFailed(BaseEvent):
	application_id: str
	session_id: str
	rule_id: str
	rule_name: str
	rule_version: str
	failure_reason: str
	is_hard_block: bool
	remediation_available: bool
	evidence_hash: str
	evaluated_at: datetime


class ComplianceCheckCompleted(BaseEvent):
	application_id: str
	session_id: str
	rules_evaluated: int
	rules_passed: int
	rules_failed: int
	rules_noted: int
	has_hard_block: bool
	overall_verdict: str
	completed_at: datetime


class ApplicationDeclined(BaseEvent):
	application_id: str
	decline_reasons: list[str]
	declined_by: str
	adverse_action_notice_required: bool
	adverse_action_codes: list[str]
	declined_at: datetime


class DecisionRequested(BaseEvent):
	application_id: str
	requested_at: datetime
	all_analyses_complete: bool
	triggered_by_event_id: str


class DecisionGenerated(BaseEvent):
	application_id: str
	orchestrator_session_id: str
	recommendation: str
	confidence: float
	approved_amount_usd: Optional[Decimal]
	conditions: list[str]
	executive_summary: str
	key_risks: list[str]
	contributing_sessions: list[str]
	model_versions: dict[str, str]
	generated_at: datetime


class ApplicationApproved(BaseEvent):
	application_id: str
	approved_amount_usd: Decimal
	interest_rate_pct: float
	term_months: int
	conditions: list[str]
	approved_by: str
	effective_date: date
	approved_at: datetime


class StoredEvent(BaseModel):
	"""
	Represents an event as it is stored in the database.
	This model maps directly to a row in the `events` table.
	"""
	event_id: uuid.UUID
	stream_id: str
	stream_position: int
	global_position: int
	event_type: str
	event_version: int
	payload: dict
	metadata: dict
	recorded_at: datetime

	@field_validator("payload", "metadata", mode="before")
	@classmethod
	def _decode_json_fields(cls, value):
		if isinstance(value, str):
			return json.loads(value)
		return value

	model_config = ConfigDict(from_attributes=True)


class StreamMetadata(BaseModel):
	model_config = ConfigDict(from_attributes=True)

	stream_id: str
	aggregate_type: str
	current_version: int
	created_at: datetime
	metadata: dict
	archived_at: Optional[datetime]

	@field_validator("metadata", mode="before")
	@classmethod
	def _decode_metadata(cls, value):
		if isinstance(value, str):
			return json.loads(value)
		return value


EVENT_REGISTRY: dict[str, type[BaseEvent]] = {
	ApplicationSubmitted.__name__: ApplicationSubmitted,
	DocumentUploadRequested.__name__: DocumentUploadRequested,
	PackageCreated.__name__: PackageCreated,
	DocumentUploaded.__name__: DocumentUploaded,
	DocumentAdded.__name__: DocumentAdded,
	AgentSessionStarted.__name__: AgentSessionStarted,
	AgentInputValidated.__name__: AgentInputValidated,
	AgentNodeExecuted.__name__: AgentNodeExecuted,
	DocumentFormatValidated.__name__: DocumentFormatValidated,
	ExtractionStarted.__name__: ExtractionStarted,
	AgentToolCalled.__name__: AgentToolCalled,
	ExtractionCompleted.__name__: ExtractionCompleted,
	QualityAssessmentCompleted.__name__: QualityAssessmentCompleted,
	PackageReadyForAnalysis.__name__: PackageReadyForAnalysis,
	AgentOutputWritten.__name__: AgentOutputWritten,
	AgentSessionCompleted.__name__: AgentSessionCompleted,
	CreditAnalysisRequested.__name__: CreditAnalysisRequested,
	CreditRecordOpened.__name__: CreditRecordOpened,
	HistoricalProfileConsumed.__name__: HistoricalProfileConsumed,
	ExtractedFactsConsumed.__name__: ExtractedFactsConsumed,
	CreditAnalysisCompleted.__name__: CreditAnalysisCompleted,
	FraudScreeningRequested.__name__: FraudScreeningRequested,
	FraudScreeningInitiated.__name__: FraudScreeningInitiated,
	FraudScreeningCompleted.__name__: FraudScreeningCompleted,
	ComplianceCheckRequested.__name__: ComplianceCheckRequested,
	ComplianceCheckInitiated.__name__: ComplianceCheckInitiated,
	ComplianceRuleNoted.__name__: ComplianceRuleNoted,
	ComplianceRulePassed.__name__: ComplianceRulePassed,
	ComplianceRuleFailed.__name__: ComplianceRuleFailed,
	ComplianceCheckCompleted.__name__: ComplianceCheckCompleted,
	ApplicationDeclined.__name__: ApplicationDeclined,
	DecisionRequested.__name__: DecisionRequested,
	DecisionGenerated.__name__: DecisionGenerated,
	ApplicationApproved.__name__: ApplicationApproved,
}

