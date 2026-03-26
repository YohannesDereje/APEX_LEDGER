import json
import os
import uuid
from datetime import datetime
from typing import Any

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

from ledger.agents.compliance_checker import ComplianceAgent
from ledger.agents.credit_analyzer import CreditAnalysisAgent
from ledger.agents.decision_orchestrator import DecisionOrchestratorAgent
from ledger.agents.document_processor import DocumentProcessingAgent
from ledger.agents.fraud_detector import FraudDetectionAgent
from ledger.auditing.integrity import run_integrity_check
from ledger.commands.handlers import SubmitApplicationCommand, handle_submit_application
from ledger.event_store import EventStore
from ledger.mcp.errors import ErrorDetail
from ledger.projections.agent_performance_ledger import AgentPerformanceLedgerProjection
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit_view import ComplianceAuditViewProjection
from ledger.projections.daemon import ProjectionDaemon
from ledger.schema.events import HumanReviewOverride


app = FastAPI()


class ApplicationSubmissionRequest(BaseModel):
    applicant_id: str
    requested_amount_usd: float
    loan_purpose: str
    loan_term_months: int
    submission_channel: str
    contact_email: str
    contact_name: str
    application_reference: str


class DocumentProcessingRequest(BaseModel):
    documents: list[dict[str, Any]]


class HumanReviewRequest(BaseModel):
    reviewer_id: str
    override_reason: str


@app.on_event("startup")
async def startup_event() -> None:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set.")

    async def init_connection(conn: asyncpg.Connection) -> None:
        await conn.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
            format="text",
        )

    app.state.pool = await asyncpg.create_pool(database_url, init=init_connection)
    _ensure_runtime_state()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    pool = getattr(app.state, "pool", None)
    if pool is not None:
        await pool.close()


def _ensure_runtime_state() -> None:
    pool = getattr(app.state, "pool", None)
    if pool is None:
        raise RuntimeError("Database pool is not initialized.")

    if getattr(app.state, "event_store", None) is None:
        app.state.event_store = EventStore(pool)

    if getattr(app.state, "application_summary_projection", None) is None:
        app.state.application_summary_projection = ApplicationSummaryProjection(pool)

    if getattr(app.state, "agent_performance_projection", None) is None:
        app.state.agent_performance_projection = AgentPerformanceLedgerProjection(pool)

    if getattr(app.state, "compliance_audit_projection", None) is None:
        app.state.compliance_audit_projection = ComplianceAuditViewProjection(pool)

    if getattr(app.state, "daemon", None) is None:
        app.state.daemon = ProjectionDaemon(
            app.state.event_store,
            [
                app.state.application_summary_projection,
                app.state.agent_performance_projection,
                app.state.compliance_audit_projection,
            ],
        )


def _error_response(code: str, detail: str) -> dict[str, str]:
    return ErrorDetail(code=code, detail=detail).model_dump()


def _get_event_store() -> EventStore:
    _ensure_runtime_state()
    return app.state.event_store


def _get_daemon() -> ProjectionDaemon:
    _ensure_runtime_state()
    return app.state.daemon


def _get_summary_projection() -> ApplicationSummaryProjection:
    _ensure_runtime_state()
    return app.state.application_summary_projection


def _get_compliance_projection() -> ComplianceAuditViewProjection:
    _ensure_runtime_state()
    return app.state.compliance_audit_projection


async def _require_summary(application_id: str) -> dict:
    summary = await _get_summary_projection().get_summary(application_id)
    if summary is None:
        raise HTTPException(
            status_code=404,
            detail=_error_response("NOT_FOUND", "Application not found."),
        )
    return summary


async def _require_state(application_id: str, expected_states: set[str], detail: str) -> dict:
    summary = await _require_summary(application_id)
    if summary.get("state") not in expected_states:
        raise HTTPException(
            status_code=409,
            detail=_error_response("PRECONDITION_FAILED", detail),
        )
    return summary


def _build_document_processing_agent(store: EventStore) -> Any:
    factory = getattr(app.state, "document_processing_agent_factory", None)
    if factory is not None:
        return factory(store)
    return DocumentProcessingAgent(store=store, agent_id="mcp-doc-processor", model_version="mcp-v1")


def _build_credit_analysis_agent(store: EventStore) -> CreditAnalysisAgent:
    factory = getattr(app.state, "credit_analysis_agent_factory", None)
    if factory is not None:
        return factory(store)
    return CreditAnalysisAgent(store=store, agent_id="mcp-credit-analyzer", model_version="mcp-v1")


def _build_fraud_detection_agent(store: EventStore) -> FraudDetectionAgent:
    factory = getattr(app.state, "fraud_detection_agent_factory", None)
    if factory is not None:
        return factory(store)
    return FraudDetectionAgent(store=store, agent_id="mcp-fraud-detector", model_version="mcp-v1")


def _build_compliance_agent(store: EventStore) -> ComplianceAgent:
    factory = getattr(app.state, "compliance_agent_factory", None)
    if factory is not None:
        return factory(store)
    return ComplianceAgent(store=store, agent_id="mcp-compliance", model_version="mcp-v1")


def _build_decision_orchestrator(store: EventStore) -> DecisionOrchestratorAgent:
    factory = getattr(app.state, "decision_orchestrator_factory", None)
    if factory is not None:
        return factory(
            store,
            _get_summary_projection(),
            app.state.agent_performance_projection,
            _get_compliance_projection(),
        )
    return DecisionOrchestratorAgent(
        store=store,
        application_summary_projection=_get_summary_projection(),
        agent_performance_projection=app.state.agent_performance_projection,
        compliance_audit_projection=_get_compliance_projection(),
        agent_id="mcp-decision-orchestrator",
        model_version="mcp-v1",
    )


@app.get("/")
async def read_root() -> dict[str, str]:
    return {"message": "Welcome to The Ledger MCP"}


@app.get("/applications/{application_id}/summary")
async def get_application_summary(application_id: str) -> dict:
    return await _require_summary(application_id)


@app.get("/applications/{application_id}/events")
async def get_application_events(application_id: str) -> list[dict[str, Any]]:
    await _require_summary(application_id)
    events = await _get_event_store().load_stream(f"loan-{application_id}")
    return jsonable_encoder([event.model_dump(mode="json") for event in events])


@app.get("/applications/{application_id}/compliance")
async def get_application_compliance(application_id: str) -> dict:
    await _require_summary(application_id)
    compliance = await _get_compliance_projection().get_current_compliance(application_id)
    if compliance is None:
        raise HTTPException(
            status_code=404,
            detail=_error_response("NOT_FOUND", "Compliance state not found."),
        )
    return jsonable_encoder(compliance)


@app.get("/applications/{application_id}/compliance/history")
async def get_application_compliance_history(application_id: str, as_of: datetime) -> dict:
    await _require_summary(application_id)
    compliance = await _get_compliance_projection().get_compliance_at(application_id, as_of)
    if compliance is None:
        raise HTTPException(
            status_code=404,
            detail=_error_response("NOT_FOUND", "Historical compliance state not found."),
        )
    return jsonable_encoder(compliance)


@app.get("/metrics/agent-performance")
async def get_agent_performance_metrics() -> list[dict[str, Any]]:
    _ensure_runtime_state()
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT *
            FROM agent_performance_ledger
            ORDER BY agent_id ASC, model_version ASC
            """
        )
    return jsonable_encoder([dict(row) for row in rows])


@app.get("/health/projections")
async def get_projection_health() -> dict[str, Any]:
    daemon = _get_daemon()
    health = {}
    for projection_name in daemon._projections.keys():
        health[projection_name] = await daemon.get_lag_for_projection(projection_name)
    return health


@app.post("/applications", status_code=202)
async def submit_application(request: ApplicationSubmissionRequest) -> dict[str, str]:
    event_store = _get_event_store()
    application_id = str(uuid.uuid4())

    command = SubmitApplicationCommand(
        application_id=application_id,
        applicant_id=request.applicant_id,
        requested_amount_usd=request.requested_amount_usd,
        loan_purpose=request.loan_purpose,
        loan_term_months=request.loan_term_months,
        submission_channel=request.submission_channel,
        contact_email=request.contact_email,
        contact_name=request.contact_name,
        application_reference=request.application_reference,
    )
    await handle_submit_application(command, event_store)

    return {
        "message": "Application submission accepted",
        "application_id": application_id,
    }


@app.post("/applications/{application_id}/start-document-processing", status_code=202)
async def start_document_processing(application_id: str, request: DocumentProcessingRequest) -> dict[str, str]:
    await _require_state(
        application_id,
        {"SUBMITTED"},
        "Application is not ready for document processing.",
    )
    agent = _build_document_processing_agent(_get_event_store())
    await agent.run(application_id=application_id, documents=request.documents)
    return {"message": "Document processing started", "application_id": application_id}


@app.post("/applications/{application_id}/start-credit-analysis", status_code=202)
async def start_credit_analysis(application_id: str) -> dict[str, str]:
    await _require_state(
        application_id,
        {"AWAITING_ANALYSIS"},
        "Application is not awaiting credit analysis.",
    )
    agent = _build_credit_analysis_agent(_get_event_store())
    await agent.run(application_id=application_id)
    return {"message": "Credit analysis started", "application_id": application_id}


@app.post("/applications/{application_id}/start-fraud-screening", status_code=202)
async def start_fraud_screening(application_id: str) -> dict[str, str]:
    await _require_state(
        application_id,
        {"FRAUD_REVIEW"},
        "Application is not ready for fraud screening.",
    )
    agent = _build_fraud_detection_agent(_get_event_store())
    await agent.run(application_id=application_id)
    return {"message": "Fraud screening started", "application_id": application_id}


@app.post("/applications/{application_id}/start-compliance-check", status_code=202)
async def start_compliance_check(application_id: str) -> dict[str, str]:
    await _require_state(
        application_id,
        {"COMPLIANCE_REVIEW"},
        "Application is not ready for compliance review.",
    )
    agent = _build_compliance_agent(_get_event_store())
    await agent.run(application_id=application_id)
    return {"message": "Compliance check started", "application_id": application_id}


@app.post("/applications/{application_id}/generate-decision", status_code=202)
async def generate_decision(application_id: str) -> dict[str, str]:
    await _require_state(
        application_id,
        {"PENDING_DECISION"},
        "Application is not ready for decision generation.",
    )
    agent = _build_decision_orchestrator(_get_event_store())
    await agent.run(application_id=application_id)
    return {"message": "Decision generation started", "application_id": application_id}


@app.post("/applications/{application_id}/human-review", status_code=202)
async def record_human_review(application_id: str, request: HumanReviewRequest) -> dict[str, str]:
    await _require_state(
        application_id,
        {"APPROVED_PENDING_HUMAN", "DECLINED_PENDING_HUMAN", "FINAL_APPROVED", "FINAL_DECLINED"},
        "Application is not eligible for human review.",
    )
    event_store = _get_event_store()
    expected_version = await event_store.stream_version(f"loan-{application_id}")
    event = HumanReviewOverride(
        application_id=application_id,
        reviewer_id=request.reviewer_id,
        override_reason=request.override_reason,
        reviewed_at=datetime.utcnow(),
    )
    await event_store.append(
        stream_id=f"loan-{application_id}",
        events=[event],
        expected_version=expected_version,
        aggregate_type="LoanApplication",
    )
    return {"message": "Human review recorded", "application_id": application_id}


@app.post("/applications/{application_id}/record-integrity-check", status_code=202)
async def record_application_integrity_check(application_id: str) -> dict[str, Any]:
    await _require_summary(application_id)
    result = await run_integrity_check(_get_event_store(), "loan", application_id)
    return jsonable_encoder(result)