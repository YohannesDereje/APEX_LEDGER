import uuid
from datetime import datetime, timezone

import pytest

from ledger.agents.compliance_checker import ComplianceAgent
from ledger.agents.credit_analyzer import CreditAnalysisAgent
from ledger.agents.decision_orchestrator import DecisionOrchestratorAgent
from ledger.agents.fraud_detector import FraudDetectionAgent
from ledger.event_store import EventStore
from ledger.projections.agent_performance_ledger import AgentPerformanceLedgerProjection
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit_view import ComplianceAuditViewProjection
from ledger.schema.events import (
    ApplicationApproved,
    ApplicationDeclined,
    ComplianceCheckRequested,
    CreditAnalysisRequested,
    DecisionGenerated,
    PackageReadyForAnalysis,
)
from ledger.tests.test_projections import projection_env


@pytest.mark.asyncio
async def test_full_agent_pipeline_from_credit_to_decision(projection_env):
    event_store: EventStore = projection_env["event_store"]

    application_id = f"app-{uuid.uuid4()}"
    package_ready = PackageReadyForAnalysis(
        package_id=f"pkg-{application_id}",
        application_id=application_id,
        documents_processed=1,
        has_quality_flags=False,
        quality_flag_count=0,
        ready_at=datetime.now(timezone.utc),
    )
    await event_store.append(
        stream_id=f"docpkg-{application_id}",
        events=[package_ready],
        expected_version=0,
        aggregate_type="DocumentPackage",
    )

    credit_requested = CreditAnalysisRequested(
        application_id=application_id,
        requested_at=datetime.now(timezone.utc),
        requested_by="test-suite",
        priority="normal",
    )
    await event_store.append(
        stream_id=f"loan-{application_id}",
        events=[credit_requested],
        expected_version=0,
        aggregate_type="LoanApplication",
    )

    credit_analysis_agent = CreditAnalysisAgent(
        store=event_store,
        agent_id="credit-analyzer-test",
        model_version="test-model-v1",
    )
    fraud_detection_agent = FraudDetectionAgent(
        store=event_store,
        agent_id="fraud-detector-test",
        model_version="test-model-v1",
    )
    compliance_agent = ComplianceAgent(
        store=event_store,
        agent_id="compliance-checker-test",
        model_version="test-model-v1",
    )
    decision_orchestrator_agent = DecisionOrchestratorAgent(
        store=event_store,
        application_summary_projection=ApplicationSummaryProjection(event_store._pool),
        agent_performance_projection=AgentPerformanceLedgerProjection(event_store._pool),
        compliance_audit_projection=ComplianceAuditViewProjection(event_store._pool),
        agent_id="decision-orchestrator-test",
        model_version="test-model-v1",
    )

    await credit_analysis_agent.run(application_id=application_id)
    await fraud_detection_agent.run(application_id=application_id)
    await compliance_agent.run(application_id=application_id)
    await decision_orchestrator_agent.run(application_id=application_id)

    loan_stream = await event_store.load_stream(f"loan-{application_id}")

    assert loan_stream[-1].event_type in {
        ApplicationApproved.__name__,
        ApplicationDeclined.__name__,
    }
    assert loan_stream[-2].event_type == DecisionGenerated.__name__
    assert any(event.event_type == "FraudScreeningRequested" for event in loan_stream)
    assert any(event.event_type == ComplianceCheckRequested.__name__ for event in loan_stream)
    assert any(event.event_type == "DecisionRequested" for event in loan_stream)
