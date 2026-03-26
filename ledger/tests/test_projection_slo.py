import asyncio
from datetime import datetime, timezone

import pytest

from ledger.event_store import EventStore
from ledger.projections.agent_performance_ledger import AgentPerformanceLedgerProjection
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit_view import ComplianceAuditViewProjection
from ledger.projections.daemon import ProjectionDaemon
from ledger.schema.events import (
    ApplicationSubmitted,
    ComplianceCheckCompleted,
    ComplianceCheckInitiated,
    ComplianceRulePassed,
    LoanPurpose,
)
from ledger.tests.test_projections import projection_env


@pytest.mark.asyncio
async def test_projection_lag_under_high_concurrency(projection_env):
    event_store: EventStore = projection_env["event_store"]

    num_workers = 50
    events_per_worker = 10
    loan_workers = num_workers // 2
    compliance_workers = num_workers - loan_workers

    async def loan_worker(worker_id: int) -> None:
        application_id = f"slo-loan-{worker_id}"
        submitted_at = datetime.now(timezone.utc)
        events = [
            ApplicationSubmitted(
                application_id=application_id,
                applicant_id=f"applicant-{worker_id}",
                requested_amount_usd=100000 + worker_id,
                loan_purpose=LoanPurpose.WORKING_CAPITAL,
                loan_term_months=24,
                submission_channel="SLO_TEST",
                contact_email=f"worker-{worker_id}@example.com",
                contact_name=f"Worker {worker_id}",
                submitted_at=submitted_at,
                application_reference=f"ref-{worker_id}",
            )
            for _ in range(events_per_worker)
        ]
        await event_store.append(
            stream_id=f"loan-{application_id}",
            events=events,
            expected_version=0,
            aggregate_type="LoanApplication",
        )

    async def compliance_worker(worker_id: int) -> None:
        application_id = f"slo-compliance-{worker_id}"
        session_id = f"session-{worker_id}"
        events = []
        for cycle in range(events_per_worker // 5):
            initiated_at = datetime.now(timezone.utc)
            events.extend(
                [
                    ComplianceCheckInitiated(
                        application_id=application_id,
                        session_id=session_id,
                        regulation_set_version="v2026.03",
                        rules_to_evaluate=["KYC_001", "AML_002"],
                        initiated_at=initiated_at,
                    ),
                    ComplianceRulePassed(
                        application_id=application_id,
                        session_id=session_id,
                        rule_id=f"KYC_001_{cycle}",
                        rule_name="Identity Verification",
                        rule_version="1.0",
                        evidence_hash=f"hash-kyc-{worker_id}-{cycle}",
                        evaluation_notes="Synthetic pass for SLO test.",
                        evaluated_at=datetime.now(timezone.utc),
                    ),
                    ComplianceRulePassed(
                        application_id=application_id,
                        session_id=session_id,
                        rule_id=f"AML_002_{cycle}",
                        rule_name="AML Screening",
                        rule_version="1.0",
                        evidence_hash=f"hash-aml-{worker_id}-{cycle}",
                        evaluation_notes="Synthetic pass for SLO test.",
                        evaluated_at=datetime.now(timezone.utc),
                    ),
                    ComplianceCheckCompleted(
                        application_id=application_id,
                        session_id=session_id,
                        rules_evaluated=2,
                        rules_passed=2,
                        rules_failed=0,
                        rules_noted=0,
                        has_hard_block=False,
                        overall_verdict="CLEAR",
                        completed_at=datetime.now(timezone.utc),
                    ),
                    ComplianceCheckCompleted(
                        application_id=application_id,
                        session_id=session_id,
                        rules_evaluated=2,
                        rules_passed=2,
                        rules_failed=0,
                        rules_noted=0,
                        has_hard_block=False,
                        overall_verdict="CLEAR",
                        completed_at=datetime.now(timezone.utc),
                    ),
                ]
            )

        await event_store.append(
            stream_id=f"compliance-{application_id}",
            events=events,
            expected_version=0,
            aggregate_type="ComplianceCheck",
        )

    workers = [loan_worker(worker_id) for worker_id in range(loan_workers)]
    workers.extend(
        compliance_worker(worker_id) for worker_id in range(compliance_workers)
    )

    await asyncio.gather(*workers)

    daemon = ProjectionDaemon(
        event_store,
        [
            ApplicationSummaryProjection(event_store._pool),
            AgentPerformanceLedgerProjection(event_store._pool),
            ComplianceAuditViewProjection(event_store._pool),
        ],
        max_retries=2,
        retry_delay_seconds=0.05,
    )
    daemon_task = asyncio.create_task(daemon.run_forever(poll_interval_seconds=0.1))

    await asyncio.sleep(5)
    daemon.stop()
    await daemon_task

    application_summary_lag = await daemon.get_lag_for_projection("application_summary")
    compliance_audit_lag = await daemon.get_lag_for_projection("compliance_audit_view")

    assert application_summary_lag is not None
    assert compliance_audit_lag is not None
    assert application_summary_lag < 500
    assert compliance_audit_lag < 2000