from datetime import datetime, timezone
import json

import pytest

from ledger.auditing.integrity import run_integrity_check
from ledger.event_store import EventStore
from ledger.schema.events import ApplicationSubmitted, CreditAnalysisRequested, LoanPurpose
from ledger.tests.test_projections import projection_env


@pytest.mark.asyncio
async def test_integrity_check_detects_tamper(projection_env):
    event_store: EventStore = projection_env["event_store"]
    pool = projection_env["pool"]
    stream_id = "loan-tamper-test"
    application_id = "tamper-test"

    submitted_event = ApplicationSubmitted(
        application_id=application_id,
        applicant_id="tamper-applicant",
        requested_amount_usd=150000,
        loan_purpose=LoanPurpose.WORKING_CAPITAL,
        loan_term_months=24,
        submission_channel="TEST",
        contact_email="tamper@example.com",
        contact_name="Tamper Test",
        submitted_at=datetime.now(timezone.utc),
        application_reference="tamper-ref",
    )
    requested_event = CreditAnalysisRequested(
        application_id=application_id,
        requested_at=datetime.now(timezone.utc),
        requested_by="integrity-test",
        priority="normal",
    )

    await event_store.append(
        stream_id=stream_id,
        events=[submitted_event, requested_event],
        expected_version=0,
        aggregate_type="LoanApplication",
    )

    baseline_result = await run_integrity_check(event_store, "loan", application_id)
    assert baseline_result["tamper_detected"] is False

    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE events
            SET payload = $1::jsonb
            WHERE stream_id = $2
              AND stream_position = 1
            """,
            json.dumps({"corrupted": True}),
            stream_id,
        )

    tamper_result = await run_integrity_check(event_store, "loan", application_id)
    assert tamper_result["tamper_detected"] is True
    assert tamper_result["chain_valid"] is False