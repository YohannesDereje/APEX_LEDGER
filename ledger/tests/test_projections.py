import json
import os
import uuid
from datetime import datetime
from pathlib import Path

import asyncpg
import pytest
from dotenv import load_dotenv

from ledger.event_store import EventStore
from ledger.mcp.main import app
from ledger.projections.agent_performance_ledger import AgentPerformanceLedgerProjection
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit_view import ComplianceAuditViewProjection
from ledger.projections.daemon import ProjectionDaemon
from ledger.schema.events import (
    ApplicationApproved,
    ApplicationSubmitted,
    ComplianceCheckInitiated,
    ComplianceRuleFailed,
    ComplianceRulePassed,
    LoanPurpose,
)


load_dotenv()


@pytest.fixture
async def projection_env():
    async def init_connection(conn):
        await conn.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
            format="text",
        )

    base_database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost/postgres",
    )
    database_url = f"{base_database_url.rsplit('/', 1)[0]}/apex_ledger_test"

    user_pass_host = database_url.split("://")[1].split("@")[0]
    host = database_url.split("@")[1].split("/")[0]
    user = user_pass_host.split(":")[0]
    password = user_pass_host.split(":")[1]

    database_name = database_url.rsplit("/", 1)[-1]
    admin_database_url = f"postgresql://{user}:{password}@{host}/postgres"

    project_root = Path(__file__).resolve().parents[2]
    schema_path = project_root / "ledger" / "schema.sql"
    projection_schema_path = project_root / "ledger" / "projections" / "schema.sql"

    admin_conn = await asyncpg.connect(admin_database_url)
    try:
        exists = await admin_conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            database_name,
        )
        if exists:
            await admin_conn.execute(f'DROP DATABASE "{database_name}"')

        await admin_conn.execute(f'CREATE DATABASE "{database_name}"')
    finally:
        await admin_conn.close()

    pool = await asyncpg.create_pool(database_url, init=init_connection)
    try:
        async with pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
            with open(schema_path, "r", encoding="utf-8") as schema_file:
                await conn.execute(schema_file.read())
            with open(projection_schema_path, "r", encoding="utf-8") as projection_schema_file:
                await conn.execute(projection_schema_file.read())

        event_store = EventStore(pool)
        app.state.pool = pool

        yield {
            "event_store": event_store,
            "pool": pool,
            "app": app,
        }

    finally:
        if hasattr(app.state, "pool"):
            del app.state.pool
        await pool.close()
        admin_conn = await asyncpg.connect(admin_database_url)
        try:
            await admin_conn.execute(
                f"""
                SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                WHERE datname = '{database_name}' AND pid <> pg_backend_pid();
                """
            )
            await admin_conn.execute(f'DROP DATABASE IF EXISTS "{database_name}"')
        finally:
            await admin_conn.close()


@pytest.mark.asyncio
async def test_application_summary_projection(projection_env):
    event_store = projection_env["event_store"]

    application_id = f"app-{uuid.uuid4()}"
    event = ApplicationSubmitted(
        application_id=application_id,
        applicant_id="applicant-001",
        requested_amount_usd=250000,
        loan_purpose=LoanPurpose.WORKING_CAPITAL,
        loan_term_months=36,
        submission_channel="WEB",
        contact_email="test@example.com",
        contact_name="Test Applicant",
        submitted_at=datetime.utcnow(),
        application_reference=application_id,
    )

    await event_store.append(
        stream_id=f"loan-{application_id}",
        events=[event],
        expected_version=0,
        aggregate_type="LoanApplication",
    )

    app_summary_projection = ApplicationSummaryProjection(event_store._pool)
    daemon = ProjectionDaemon(event_store, [app_summary_projection])
    await daemon._process_batch()

    async with event_store._pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            application_id,
        )

    assert row is not None
    assert row["state"] == "SUBMITTED"
    assert row["applicant_id"] == "applicant-001"


@pytest.mark.asyncio
async def test_application_summary_projection_update(projection_env):
    event_store = projection_env["event_store"]

    application_id = f"app-{uuid.uuid4()}"
    submitted_event = ApplicationSubmitted(
        application_id=application_id,
        applicant_id="applicant-002",
        requested_amount_usd=300000,
        loan_purpose=LoanPurpose.EQUIPMENT,
        loan_term_months=48,
        submission_channel="WEB",
        contact_email="approved@example.com",
        contact_name="Approved Applicant",
        submitted_at=datetime.utcnow(),
        application_reference=application_id,
    )
    approved_event = ApplicationApproved(
        application_id=application_id,
        approved_amount_usd=275000,
        interest_rate_pct=7.25,
        term_months=48,
        conditions=["Board resolution required"],
        approved_by="auto",
        effective_date="2026-03-24",
        approved_at=datetime.utcnow(),
    )

    await event_store.append(
        stream_id=f"loan-{application_id}",
        events=[submitted_event],
        expected_version=0,
        aggregate_type="LoanApplication",
    )
    await event_store.append(
        stream_id=f"loan-{application_id}",
        events=[approved_event],
        expected_version=1,
        aggregate_type="LoanApplication",
    )

    app_summary_projection = ApplicationSummaryProjection(event_store._pool)
    daemon = ProjectionDaemon(event_store, [app_summary_projection])
    await daemon._process_batch()

    async with event_store._pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            application_id,
        )

    assert row is not None
    assert row["state"] == "FINAL_APPROVED"
    assert row["approved_amount_usd"] is not None


@pytest.mark.asyncio
async def test_compliance_audit_view_temporal_query(projection_env):
    event_store = projection_env["event_store"]

    application_id = f"app-{uuid.uuid4()}"
    stream_id = f"loan-{application_id}"

    initiated_event = ComplianceCheckInitiated(
        application_id=application_id,
        session_id=f"session-{uuid.uuid4()}",
        regulation_set_version="v2026.03",
        rules_to_evaluate=["KYC_001", "AML_002"],
        initiated_at=datetime.utcnow(),
    )
    passed_event = ComplianceRulePassed(
        application_id=application_id,
        session_id=initiated_event.session_id,
        rule_id="KYC_001",
        rule_name="Identity Verification",
        rule_version="1.0",
        evidence_hash="evidence-pass-001",
        evaluation_notes="Government ID validated",
        evaluated_at=datetime.utcnow(),
    )
    failed_event = ComplianceRuleFailed(
        application_id=application_id,
        session_id=initiated_event.session_id,
        rule_id="AML_002",
        rule_name="Sanctions Screening",
        rule_version="1.0",
        failure_reason="Potential sanctions match",
        is_hard_block=True,
        remediation_available=False,
        evidence_hash="evidence-fail-001",
        evaluated_at=datetime.utcnow(),
    )

    await event_store.append(
        stream_id=stream_id,
        events=[initiated_event, passed_event, failed_event],
        expected_version=0,
        aggregate_type="LoanApplication",
    )

    app_summary_projection = ApplicationSummaryProjection(event_store._pool)
    agent_performance_projection = AgentPerformanceLedgerProjection(event_store._pool)
    compliance_projection = ComplianceAuditViewProjection(event_store._pool)

    daemon = ProjectionDaemon(
        event_store,
        [
            app_summary_projection,
            agent_performance_projection,
            compliance_projection,
        ],
    )
    await daemon._process_batch()

    compliance_projection = daemon._projections["compliance_audit_view"]

    async with event_store._pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT event_type, recorded_at
            FROM events
            WHERE stream_id = $1
              AND event_type IN ($2, $3)
            ORDER BY global_position ASC
            """,
            stream_id,
            ComplianceRulePassed.__name__,
            ComplianceRuleFailed.__name__,
        )

    assert len(rows) == 2
    passed_recorded_at = rows[0]["recorded_at"]
    failed_recorded_at = rows[1]["recorded_at"]
    assert passed_recorded_at < failed_recorded_at

    timestamp = passed_recorded_at + ((failed_recorded_at - passed_recorded_at) / 2)
    snapshot = await compliance_projection.get_compliance_at(application_id, timestamp)

    assert snapshot is not None
    assert len(snapshot.get("passed_rules", [])) == 1
    assert snapshot.get("failed_rules", []) == []
