import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ledger.agents.compliance_checker import ComplianceAgent
from ledger.agents.credit_analyzer import CreditAnalysisAgent
from ledger.agents.decision_orchestrator import DecisionOrchestratorAgent
from ledger.agents.fraud_detector import FraudDetectionAgent
from ledger.event_store import EventStore
from ledger.projections.agent_performance_ledger import AgentPerformanceLedgerProjection
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit_view import ComplianceAuditViewProjection
from ledger.schema.events import CreditAnalysisRequested, DecisionGenerated


async def _initialize_schema(database_url: str, schema_path: Path, projection_schema_path: Path) -> None:
    db_conn = await asyncpg.connect(database_url)
    try:
        await db_conn.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
        with open(schema_path, "r", encoding="utf-8") as schema_file:
            await db_conn.execute(schema_file.read())
        with open(projection_schema_path, "r", encoding="utf-8") as projection_schema_file:
            await db_conn.execute(projection_schema_file.read())
    finally:
        await db_conn.close()


async def main():
    load_dotenv()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set.")

    schema_path = PROJECT_ROOT / "ledger" / "schema.sql"
    projection_schema_path = PROJECT_ROOT / "ledger" / "projections" / "schema.sql"

    user_pass_host = database_url.split("://")[1].split("@")[0]
    host = database_url.split("@")[1].split("/")[0]
    user = user_pass_host.split(":")[0]
    password = user_pass_host.split(":")[1]

    database_name = database_url.rsplit("/", 1)[-1]
    admin_database_url = f"postgresql://{user}:{password}@{host}/postgres"

    async def init_connection(conn):
        await conn.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
            format="text",
        )

    admin_conn = await asyncpg.connect(admin_database_url)
    try:
        exists = await admin_conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            database_name,
        )
        if not exists:
            await admin_conn.execute(f'CREATE DATABASE "{database_name}"')
            print(f"Database '{database_name}' created. Now creating tables...")
            await _initialize_schema(database_url, schema_path, projection_schema_path)
    finally:
        await admin_conn.close()

    schema_check_conn = await asyncpg.connect(database_url)
    try:
        core_table_exists = await schema_check_conn.fetchval(
            "SELECT to_regclass('public.event_streams')"
        )
        projection_table_exists = await schema_check_conn.fetchval(
            "SELECT to_regclass('public.application_summary')"
        )
    finally:
        await schema_check_conn.close()

    if not core_table_exists or not projection_table_exists:
        print(f"Database '{database_name}' is missing required tables. Initializing schema...")
        await _initialize_schema(database_url, schema_path, projection_schema_path)

    pool = await asyncpg.create_pool(database_url, init=init_connection)
    event_store = EventStore(pool)

    try:
        documents_dir = PROJECT_ROOT / "documents"
        if not documents_dir.exists():
            raise RuntimeError(f"Documents directory not found: {documents_dir}")

        application_ids = sorted(
            path.name
            for path in documents_dir.iterdir()
            if path.is_dir()
        )

        application_summary_projection = ApplicationSummaryProjection(event_store._pool)
        agent_performance_projection = AgentPerformanceLedgerProjection(event_store._pool)
        compliance_audit_projection = ComplianceAuditViewProjection(event_store._pool)

        for application_id in application_ids:
            docpkg_stream = await event_store.load_stream(f"docpkg-{application_id}")
            if not docpkg_stream:
                continue

            loan_stream = await event_store.load_stream(f"loan-{application_id}")
            if any(event.event_type == DecisionGenerated.__name__ for event in loan_stream):
                continue

            print(f"Running downstream pipeline for application: {application_id}")

            credit_requested = CreditAnalysisRequested(
                application_id=application_id,
                requested_at=datetime.now(timezone.utc),
                requested_by="full-pipeline-backfill",
                priority="normal",
            )
            await event_store.append(
                stream_id=f"loan-{application_id}",
                events=[credit_requested],
                expected_version=len(loan_stream),
                aggregate_type="LoanApplication",
            )

            credit_analysis_agent = CreditAnalysisAgent(
                store=event_store,
                agent_id="credit-analyzer-backfill",
                model_version="backfill-v1",
            )
            fraud_detection_agent = FraudDetectionAgent(
                store=event_store,
                agent_id="fraud-detector-backfill",
                model_version="backfill-v1",
            )
            compliance_agent = ComplianceAgent(
                store=event_store,
                agent_id="compliance-checker-backfill",
                model_version="backfill-v1",
            )
            decision_orchestrator_agent = DecisionOrchestratorAgent(
                store=event_store,
                application_summary_projection=application_summary_projection,
                agent_performance_projection=agent_performance_projection,
                compliance_audit_projection=compliance_audit_projection,
                agent_id="decision-orchestrator-backfill",
                model_version="backfill-v1",
            )

            await credit_analysis_agent.run(application_id=application_id)
            await fraud_detection_agent.run(application_id=application_id)
            await compliance_agent.run(application_id=application_id)
            await decision_orchestrator_agent.run(application_id=application_id)
            print(f"Completed full pipeline for application: {application_id}")
    finally:
        await event_store._pool.close()


if __name__ == "__main__":
    asyncio.run(main())