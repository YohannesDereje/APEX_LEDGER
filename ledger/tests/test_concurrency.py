import asyncio
import json
import os
import uuid

import asyncpg
import pytest

from ledger.event_store import EventStore
from ledger.schema.events import BaseEvent, OptimisticConcurrencyError

from dotenv import load_dotenv
load_dotenv() # <--- ADD THIS LINE



class CreditAnalysisCompleted(BaseEvent):
	decision: str


@pytest.fixture
async def event_store():
    async def init_connection(conn):
        await conn.set_type_codec(
            'jsonb',
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog',
            format='text',
        )

    # This correctly loads your .env file
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost/apex_ledger_test",
    )

    # --- START OF THE FIX ---
    # Deconstruct the URL to safely get user, password, and host
    # Example: postgresql://user:password@host:port/database
    user_pass_host = database_url.split("://")[1].split("@")[0]
    host = database_url.split("@")[1].split("/")[0]
    user = user_pass_host.split(":")[0]
    password = user_pass_host.split(":")[1]
    
    database_name = database_url.rsplit("/", 1)[-1]
    
    # Explicitly build the admin URL with the correct credentials
    admin_database_url = f"postgresql://{user}:{password}@{host}/postgres"
    # --- END OF THE FIX ---

    schema_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "schema.sql",
    )

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
        
        yield EventStore(pool)

    finally:
        await pool.close()
        admin_conn = await asyncpg.connect(admin_database_url)
        try:
            # Disconnect active sessions before dropping
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
async def test_double_decision_concurrency(event_store):
    stream_id = f"loan-concurrency-test-{uuid.uuid4()}"

    seed_events = [
        CreditAnalysisCompleted(decision="approved"),
        CreditAnalysisCompleted(decision="manual_review"),
        CreditAnalysisCompleted(decision="approved"),
    ]
    await event_store.append(
        stream_id=stream_id,
        events=seed_events,
        expected_version=0,
        aggregate_type="loan",
    )

    async def competing_task(task_id: str):
        return await event_store.append(
            stream_id=stream_id,
            events=[CreditAnalysisCompleted(decision=f"decision-{task_id}")],
            expected_version=3,
            aggregate_type="loan",
        )

    results = await asyncio.gather(
        *[competing_task("A"), competing_task("B")],
        return_exceptions=True,
    )

    winner_version = None
    loser_error = None
    for result in results:
        if isinstance(result, OptimisticConcurrencyError):
            loser_error = result
        elif isinstance(result, int):
            winner_version = result

    final_stream = await event_store.load_stream(stream_id)

    print("\n=== Concurrency Proof Report ===")
    print(f"Winner found: version advanced to {winner_version}")
    print(f"Loser found: received OptimisticConcurrencyError -> {loser_error}")
    print(f"Final stream event count: {len(final_stream)}")
    print(f"Final event stream_position: {final_stream[3].stream_position}")
    print("=== End Proof Report ===\n")

    assert isinstance(results[0], OptimisticConcurrencyError) or isinstance(
        results[1], OptimisticConcurrencyError
    )
    assert winner_version is not None
    assert loser_error is not None
    assert len(final_stream) == 4
    assert final_stream[3].stream_position == 4
