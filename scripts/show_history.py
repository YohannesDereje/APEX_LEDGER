import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import asyncpg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ledger.event_store import EventStore


STREAM_PREFIXES = ("loan-", "docpkg-", "credit-", "fraud-", "compliance-")


def _resolve_dev_database_url() -> str:
    base_database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost/postgres",
    )
    return f"{base_database_url.rsplit('/', 1)[0]}/apex_ledger_dev"


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
        format="text",
    )


async def _find_related_stream_ids(pool: asyncpg.Pool, application_id: str) -> list[str]:
    direct_stream_ids = [f"{prefix}{application_id}" for prefix in STREAM_PREFIXES]

    async with pool.acquire() as conn:
        agent_rows = await conn.fetch(
            """
            SELECT DISTINCT stream_id
            FROM events
            WHERE stream_id LIKE 'agent-%'
              AND payload->>'application_id' = $1
            ORDER BY stream_id ASC
            """,
            application_id,
        )

    agent_stream_ids = [row["stream_id"] for row in agent_rows]
    return direct_stream_ids + agent_stream_ids


def _format_scalar(value: Any) -> str:
    if isinstance(value, str):
        text = value
    else:
        text = json.dumps(value, default=str)

    if len(text) > 120:
        return f"{text[:117]}..."
    return text


def _summarize_payload(payload: dict[str, Any]) -> str:
    if not payload:
        return "{}"

    preferred_order = [
        "application_id",
        "session_id",
        "applicant_id",
        "decision",
        "recommendation",
        "overall_verdict",
        "risk_level",
        "fraud_score",
        "requested_amount_usd",
        "approved_amount_usd",
        "output_summary",
        "triggered_by_event_id",
    ]
    ordered_keys = [key for key in preferred_order if key in payload]
    ordered_keys.extend(key for key in payload if key not in ordered_keys)

    parts: list[str] = []
    for key in ordered_keys:
        parts.append(f"{key}={_format_scalar(payload[key])}")

    return ", ".join(parts)


async def _load_history(store: EventStore, stream_ids: list[str]) -> list[Any]:
    events = []
    for stream_id in stream_ids:
        stream_events = await store.load_stream(stream_id)
        events.extend(stream_events)

    events.sort(key=lambda event: event.global_position)
    return events


def _print_history(application_id: str, events: list[Any]) -> None:
    print(f"Application history for {application_id}")
    print("=" * 100)

    if not events:
        print("No events found for this application.")
        return

    for event in events:
        print(
            f"[{event.recorded_at.isoformat()}] "
            f"{event.stream_id} "
            f"#{event.stream_position} "
            f"{event.event_type}"
        )
        print(f"  payload: {_summarize_payload(event.payload)}")
        print()


async def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Show the complete event history for a single application.",
    )
    parser.add_argument(
        "--application",
        required=True,
        help="Application ID to inspect.",
    )
    args = parser.parse_args()

    database_url = _resolve_dev_database_url()
    pool = await asyncpg.create_pool(database_url, init=_init_connection)
    store = EventStore(pool)

    try:
        stream_ids = await _find_related_stream_ids(pool, args.application)
        events = await _load_history(store, stream_ids)
        _print_history(args.application, events)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())