from datetime import datetime
from typing import Optional

import asyncpg

from ledger.schema.events import ApplicationApproved, ApplicationSubmitted


class ApplicationSummaryProjection:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @property
    def name(self) -> str:
        return "application_summary"

    def get_subscribed_event_types(self) -> list[str]:
        return [
            ApplicationSubmitted.__name__,
            ApplicationApproved.__name__,
        ]

    async def get_summary(self, application_id: str) -> Optional[dict]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM application_summary WHERE application_id = $1",
                application_id,
            )

        if row is None:
            return None

        return dict(row)

    async def on_ApplicationSubmitted(self, payload: dict, metadata: dict) -> None:
        recorded_at = metadata.get("recorded_at")

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO application_summary (
                    application_id,
                    state,
                    applicant_id,
                    requested_amount_usd,
                    last_event_type,
                    last_event_at
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (application_id) DO UPDATE
                SET
                    state = EXCLUDED.state,
                    applicant_id = EXCLUDED.applicant_id,
                    requested_amount_usd = EXCLUDED.requested_amount_usd,
                    last_event_type = EXCLUDED.last_event_type,
                    last_event_at = EXCLUDED.last_event_at
                WHERE application_summary.last_event_at IS NULL
                   OR application_summary.last_event_at < EXCLUDED.last_event_at
                """,
                payload["application_id"],
                "SUBMITTED",
                payload.get("applicant_id"),
                payload.get("requested_amount_usd"),
                "ApplicationSubmitted",
                recorded_at,
            )

    async def on_ApplicationApproved(self, payload: dict, metadata: dict) -> None:
        recorded_at = metadata.get("recorded_at")
        approved_at = payload.get("approved_at")
        if isinstance(approved_at, str):
            approved_at = datetime.fromisoformat(approved_at)

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE application_summary
                SET
                    state = $2,
                    approved_amount_usd = $3,
                    final_decision_at = $4,
                    last_event_type = $5,
                    last_event_at = $6
                WHERE application_id = $1
                                    AND last_event_at < $6
                """,
                payload["application_id"],
                "FINAL_APPROVED",
                payload.get("approved_amount_usd"),
                approved_at,
                "ApplicationApproved",
                recorded_at,
            )

    async def handle_event(self, event_type: str, payload: dict, metadata: dict) -> None:
        handler_name = f"on_{event_type}"
        handler = getattr(self, handler_name, None)
        if handler is None:
            return

        await handler(payload, metadata)
