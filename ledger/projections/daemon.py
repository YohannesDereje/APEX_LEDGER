import asyncio
import logging
from typing import Any, Optional

import asyncpg

from ledger.event_store import EventStore
from ledger.projections.agent_performance_ledger import AgentPerformanceLedgerProjection
from ledger.projections.application_summary import ApplicationSummaryProjection


logger = logging.getLogger(__name__)


class CheckpointStore:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_checkpoint(self, projection_name: str) -> int:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT last_position
                FROM projection_checkpoints
                WHERE projection_name = $1
                """,
                projection_name,
            )
            return int(row["last_position"]) if row else 0

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO projection_checkpoints (projection_name, last_position)
                VALUES ($1, $2)
                ON CONFLICT (projection_name)
                DO UPDATE SET last_position = EXCLUDED.last_position
                """,
                projection_name,
                position,
            )


class ProjectionDaemon:
    def __init__(
        self,
        event_store: EventStore,
        projections: list[Any],
        max_retries: int = 3,
        retry_delay_seconds: float = 1.0,
    ):
        self._event_store = event_store
        self._projections = {projection.name: projection for projection in projections}
        self._checkpoint_store = CheckpointStore(event_store._pool)
        self._running = True
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds

    async def run_forever(self, poll_interval_seconds: float = 1.0) -> None:
        while self._running:
            try:
                await self._process_batch()
            except Exception:
                logger.exception("Projection daemon batch failed")
            await asyncio.sleep(poll_interval_seconds)

    def stop(self) -> None:
        self._running = False

    async def _process_batch(self) -> None:
        if not self._projections:
            logger.debug("No projections registered; skipping batch")
            return

        checkpoints: dict[str, int] = {}
        for projection_name in self._projections.keys():
            checkpoints[projection_name] = await self._checkpoint_store.get_checkpoint(projection_name)

        min_checkpoint = min(checkpoints.values())

        events = [
            event
            async for event in self._event_store.load_all(from_global_position=min_checkpoint)
        ]

        if not events:
            logger.debug("No new events after minimum checkpoint %s", min_checkpoint)
            return

        logger.info(
            "Processing batch of %s events from global_position > %s",
            len(events),
            min_checkpoint,
        )

        for event in events:
            skip_event = False
            for projection in self._projections.values():
                if event.event_type in projection.get_subscribed_event_types():
                    projection_metadata = {
                        **event.metadata,
                        "event_id": str(event.event_id),
                        "stream_id": event.stream_id,
                        "stream_position": event.stream_position,
                        "global_position": event.global_position,
                        "recorded_at": event.recorded_at,
                    }

                    attempt = 0
                    while True:
                        try:
                            await projection.handle_event(
                                event_type=event.event_type,
                                payload=event.payload,
                                metadata=projection_metadata,
                            )
                            break
                        except Exception:
                            projection_name = projection.name
                            logger.exception(
                                "Projection '%s' failed for global_position=%s on attempt %s",
                                projection_name,
                                event.global_position,
                                attempt + 1,
                            )

                            if attempt >= self.max_retries:
                                logger.critical(
                                    "Skipping poison pill event for projection '%s' at global_position=%s",
                                    projection_name,
                                    event.global_position,
                                )
                                skip_event = True
                                break

                            attempt += 1
                            await asyncio.sleep(self.retry_delay_seconds)

                    if skip_event:
                        break

            if skip_event:
                continue

        last_position = int(events[-1].global_position)
        for projection_name in self._projections.keys():
            await self._checkpoint_store.save_checkpoint(projection_name, last_position)
            logger.info(
                "Saved checkpoint for %s at global_position=%s",
                projection_name,
                last_position,
            )

    async def get_lag_for_projection(self, projection_name: str) -> Optional[float]:
        if projection_name not in self._projections:
            return None

        async with self._event_store._pool.acquire() as conn:
            latest_row = await conn.fetchrow(
                """
                SELECT recorded_at
                FROM events
                ORDER BY global_position DESC
                LIMIT 1
                """
            )

            if not latest_row or not latest_row["recorded_at"]:
                return None

            last_position = await self._checkpoint_store.get_checkpoint(projection_name)

            checkpoint_row = await conn.fetchrow(
                """
                SELECT recorded_at
                FROM events
                WHERE global_position = $1
                """,
                last_position,
            )

            if not checkpoint_row or not checkpoint_row["recorded_at"]:
                return None

            latest_event_time = latest_row["recorded_at"]
            checkpoint_time = checkpoint_row["recorded_at"]

            lag_ms = (latest_event_time - checkpoint_time).total_seconds() * 1000
            return float(lag_ms)
