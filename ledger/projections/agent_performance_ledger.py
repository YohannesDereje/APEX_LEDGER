import asyncpg

from ledger.schema.events import AgentSessionCompleted, DecisionGenerated


class AgentPerformanceLedgerProjection:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @property
    def name(self) -> str:
        return "agent_performance_ledger"

    def get_subscribed_event_types(self) -> list[str]:
        return [
            AgentSessionCompleted.__name__,
            DecisionGenerated.__name__,
        ]

    async def on_AgentSessionCompleted(self, payload: dict, metadata: dict) -> None:
        recorded_at = metadata.get("recorded_at") or payload.get("completed_at")
        agent_id = payload.get("agent_id") or str(payload.get("agent_type") or "unknown_agent")
        model_version = payload.get("model_version") or "unknown_model"
        total_duration_ms = float(payload.get("total_duration_ms") or 0)

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO agent_performance_ledger (
                    agent_id,
                    model_version,
                    analyses_completed,
                    decisions_generated,
                    avg_duration_ms,
                    first_seen_at,
                    last_seen_at
                )
                VALUES ($1, $2, 1, 0, $3, $4, $4)
                ON CONFLICT (agent_id, model_version) DO UPDATE
                SET
                    analyses_completed = agent_performance_ledger.analyses_completed + 1,
                    last_seen_at = EXCLUDED.last_seen_at,
                    avg_duration_ms = (
                        (agent_performance_ledger.avg_duration_ms * agent_performance_ledger.analyses_completed)
                        + EXCLUDED.avg_duration_ms
                    ) / (agent_performance_ledger.analyses_completed + 1)
                """,
                agent_id,
                model_version,
                total_duration_ms,
                recorded_at,
            )

    async def handle_event(self, event_type: str, payload: dict, metadata: dict) -> None:
        handler_name = f"on_{event_type}"
        handler = getattr(self, handler_name, None)
        if handler is None:
            return

        await handler(payload, metadata)
