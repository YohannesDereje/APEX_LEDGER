import json
import uuid
from datetime import datetime
from typing import Optional

import asyncpg

from ledger.schema.events import (
    ComplianceCheckCompleted,
    ComplianceCheckInitiated,
    ComplianceRuleFailed,
    ComplianceRulePassed,
)


class ComplianceAuditViewProjection:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @property
    def name(self) -> str:
        return "compliance_audit_view"

    def get_subscribed_event_types(self) -> list[str]:
        return [
            ComplianceCheckInitiated.__name__,
            ComplianceRulePassed.__name__,
            ComplianceRuleFailed.__name__,
            ComplianceCheckCompleted.__name__,
        ]

    @staticmethod
    def _coerce_state(value) -> dict:
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, str):
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        return dict(value)

    async def handle_event(
        self,
        event_type: str,
        payload: dict,
        metadata: dict,
        table_name: str = "compliance_audit_snapshots",
    ) -> None:
        payload = payload or {}
        metadata = metadata or {}
        application_id = payload.get("application_id")
        if not application_id:
            return

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT state
                FROM {table_name}
                WHERE application_id = $1
                ORDER BY state_as_of_event_at DESC
                LIMIT 1
                """.format(table_name=table_name),
                application_id,
            )

            new_state = self._coerce_state(row["state"]) if row else {}

            if event_type == ComplianceCheckInitiated.__name__:
                initiated_event = ComplianceCheckInitiated.model_validate(payload)
                new_state = {
                    "application_id": initiated_event.application_id,
                    "session_id": initiated_event.session_id,
                    "regulation_set_version": initiated_event.regulation_set_version,
                    "rules_to_evaluate": list(initiated_event.rules_to_evaluate),
                    "initiated_at": initiated_event.initiated_at.isoformat(),
                    "passed_rules": [],
                    "failed_rules": [],
                    "overall_verdict": None,
                    "has_hard_block": False,
                }

            elif event_type == ComplianceRulePassed.__name__:
                passed_event = ComplianceRulePassed.model_validate(payload)
                passed_rules = new_state.setdefault("passed_rules", [])
                passed_rules.append(
                    {
                        "rule_id": passed_event.rule_id,
                        "rule_name": passed_event.rule_name,
                        "rule_version": passed_event.rule_version,
                        "evidence_hash": passed_event.evidence_hash,
                        "evaluation_notes": passed_event.evaluation_notes,
                        "evaluated_at": passed_event.evaluated_at.isoformat(),
                    }
                )

            elif event_type == ComplianceRuleFailed.__name__:
                failed_event = ComplianceRuleFailed.model_validate(payload)
                failed_rules = new_state.setdefault("failed_rules", [])
                failed_rules.append(
                    {
                        "rule_id": failed_event.rule_id,
                        "rule_name": failed_event.rule_name,
                        "rule_version": failed_event.rule_version,
                        "failure_reason": failed_event.failure_reason,
                        "is_hard_block": failed_event.is_hard_block,
                        "remediation_available": failed_event.remediation_available,
                        "evidence_hash": failed_event.evidence_hash,
                        "evaluated_at": failed_event.evaluated_at.isoformat(),
                    }
                )

            elif event_type == ComplianceCheckCompleted.__name__:
                completed_event = ComplianceCheckCompleted.model_validate(payload)
                new_state["overall_verdict"] = completed_event.overall_verdict
                new_state["has_hard_block"] = completed_event.has_hard_block
                new_state["rules_evaluated"] = completed_event.rules_evaluated
                new_state["rules_passed"] = completed_event.rules_passed
                new_state["rules_failed"] = completed_event.rules_failed
                new_state["rules_noted"] = completed_event.rules_noted
                new_state["completed_at"] = completed_event.completed_at.isoformat()

            event_id_value = metadata.get("event_id")
            recorded_at_value = metadata.get("recorded_at")
            if not event_id_value or not recorded_at_value:
                return

            source_event_id: uuid.UUID = (
                event_id_value if isinstance(event_id_value, uuid.UUID) else uuid.UUID(str(event_id_value))
            )
            state_as_of_event_at: datetime = (
                recorded_at_value
                if isinstance(recorded_at_value, datetime)
                else datetime.fromisoformat(str(recorded_at_value))
            )

            await conn.execute(
                """
                INSERT INTO {table_name} (
                    application_id,
                    state,
                    state_as_of_event_at,
                    source_event_id
                )
                VALUES ($1, $2::jsonb, $3, $4)
                """.format(table_name=table_name),
                application_id,
                json.dumps(new_state),
                state_as_of_event_at,
                source_event_id,
            )

    async def rebuild_from_scratch(self, daemon) -> None:
        blue_table = "compliance_audit_snapshots"
        green_table = "compliance_audit_snapshots_green"
        backup_table = "compliance_audit_snapshots_backup"

        async with self._pool.acquire() as conn:
            await conn.execute(f"DROP TABLE IF EXISTS {green_table}")
            await conn.execute(f"CREATE TABLE {green_table} (LIKE {blue_table} INCLUDING ALL)")

        async for event in daemon._event_store.load_all(from_global_position=0):
            if event.event_type not in self.get_subscribed_event_types():
                continue

            projection_metadata = {
                **event.metadata,
                "event_id": str(event.event_id),
                "stream_id": event.stream_id,
                "stream_position": event.stream_position,
                "global_position": event.global_position,
                "recorded_at": event.recorded_at,
            }

            await self.handle_event(
                event_type=event.event_type,
                payload=event.payload,
                metadata=projection_metadata,
                table_name=green_table,
            )

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(f"DROP TABLE IF EXISTS {backup_table}")
                await conn.execute(f"ALTER TABLE {blue_table} RENAME TO {backup_table}")
                await conn.execute(f"ALTER TABLE {green_table} RENAME TO {blue_table}")

            await conn.execute(f"DROP TABLE {backup_table}")

        await daemon._checkpoint_store.save_checkpoint(self.name, 0)

    async def get_current_compliance(self, application_id: str) -> Optional[dict]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT state
                FROM compliance_audit_snapshots
                WHERE application_id = $1
                ORDER BY state_as_of_event_at DESC
                LIMIT 1
                """,
                application_id,
            )

        if not row or row["state"] is None:
            return None
        return self._coerce_state(row["state"])

    async def get_compliance_at(self, application_id: str, timestamp: datetime) -> Optional[dict]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT state
                FROM compliance_audit_snapshots
                WHERE application_id = $1
                  AND state_as_of_event_at <= $2
                ORDER BY state_as_of_event_at DESC
                LIMIT 1
                """,
                application_id,
                timestamp,
            )

        if not row or row["state"] is None:
            return None
        return self._coerce_state(row["state"])
