from datetime import datetime
from typing import Optional

import asyncpg

from ledger.schema.events import (
    ApplicationApproved,
    ApplicationDeclined,
    ApplicationSubmitted,
    ComplianceCheckCompleted,
    ComplianceCheckRequested,
    CreditAnalysisCompleted,
    DecisionGenerated,
    DecisionRequested,
    FraudScreeningCompleted,
    FraudScreeningRequested,
    HumanReviewOverride,
    PackageReadyForAnalysis,
)


class ApplicationSummaryProjection:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @property
    def name(self) -> str:
        return "application_summary"

    def get_subscribed_event_types(self) -> list[str]:
        return [
            ApplicationSubmitted.__name__,
            PackageReadyForAnalysis.__name__,
            CreditAnalysisCompleted.__name__,
            FraudScreeningRequested.__name__,
            FraudScreeningCompleted.__name__,
            ComplianceCheckRequested.__name__,
            ComplianceCheckCompleted.__name__,
            DecisionRequested.__name__,
            DecisionGenerated.__name__,
            ApplicationApproved.__name__,
            ApplicationDeclined.__name__,
            HumanReviewOverride.__name__,
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

    async def _update_summary(self, application_id: str, values: dict, recorded_at) -> None:
        assignments = []
        parameters = [application_id]

        for index, (column, value) in enumerate(values.items(), start=2):
            assignments.append(f"{column} = ${index}")
            parameters.append(value)

        last_placeholder = len(parameters) + 1
        assignments.append(f"last_event_at = ${last_placeholder}")
        parameters.append(recorded_at)

        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE application_summary
                SET {', '.join(assignments)}
                WHERE application_id = $1
                  AND (last_event_at IS NULL OR last_event_at <= ${last_placeholder})
                """,
                *parameters,
            )

    async def on_PackageReadyForAnalysis(self, payload: dict, metadata: dict) -> None:
        await self._update_summary(
            payload["application_id"],
            {
                "state": "AWAITING_ANALYSIS",
                "last_event_type": "PackageReadyForAnalysis",
            },
            metadata.get("recorded_at"),
        )

    async def on_CreditAnalysisCompleted(self, payload: dict, metadata: dict) -> None:
        decision = payload.get("decision", {})
        await self._update_summary(
            payload["application_id"],
            {
                "state": "ANALYSIS_COMPLETE",
                "risk_tier": decision.get("risk_tier"),
                "last_event_type": "CreditAnalysisCompleted",
            },
            metadata.get("recorded_at"),
        )

    async def on_FraudScreeningRequested(self, payload: dict, metadata: dict) -> None:
        await self._update_summary(
            payload["application_id"],
            {
                "state": "FRAUD_REVIEW",
                "last_event_type": "FraudScreeningRequested",
            },
            metadata.get("recorded_at"),
        )

    async def on_FraudScreeningCompleted(self, payload: dict, metadata: dict) -> None:
        await self._update_summary(
            payload["application_id"],
            {
                "state": "FRAUD_REVIEW_COMPLETE",
                "fraud_score": payload.get("fraud_score"),
                "last_event_type": "FraudScreeningCompleted",
            },
            metadata.get("recorded_at"),
        )

    async def on_ComplianceCheckRequested(self, payload: dict, metadata: dict) -> None:
        await self._update_summary(
            payload["application_id"],
            {
                "state": "COMPLIANCE_REVIEW",
                "last_event_type": "ComplianceCheckRequested",
            },
            metadata.get("recorded_at"),
        )

    async def on_ComplianceCheckCompleted(self, payload: dict, metadata: dict) -> None:
        await self._update_summary(
            payload["application_id"],
            {
                "state": "COMPLIANCE_REVIEW_COMPLETE",
                "compliance_status": payload.get("overall_verdict"),
                "last_event_type": "ComplianceCheckCompleted",
            },
            metadata.get("recorded_at"),
        )

    async def on_DecisionRequested(self, payload: dict, metadata: dict) -> None:
        await self._update_summary(
            payload["application_id"],
            {
                "state": "PENDING_DECISION",
                "last_event_type": "DecisionRequested",
            },
            metadata.get("recorded_at"),
        )

    async def on_DecisionGenerated(self, payload: dict, metadata: dict) -> None:
        recommendation = payload.get("recommendation")
        state = "PENDING_DECISION"
        if recommendation == "APPROVE":
            state = "APPROVED_PENDING_HUMAN"
        elif recommendation == "DECLINE":
            state = "DECLINED_PENDING_HUMAN"

        await self._update_summary(
            payload["application_id"],
            {
                "state": state,
                "decision": recommendation,
                "approved_amount_usd": payload.get("approved_amount_usd"),
                "last_event_type": "DecisionGenerated",
            },
            metadata.get("recorded_at"),
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

    async def on_ApplicationDeclined(self, payload: dict, metadata: dict) -> None:
        declined_at = payload.get("declined_at")
        if isinstance(declined_at, str):
            declined_at = datetime.fromisoformat(declined_at)

        await self._update_summary(
            payload["application_id"],
            {
                "state": "FINAL_DECLINED",
                "decision": "DECLINE",
                "final_decision_at": declined_at,
                "last_event_type": "ApplicationDeclined",
            },
            metadata.get("recorded_at"),
        )

    async def on_HumanReviewOverride(self, payload: dict, metadata: dict) -> None:
        await self._update_summary(
            payload["application_id"],
            {
                "human_reviewer_id": payload.get("reviewer_id"),
                "last_event_type": "HumanReviewOverride",
            },
            metadata.get("recorded_at"),
        )

    async def handle_event(self, event_type: str, payload: dict, metadata: dict) -> None:
        handler_name = f"on_{event_type}"
        handler = getattr(self, handler_name, None)
        if handler is None:
            return

        await handler(payload, metadata)
