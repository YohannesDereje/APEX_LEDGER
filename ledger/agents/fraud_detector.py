import asyncio
import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from ledger.agents.base_agent import BaseApexAgent
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
from ledger.event_store import EventStore
from ledger.schema.events import (
    AgentType,
    ComplianceCheckRequested,
    FraudScreeningCompleted,
    OptimisticConcurrencyError,
)


class FraudDetectionState(TypedDict):
    application_id: str
    session: AgentSessionAggregate
    fraud_decision: dict[str, Any] | None


class FraudDetectionAgent(BaseApexAgent):
    def __init__(
        self,
        store: EventStore,
        agent_id: str,
        model_version: str,
        **shared_config: Any,
    ) -> None:
        super().__init__(
            store=store,
            agent_id=agent_id,
            model_version=model_version,
            agent_type=AgentType.FRAUD_DETECTION,
            **shared_config,
        )
        self.graph = None
        self.build_graph()

    @staticmethod
    def _hash_input(data: dict[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(data, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

    async def _node_load_data(
        self,
        state: FraudDetectionState,
    ) -> FraudDetectionState:
        session = await self._record_node_execution(
            state["session"],
            "load_data",
            input_keys=["application_id"],
            output_keys=["application_id"],
            llm_called=False,
        )

        updated_state = dict(state)
        updated_state["session"] = session
        return updated_state

    async def _node_analyze_fraud_risk(
        self,
        state: FraudDetectionState,
    ) -> FraudDetectionState:
        model_name = os.getenv("VISION_MODEL_NAME", "qwen/qwen-7b-chat")
        print(f"Simulating LLM call to model: {model_name}")

        fraud_decision = {
            "fraud_score": 0.15,
            "risk_level": "LOW",
            "anomalies_found": 0,
            "recommendation": "PROCEED",
        }

        session = await self._record_node_execution(
            state["session"],
            "analyze_fraud_risk",
            input_keys=["application_id"],
            output_keys=["fraud_decision"],
            llm_called=True,
            llm_tokens_input=620,
            llm_tokens_output=140,
            llm_cost_usd=0.01,
            duration_ms=160,
        )

        updated_state = dict(state)
        updated_state["fraud_decision"] = fraud_decision
        updated_state["session"] = session
        return updated_state

    async def _node_write_output(
        self,
        state: FraudDetectionState,
    ) -> FraudDetectionState:
        application_id = state["application_id"]
        session = state["session"]
        fraud_decision = state.get("fraud_decision")

        if fraud_decision is None:
            raise ValueError("FraudDetectionAgent requires a fraud decision before writing output.")

        completed_at = datetime.now(timezone.utc)
        fraud_completed = FraudScreeningCompleted(
            application_id=application_id,
            session_id=str(session.session_id),
            fraud_score=float(fraud_decision["fraud_score"]),
            risk_level=str(fraud_decision["risk_level"]),
            anomalies_found=int(fraud_decision["anomalies_found"]),
            recommendation=str(fraud_decision["recommendation"]),
            screening_model_version=self.model_version,
            input_data_hash=self._hash_input(
                {
                    "application_id": application_id,
                    "session_id": str(session.session_id),
                    "fraud_decision": fraud_decision,
                }
            ),
            completed_at=completed_at,
        )

        fraud_stream_id = f"fraud-{application_id}"
        fraud_stream_version = await self.store.stream_version(fraud_stream_id)
        fraud_stream_version = await self.store.append(
            stream_id=fraud_stream_id,
            events=[fraud_completed],
            expected_version=fraud_stream_version,
            aggregate_type="FraudScreening",
        )

        compliance_requested = ComplianceCheckRequested(
            application_id=application_id,
            requested_at=datetime.now(timezone.utc),
            triggered_by_event_id=f"{fraud_stream_id}:{fraud_stream_version}",
            regulation_set_version=str(self.shared_config.get("regulation_set_version", "v1")),
            rules_to_evaluate=list(
                self.shared_config.get(
                    "rules_to_evaluate",
                    ["KYC_SCREENING", "SANCTIONS_SCREENING", "DOCUMENT_CONSISTENCY"],
                )
            ),
        )

        attempts = 0
        while True:
            loan_application = await LoanApplicationAggregate.load(self.store, application_id)
            try:
                await self.store.append(
                    stream_id=f"loan-{application_id}",
                    events=[compliance_requested],
                    expected_version=loan_application.version,
                    aggregate_type="LoanApplication",
                )
                break
            except OptimisticConcurrencyError:
                attempts += 1
                if attempts > self.max_retries:
                    raise
                await asyncio.sleep(0)

        session = await self._record_node_execution(
            session,
            "write_output",
            input_keys=["application_id", "fraud_decision"],
            output_keys=["fraud_screening_completed", "compliance_check_requested"],
            llm_called=False,
        )

        updated_state = dict(state)
        updated_state["session"] = session
        return updated_state

    def build_graph(self) -> None:
        graph = StateGraph(FraudDetectionState)

        graph.add_node("load_data", self._node_load_data)
        graph.add_node("analyze_fraud_risk", self._node_analyze_fraud_risk)
        graph.add_node("write_output", self._node_write_output)

        graph.add_edge(START, "load_data")
        graph.add_edge("load_data", "analyze_fraud_risk")
        graph.add_edge("analyze_fraud_risk", "write_output")
        graph.add_edge("write_output", END)

        self.graph = graph.compile()

    async def run(self, application_id: str) -> FraudDetectionState:
        session = await self._start_session(application_id)
        return await self.graph.ainvoke(
            {
                "application_id": application_id,
                "session": session,
                "fraud_decision": None,
            }
        )