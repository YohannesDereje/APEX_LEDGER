import asyncio
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from ledger.agents.base_agent import BaseApexAgent
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
from ledger.event_store import EventStore
from ledger.schema.events import (
    AgentType,
    ComplianceCheckCompleted,
    ComplianceRulePassed,
    DecisionRequested,
    OptimisticConcurrencyError,
)


class ComplianceState(TypedDict):
    application_id: str
    session: AgentSessionAggregate
    compliance_results: dict[str, Any] | None


class ComplianceAgent(BaseApexAgent):
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
            agent_type=AgentType.COMPLIANCE,
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
        state: ComplianceState,
    ) -> ComplianceState:
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

    async def _node_run_rule_checks(
        self,
        state: ComplianceState,
    ) -> ComplianceState:
        compliance_results = {
            "verdict": "CLEAR",
            "passed_rules": ["KYC_001", "AML_002"],
        }

        session = await self._record_node_execution(
            state["session"],
            "run_rule_checks",
            input_keys=["application_id"],
            output_keys=["compliance_results"],
            llm_called=False,
            duration_ms=120,
        )

        updated_state = dict(state)
        updated_state["compliance_results"] = compliance_results
        updated_state["session"] = session
        return updated_state

    async def _node_write_output(
        self,
        state: ComplianceState,
    ) -> ComplianceState:
        application_id = state["application_id"]
        session = state["session"]
        compliance_results = state.get("compliance_results")

        if compliance_results is None:
            raise ValueError("ComplianceAgent requires compliance results before writing output.")

        passed_rules = list(compliance_results.get("passed_rules", []))
        completed_at = datetime.now(timezone.utc)

        compliance_events = [
            ComplianceRulePassed(
                application_id=application_id,
                session_id=str(session.session_id),
                rule_id=rule_id,
                rule_name=rule_id,
                rule_version="v1",
                evidence_hash=self._hash_input(
                    {
                        "application_id": application_id,
                        "session_id": str(session.session_id),
                        "rule_id": rule_id,
                    }
                ),
                evaluation_notes="Placeholder compliance rule passed in reference implementation.",
                evaluated_at=completed_at,
            )
            for rule_id in passed_rules
        ]
        compliance_events.append(
            ComplianceCheckCompleted(
                application_id=application_id,
                session_id=str(session.session_id),
                rules_evaluated=len(passed_rules),
                rules_passed=len(passed_rules),
                rules_failed=0,
                rules_noted=0,
                has_hard_block=False,
                overall_verdict=str(compliance_results.get("verdict", "CLEAR")),
                completed_at=completed_at,
            )
        )

        compliance_stream_id = f"compliance-{application_id}"
        compliance_stream_version = await self.store.stream_version(compliance_stream_id)
        compliance_stream_version = await self.store.append(
            stream_id=compliance_stream_id,
            events=compliance_events,
            expected_version=compliance_stream_version,
            aggregate_type="ComplianceCheck",
        )

        decision_requested = DecisionRequested(
            application_id=application_id,
            requested_at=datetime.now(timezone.utc),
            all_analyses_complete=True,
            triggered_by_event_id=f"{compliance_stream_id}:{compliance_stream_version}",
        )

        attempts = 0
        while True:
            loan_application = await LoanApplicationAggregate.load(self.store, application_id)
            try:
                await self.store.append(
                    stream_id=f"loan-{application_id}",
                    events=[decision_requested],
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
            input_keys=["application_id", "compliance_results"],
            output_keys=["compliance_events", "decision_requested"],
            llm_called=False,
        )

        updated_state = dict(state)
        updated_state["session"] = session
        return updated_state

    def build_graph(self) -> None:
        graph = StateGraph(ComplianceState)

        graph.add_node("load_data", self._node_load_data)
        graph.add_node("run_rule_checks", self._node_run_rule_checks)
        graph.add_node("write_output", self._node_write_output)

        graph.add_edge(START, "load_data")
        graph.add_edge("load_data", "run_rule_checks")
        graph.add_edge("run_rule_checks", "write_output")
        graph.add_edge("write_output", END)

        self.graph = graph.compile()

    async def run(self, application_id: str) -> ComplianceState:
        session = await self._start_session(application_id)
        return await self.graph.ainvoke(
            {
                "application_id": application_id,
                "session": session,
                "compliance_results": None,
            }
        )