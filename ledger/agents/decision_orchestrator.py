import asyncio
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from ledger.agents.base_agent import BaseApexAgent
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
from ledger.event_store import EventStore
from ledger.projections.agent_performance_ledger import AgentPerformanceLedgerProjection
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit_view import ComplianceAuditViewProjection
from ledger.schema.events import (
    AgentType,
    ApplicationApproved,
    ApplicationDeclined,
    DecisionGenerated,
    OptimisticConcurrencyError,
)


class DecisionOrchestratorState(TypedDict):
    application_id: str
    session: AgentSessionAggregate
    credit_analysis_output: dict[str, Any] | None
    fraud_screening_output: dict[str, Any] | None
    compliance_output: dict[str, Any] | None
    overall_decision: dict[str, Any] | None


class DecisionOrchestratorAgent(BaseApexAgent):
    def __init__(
        self,
        store: EventStore,
        application_summary_projection: ApplicationSummaryProjection,
        agent_performance_projection: AgentPerformanceLedgerProjection,
        compliance_audit_projection: ComplianceAuditViewProjection,
        agent_id: str,
        model_version: str,
        **shared_config: Any,
    ) -> None:
        super().__init__(
            store=store,
            agent_id=agent_id,
            model_version=model_version,
            agent_type=AgentType.DECISION_ORCHESTRATOR,
            **shared_config,
        )
        self.application_summary_projection = application_summary_projection
        self.agent_performance_projection = agent_performance_projection
        self.compliance_audit_projection = compliance_audit_projection
        self.graph = None
        self.build_graph()

    async def _node_load_all_analyses(
        self,
        state: DecisionOrchestratorState,
    ) -> DecisionOrchestratorState:
        credit_output = {
            "risk_tier": "MEDIUM",
            "recommended_limit_usd": 350000,
            "summary": "Credit analysis indicates moderate risk with acceptable coverage.",
        }
        fraud_output = {
            "fraud_score": 0.15,
            "recommendation": "PROCEED",
            "summary": "No suspicious anomalies detected in the application package.",
        }
        compliance_output = {
            "overall_verdict": "CLEAR",
            "passed_rules": ["KYC_001", "AML_002"],
            "summary": "Required policy rules passed with no hard blocks.",
        }

        session = await self._record_node_execution(
            state["session"],
            "load_all_analyses",
            input_keys=["application_id"],
            output_keys=["credit_analysis_output", "fraud_screening_output", "compliance_output"],
            llm_called=False,
        )

        updated_state = dict(state)
        updated_state["credit_analysis_output"] = credit_output
        updated_state["fraud_screening_output"] = fraud_output
        updated_state["compliance_output"] = compliance_output
        updated_state["session"] = session
        return updated_state

    async def _node_synthesize_decision(
        self,
        state: DecisionOrchestratorState,
    ) -> DecisionOrchestratorState:
        model_name = os.getenv("VISION_MODEL_NAME", "qwen/qwen-7b-chat")
        print(f"Simulating LLM call to model: {model_name}")

        overall_decision = {
            "recommendation": "APPROVE",
            "confidence": 0.89,
            "approved_amount_usd": Decimal("300000"),
            "conditions": ["Obtain updated tax clearance certificate."],
            "executive_summary": "Cross-agent results support approval with standard post-close conditions.",
            "key_risks": ["Moderate leverage profile requires covenant monitoring."],
        }

        session = await self._record_node_execution(
            state["session"],
            "synthesize_decision",
            input_keys=["credit_analysis_output", "fraud_screening_output", "compliance_output"],
            output_keys=["overall_decision"],
            llm_called=True,
            llm_tokens_input=1020,
            llm_tokens_output=260,
            llm_cost_usd=0.02,
            duration_ms=220,
        )

        updated_state = dict(state)
        updated_state["overall_decision"] = overall_decision
        updated_state["session"] = session
        return updated_state

    async def _node_write_output(
        self,
        state: DecisionOrchestratorState,
    ) -> DecisionOrchestratorState:
        application_id = state["application_id"]
        session = state["session"]
        overall_decision = state.get("overall_decision")

        if overall_decision is None:
            raise ValueError("DecisionOrchestratorAgent requires an overall decision before writing output.")

        recommendation = str(overall_decision.get("recommendation", "DECLINE"))
        generated_at = datetime.now(timezone.utc)
        decision_generated = DecisionGenerated(
            application_id=application_id,
            orchestrator_session_id=str(session.session_id),
            recommendation=recommendation,
            confidence=float(overall_decision.get("confidence", 0.0)),
            approved_amount_usd=overall_decision.get("approved_amount_usd"),
            conditions=list(overall_decision.get("conditions", [])),
            executive_summary=str(overall_decision.get("executive_summary", "")),
            key_risks=list(overall_decision.get("key_risks", [])),
            contributing_sessions=[str(session.session_id)],
            model_versions={"decision_orchestrator": self.model_version},
            generated_at=generated_at,
        )

        if recommendation == "APPROVE":
            terminal_event = ApplicationApproved(
                application_id=application_id,
                approved_amount_usd=Decimal(str(overall_decision.get("approved_amount_usd", "0"))),
                interest_rate_pct=float(self.shared_config.get("interest_rate_pct", 11.5)),
                term_months=int(self.shared_config.get("term_months", 36)),
                conditions=list(overall_decision.get("conditions", [])),
                approved_by=self.agent_id,
                effective_date=date.today(),
                approved_at=generated_at,
            )
        else:
            terminal_event = ApplicationDeclined(
                application_id=application_id,
                decline_reasons=list(overall_decision.get("key_risks", ["Insufficient support for approval."])),
                declined_by=self.agent_id,
                adverse_action_notice_required=True,
                adverse_action_codes=["SYSTEM_DECISION_DECLINE"],
                declined_at=generated_at,
            )

        attempts = 0
        while True:
            loan_application = await LoanApplicationAggregate.load(self.store, application_id)
            try:
                await self.store.append(
                    stream_id=f"loan-{application_id}",
                    events=[decision_generated, terminal_event],
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
            input_keys=["application_id", "overall_decision"],
            output_keys=["decision_generated", terminal_event.__class__.__name__],
            llm_called=False,
        )

        updated_state = dict(state)
        updated_state["session"] = session
        return updated_state

    def build_graph(self) -> None:
        graph = StateGraph(DecisionOrchestratorState)

        graph.add_node("load_all_analyses", self._node_load_all_analyses)
        graph.add_node("synthesize_decision", self._node_synthesize_decision)
        graph.add_node("write_output", self._node_write_output)

        graph.add_edge(START, "load_all_analyses")
        graph.add_edge("load_all_analyses", "synthesize_decision")
        graph.add_edge("synthesize_decision", "write_output")
        graph.add_edge("write_output", END)

        self.graph = graph.compile()

    async def run(self, application_id: str) -> DecisionOrchestratorState:
        session = await self._start_session(application_id)
        return await self.graph.ainvoke(
            {
                "application_id": application_id,
                "session": session,
                "credit_analysis_output": None,
                "fraud_screening_output": None,
                "compliance_output": None,
                "overall_decision": None,
            }
        )