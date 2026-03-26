import asyncio
import hashlib
import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from ledger.agents.base_agent import BaseApexAgent
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
from ledger.event_store import EventStore
from ledger.schema.events import (
    AgentType,
    CreditAnalysisCompleted,
    CreditDecision,
    FinancialFacts,
    FraudScreeningRequested,
    OptimisticConcurrencyError,
    RiskTier,
)


class CreditAnalysisState(TypedDict):
    application_id: str
    session: AgentSessionAggregate
    financial_facts: FinancialFacts | None
    credit_decision: CreditDecision | None


class CreditAnalysisAgent(BaseApexAgent):
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
            agent_type=AgentType.CREDIT_ANALYSIS,
            **shared_config,
        )
        self.graph = None
        self.build_graph()

    @staticmethod
    def _placeholder_financial_facts() -> FinancialFacts:
        return FinancialFacts(
            total_revenue=Decimal("1250000"),
            gross_profit=Decimal("510000"),
            operating_income=Decimal("240000"),
            net_income=Decimal("175000"),
            ebitda=Decimal("265000"),
            interest_expense=Decimal("42000"),
            depreciation_amortization=Decimal("25000"),
            total_assets=Decimal("1850000"),
            total_liabilities=Decimal("820000"),
            total_equity=Decimal("1030000"),
            cash_and_equivalents=Decimal("215000"),
            current_assets=Decimal("690000"),
            current_liabilities=Decimal("310000"),
            long_term_debt=Decimal("355000"),
            accounts_receivable=Decimal("140000"),
            inventory=Decimal("95000"),
            operating_cash_flow=Decimal("205000"),
            debt_to_equity=0.80,
            current_ratio=2.23,
            debt_to_ebitda=1.34,
            interest_coverage=6.31,
            gross_margin=0.41,
            net_margin=0.14,
            fiscal_year_end=date(2024, 12, 31),
            field_confidence={
                "total_revenue": 0.91,
                "ebitda": 0.86,
                "current_ratio": 0.84,
            },
            page_references={
                "total_revenue": "income_statement_2024.pdf:1",
                "ebitda": "income_statement_2024.pdf:1",
                "current_ratio": "balance_sheet_2024.pdf:1",
            },
            balance_sheet_balances=True,
        )

    @staticmethod
    def _hash_input(data: dict[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(data, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

    async def _node_load_financial_data(
        self,
        state: CreditAnalysisState,
    ) -> CreditAnalysisState:
        financial_facts = self._placeholder_financial_facts()
        session = await self._record_node_execution(
            state["session"],
            "load_financial_data",
            input_keys=["application_id"],
            output_keys=["financial_facts"],
            llm_called=False,
        )

        updated_state = dict(state)
        updated_state["financial_facts"] = financial_facts
        updated_state["session"] = session
        return updated_state

    async def _node_analyze_credit_risk(
        self,
        state: CreditAnalysisState,
    ) -> CreditAnalysisState:
        model_name = os.getenv("VISION_MODEL_NAME", "qwen/qwen-7b-chat")
        api_base = os.getenv("OPENAI_API_BASE")
        print(f"Simulating LLM call to model: {model_name} at base: {api_base}")

        credit_decision = CreditDecision(
            risk_tier=RiskTier.MEDIUM,
            recommended_limit_usd=Decimal("350000"),
            confidence=0.82,
            rationale=(
                "Stable operating cash flow and solid interest coverage support a moderate "
                "credit limit, but leverage and customer concentration warrant monitoring."
            ),
            key_concerns=[
                "Customer concentration is moderately elevated.",
                "Leverage could tighten if revenue softens.",
            ],
            data_quality_caveats=[
                "Financial facts are placeholder values for the reference implementation.",
            ],
        )
        session = await self._record_node_execution(
            state["session"],
            "analyze_credit_risk",
            input_keys=["application_id", "financial_facts"],
            output_keys=["credit_decision"],
            llm_called=True,
            llm_tokens_input=850,
            llm_tokens_output=220,
            llm_cost_usd=0.01,
            duration_ms=180,
        )

        updated_state = dict(state)
        updated_state["credit_decision"] = credit_decision
        updated_state["session"] = session
        return updated_state

    async def _node_write_output(
        self,
        state: CreditAnalysisState,
    ) -> CreditAnalysisState:
        application_id = state["application_id"]
        session = state["session"]
        financial_facts = state.get("financial_facts")
        credit_decision = state.get("credit_decision")

        if financial_facts is None or credit_decision is None:
            raise ValueError("CreditAnalysisAgent requires financial facts and a credit decision before writing output.")

        completed_at = datetime.now(timezone.utc)
        credit_completed = CreditAnalysisCompleted(
            application_id=application_id,
            session_id=str(session.session_id),
            decision=credit_decision,
            model_version=self.model_version,
            model_deployment_id=self.shared_config.get("model_deployment_id", self.model_version),
            input_data_hash=self._hash_input(
                {
                    "application_id": application_id,
                    "session_id": str(session.session_id),
                    "financial_facts": financial_facts.model_dump(mode="json"),
                    "credit_decision": credit_decision.model_dump(mode="json"),
                }
            ),
            analysis_duration_ms=180,
            completed_at=completed_at,
        )

        credit_stream_id = f"credit-{application_id}"
        credit_stream_version = await self.store.stream_version(credit_stream_id)
        credit_stream_version = await self.store.append(
            stream_id=credit_stream_id,
            events=[credit_completed],
            expected_version=credit_stream_version,
            aggregate_type="CreditAnalysis",
        )

        fraud_requested = FraudScreeningRequested(
            application_id=application_id,
            requested_at=datetime.now(timezone.utc),
            triggered_by_event_id=f"{credit_stream_id}:{credit_stream_version}",
        )

        attempts = 0
        while True:
            loan_application = await LoanApplicationAggregate.load(self.store, application_id)
            try:
                await self.store.append(
                    stream_id=f"loan-{application_id}",
                    events=[fraud_requested],
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
            input_keys=["application_id", "financial_facts", "credit_decision"],
            output_keys=["credit_analysis_completed", "fraud_screening_requested"],
            llm_called=False,
        )

        updated_state = dict(state)
        updated_state["session"] = session
        return updated_state

    def build_graph(self) -> None:
        graph = StateGraph(CreditAnalysisState)

        graph.add_node("load_financial_data", self._node_load_financial_data)
        graph.add_node("analyze_credit_risk", self._node_analyze_credit_risk)
        graph.add_node("write_output", self._node_write_output)

        graph.add_edge(START, "load_financial_data")
        graph.add_edge("load_financial_data", "analyze_credit_risk")
        graph.add_edge("analyze_credit_risk", "write_output")
        graph.add_edge("write_output", END)

        self.graph = graph.compile()

    async def run(self, application_id: str) -> CreditAnalysisState:
        session = await self._start_session(application_id)
        return await self.graph.ainvoke(
            {
                "application_id": application_id,
                "session": session,
                "financial_facts": None,
                "credit_decision": None,
            }
        )