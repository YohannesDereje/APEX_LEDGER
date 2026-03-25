import abc
import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, Optional, Sequence

from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.event_store import EventStore
from ledger.schema.events import (
    AgentNodeExecuted,
    AgentSessionStarted,
    AgentType,
    OptimisticConcurrencyError,
)


class BaseApexAgent(abc.ABC):
    def __init__(
        self,
        store: EventStore,
        agent_id: str,
        model_version: str,
        agent_type: AgentType,
        *,
        langgraph_graph_version: str = "v1",
        context_source: str = "runtime",
        context_token_count: int = 0,
        max_retries: int = 1,
        shared_config: Optional[dict[str, Any]] = None,
    ) -> None:
        self.store = store
        self.agent_id = agent_id
        self.model_version = model_version
        self.agent_type = agent_type
        self.langgraph_graph_version = langgraph_graph_version
        self.context_source = context_source
        self.context_token_count = context_token_count
        self.max_retries = max_retries
        self.shared_config = shared_config or {}

    async def _start_session(self, application_id: str) -> AgentSessionAggregate:
        session_id = uuid.uuid4()
        started_event = AgentSessionStarted(
            session_id=str(session_id),
            agent_type=self.agent_type,
            agent_id=self.agent_id,
            application_id=application_id,
            model_version=self.model_version,
            langgraph_graph_version=self.langgraph_graph_version,
            context_source=self.context_source,
            context_token_count=self.context_token_count,
            started_at=datetime.now(timezone.utc),
        )

        await self.store.append(
            stream_id=f"agent-{session_id}",
            events=[started_event],
            expected_version=0,
            aggregate_type="AgentSession",
        )

        return await AgentSessionAggregate.load(self.store, session_id)

    async def _record_node_execution(
        self,
        session: AgentSessionAggregate,
        node_name: str,
        *,
        input_keys: Sequence[str],
        output_keys: Sequence[str],
        llm_called: bool,
        node_sequence: Optional[int] = None,
        llm_tokens_input: Optional[int] = None,
        llm_tokens_output: Optional[int] = None,
        llm_cost_usd: Optional[float] = None,
        duration_ms: int = 0,
    ) -> AgentSessionAggregate:
        session.assert_session_started()
        sequence = node_sequence if node_sequence is not None else session.version + 1

        node_event = AgentNodeExecuted(
            session_id=str(session.session_id),
            agent_type=self.agent_type,
            node_name=node_name,
            node_sequence=sequence,
            input_keys=list(input_keys),
            output_keys=list(output_keys),
            llm_called=llm_called,
            llm_tokens_input=llm_tokens_input,
            llm_tokens_output=llm_tokens_output,
            llm_cost_usd=llm_cost_usd,
            duration_ms=duration_ms,
            executed_at=datetime.now(timezone.utc),
        )

        attempts = 0
        while True:
            try:
                await self.store.append(
                    stream_id=f"agent-{session.session_id}",
                    events=[node_event],
                    expected_version=session.version,
                    aggregate_type="AgentSession",
                )
                return await AgentSessionAggregate.load(self.store, session.session_id)
            except OptimisticConcurrencyError:
                attempts += 1
                if attempts > self.max_retries:
                    raise
                session = await AgentSessionAggregate.load(self.store, session.session_id)
                sequence = node_sequence if node_sequence is not None else session.version + 1
                node_event = node_event.model_copy(update={"node_sequence": sequence})
                await asyncio.sleep(0)

    @abc.abstractmethod
    async def run(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError