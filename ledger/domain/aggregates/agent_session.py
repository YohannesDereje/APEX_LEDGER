import uuid
from typing import Optional

from ledger.event_store import EventStore
from ledger.schema.events import AgentSessionStarted, DomainError, StoredEvent


class AgentSessionAggregate:
	def __init__(self, session_id: uuid.UUID):
		self.session_id: uuid.UUID = session_id
		self.version: int = 0
		self.has_started: bool = False
		self.model_version: Optional[str] = None

	@classmethod
	async def load(cls, store: EventStore, session_id: uuid.UUID) -> "AgentSessionAggregate":
		agg = cls(session_id=session_id)
		stream_id = f"agent-{session_id}"
		events = await store.load_stream(stream_id)

		for event in events:
			agg._apply(event)

		return agg

	def _apply(self, event: StoredEvent) -> None:
		handler_name = f"_on_{event.event_type}"
		handler = getattr(self, handler_name, None)
		if handler:
			handler(event)

		self.version = event.stream_position

	def _on_AgentSessionStarted(self, event: StoredEvent) -> None:
		if self.version != 0:
			raise DomainError("AgentSessionStarted must be the first event in the stream.")

		self.has_started = True
		self.model_version = event.payload["model_version"]

	def assert_session_started(self) -> None:
		if not self.has_started:
			raise DomainError("Agent session has not been started.")

