import json
import asyncpg
from typing import AsyncIterator, List, Optional

from ledger.events.upcasting import registry
from ledger.schema.events import (
	BaseEvent,
	OptimisticConcurrencyError,
	OutboxMessage,
	StoredEvent,
	StreamMetadata,
)


class EventStore:
	def __init__(self, pool: asyncpg.Pool):
		self._pool = pool

	async def append(
		self,
		stream_id: str,
		events: List[BaseEvent],
		expected_version: int,
		aggregate_type: str,
		correlation_id: Optional[str] = None,
		causation_id: Optional[str] = None,
	) -> int:
		if not events:
			return expected_version

		try:
			async with self._pool.acquire() as conn:
				async with conn.transaction():
					row: Optional[asyncpg.Record] = await conn.fetchrow(
						"""
						SELECT current_version
						FROM event_streams
						WHERE stream_id = $1
						FOR UPDATE
						""",
						stream_id,
					)

					current_version = int(row["current_version"]) if row else 0

					if expected_version != current_version:
						raise OptimisticConcurrencyError(expected_version, current_version, stream_id)

					new_stream_version = current_version + len(events)
					for index, event in enumerate(events, start=1):
						stream_position = current_version + index
						event_type = event.__class__.__name__
						payload = event.model_dump_json()
						event_metadata = {
							"correlation_id": correlation_id,
							"causation_id": causation_id,
						}

						inserted = await conn.fetchrow(
							"""
							INSERT INTO events (stream_id, stream_position, event_type, payload, metadata)
							VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
							RETURNING event_id
							""",
							stream_id,
							stream_position,
							event_type,
							payload,
							json.dumps(event_metadata),
						)
						new_event_id = inserted["event_id"]

						if event.destination:
							await conn.execute(
								"""
								INSERT INTO outbox (event_id, destination, payload)
								VALUES ($1, $2, $3::jsonb)
								""",
								new_event_id,
								event.destination,
								payload,
							)

					if current_version == 0:
						await conn.execute(
							"""
							INSERT INTO event_streams (stream_id, aggregate_type, current_version)
							VALUES ($1, $2, $3)
							""",
							stream_id,
							aggregate_type,
							new_stream_version,
						)
					else:
						await conn.execute(
							"""
							UPDATE event_streams
							SET current_version = $2
							WHERE stream_id = $1
							""",
							stream_id,
							new_stream_version,
						)

					return new_stream_version
		except asyncpg.PostgresError as exc:
			raise RuntimeError("Failed to append events to event store.") from exc

	async def load_stream(self, stream_id: str) -> List[StoredEvent]:
		async with self._pool.acquire() as conn:
			records = await conn.fetch(
				"""
				SELECT *
				FROM events
				WHERE stream_id = $1
				ORDER BY stream_position ASC
				""",
				stream_id,
			)

			return [registry.upcast(StoredEvent(**record)) for record in records]


	async def load_all(
		self, from_global_position: int = 0, batch_size: int = 500
	) -> AsyncIterator[StoredEvent]:
		async with self._pool.acquire() as conn:
			while True:
				records = await conn.fetch(
					"""
					SELECT *
					FROM events
					WHERE global_position > $1
					ORDER BY global_position ASC
					LIMIT $2
					""",
					from_global_position,
					batch_size,
				)

				if not records:
					break

				for record in records:
					yield registry.upcast(StoredEvent(**record))


				from_global_position = int(records[-1]["global_position"])

	async def stream_version(self, stream_id: str) -> int:
		async with self._pool.acquire() as conn:
			row: Optional[asyncpg.Record] = await conn.fetchrow(
				"""
				SELECT current_version
				FROM event_streams
				WHERE stream_id = $1
				""",
				stream_id,
			)

			return int(row["current_version"]) if row else 0

	async def archive_stream(self, stream_id: str) -> None:
		async with self._pool.acquire() as conn:
			await conn.execute(
				"""
				UPDATE event_streams
				SET archived_at = NOW()
				WHERE stream_id = $1
				""",
				stream_id,
			)

	async def get_stream_metadata(self, stream_id: str) -> Optional[StreamMetadata]:
		async with self._pool.acquire() as conn:
			row: Optional[asyncpg.Record] = await conn.fetchrow(
				"""
				SELECT *
				FROM event_streams
				WHERE stream_id = $1
				""",
				stream_id,
			)

			if not row:
				return None

			return StreamMetadata(**row)

	async def get_unpublished_outbox_events(self, limit: int = 100) -> List[OutboxMessage]:
		async with self._pool.acquire() as conn:
			records = await conn.fetch(
				"""
				SELECT id, event_id, destination, payload
				FROM outbox
				WHERE published_at IS NULL
				ORDER BY created_at ASC
				LIMIT $1
				""",
				limit,
			)

			return [OutboxMessage(**record) for record in records]


