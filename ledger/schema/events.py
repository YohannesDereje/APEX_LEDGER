import json
import uuid
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator

# ==============================================================================
# Custom Exceptions
# ==============================================================================

class OptimisticConcurrencyError(Exception):
	"""
	Raised when an event store operation fails due to a stream version mismatch.
	This is an expected error in concurrent systems and should be handled by
	reloading the aggregate state and retrying the operation.
	"""
	def __init__(self, expected_version: int, actual_version: int, stream_id: str):
		self.expected_version = expected_version
		self.actual_version = actual_version
		self.stream_id = stream_id
		super().__init__(
			f"Optimistic concurrency conflict on stream '{stream_id}'. "
			f"Expected version {expected_version}, but found {actual_version}."
		)

class DomainError(Exception):
	"""
	Base exception for domain logic violations (e.g., business rule errors).
	"""
	pass

# ==============================================================================
# Core Event Models
# ==============================================================================

class BaseEvent(BaseModel):
	"""
	An abstract base class for all domain events.
	All specific events (e.g., ApplicationSubmitted) will inherit from this.
	"""
	pass


class StoredEvent(BaseModel):
	"""
	Represents an event as it is stored in the database.
	This model maps directly to a row in the `events` table.
	"""
	event_id: uuid.UUID
	stream_id: str
	stream_position: int
	global_position: int
	event_type: str
	event_version: int
	payload: dict
	metadata: dict
	recorded_at: datetime

	@field_validator("payload", "metadata", mode="before")
	@classmethod
	def _decode_json_fields(cls, value):
		if isinstance(value, str):
			return json.loads(value)
		return value

	class Config:
		# Allows the model to be created from database records (which are not dicts)
		from_attributes = True


class StreamMetadata(BaseModel):
	stream_id: str
	aggregate_type: str
	current_version: int
	created_at: datetime
	metadata: dict
	archived_at: Optional[datetime]

	@field_validator("metadata", mode="before")
	@classmethod
	def _decode_metadata(cls, value):
		if isinstance(value, str):
			return json.loads(value)
		return value

	class Config:
		from_attributes = True

