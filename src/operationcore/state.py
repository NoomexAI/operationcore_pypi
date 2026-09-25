"""Shared lifecycle values returned by the operation runtime and its store."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class LifecycleStatus(StrEnum):
    """The fixed states shared by operation and task lifecycles."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class EventStreamState:
    """The durable position and completion state of one event stream."""

    last_event_id: int
    is_finished: bool
    finished_at: datetime | None = None
