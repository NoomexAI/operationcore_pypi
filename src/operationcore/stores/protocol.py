"""Persistence behavior required by the operation runtime."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from ..events import Event
from ..state import EventStreamState
from .transitions import LifecycleTransition


class OperationStore(Protocol):
    """Store operations currently required by an event stream."""

    @property
    def is_dirty(self) -> bool:
        """Whether committed writes have not reached the latest checkpoint."""
        ...

    @property
    def synced_generation(self) -> int:
        """The latest write generation included in a checkpoint."""
        ...

    async def append_event(
        self,
        event: Event,
        transition: LifecycleTransition | None = None,
    ) -> int:
        """Atomically append an event and apply its lifecycle transition."""
        ...

    async def read_events(
        self,
        operation_id: UUID,
        after_event_id: int,
        *,
        limit: int | None = None,
    ) -> list[Event]:
        """Read retained events after a stream-local event identifier."""
        ...

    async def read_event_stream_state(
        self,
        operation_id: UUID,
    ) -> EventStreamState:
        """Read the current durable position and completion state."""
        ...

    async def checkpoint(self) -> bool:
        """Synchronize committed writes to the store's checkpoint boundary."""
        ...
