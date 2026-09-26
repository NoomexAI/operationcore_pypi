"""Persistence behavior required by the operation runtime."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol
from uuid import UUID

from ..events import Event
from ..records import OperationRecord, OperationTaskRecord
from ..state import EventStreamState
from ..state import LifecycleStatus
from .transitions import LifecycleTransition


class OperationStore(Protocol):
    """Persistence operations required by the operation runtime."""

    async def start(self) -> None:
        """Open the store and initialize its durable state."""
        ...

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

    async def register_task(
        self,
        record: OperationTaskRecord,
        event: Event,
        transition: LifecycleTransition,
    ) -> int:
        """Atomically persist a task and its queued lifecycle event."""
        ...

    async def read_operation(
        self,
        operation_id: UUID,
    ) -> OperationRecord | None:
        """Read one operation projection."""
        ...

    async def list_operations(
        self,
        *,
        status: LifecycleStatus | None,
        after_operation_id: UUID | None,
        limit: int,
    ) -> list[OperationRecord]:
        """List operation projections in newest-first order."""
        ...

    async def read_task(
        self,
        task_id: UUID,
    ) -> OperationTaskRecord | None:
        """Read one task projection."""
        ...

    async def read_task_terminal_event(
        self,
        operation_id: UUID,
        task_id: UUID,
    ) -> Event | None:
        """Read the terminal lifecycle event for a task."""
        ...

    async def list_tasks(
        self,
        *,
        operation_id: UUID | None,
        status: LifecycleStatus | None,
        after_task_id: UUID | None,
        limit: int,
        retryable_only: bool = False,
    ) -> list[OperationTaskRecord]:
        """List task projections in newest-first order."""
        ...

    async def read_retry(
        self,
        task_id: UUID,
    ) -> OperationTaskRecord | None:
        """Read the task that directly retries the supplied task."""
        ...

    async def unfinished_tasks(
        self,
        operation_id: UUID,
    ) -> list[OperationTaskRecord]:
        """Read nonterminal tasks belonging to an operation."""
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

    async def operation_ids(self) -> list[UUID]:
        """List all stored operation identifiers."""
        ...

    async def unfinished_operation_ids(self) -> list[UUID]:
        """List identifiers for nonterminal operations."""
        ...

    async def expired_operation_ids(
        self,
        cutoff: datetime,
        *,
        limit: int,
    ) -> list[UUID]:
        """List terminal operation identifiers older than a cutoff."""
        ...

    async def delete_operation(self, operation_id: UUID) -> bool:
        """Delete an operation and its cascading task and event rows."""
        ...

    async def close(self) -> None:
        """Close the store and release its resources."""
        ...
