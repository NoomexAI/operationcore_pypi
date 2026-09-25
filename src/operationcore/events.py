"""Persistent per-operation event streams for Raven."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from .async_utils import await_completion
from .errors import ErrorCode, OperationError

if TYPE_CHECKING:
    from .config import EventConfig


class EventType(str):
    """Lifecycle events required by the operation runtime."""

    OPERATION_QUEUED = "operation.queued"
    OPERATION_STARTED = "operation.started"
    OPERATION_COMPLETED = "operation.completed"
    OPERATION_FAILED = "operation.failed"
    OPERATION_CANCELLED = "operation.cancelled"
    OPERATION_TASK_QUEUED = "operation.task.queued"
    OPERATION_TASK_STARTED = "operation.task.started"
    OPERATION_TASK_COMPLETED = "operation.task.completed"
    OPERATION_TASK_FAILED = "operation.task.failed"
    OPERATION_TASK_CANCELLED = "operation.task.cancelled"


class Event(BaseModel):
    """An event stored in an operation's event stream."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    type: str
    data: dict[str, Any] = Field(default_factory=dict)
    operation_id: UUID | None = None
    task_id: UUID | None = None
    task_name: str | None = None
    event_id: int | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    is_final: bool = False



class EventStore(Protocol):
    """Storage behavior required by an event stream."""

    @property
    def is_dirty(self) -> bool: ...


    @property
    def synced_generation(self) -> int: ...


    async def append(self, event: Event) -> int: ...


    async def read_after(
        self,
        operation_id: str,
        after_event_id: int,
        *,
        limit: int | None = None,
    ) -> list[Event]: ...


    async def read_metadata(
        self,
        operation_id: str,
    ) -> tuple[int, bool, datetime | None]: ...


    async def checkpoint(self) -> bool: ...



class EventStream:
    """The persistent event stream belonging to one operation."""

    def __init__(
        self,
        operation_id: str,
        store: EventStore,
        health_check: Callable[[], None] | None = None,
        sync_failure: Callable[[OperationError, str | None], Awaitable[None]] | None = None,
        config: EventConfig | None = None,
    ) -> None:
        if config is None:
            from .config import EventConfig

            config = EventConfig()
        self.config = config
        self.event_type = config.event_type
        self.error_code = config.error_code
        self._validate_page_size(config.replay_page_size, self.error_code)
        self.operation_id = operation_id
        self._store = store
        self._health_check = health_check
        self._sync_failure = sync_failure
        self._replay_page_size = config.replay_page_size
        self._condition = asyncio.Condition()
        self._last_event_id = 0
        self._last_write_generation = 0
        self._finished = False
        self._finished_at: datetime | None = None
        self._sync_error: OperationError | None = None
        self._loaded = False
        self._closed = False
        self._active_event_readers = 0
        self._cleanup_reserved = False


    @property
    def is_dirty(self) -> bool:
        return self._last_write_generation > self._store.synced_generation


    async def publish(self, event: Event) -> Event:
        """Persist an event and synchronize lifecycle boundaries immediately."""
        self._ensure_open()
        self._raise_if_unhealthy()

        async with self._condition:
            self._ensure_open()
            await self._load()
            self._raise_if_unhealthy()
            if self._finished:
                raise OperationError(
                    self.error_code.EVENT_STREAM_FINISHED,
                    f"Operation '{self.operation_id}' is already finished.",
                )

            persisted = event.model_copy(
                update={
                    "operation_id": UUID(self.operation_id),
                    "event_id": self._last_event_id + 1,
                }
            )
            commit = asyncio.create_task(self._commit(persisted))
            generation, cancellation_requested = await await_completion(commit)
            self._apply_persisted_event(persisted, generation)
            if cancellation_requested:
                raise asyncio.CancelledError
            return persisted


    async def read(
        self,
        after_event_id: int = 0,
        *,
        limit: int | None = None,
    ) -> list[Event]:
        """Read this stream's retained events after a cursor."""
        self._validate_cursor(after_event_id, self.error_code)
        self._validate_page_size(limit, self.error_code)

        async with self._condition:
            if self._cleanup_reserved:
                raise OperationError(
                    self.error_code.EVENT_STREAM_CLOSED,
                    "Event stream is being removed.",
                )
            await self._load()
            self._raise_if_unhealthy()
            self._validate_available_cursor(after_event_id)
            return await self._store.read_after(
                self.operation_id,
                after_event_id,
                limit=limit,
            )


    async def events(self, after_event_id: int = 0) -> AsyncIterator[Event]:
        """Yield retained events after a cursor, then wait for new events."""
        self._validate_cursor(after_event_id, self.error_code)
        cursor = after_event_id

        async with self._condition:
            if self._cleanup_reserved:
                raise OperationError(
                    self.error_code.EVENT_STREAM_CLOSED,
                    "Event stream is being removed.",
                )
            await self._load()
            self._raise_if_unhealthy()
            self._validate_available_cursor(after_event_id)
            self._active_event_readers += 1

        try:
            while True:
                async with self._condition:
                    await self._load()
                    self._raise_if_unhealthy()
                    events = await self._store.read_after(
                        self.operation_id,
                        cursor,
                        limit=self._replay_page_size,
                    )

                    if not events:
                        if self._finished or self._closed:
                            return
                        await self._condition.wait()
                        continue

                for event in events:
                    cursor = event.event_id or cursor
                    yield event
        finally:
            async with self._condition:
                self._active_event_readers -= 1
                self._condition.notify_all()


    async def sync(self) -> bool:
        """Checkpoint pending writes to durable storage."""
        self._ensure_open()

        async with self._condition:
            await self._load()
            return await self._sync_locked()


    async def mark_sync_failed(self, error: OperationError) -> None:
        """Make a manager-level synchronization failure visible to readers."""
        async with self._condition:
            if self._sync_error is None:
                self._sync_error = error
            self._condition.notify_all()


    async def close(self) -> None:
        """Checkpoint pending writes and stop live stream activity."""
        async with self._condition:
            if self._closed:
                return
            if self._store.is_dirty:
                await self._sync_locked()
            self._closed = True
            self._condition.notify_all()


    async def _reserve_cleanup(self) -> bool:
        """Prevent new readers when a finished, idle stream can be removed."""
        async with self._condition:
            await self._load()
            if (
                self._closed
                or not self._finished
                or self._active_event_readers > 0
            ):
                return False
            self._cleanup_reserved = True
            self._closed = True
            self._condition.notify_all()
            return True


    async def _release_cleanup(self) -> None:
        """Restore a stream whose reserved database deletion failed."""
        async with self._condition:
            if not self._cleanup_reserved:
                return
            self._cleanup_reserved = False
            self._closed = False
            self._condition.notify_all()


    def _ensure_open(self) -> None:
        if self._closed:
            raise OperationError(
                self.error_code.EVENT_STREAM_CLOSED,
                "Event stream is closed.",
            )


    def _raise_if_unhealthy(self) -> None:
        if self._health_check is not None:
            self._health_check()
        if self._sync_error is not None:
            raise self._sync_error


    @staticmethod
    def _validate_cursor(
        after_event_id: int,
        error_code: type[ErrorCode],
    ) -> None:
        if after_event_id < 0:
            raise OperationError(
                error_code.INVALID_EVENT_CURSOR,
                "after_event_id cannot be negative.",
            )


    @staticmethod
    def _validate_page_size(
        limit: int | None,
        error_code: type[ErrorCode],
    ) -> None:
        if limit is None:
            return
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise OperationError(
                error_code.INVALID_EVENT_PAGE_SIZE,
                "Event page size must be a positive integer.",
            )


    def _validate_available_cursor(self, after_event_id: int) -> None:
        if after_event_id <= self._last_event_id:
            return
        raise OperationError(
            self.error_code.EVENT_HISTORY_GAP,
            "The requested event cursor is ahead of the recovered event history.",
            details={
                "operation_id": self.operation_id,
                "requested_after_event_id": after_event_id,
                "available_last_event_id": self._last_event_id,
            },
        )


    async def _load(self) -> None:
        if self._loaded:
            return
        self._last_event_id, self._finished, self._finished_at = (
            await self._store.read_metadata(self.operation_id)
        )
        self._loaded = True


    async def _commit(self, event: Event) -> int:
        generation = await self._store.append(event)
        if event.is_final or event.type in self.config.immediate_sync_event_types:
            await self._sync_locked()
        return generation


    def _apply_persisted_event(self, event: Event, generation: int) -> None:
        self._last_write_generation = generation
        self._last_event_id = event.event_id or self._last_event_id
        self._finished = event.is_final
        if event.is_final:
            self._finished_at = event.timestamp
        self._condition.notify_all()


    async def _sync_locked(self) -> bool:
        if not self._store.is_dirty:
            return False

        try:
            return await self._store.checkpoint()
        except Exception as exc:
            error = (
                exc
                if isinstance(exc, OperationError)
                and exc.code == self.error_code.OPERATION_SYNC_FAILED
                else OperationError(
                    self.error_code.OPERATION_SYNC_FAILED,
                    "The operation database could not be synchronized to durable storage.",
                )
            )
            self._sync_error = error
            if self._sync_failure is not None:
                await self._sync_failure(error, self.operation_id)
            self._condition.notify_all()
            raise error from exc
