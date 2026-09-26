"""Event names and durable event values for the operation runtime."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from .async_utils import await_completion
from .errors import ErrorCode, OperationError

if TYPE_CHECKING:
    from .records import OperationTaskRecord
    from .stores.protocol import OperationStore
    from .stores.transitions import LifecycleTransition


class EventType(str):
    """Lifecycle events required by the operation runtime."""

    OPERATION_LIFECYCLE_QUEUED = "operation.lifecycle.queued"
    OPERATION_LIFECYCLE_STARTED = "operation.lifecycle.started"
    OPERATION_LIFECYCLE_COMPLETED = "operation.lifecycle.completed"
    OPERATION_LIFECYCLE_FAILED = "operation.lifecycle.failed"
    OPERATION_LIFECYCLE_CANCELLED = "operation.lifecycle.cancelled"
    OPERATION_LIFECYCLE_TASK_QUEUED = "operation.lifecycle.task.queued"
    OPERATION_LIFECYCLE_TASK_STARTED = "operation.lifecycle.task.started"
    OPERATION_LIFECYCLE_TASK_COMPLETED = "operation.lifecycle.task.completed"
    OPERATION_LIFECYCLE_TASK_FAILED = "operation.lifecycle.task.failed"
    OPERATION_LIFECYCLE_TASK_CANCELLED = "operation.lifecycle.task.cancelled"

    @classmethod
    def to_dict(cls) -> dict[str, str]:
        """Return the inherited and locally defined event types."""
        attributes: dict[str, None] = {}
        for base in reversed(cls.__mro__):
            if not issubclass(base, EventType):
                continue
            for attribute in vars(base):
                if attribute.isupper():
                    attributes[attribute] = None

        return {
            attribute: value
            for attribute in attributes
            if isinstance(value := getattr(cls, attribute, None), str)
        }

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for attribute, expected in vars(EventType).items():
            if not attribute.isupper() or not isinstance(expected, str):
                continue
            supplied = cls.__dict__.get(attribute, expected)
            if supplied != expected:
                raise TypeError(
                    f"{cls.__name__} cannot override "
                    f"EventType.{attribute}; add a new event type instead."
                )


class Event(BaseModel):
    """An event stored in an operation's event stream."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    type: str = Field(min_length=1)
    data: dict[str, Any] = Field(default_factory=dict)
    operation_id: UUID | None = None
    task_id: UUID | None = None
    task_name: str | None = None
    event_id: int | None = Field(default=None, ge=1)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    is_final: bool = False


class EventStream:
    """The persistent event stream belonging to one operation."""

    def __init__(
        self,
        operation_id: UUID,
        store: OperationStore,
        *,
        error_code: type[ErrorCode] = ErrorCode,
        replay_page_size: int = 256,
        health_check: Callable[[], None] | None = None,
        sync_failure: Callable[[OperationError, UUID], Awaitable[None]] | None = None,
    ) -> None:
        self._validate_page_size(replay_page_size, error_code)
        self.operation_id = operation_id
        self.error_code = error_code
        self._store = store
        self._health_check = health_check
        self._sync_failure = sync_failure
        self._replay_page_size = replay_page_size
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
        """Whether this stream has writes beyond the latest checkpoint."""
        return self._last_write_generation > self._store.synced_generation

    async def publish(
        self,
        event: Event,
        transition: LifecycleTransition | None = None,
        *,
        checkpoint: bool = False,
    ) -> Event:
        """Persist an event and then make it visible to live readers."""
        self._ensure_open()
        self._raise_if_unhealthy()

        async with self._condition:
            self._ensure_open()
            await self._load()
            self._raise_if_unhealthy()
            if self._finished:
                raise OperationError(
                    self.error_code.OPERATION_RUNTIME_EVENT_STREAM_FINISHED,
                    f"Operation '{self.operation_id}' is already finished.",
                )

            persisted = event.model_copy(
                update={
                    "operation_id": self.operation_id,
                    "event_id": self._last_event_id + 1,
                }
            )
            commit = asyncio.create_task(
                self._store.append_event(persisted, transition)
            )
            generation, cancellation_requested = await await_completion(commit)
            self._apply_persisted_event(persisted, generation)
            if checkpoint or event.is_final:
                _, sync_cancelled = await await_completion(
                    asyncio.create_task(self._sync_locked())
                )
                cancellation_requested = cancellation_requested or sync_cancelled
            if cancellation_requested:
                raise asyncio.CancelledError
            return persisted

    async def register_task(
        self,
        record: OperationTaskRecord,
        event: Event,
        transition: LifecycleTransition,
        *,
        checkpoint: bool = False,
    ) -> Event:
        """Atomically persist a task and its queued lifecycle event."""
        self._ensure_open()
        self._raise_if_unhealthy()

        async with self._condition:
            self._ensure_open()
            await self._load()
            self._raise_if_unhealthy()
            if self._finished:
                raise OperationError(
                    self.error_code.OPERATION_RUNTIME_EVENT_STREAM_FINISHED,
                    f"Operation '{self.operation_id}' is already finished.",
                )

            persisted = event.model_copy(
                update={
                    "operation_id": self.operation_id,
                    "event_id": self._last_event_id + 1,
                }
            )
            commit = asyncio.create_task(
                self._store.register_task(record, persisted, transition)
            )
            generation, cancellation_requested = await await_completion(commit)
            self._apply_persisted_event(persisted, generation)
            if checkpoint:
                _, sync_cancelled = await await_completion(
                    asyncio.create_task(self._sync_locked())
                )
                cancellation_requested = cancellation_requested or sync_cancelled
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
                    self.error_code.OPERATION_RUNTIME_EVENT_STREAM_CLOSED,
                    "Event stream is being removed.",
                )
            await self._load()
            self._raise_if_unhealthy()
            self._validate_available_cursor(after_event_id)
            return await self._store.read_events(
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
                    self.error_code.OPERATION_RUNTIME_EVENT_STREAM_CLOSED,
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
                    events = await self._store.read_events(
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
            if self._closed or not self._finished or self._active_event_readers > 0:
                return False
            self._cleanup_reserved = True
            self._closed = True
            self._condition.notify_all()
            return True

    async def _release_cleanup(self) -> None:
        """Restore a stream whose reserved store deletion failed."""
        async with self._condition:
            if not self._cleanup_reserved:
                return
            self._cleanup_reserved = False
            self._closed = False
            self._condition.notify_all()

    def _ensure_open(self) -> None:
        if self._closed:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_EVENT_STREAM_CLOSED,
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
                error_code.OPERATION_RUNTIME_INVALID_EVENT_CURSOR,
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
                error_code.OPERATION_RUNTIME_INVALID_EVENT_PAGE_SIZE,
                "Event page size must be a positive integer.",
            )

    def _validate_available_cursor(self, after_event_id: int) -> None:
        if after_event_id <= self._last_event_id:
            return
        raise OperationError(
            self.error_code.OPERATION_RUNTIME_EVENT_HISTORY_GAP,
            "The requested event cursor is ahead of the recovered event history.",
            details={
                "operation_id": str(self.operation_id),
                "requested_after_event_id": after_event_id,
                "available_last_event_id": self._last_event_id,
            },
        )

    async def _load(self) -> None:
        if self._loaded:
            return
        state = await self._store.read_event_stream_state(self.operation_id)
        self._last_event_id = state.last_event_id
        self._finished = state.is_finished
        self._finished_at = state.finished_at
        self._loaded = True

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
                and exc.code == self.error_code.OPERATION_RUNTIME_SYNC_FAILED
                else OperationError(
                    self.error_code.OPERATION_RUNTIME_SYNC_FAILED,
                    "The operation store could not be synchronized to durable storage.",
                )
            )
            self._sync_error = error
            if self._sync_failure is not None:
                await self._sync_failure(error, self.operation_id)
            self._condition.notify_all()
            raise error from exc
