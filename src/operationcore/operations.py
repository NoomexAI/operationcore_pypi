"""Operation, task, manager, recovery, synchronization, and cleanup behavior."""

from __future__ import annotations

import asyncio
import copy
import json
import math
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable
from contextvars import ContextVar, Token
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from .async_utils import await_completion
from .errors import ErrorCode, OperationError, error_payload
from .events import Event, EventStream, EventType
from .records import OperationCleanupResult, OperationRecord, OperationTaskRecord
from .retry import RetryPolicy
from .settings import OperationSettings
from .state import LifecycleStatus
from .stores.protocol import OperationStore
from .stores.transitions import LifecycleTransition


OperationWorker = Callable[["Operation"], Awaitable[Any]]
TaskWorker = Callable[["Operation"], Awaitable[Any]]

_CURRENT_TASK: ContextVar[tuple[UUID, str] | None] = ContextVar(
    "operationcore_current_task",
    default=None,
)


class OperationTask:
    """Handle and result container for one invocation inside an operation."""

    def __init__(
        self,
        operation: Operation,
        name: str,
        worker: TaskWorker,
        *,
        root: bool,
        parent_task_id: UUID | None,
        retry_policy: RetryPolicy,
        retry_input: dict[str, Any] | None,
        attempt: int,
        retry_of_operation_id: UUID | None,
        retry_of_task_id: UUID | None,
        task_id: UUID | None = None,
    ) -> None:
        self.operation = operation
        self.task_id = task_id or uuid4()
        self.name = name
        self._worker = worker
        self._root = root
        self._parent_task_id = parent_task_id
        self._retry_policy = retry_policy
        self._retry_input = retry_input
        self._attempt = attempt
        self._retry_of_operation_id = retry_of_operation_id
        self._retry_of_task_id = retry_of_task_id
        self._created_at = datetime.now(timezone.utc)
        self._status = LifecycleStatus.QUEUED
        self._result: Any = None
        self._error: BaseException | None = None
        self._task: asyncio.Task[Any] | None = None
        self._result_observed = False

    @property
    def operation_id(self) -> UUID:
        return self.operation.operation_id

    @property
    def status(self) -> LifecycleStatus:
        return self._status

    @property
    def result_value(self) -> Any:
        """Return a completed result without waiting; prefer ``result()``."""
        return self._result

    @property
    def error(self) -> BaseException | None:
        return self._error

    @property
    def is_finished(self) -> bool:
        return self._status.is_terminal

    @property
    def is_root(self) -> bool:
        return self._root

    @property
    def parent_task_id(self) -> UUID | None:
        return self._parent_task_id

    @property
    def result_observed(self) -> bool:
        return self._result_observed

    @property
    def retry_policy(self) -> RetryPolicy:
        return self._retry_policy

    @property
    def max_attempts(self) -> int:
        return self._retry_policy.max_attempts

    @property
    def retryable_error_codes(self) -> frozenset[str]:
        return self._retry_policy.retryable_error_codes

    @property
    def retry_input(self) -> dict[str, Any] | None:
        return copy.deepcopy(self._retry_input)

    @property
    def attempt(self) -> int:
        return self._attempt

    @property
    def retry_of_operation_id(self) -> UUID | None:
        return self._retry_of_operation_id

    @property
    def retry_of_task_id(self) -> UUID | None:
        return self._retry_of_task_id

    def to_record(self) -> OperationTaskRecord:
        return OperationTaskRecord(
            task_id=self.task_id,
            operation_id=self.operation_id,
            name=self.name,
            is_root=self.is_root,
            status=LifecycleStatus.QUEUED,
            retry_policy=self.retry_policy,
            retry_input=self.retry_input,
            attempt=self.attempt,
            retry_of_operation_id=self.retry_of_operation_id,
            retry_of_task_id=self.retry_of_task_id,
            created_at=self._created_at,
        )

    async def _persist(self) -> bool:
        persistence = asyncio.create_task(self.operation._persist_task(self))
        _, cancellation_requested = await await_completion(persistence)
        return cancellation_requested

    async def start(self) -> None:
        """Start a child task after its queued record has been persisted."""
        if self._root:
            raise RuntimeError("root operation tasks are started by Operation.run")
        if self._task is not None:
            raise RuntimeError(f"task '{self.task_id}' has already started")
        self._task = asyncio.create_task(
            self._run(),
            name=f"operationcore-task-{self.task_id}",
        )

    async def _run_root(self, operation: Operation) -> Any:
        return await self._run()

    async def _run(self) -> Any:
        token: Token[tuple[UUID, str] | None] = _CURRENT_TASK.set(
            (self.task_id, self.name)
        )
        try:
            cancellation_requested = await self._publish_lifecycle(
                LifecycleStatus.RUNNING,
                self.operation.event_type.OPERATION_LIFECYCLE_TASK_STARTED,
            )
            self._status = LifecycleStatus.RUNNING
            if cancellation_requested:
                raise asyncio.CancelledError
            self.operation.raise_if_cancelled()
            result = await self._worker(self.operation)
            cancellation_requested = await self._publish_lifecycle(
                LifecycleStatus.COMPLETED,
                self.operation.event_type.OPERATION_LIFECYCLE_TASK_COMPLETED,
            )
            self._result = result
            self._status = LifecycleStatus.COMPLETED
            if cancellation_requested:
                raise asyncio.CancelledError
            return self._result
        except asyncio.CancelledError as exc:
            await self._finalize_cancelled(exc)
            if self._root:
                raise
            return None
        except Exception as exc:
            cancellation_requested = False
            payload = error_payload(exc, self.operation.error_code)
            try:
                cancellation_requested = await self._publish_lifecycle(
                    LifecycleStatus.FAILED,
                    self.operation.event_type.OPERATION_LIFECYCLE_TASK_FAILED,
                    error=payload,
                )
            finally:
                self._status = LifecycleStatus.FAILED
                self._error = exc
            if cancellation_requested:
                raise asyncio.CancelledError
            if self._root:
                raise
            return None
        finally:
            _CURRENT_TASK.reset(token)

    async def result(self) -> Any:
        """Wait for this invocation and return or raise its native result."""
        if self._root:
            await self.operation.wait()
        elif self._task is not None:
            await asyncio.shield(self._task)
        self._result_observed = True
        if self._status == LifecycleStatus.CANCELLED:
            raise OperationError(
                self.operation.error_code.OPERATION_RUNTIME_CANCELLED,
                f"Operation task '{self.task_id}' was cancelled.",
            )
        if self._error is not None:
            raise self._error
        if self._root and self.operation.status == LifecycleStatus.FAILED:
            if self.operation._terminal_exception is not None:
                raise self.operation._terminal_exception
            payload = self.operation.error or {}
            code = payload.get("code")
            message = payload.get("message")
            raise OperationError(
                code if isinstance(code, str) else self.operation.error_code.OPERATION_RUNTIME_INTERNAL_ERROR,
                message if isinstance(message, str) else "The operation failed.",
                details=(
                    payload.get("details")
                    if isinstance(payload.get("details"), dict)
                    else None
                ),
            )
        return self._result

    async def cancel(self) -> None:
        """Cancel this task, or its root operation when it is the root task."""
        if self._root:
            await self.operation.cancel()
            return
        task = self._task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if not self.is_finished:
            await self._finalize_cancelled()

    async def events(self, after_event_id: int = 0) -> AsyncIterator[Event]:
        """Yield events belonging to this task and its descendants."""
        if self._root:
            async for event in self.operation.events(after_event_id=after_event_id):
                yield event
                if event.is_final:
                    return
            return

        terminal_event = await self.operation._read_task_terminal_event(self.task_id)
        if (
            terminal_event is not None
            and terminal_event.event_id is not None
            and terminal_event.event_id <= after_event_id
        ):
            return

        terminal_event_types = (
            self.operation.event_type.OPERATION_LIFECYCLE_TASK_COMPLETED,
            self.operation.event_type.OPERATION_LIFECYCLE_TASK_FAILED,
            self.operation.event_type.OPERATION_LIFECYCLE_TASK_CANCELLED,
        )
        async for event in self.operation.events(after_event_id=after_event_id):
            if self.operation._event_belongs_to_task(event, self.task_id):
                yield event
                if event.task_id == self.task_id and event.type in terminal_event_types:
                    return

    async def _wait_for_worker(self) -> None:
        if self._task is not None:
            await asyncio.shield(self._task)

    async def _finalize_cancelled(
        self,
        error: asyncio.CancelledError | None = None,
    ) -> None:
        if self.is_finished:
            return
        cancellation = error or asyncio.CancelledError()
        try:
            await self._publish_lifecycle(
                LifecycleStatus.CANCELLED,
                self.operation.event_type.OPERATION_LIFECYCLE_TASK_CANCELLED,
            )
        finally:
            self._status = LifecycleStatus.CANCELLED
            self._error = cancellation

    async def _publish_lifecycle(
        self,
        status: LifecycleStatus,
        event_type: str,
        *,
        error: dict[str, Any] | None = None,
    ) -> bool:
        publication = asyncio.create_task(
            self.operation._publish_task_lifecycle(
                self,
                status=status,
                event_type=event_type,
                error=error,
            )
        )
        _, cancellation_requested = await await_completion(publication)
        return cancellation_requested


class Operation:
    """One root operation, its event stream, and its child task registry."""

    def __init__(
        self,
        operation_id: UUID,
        name: str,
        stream: EventStream,
        store: OperationStore,
        event_type: type[EventType],
        error_code: type[ErrorCode],
        settings: OperationSettings,
        *,
        created_at: datetime | None = None,
        on_finished: Callable[[Operation], None] | None = None,
    ) -> None:
        self.operation_id = operation_id
        self.name = name
        self._stream = stream
        self._store = store
        self.settings = settings
        self.event_type = event_type
        self.error_code = error_code
        self._created_at = created_at or datetime.now(timezone.utc)
        self._started_at: datetime | None = None
        self._finished_at: datetime | None = None
        self._status = LifecycleStatus.QUEUED
        self._result: Any = None
        self._error: dict[str, Any] | None = None
        self._cancellation_requested = False
        self._task: asyncio.Task[Any] | None = None
        self._tasks: dict[UUID, OperationTask] = {}
        self._terminal_exception: BaseException | None = None
        self._restored = False
        self._on_finished = on_finished
        self._lock = asyncio.Lock()

    @property
    def status(self) -> LifecycleStatus:
        return self._status

    @property
    def created_at(self) -> datetime:
        return self._created_at

    @property
    def started_at(self) -> datetime | None:
        return self._started_at

    @property
    def finished_at(self) -> datetime | None:
        return self._finished_at

    @property
    def result(self) -> Any:
        return self._result

    @property
    def error(self) -> dict[str, Any] | None:
        return copy.deepcopy(self._error)

    @property
    def cancellation_requested(self) -> bool:
        return self._cancellation_requested

    @property
    def is_finished(self) -> bool:
        return self._status.is_terminal

    @property
    def tasks(self) -> tuple[OperationTask, ...]:
        return tuple(self._tasks.values())

    async def get_task(self, task_id: UUID | str) -> OperationTaskRecord:
        parsed_id = self._parse_task_id(task_id)
        record = await self._store.read_task(parsed_id)
        if record is None or record.operation_id != self.operation_id:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_TASK_NOT_FOUND,
                f"Operation task '{parsed_id}' was not found in operation "
                f"'{self.operation_id}'.",
            )
        return record

    async def get_retry(
        self,
        task_id: UUID | str,
    ) -> OperationTaskRecord | None:
        task = await self.get_task(task_id)
        return await self._store.read_retry(task.task_id)

    async def list_tasks(
        self,
        *,
        status: LifecycleStatus | str | None = None,
        limit: int | None = None,
        after_task_id: UUID | str | None = None,
    ) -> list[OperationTaskRecord]:
        parsed_status = self._parse_status(status)
        parsed_cursor: UUID | None = None
        if after_task_id is not None:
            parsed_cursor = self._parse_task_id(after_task_id)
            cursor = await self._store.read_task(parsed_cursor)
            if cursor is None or cursor.operation_id != self.operation_id:
                raise OperationError(
                    self.error_code.OPERATION_RUNTIME_TASK_NOT_FOUND,
                    f"Operation task '{parsed_cursor}' was not found in operation "
                    f"'{self.operation_id}'.",
                )
        selected_limit = self.settings.operation_page_size if limit is None else limit
        self._validate_page_size(selected_limit)
        return await self._store.list_tasks(
            operation_id=self.operation_id,
            status=parsed_status,
            after_task_id=parsed_cursor,
            limit=selected_limit,
        )

    async def run(
        self,
        name: str,
        worker: TaskWorker,
        *,
        retry_input: dict[str, Any] | None = None,
        retry_of: OperationTaskRecord | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> OperationTask:
        """Run the root task or a child task within this operation."""
        if not isinstance(name, str) or not name.strip():
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_NAME,
                "Task name cannot be empty.",
            )
        if retry_of is not None:
            if not retry_of.can_retry or retry_of.name != name:
                raise OperationError(
                    self.error_code.OPERATION_RUNTIME_TASK_NOT_RETRYABLE,
                    f"Operation task '{retry_of.task_id}' cannot be retried as '{name}'.",
                )
            if retry_policy is not None and retry_policy != retry_of.retry_policy:
                raise OperationError(
                    self.error_code.OPERATION_RUNTIME_TASK_NOT_RETRYABLE,
                    "A retry must use the original task's retry policy.",
                )
            selected_policy = retry_of.retry_policy
            retry_input = retry_of.retry_input if retry_input is None else retry_input
            attempt = retry_of.attempt + 1
            retry_of_operation_id = retry_of.operation_id
            retry_of_task_id = retry_of.task_id
        else:
            selected_policy = retry_policy or RetryPolicy()
            attempt = 1
            retry_of_operation_id = None
            retry_of_task_id = None

        normalized_retry_input = self._normalize_retry_input(
            retry_input,
            selected_policy,
        )

        async with self._lock:
            if self.is_finished:
                raise OperationError(
                    self.error_code.OPERATION_RUNTIME_FINISHED,
                    f"Operation '{self.operation_id}' is already finished.",
                )
            if self._cancellation_requested:
                raise asyncio.CancelledError

            is_root = self._task is None
            if is_root and name != self.name:
                raise OperationError(
                    self.error_code.OPERATION_RUNTIME_INVALID_NAME,
                    "The root task name must match the operation name.",
                    details={"operation_name": self.name, "task_name": name},
                )
            if not is_root and self._status != LifecycleStatus.RUNNING:
                raise OperationError(
                    self.error_code.OPERATION_RUNTIME_FINISHED,
                    f"Operation '{self.operation_id}' is not accepting child tasks.",
                )

            current_task = _CURRENT_TASK.get()
            task = OperationTask(
                self,
                name,
                worker,
                root=is_root,
                parent_task_id=(
                    None
                    if is_root or current_task is None
                    else current_task[0]
                ),
                retry_policy=selected_policy,
                retry_input=normalized_retry_input,
                attempt=attempt,
                retry_of_operation_id=retry_of_operation_id,
                retry_of_task_id=retry_of_task_id,
            )
            registration_cancelled = await task._persist()
            self._register_task(task)
            if registration_cancelled:
                self._cancellation_requested = True
            elif is_root:
                self._task = asyncio.create_task(
                    self._run(task._run_root),
                    name=f"operationcore-operation-{self.operation_id}",
                )
                return task
            else:
                await task.start()
                return task

        await task._finalize_cancelled()
        if is_root:
            await await_completion(asyncio.create_task(self._finish_cancelled()))
        raise asyncio.CancelledError

    async def wait(self) -> Operation:
        task = self._task
        if task is not None:
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                if not task.cancelled():
                    raise
        return self

    async def cancel(self) -> Operation:
        async with self._lock:
            if self.is_finished:
                return self
            self._cancellation_requested = True
            root_task = self._task
            children = tuple(task for task in self._tasks.values() if not task.is_root)

        await asyncio.gather(
            *(task.cancel() for task in children),
            return_exceptions=True,
        )
        if root_task is not None and not root_task.done():
            root_task.cancel()
            await asyncio.gather(root_task, return_exceptions=True)

        root_handle = next((task for task in self._tasks.values() if task.is_root), None)
        if root_handle is not None and not root_handle.is_finished:
            await root_handle._finalize_cancelled()
        if not self.is_finished:
            await self._finish_cancelled()
        return self

    async def publish(self, event: Event) -> Event:
        """Publish an application event with active task correlation."""
        if self.is_finished:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_FINISHED,
                f"Operation '{self.operation_id}' is already finished.",
            )
        if event.is_final:
            raise ValueError("workers cannot publish final operation events")
        if event.operation_id is not None and event.operation_id != self.operation_id:
            raise ValueError("event operation_id does not match the operation")

        current_task = _CURRENT_TASK.get()
        updates: dict[str, Any] = {"operation_id": self.operation_id}
        if current_task is not None:
            updates["task_id"] = event.task_id or current_task[0]
            updates["task_name"] = event.task_name or current_task[1]
        return await self._stream.publish(event.model_copy(update=updates))

    def raise_if_cancelled(self) -> None:
        if self._cancellation_requested:
            raise asyncio.CancelledError

    async def events(self, after_event_id: int = 0) -> AsyncIterator[Event]:
        async for event in self._stream.events(after_event_id=after_event_id):
            yield event

    async def read_events(
        self,
        after_event_id: int = 0,
        *,
        limit: int | None = None,
    ) -> list[Event]:
        return await self._stream.read(after_event_id, limit=limit)

    async def _run(self, worker: OperationWorker) -> None:
        try:
            await self._mark_running()
            result = await worker(self)
            await self._join_children()
            if self._cancellation_requested:
                await self._finish_cancelled()
            else:
                await self._finish_completed(result)
        except asyncio.CancelledError as exc:
            await self._cancel_children()
            try:
                root = next((task for task in self._tasks.values() if task.is_root), None)
                if root is not None and not root.is_finished:
                    await root._finalize_cancelled(exc)
                await self._finish_cancelled()
            except BaseException as finish_error:
                self._set_local_terminal_failure(finish_error)
            self._terminal_exception = exc
        except Exception as exc:
            await self._cancel_children()
            try:
                await self._finish_failed(exc)
            except BaseException as finish_error:
                self._set_local_terminal_failure(finish_error)

    async def _join_children(self) -> None:
        while True:
            async with self._lock:
                children = tuple(
                    task
                    for task in self._tasks.values()
                    if not task.is_root and not task.is_finished
                )
            if not children:
                break
            await asyncio.gather(
                *(task._wait_for_worker() for task in children),
                return_exceptions=True,
            )

        failed = next(
            (
                task
                for task in self._tasks.values()
                if not task.is_root
                and not task.result_observed
                and task.status == LifecycleStatus.FAILED
            ),
            None,
        )
        if failed is not None:
            if failed.error is not None:
                raise failed.error
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INTERNAL_ERROR,
                f"Operation task '{failed.task_id}' failed.",
            )
        if any(
            not task.is_root
            and not task.result_observed
            and task.status == LifecycleStatus.CANCELLED
            for task in self._tasks.values()
        ):
            raise asyncio.CancelledError

    async def _cancel_children(self) -> None:
        children = tuple(task for task in self._tasks.values() if not task.is_root)
        await asyncio.gather(
            *(task.cancel() for task in children if not task.is_finished),
            return_exceptions=True,
        )

    async def _mark_running(self) -> None:
        async with self._lock:
            if self._cancellation_requested:
                raise asyncio.CancelledError
            persisted = await self._stream.publish(
                Event(
                    type=self.event_type.OPERATION_LIFECYCLE_STARTED,
                    data={"name": self.name},
                ),
                LifecycleTransition(operation_status=LifecycleStatus.RUNNING),
                checkpoint=True,
            )
            self._status = LifecycleStatus.RUNNING
            self._started_at = persisted.timestamp

    async def _persist_queued(self) -> None:
        persisted = await self._stream.publish(
            Event(
                type=self.event_type.OPERATION_LIFECYCLE_QUEUED,
                data={"name": self.name},
            ),
            LifecycleTransition(
                operation_name=self.name,
                operation_status=LifecycleStatus.QUEUED,
            ),
            checkpoint=True,
        )
        self._created_at = persisted.timestamp

    async def _finish_completed(self, result: Any) -> None:
        await self._finish(
            status=LifecycleStatus.COMPLETED,
            event_type=self.event_type.OPERATION_LIFECYCLE_COMPLETED,
            result=result,
        )

    async def _finish_failed(self, error: BaseException) -> None:
        await self._finish(
            status=LifecycleStatus.FAILED,
            event_type=self.event_type.OPERATION_LIFECYCLE_FAILED,
            error=error_payload(error, self.error_code),
        )
        self._terminal_exception = error

    async def _finish_cancelled(self) -> None:
        await self._finish(
            status=LifecycleStatus.CANCELLED,
            event_type=self.event_type.OPERATION_LIFECYCLE_CANCELLED,
        )

    async def _finish(
        self,
        *,
        status: LifecycleStatus,
        event_type: str,
        result: Any = None,
        error: dict[str, Any] | None = None,
        event_data: dict[str, Any] | None = None,
    ) -> None:
        async with self._lock:
            if self.is_finished:
                return
            data: dict[str, Any] = {"name": self.name, **(event_data or {})}
            if error is not None:
                data["error"] = error
            persisted = await self._stream.publish(
                Event(type=event_type, data=data, is_final=True),
                LifecycleTransition(
                    operation_status=status,
                    operation_error=error,
                ),
            )
            self._status = status
            self._finished_at = persisted.timestamp
            self._result = result
            self._error = error
        if self._on_finished is not None:
            self._on_finished(self)

    async def _publish_task_lifecycle(
        self,
        task: OperationTask,
        *,
        status: LifecycleStatus,
        event_type: str,
        error: dict[str, Any] | None = None,
    ) -> Event:
        data: dict[str, Any] = {"name": task.name}
        if error is not None:
            data["error"] = error
        return await self._stream.publish(
            Event(
                type=event_type,
                data=data,
                task_id=task.task_id,
                task_name=task.name,
            ),
            LifecycleTransition(
                task_id=task.task_id,
                task_status=status,
                task_error=error,
            ),
        )

    async def _persist_task(self, task: OperationTask) -> None:
        await self._stream.register_task(
            task.to_record(),
            Event(
                type=self.event_type.OPERATION_LIFECYCLE_TASK_QUEUED,
                data={"name": task.name},
                task_id=task.task_id,
                task_name=task.name,
            ),
            LifecycleTransition(
                task_id=task.task_id,
                task_status=LifecycleStatus.QUEUED,
            ),
            checkpoint=task.retry_policy.enabled and task.retry_input is not None,
        )

    async def _recover_interrupted_tasks(self) -> None:
        interruption = OperationError(
            self.error_code.OPERATION_RUNTIME_INTERRUPTED,
            "The operation task was interrupted by a previous process termination.",
        )
        payload = interruption.as_payload()
        for record in await self._store.unfinished_tasks(self.operation_id):
            await self._stream.publish(
                Event(
                    type=self.event_type.OPERATION_LIFECYCLE_TASK_FAILED,
                    task_id=record.task_id,
                    task_name=record.name,
                    data={
                        "name": record.name,
                        "error": payload,
                        "previous_status": record.status.value,
                        "recovered_after_restart": True,
                    },
                ),
                LifecycleTransition(
                    task_id=record.task_id,
                    task_status=LifecycleStatus.FAILED,
                    task_error=payload,
                ),
            )

    async def _recover_interrupted(self) -> bool:
        async with self._lock:
            if self.is_finished:
                return False
            if self._task is not None:
                raise RuntimeError("an active operation cannot be recovered as interrupted")
            previous_status = self._status
        interruption = OperationError(
            self.error_code.OPERATION_RUNTIME_INTERRUPTED,
            "The operation was interrupted by a previous process termination.",
        )
        await self._finish(
            status=LifecycleStatus.FAILED,
            event_type=self.event_type.OPERATION_LIFECYCLE_FAILED,
            error=interruption.as_payload(),
            event_data={
                "previous_status": previous_status.value,
                "recovered_after_restart": True,
            },
        )
        return True

    @classmethod
    def from_record(
        cls,
        record: OperationRecord,
        stream: EventStream,
        store: OperationStore,
        event_type: type[EventType],
        error_code: type[ErrorCode],
        settings: OperationSettings,
        *,
        on_finished: Callable[[Operation], None] | None = None,
    ) -> Operation:
        operation = cls(
            operation_id=record.operation_id,
            name=record.name,
            stream=stream,
            store=store,
            event_type=event_type,
            error_code=error_code,
            settings=settings,
            created_at=record.created_at,
            on_finished=on_finished,
        )
        operation._status = record.status
        operation._started_at = record.started_at
        operation._finished_at = record.finished_at
        operation._error = record.error
        operation._restored = True
        return operation

    def _register_task(self, task: OperationTask) -> None:
        self._tasks[task.task_id] = task

    def _event_belongs_to_task(self, event: Event, task_id: UUID) -> bool:
        current_id = event.task_id
        visited: set[UUID] = set()
        while current_id is not None and current_id not in visited:
            if current_id == task_id:
                return True
            visited.add(current_id)
            task = self._tasks.get(current_id)
            current_id = task.parent_task_id if task is not None else None
        return False

    async def _read_task_terminal_event(self, task_id: UUID) -> Event | None:
        return await self._store.read_task_terminal_event(self.operation_id, task_id)

    def _normalize_retry_input(
        self,
        retry_input: dict[str, Any] | None,
        retry_policy: RetryPolicy,
    ) -> dict[str, Any] | None:
        if retry_input is None:
            return None
        if not retry_policy.enabled:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_TASK_NOT_RETRYABLE,
                "This task's retry policy does not permit retry input.",
            )
        try:
            normalized = json.loads(json.dumps(retry_input, allow_nan=False))
        except (TypeError, ValueError) as exc:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_RETRY_INPUT,
                "Retry input must be JSON serializable.",
            ) from exc
        if not isinstance(normalized, dict):
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_RETRY_INPUT,
                "Retry input must be a JSON object.",
            )
        return normalized

    def _set_local_terminal_failure(self, error: BaseException) -> None:
        self._status = LifecycleStatus.FAILED
        self._finished_at = datetime.now(timezone.utc)
        self._result = None
        self._error = error_payload(error, self.error_code)
        self._terminal_exception = error

    def _parse_task_id(self, task_id: UUID | str) -> UUID:
        try:
            return task_id if isinstance(task_id, UUID) else UUID(task_id)
        except (TypeError, ValueError) as exc:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_TASK_NOT_FOUND,
                "task_id must be a valid UUID.",
            ) from exc

    def _parse_status(
        self,
        status: LifecycleStatus | str | None,
    ) -> LifecycleStatus | None:
        if status is None or isinstance(status, LifecycleStatus):
            return status
        try:
            return LifecycleStatus(status)
        except (TypeError, ValueError) as exc:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_STATUS,
                f"Unknown operation status '{status}'.",
            ) from exc

    def _validate_page_size(self, limit: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_PAGE_SIZE,
                "Operation page size must be a positive integer.",
            )


class OperationManager:
    """Own operation persistence, streams, lifecycle, and recovery."""

    def __init__(
        self,
        store: OperationStore,
        event_type: type[EventType],
        error_code: type[ErrorCode],
        settings: OperationSettings,
    ) -> None:
        if not math.isfinite(settings.sync_interval_seconds):
            raise OperationError(
                error_code.OPERATION_RUNTIME_INVALID_SYNC_INTERVAL,
                "Operation synchronization interval must be finite.",
            )
        self.store = store
        self.settings = settings
        self.event_type = event_type
        self.error_code = error_code
        self._operations: OrderedDict[UUID, Operation] = OrderedDict()
        self._sync_service = OperationSyncService(
            self,
            settings.sync_interval_seconds,
        )
        self.cleanup = (
            OperationCleanupService(
                self,
                settings.retention,
                batch_size=settings.cleanup_batch_size,
            )
            if settings.retention is not None
            else None
        )
        self._sync_error: OperationError | None = None
        self._lock = asyncio.Lock()
        self._recovery_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._started = False
        self._closed = False

    @property
    def is_healthy(self) -> bool:
        return self._sync_error is None

    async def start(self) -> None:
        async with self._lifecycle_lock:
            self._ensure_open()
            self._ensure_sync_healthy()
            if self._started:
                return
            await self.store.start()
            await self._sync_service.start()
            self._started = True

    async def recover(self) -> list[Operation]:
        """Finalize nonterminal operations left by an earlier process."""
        async with self._recovery_lock:
            self._ensure_open()
            await self.start()
            recovered: list[Operation] = []
            for operation_id in await self.store.unfinished_operation_ids():
                async with self._lock:
                    operation = self._operations.get(operation_id)
                if operation is None:
                    record = await self.store.read_operation(operation_id)
                    if record is None:
                        continue
                    operation = self._operation_from_record(record)
                elif not operation._restored or operation._task is not None:
                    continue
                if operation.is_finished:
                    continue
                await operation._recover_interrupted_tasks()
                await operation._recover_interrupted()
                await self._remember_operation(operation)
                recovered.append(operation)
            return recovered

    async def create(self, name: str) -> Operation:
        if not isinstance(name, str) or not name.strip():
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_NAME,
                "Operation name cannot be empty.",
            )
        self._ensure_open()
        await self.start()
        operation_id = uuid4()
        operation = Operation(
            operation_id,
            name,
            self._create_stream(operation_id),
            self.store,
            self.event_type,
            self.error_code,
            self.settings,
            on_finished=self._operation_finished,
        )
        async with self._lock:
            self._ensure_open()
            self._operations[operation_id] = operation
        try:
            await operation._persist_queued()
        except BaseException:
            async with self._lock:
                self._operations.pop(operation_id, None)
            raise
        return operation

    async def get(self, operation_id: UUID | str) -> Operation:
        self._ensure_open()
        await self.start()
        parsed_id = self._parse_operation_id(operation_id)
        async with self._lock:
            operation = self._operations.get(parsed_id)
            if operation is not None:
                self._operations.move_to_end(parsed_id)
                return operation
        record = await self.store.read_operation(parsed_id)
        if record is None:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_NOT_FOUND,
                f"Operation '{parsed_id}' was not found.",
            )
        return await self._remember_operation(self._operation_from_record(record))

    async def get_record(self, operation_id: UUID | str) -> OperationRecord:
        self._ensure_open()
        await self.start()
        parsed_id = self._parse_operation_id(operation_id)
        record = await self.store.read_operation(parsed_id)
        if record is None:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_NOT_FOUND,
                f"Operation '{parsed_id}' was not found.",
            )
        return record

    async def wait(self, operation_id: UUID | str) -> Operation:
        return await (await self.get(operation_id)).wait()

    async def cancel(self, operation_id: UUID | str) -> Operation:
        return await (await self.get(operation_id)).cancel()

    async def events(
        self,
        operation_id: UUID | str,
        *,
        after_event_id: int = 0,
    ) -> AsyncIterator[Event]:
        operation = await self.get(operation_id)
        async for event in operation.events(after_event_id=after_event_id):
            yield event

    async def read_events(
        self,
        operation_id: UUID | str,
        *,
        after_event_id: int = 0,
        limit: int | None = None,
    ) -> list[Event]:
        return await (await self.get(operation_id)).read_events(
            after_event_id,
            limit=limit,
        )

    async def list_operations(
        self,
        *,
        status: LifecycleStatus | str | None = None,
        limit: int | None = None,
        after_operation_id: UUID | str | None = None,
    ) -> list[OperationRecord]:
        self._ensure_open()
        await self.start()
        selected_limit = self.settings.operation_page_size if limit is None else limit
        self._validate_page_size(selected_limit)
        parsed_status = self._parse_status(status)
        cursor = (
            self._parse_operation_id(after_operation_id)
            if after_operation_id is not None
            else None
        )
        if cursor is not None and await self.store.read_operation(cursor) is None:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_NOT_FOUND,
                f"Operation '{cursor}' was not found.",
            )
        return await self.store.list_operations(
            status=parsed_status,
            after_operation_id=cursor,
            limit=selected_limit,
        )

    async def list_retryable_tasks(
        self,
        *,
        limit: int | None = None,
        after_task_id: UUID | str | None = None,
    ) -> list[OperationTaskRecord]:
        self._ensure_open()
        await self.start()
        selected_limit = self.settings.operation_page_size if limit is None else limit
        self._validate_page_size(selected_limit)
        cursor = (
            self._parse_task_id(after_task_id)
            if after_task_id is not None
            else None
        )
        if cursor is not None and await self.store.read_task(cursor) is None:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_TASK_NOT_FOUND,
                f"Operation task '{cursor}' was not found.",
            )
        results: list[OperationTaskRecord] = []
        while len(results) < selected_limit:
            records = await self.store.list_tasks(
                operation_id=None,
                status=LifecycleStatus.FAILED,
                after_task_id=cursor,
                limit=selected_limit,
                retryable_only=True,
            )
            if not records:
                break
            for record in records:
                if record.can_retry:
                    results.append(record)
                    if len(results) == selected_limit:
                        break
            cursor = records[-1].task_id
            if len(records) < selected_limit:
                break
        return results

    async def stored_operation_ids(self) -> list[UUID]:
        self._ensure_open()
        await self.start()
        return await self.store.operation_ids()

    async def unfinished_operation_ids(self) -> list[UUID]:
        self._ensure_open()
        await self.start()
        return await self.store.unfinished_operation_ids()

    async def sync_dirty(self) -> list[UUID]:
        self._ensure_open()
        self._ensure_sync_healthy()
        async with self._lock:
            dirty = [
                operation_id
                for operation_id, operation in self._operations.items()
                if operation._stream.is_dirty
            ]
        if not self.store.is_dirty:
            return []
        try:
            await self.store.checkpoint()
        except OperationError as exc:
            await self._record_sync_failure(exc)
            raise
        return dirty

    async def cancel_active(self) -> None:
        async with self._lock:
            operations = [
                operation
                for operation in self._operations.values()
                if not operation.is_finished
            ]
        await asyncio.gather(
            *(operation.cancel() for operation in operations),
            return_exceptions=True,
        )
        await asyncio.gather(
            *(operation.wait() for operation in operations),
            return_exceptions=True,
        )

    async def close(self) -> None:
        async with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
        await self.cancel_active()
        await self._sync_service.close()

        close_error: BaseException | None = None
        if self.store.is_dirty:
            try:
                await self.store.checkpoint()
            except BaseException as exc:
                close_error = exc
        async with self._lock:
            streams = [operation._stream for operation in self._operations.values()]
        stream_results = await asyncio.gather(
            *(stream.close() for stream in streams),
            return_exceptions=True,
        )
        await self.store.close()
        if close_error is not None:
            raise close_error
        for result in stream_results:
            if isinstance(result, BaseException):
                raise result

    def _operation_from_record(self, record: OperationRecord) -> Operation:
        return Operation.from_record(
            record,
            self._create_stream(record.operation_id),
            self.store,
            self.event_type,
            self.error_code,
            self.settings,
            on_finished=self._operation_finished,
        )

    def _create_stream(self, operation_id: UUID) -> EventStream:
        return EventStream(
            operation_id,
            self.store,
            error_code=self.error_code,
            replay_page_size=self.settings.event_replay_page_size,
            health_check=self._ensure_sync_healthy,
            sync_failure=self._record_sync_failure,
        )

    async def _remember_operation(self, operation: Operation) -> Operation:
        async with self._lock:
            current = self._operations.get(operation.operation_id)
            if current is not None:
                self._operations.move_to_end(operation.operation_id)
                return current
            self._operations[operation.operation_id] = operation
            self._operations.move_to_end(operation.operation_id)
            self._evict_finished_operations()
            return operation

    def _operation_finished(self, operation: Operation) -> None:
        if self._operations.get(operation.operation_id) is not operation:
            return
        self._operations.move_to_end(operation.operation_id)
        self._evict_finished_operations()

    def _evict_finished_operations(self) -> None:
        finished = [
            operation_id
            for operation_id, operation in self._operations.items()
            if operation.is_finished
        ]
        excess = len(finished) - self.settings.finished_operation_cache_size
        for operation_id in finished[: max(excess, 0)]:
            self._operations.pop(operation_id, None)

    async def _delete_finished(self, operation_id: UUID) -> bool:
        async with self._lock:
            operation = self._operations.get(operation_id)
            if operation is not None:
                if not operation.is_finished:
                    return False
                if not await operation._stream._reserve_cleanup():
                    return False
            try:
                deleted = await self.store.delete_operation(operation_id)
            except BaseException:
                if operation is not None:
                    await operation._stream._release_cleanup()
                raise
            if not deleted:
                if operation is not None:
                    await operation._stream._release_cleanup()
                return False
            if operation is not None and self._operations.get(operation_id) is operation:
                self._operations.pop(operation_id, None)
            return True

    async def _record_sync_failure(
        self,
        error: OperationError,
        source_operation_id: UUID | None = None,
    ) -> None:
        if self._sync_error is None:
            self._sync_error = error
        active_error = self._sync_error or error
        async with self._lock:
            streams = [
                operation._stream
                for operation_id, operation in self._operations.items()
                if operation_id != source_operation_id
            ]
        await asyncio.gather(
            *(stream.mark_sync_failed(active_error) for stream in streams)
        )

    def _ensure_sync_healthy(self) -> None:
        if self._sync_error is not None:
            raise self._sync_error

    def _parse_operation_id(self, operation_id: UUID | str) -> UUID:
        try:
            return operation_id if isinstance(operation_id, UUID) else UUID(operation_id)
        except (TypeError, ValueError) as exc:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_ID,
                "operation_id must be a valid UUID.",
            ) from exc

    def _parse_task_id(self, task_id: UUID | str) -> UUID:
        try:
            return task_id if isinstance(task_id, UUID) else UUID(task_id)
        except (TypeError, ValueError) as exc:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_TASK_NOT_FOUND,
                "task_id must be a valid UUID.",
            ) from exc

    def _parse_status(
        self,
        status: LifecycleStatus | str | None,
    ) -> LifecycleStatus | None:
        if status is None or isinstance(status, LifecycleStatus):
            return status
        try:
            return LifecycleStatus(status)
        except (TypeError, ValueError) as exc:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_STATUS,
                f"Unknown operation status '{status}'.",
            ) from exc

    def _validate_page_size(self, limit: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_PAGE_SIZE,
                "Operation page size must be a positive integer.",
            )

    def _ensure_open(self) -> None:
        if self._closed:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_MANAGER_CLOSED,
                "Operation manager is closed.",
            )


class OperationSyncService:
    """Periodically checkpoint one manager's dirty operation store."""

    def __init__(self, manager: OperationManager, interval: float) -> None:
        self._manager = manager
        self.interval = interval
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    async def start(self) -> None:
        if self._closed:
            raise OperationError(
                self._manager.error_code.OPERATION_RUNTIME_MANAGER_CLOSED,
                "Operation synchronization service is closed.",
            )
        if self._task is None:
            self._task = asyncio.create_task(
                self._run(),
                name="operationcore-sync",
            )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except TimeoutError:
                pass
            if self._stop.is_set():
                return
            try:
                await self._manager.sync_dirty()
            except OperationError:
                return


class OperationCleanupService:
    """Delete bounded batches of finished operations after retention expires."""

    def __init__(
        self,
        manager: OperationManager,
        retention: timedelta,
        *,
        batch_size: int,
    ) -> None:
        if retention < timedelta(0):
            raise OperationError(
                manager.error_code.OPERATION_RUNTIME_INVALID_RETENTION,
                "retention cannot be negative.",
            )
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or batch_size <= 0
        ):
            raise OperationError(
                manager.error_code.OPERATION_RUNTIME_INVALID_CLEANUP_BATCH_SIZE,
                "Cleanup batch size must be a positive integer.",
            )
        self._manager = manager
        self.retention = retention
        self.batch_size = batch_size
        self._lock = asyncio.Lock()

    async def run_once(
        self,
        now: datetime | None = None,
    ) -> OperationCleanupResult:
        async with self._lock:
            current_time = now or datetime.now(timezone.utc)
            if current_time.tzinfo is None:
                current_time = current_time.replace(tzinfo=timezone.utc)
            cutoff = current_time.astimezone(timezone.utc) - self.retention
            operation_ids = await self._manager.store.expired_operation_ids(
                cutoff,
                limit=self.batch_size,
            )

            deleted: list[UUID] = []
            skipped: list[UUID] = []
            failures: dict[UUID, dict[str, Any]] = {}
            for operation_id in operation_ids:
                try:
                    was_deleted = await self._manager._delete_finished(operation_id)
                except Exception as exc:
                    failures[operation_id] = error_payload(
                        exc,
                        self._manager.error_code,
                    )
                    continue
                (deleted if was_deleted else skipped).append(operation_id)

            return OperationCleanupResult(
                deleted_operation_ids=tuple(deleted),
                skipped_operation_ids=tuple(skipped),
                failures=failures,
            )
