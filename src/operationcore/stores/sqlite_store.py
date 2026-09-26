"""SQLite implementation of the operation store contract."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any, BinaryIO
from uuid import UUID

from ..errors import ErrorCode, OperationError
from ..events import Event
from ..records import OperationRecord, OperationTaskRecord
from ..retry import RetryPolicy
from ..state import EventStreamState, LifecycleStatus
from .transitions import LifecycleTransition


_SCHEMA_VERSION = 2
class SQLiteOperationStore:
    """Persist operation event streams and lifecycle projections in SQLite."""

    def __init__(
        self,
        path: str | Path,
        *,
        error_code: type[ErrorCode] = ErrorCode,
    ) -> None:
        self.path = Path(path)
        self.error_code = error_code
        self.ownership_path = self.path.with_suffix(f"{self.path.suffix}.lock")
        self._connection: sqlite3.Connection | None = None
        self._ownership_handle: BinaryIO | None = None
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="operationcore-store",
        )
        self._lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        self._write_generation = 0
        self._synced_generation = 0
        self._closed = False

    @property
    def is_dirty(self) -> bool:
        """Whether committed writes have not reached the latest checkpoint."""
        return self._write_generation > self._synced_generation

    @property
    def synced_generation(self) -> int:
        """The latest write generation included in a checkpoint."""
        return self._synced_generation

    async def start(self) -> None:
        """Open the database lazily and initialize its schema."""
        if self._closed:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_EVENT_STREAM_CLOSED,
                "Operation store is closed.",
            )
        if self._connection is not None:
            return

        async with self._start_lock:
            if self._connection is not None:
                return
            try:
                await self._run_in_store_thread(self._open_connection)
            except OperationError:
                raise
            except Exception as exc:
                raise OperationError(
                    self.error_code.OPERATION_RUNTIME_DATABASE_FAILED,
                    "The operation store could not be opened.",
                ) from exc

    async def append_event(
        self,
        event: Event,
        transition: LifecycleTransition | None = None,
    ) -> int:
        """Atomically append an event and apply its lifecycle transition."""
        await self.start()
        async with self._lock:
            try:
                await self._run_in_store_thread(
                    self._append_event,
                    event,
                    transition,
                )
            except OperationError:
                raise
            except Exception as exc:
                raise OperationError(
                    self.error_code.OPERATION_RUNTIME_DATABASE_FAILED,
                    "The event could not be persisted.",
                    details={"operation_id": str(event.operation_id)},
                ) from exc
            self._write_generation += 1
            return self._write_generation

    async def register_task(
        self,
        record: OperationTaskRecord,
        event: Event,
        transition: LifecycleTransition,
    ) -> int:
        """Atomically persist a task and its queued lifecycle event."""
        if record.status != LifecycleStatus.QUEUED:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_STATUS,
                "A newly registered task must be queued.",
                details={"task_id": str(record.task_id)},
            )
        if (
            transition.task_id != record.task_id
            or transition.task_status != LifecycleStatus.QUEUED
        ):
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_STATUS,
                "Task registration requires its queued lifecycle transition.",
                details={"task_id": str(record.task_id)},
            )

        await self.start()
        async with self._lock:
            try:
                await self._run_in_store_thread(
                    self._register_task,
                    record,
                    event,
                    transition,
                )
            except OperationError:
                raise
            except Exception as exc:
                raise self._database_error(
                    "The operation task could not be persisted."
                ) from exc
            self._write_generation += 1
            return self._write_generation

    async def read_operation(
        self,
        operation_id: UUID,
    ) -> OperationRecord | None:
        """Read one durable operation projection."""
        await self.start()
        async with self._lock:
            try:
                row = await self._run_in_store_thread(
                    self._read_operation,
                    str(operation_id),
                )
            except Exception as exc:
                raise self._database_error("The operation could not be read.") from exc
        if row is None:
            return None
        return self._operation_from_row(row, operation_id=operation_id)

    async def list_operations(
        self,
        *,
        status: LifecycleStatus | None,
        after_operation_id: UUID | None,
        limit: int,
    ) -> list[OperationRecord]:
        """List durable operations in newest-first order."""
        self._validate_limit(limit)
        await self.start()
        async with self._lock:
            try:
                rows = await self._run_in_store_thread(
                    self._list_operations,
                    status.value if status is not None else None,
                    str(after_operation_id) if after_operation_id is not None else None,
                    limit,
                )
            except Exception as exc:
                raise self._database_error("Operations could not be listed.") from exc
        return [self._operation_from_row(row) for row in rows]

    async def read_task(
        self,
        task_id: UUID,
    ) -> OperationTaskRecord | None:
        """Read one durable task projection."""
        await self.start()
        async with self._lock:
            try:
                row = await self._run_in_store_thread(
                    self._read_task,
                    str(task_id),
                )
            except Exception as exc:
                raise self._database_error(
                    "The operation task could not be read."
                ) from exc
        if row is None:
            return None
        return self._task_from_row(row, task_id=task_id)

    async def read_task_terminal_event(
        self,
        operation_id: UUID,
        task_id: UUID,
    ) -> Event | None:
        """Read the terminal lifecycle event for a task."""
        await self.start()
        async with self._lock:
            try:
                row = await self._run_in_store_thread(
                    self._read_task_terminal_event,
                    str(operation_id),
                    str(task_id),
                )
            except Exception as exc:
                raise self._database_error(
                    "The operation task terminal event could not be read."
                ) from exc
        if row is None:
            return None
        try:
            return self._event_from_row(row)
        except Exception as exc:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "The operation store contains an invalid task terminal event.",
                details={
                    "operation_id": str(operation_id),
                    "task_id": str(task_id),
                },
            ) from exc

    async def list_tasks(
        self,
        *,
        operation_id: UUID | None,
        status: LifecycleStatus | None,
        after_task_id: UUID | None,
        limit: int,
        retryable_only: bool = False,
    ) -> list[OperationTaskRecord]:
        """List durable tasks in newest-first order."""
        self._validate_limit(limit)
        await self.start()
        async with self._lock:
            try:
                rows = await self._run_in_store_thread(
                    self._list_tasks,
                    str(operation_id) if operation_id is not None else None,
                    status.value if status is not None else None,
                    str(after_task_id) if after_task_id is not None else None,
                    limit,
                    retryable_only,
                )
            except Exception as exc:
                raise self._database_error("Operation tasks could not be listed.") from exc
        return [self._task_from_row(row) for row in rows]

    async def read_retry(
        self,
        task_id: UUID,
    ) -> OperationTaskRecord | None:
        """Read the task that directly retries the supplied task."""
        await self.start()
        async with self._lock:
            try:
                row = await self._run_in_store_thread(
                    self._read_retry,
                    str(task_id),
                )
            except Exception as exc:
                raise self._database_error("The operation retry could not be read.") from exc
        if row is None:
            return None
        return self._task_from_row(row)

    async def unfinished_tasks(
        self,
        operation_id: UUID,
    ) -> list[OperationTaskRecord]:
        """Read nonterminal tasks belonging to an operation."""
        await self.start()
        async with self._lock:
            try:
                rows = await self._run_in_store_thread(
                    self._unfinished_tasks,
                    str(operation_id),
                )
            except Exception as exc:
                raise self._database_error(
                    "Unfinished operation tasks could not be read."
                ) from exc
        return [self._task_from_row(row) for row in rows]

    async def read_events(
        self,
        operation_id: UUID,
        after_event_id: int,
        *,
        limit: int | None = None,
    ) -> list[Event]:
        """Read retained events after a stream-local event identifier."""
        if after_event_id < 0:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_EVENT_CURSOR,
                "after_event_id cannot be negative.",
            )
        if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0
        ):
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_EVENT_PAGE_SIZE,
                "Event page size must be a positive integer.",
            )

        await self.start()
        async with self._lock:
            try:
                rows = await self._run_in_store_thread(
                    self._read_events,
                    str(operation_id),
                    after_event_id,
                    limit,
                )
            except Exception as exc:
                raise self._database_error("Events could not be read.") from exc

        try:
            return [self._event_from_row(row) for row in rows]
        except Exception as exc:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "The operation store contains an invalid event record.",
                details={"operation_id": str(operation_id)},
            ) from exc

    async def read_event_stream_state(
        self,
        operation_id: UUID,
    ) -> EventStreamState:
        """Read the durable position and completion state of an event stream."""
        await self.start()
        async with self._lock:
            try:
                row = await self._run_in_store_thread(
                    self._read_event_stream_state,
                    str(operation_id),
                )
            except Exception as exc:
                raise self._database_error(
                    "Event stream state could not be read."
                ) from exc

        if row is None:
            return EventStreamState(last_event_id=0, is_finished=False)
        try:
            return EventStreamState(
                last_event_id=int(row["last_event_id"]),
                is_finished=bool(row["is_finished"]),
                finished_at=(
                    datetime.fromisoformat(str(row["finished_at"]))
                    if row["finished_at"] is not None
                    else None
                ),
            )
        except Exception as exc:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "The operation store contains invalid event stream state.",
                details={"operation_id": str(operation_id)},
            ) from exc

    async def operation_ids(self) -> list[UUID]:
        """List all stored operation identifiers."""
        await self.start()
        async with self._lock:
            try:
                rows = await self._run_in_store_thread(self._operation_ids)
            except Exception as exc:
                raise self._database_error(
                    "Operation identifiers could not be read."
                ) from exc
        try:
            return [UUID(str(row["operation_id"])) for row in rows]
        except Exception as exc:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "The operation store contains an invalid operation identifier.",
            ) from exc

    async def unfinished_operation_ids(self) -> list[UUID]:
        """List identifiers for nonterminal operations."""
        await self.start()
        async with self._lock:
            try:
                rows = await self._run_in_store_thread(
                    self._unfinished_operation_ids
                )
            except Exception as exc:
                raise self._database_error(
                    "Unfinished operation identifiers could not be read."
                ) from exc
        try:
            return [UUID(str(row["operation_id"])) for row in rows]
        except Exception as exc:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "The operation store contains an invalid operation identifier.",
            ) from exc

    async def expired_operation_ids(
        self,
        cutoff: datetime,
        *,
        limit: int,
    ) -> list[UUID]:
        """List terminal operation identifiers older than a cutoff."""
        self._validate_limit(limit)
        await self.start()
        async with self._lock:
            try:
                rows = await self._run_in_store_thread(
                    self._expired_operation_ids,
                    cutoff.isoformat(),
                    limit,
                )
            except Exception as exc:
                raise self._database_error(
                    "Expired operation identifiers could not be read."
                ) from exc
        try:
            return [UUID(str(row["operation_id"])) for row in rows]
        except Exception as exc:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "The operation store contains an invalid operation identifier.",
            ) from exc

    async def delete_operation(self, operation_id: UUID) -> bool:
        """Delete an operation and its cascading task and event rows."""
        await self.start()
        async with self._lock:
            try:
                changed = await self._run_in_store_thread(
                    self._delete_operation,
                    str(operation_id),
                )
            except Exception as exc:
                raise self._database_error(
                    "The operation could not be deleted."
                ) from exc
            if changed:
                self._write_generation += 1
            return changed

    async def checkpoint(self) -> bool:
        """Checkpoint committed WAL writes into the database file."""
        await self.start()
        async with self._lock:
            if not self.is_dirty:
                return False
            try:
                await self._run_in_store_thread(self._checkpoint)
            except Exception as exc:
                raise OperationError(
                    self.error_code.OPERATION_RUNTIME_SYNC_FAILED,
                    "The operation store could not be synchronized to durable storage.",
                ) from exc
            self._synced_generation = self._write_generation
            return True

    async def close(self) -> None:
        """Close the database and release process ownership."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                await self._run_in_store_thread(self._close_connection)
            finally:
                self._executor.shutdown(wait=False)

    async def _run_in_store_thread(self, function: Any, *args: Any) -> Any:
        """Run one database call without releasing ownership before it exits."""
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(self._executor, partial(function, *args))
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            await asyncio.shield(future)
            raise

    def _open_connection(self) -> None:
        if self._connection is not None:
            return
        self._acquire_ownership()
        try:
            self._connection = self._open()
        except BaseException:
            self._release_ownership()
            raise

    def _close_connection(self) -> None:
        connection = self._connection
        self._connection = None
        try:
            if connection is not None:
                connection.close()
        finally:
            self._release_ownership()

    def _acquire_ownership(self) -> None:
        if self._ownership_handle is not None:
            return

        self.ownership_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.ownership_path.open("a+b")
        try:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            self._lock_ownership_file(handle)
        except OSError as exc:
            handle.close()
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_IN_USE,
                "The operation store is already owned by another process.",
            ) from exc
        except BaseException:
            handle.close()
            raise
        self._ownership_handle = handle

    def _release_ownership(self) -> None:
        handle = self._ownership_handle
        self._ownership_handle = None
        if handle is None:
            return
        try:
            self._unlock_ownership_file(handle)
        finally:
            handle.close()

    @staticmethod
    def _lock_ownership_file(handle: BinaryIO) -> None:
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return

        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def _unlock_ownership_file(handle: BinaryIO) -> None:
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            return

        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _open(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row

        try:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version < 0 or version > _SCHEMA_VERSION:
                raise OperationError(
                    self.error_code.OPERATION_RUNTIME_UNSUPPORTED_DATABASE_VERSION,
                    "The operation store uses an unsupported schema version.",
                    details={"schema_version": version},
                )
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA foreign_keys = ON")
            journal_mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()
            if journal_mode is None or str(journal_mode[0]).lower() != "wal":
                raise sqlite3.OperationalError("WAL mode could not be enabled")
            connection.execute("PRAGMA synchronous = NORMAL")
            if version == 1:
                event_columns = {
                    str(row["name"])
                    for row in connection.execute(
                        "PRAGMA table_info(events)"
                    ).fetchall()
                }
                if "operation_status" not in event_columns:
                    connection.execute(
                        "ALTER TABLE events ADD COLUMN operation_status TEXT"
                    )
                if "task_status" not in event_columns:
                    connection.execute(
                        "ALTER TABLE events ADD COLUMN task_status TEXT"
                    )
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS operations (
                    operation_id TEXT PRIMARY KEY NOT NULL,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_event_id INTEGER NOT NULL CHECK (last_event_id >= 0),
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    is_finished INTEGER NOT NULL CHECK (is_finished IN (0, 1)),
                    error_json TEXT
                );

                CREATE TABLE IF NOT EXISTS events (
                    operation_id TEXT NOT NULL,
                    event_id INTEGER NOT NULL CHECK (event_id > 0),
                    type TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    task_id TEXT,
                    task_name TEXT,
                    operation_status TEXT,
                    task_status TEXT,
                    is_final INTEGER NOT NULL CHECK (is_final IN (0, 1)),
                    data_json TEXT NOT NULL,
                    PRIMARY KEY (operation_id, event_id),
                    FOREIGN KEY (operation_id)
                        REFERENCES operations(operation_id)
                        ON DELETE CASCADE
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY NOT NULL,
                    operation_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    is_root INTEGER NOT NULL CHECK (is_root IN (0, 1)),
                    status TEXT NOT NULL,
                    max_attempts INTEGER NOT NULL DEFAULT 1
                        CHECK (max_attempts > 0),
                    retryable_error_codes_json TEXT NOT NULL DEFAULT '[]',
                    retry_input_json TEXT,
                    attempt INTEGER NOT NULL DEFAULT 1 CHECK (attempt > 0),
                    retry_of_operation_id TEXT,
                    retry_of_task_id TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    error_json TEXT,
                    FOREIGN KEY (operation_id)
                        REFERENCES operations(operation_id)
                        ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS operations_finished_at
                    ON operations(finished_at)
                    WHERE finished_at IS NOT NULL;

                CREATE INDEX IF NOT EXISTS tasks_operation_id
                    ON tasks(operation_id, created_at);

                CREATE INDEX IF NOT EXISTS tasks_retry_of_task_id
                    ON tasks(retry_of_task_id)
                    WHERE retry_of_task_id IS NOT NULL;

                CREATE UNIQUE INDEX IF NOT EXISTS tasks_single_direct_retry
                    ON tasks(retry_of_task_id)
                    WHERE retry_of_task_id IS NOT NULL;
                """
            )
            connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            return connection
        except Exception:
            connection.close()
            raise

    def _register_task(
        self,
        record: OperationTaskRecord,
        event: Event,
        transition: LifecycleTransition,
    ) -> None:
        connection = self._require_connection()
        retry_input_json = (
            json.dumps(
                record.retry_input,
                separators=(",", ":"),
                allow_nan=False,
            )
            if record.retry_input is not None
            else None
        )
        retryable_error_codes_json = json.dumps(
            sorted(record.retryable_error_codes),
            separators=(",", ":"),
        )
        error_json = self._serialize_error(record.error)
        retry_of_task_id = (
            str(record.retry_of_task_id)
            if record.retry_of_task_id is not None
            else None
        )

        try:
            connection.execute("BEGIN IMMEDIATE")
            if retry_of_task_id is not None:
                existing = self._read_retry(retry_of_task_id)
                if existing is not None:
                    raise OperationError(
                        self.error_code.OPERATION_RUNTIME_TASK_ALREADY_RETRIED,
                        f"Operation task '{retry_of_task_id}' already has a retry.",
                        details={
                            "retry_of_task_id": retry_of_task_id,
                            "existing_task_id": str(existing["task_id"]),
                            "existing_operation_id": str(existing["operation_id"]),
                            "existing_status": str(existing["status"]),
                        },
                    )

            connection.execute(
                """
                INSERT INTO tasks (
                    task_id,
                    operation_id,
                    name,
                    is_root,
                    status,
                    max_attempts,
                    retryable_error_codes_json,
                    retry_input_json,
                    attempt,
                    retry_of_operation_id,
                    retry_of_task_id,
                    created_at,
                    started_at,
                    finished_at,
                    error_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(record.task_id),
                    str(record.operation_id),
                    record.name,
                    int(record.is_root),
                    record.status.value,
                    record.max_attempts,
                    retryable_error_codes_json,
                    retry_input_json,
                    record.attempt,
                    (
                        str(record.retry_of_operation_id)
                        if record.retry_of_operation_id is not None
                        else None
                    ),
                    retry_of_task_id,
                    record.created_at.isoformat(),
                    (
                        record.started_at.isoformat()
                        if record.started_at is not None
                        else None
                    ),
                    (
                        record.finished_at.isoformat()
                        if record.finished_at is not None
                        else None
                    ),
                    error_json,
                ),
            )
            self._append_event_rows(connection, event, transition)
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise

    def _read_operation(self, operation_id: str) -> sqlite3.Row | None:
        return self._require_connection().execute(
            """
            SELECT
                operation_id,
                name,
                status,
                last_event_id,
                created_at,
                started_at,
                finished_at,
                error_json
            FROM operations
            WHERE operation_id = ?
            """,
            (operation_id,),
        ).fetchone()

    def _list_operations(
        self,
        status: str | None,
        after_operation_id: str | None,
        limit: int,
    ) -> list[sqlite3.Row]:
        connection = self._require_connection()
        conditions: list[str] = []
        parameters: list[Any] = []
        if status is not None:
            conditions.append("status = ?")
            parameters.append(status)
        if after_operation_id is not None:
            cursor = connection.execute(
                """
                SELECT created_at, operation_id
                FROM operations
                WHERE operation_id = ?
                """,
                (after_operation_id,),
            ).fetchone()
            if cursor is None:
                return []
            conditions.append(
                "(created_at < ? OR (created_at = ? AND operation_id < ?))"
            )
            parameters.extend(
                [
                    cursor["created_at"],
                    cursor["created_at"],
                    cursor["operation_id"],
                ]
            )

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        parameters.append(limit)
        return connection.execute(
            f"""
            SELECT
                operation_id,
                name,
                status,
                last_event_id,
                created_at,
                started_at,
                finished_at,
                error_json
            FROM operations
            {where}
            ORDER BY created_at DESC, operation_id DESC
            LIMIT ?
            """,
            parameters,
        ).fetchall()

    def _read_task(self, task_id: str) -> sqlite3.Row | None:
        return self._require_connection().execute(
            """
            SELECT
                task_id,
                operation_id,
                name,
                is_root,
                status,
                max_attempts,
                retryable_error_codes_json,
                retry_input_json,
                attempt,
                retry_of_operation_id,
                retry_of_task_id,
                created_at,
                started_at,
                finished_at,
                error_json
            FROM tasks
            WHERE task_id = ?
            """,
            (task_id,),
        ).fetchone()

    def _read_task_terminal_event(
        self,
        operation_id: str,
        task_id: str,
    ) -> sqlite3.Row | None:
        return self._require_connection().execute(
            """
            SELECT
                operation_id,
                event_id,
                type,
                timestamp,
                task_id,
                task_name,
                is_final,
                data_json
            FROM events
            WHERE operation_id = ?
              AND task_id = ?
              AND task_status IN ('completed', 'failed', 'cancelled')
            ORDER BY event_id DESC
            LIMIT 1
            """,
            (operation_id, task_id),
        ).fetchone()

    def _list_tasks(
        self,
        operation_id: str | None,
        status: str | None,
        after_task_id: str | None,
        limit: int,
        retryable_only: bool,
    ) -> list[sqlite3.Row]:
        connection = self._require_connection()
        conditions: list[str] = []
        parameters: list[Any] = []
        if operation_id is not None:
            conditions.append("tasks.operation_id = ?")
            parameters.append(operation_id)
        if status is not None:
            conditions.append("tasks.status = ?")
            parameters.append(status)
        if retryable_only:
            conditions.extend(
                [
                    "tasks.status = 'failed'",
                    "tasks.max_attempts > tasks.attempt",
                    "tasks.retryable_error_codes_json <> '[]'",
                    "tasks.retry_input_json IS NOT NULL",
                    "NOT EXISTS ("
                    "SELECT 1 FROM tasks AS retry "
                    "WHERE retry.retry_of_task_id = tasks.task_id)",
                ]
            )
        if after_task_id is not None:
            cursor = connection.execute(
                """
                SELECT created_at, task_id
                FROM tasks
                WHERE task_id = ?
                """,
                (after_task_id,),
            ).fetchone()
            if cursor is None:
                return []
            conditions.append(
                "(tasks.created_at < ? OR "
                "(tasks.created_at = ? AND tasks.task_id < ?))"
            )
            parameters.extend(
                [
                    cursor["created_at"],
                    cursor["created_at"],
                    cursor["task_id"],
                ]
            )

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        parameters.append(limit)
        return connection.execute(
            f"""
            SELECT
                task_id,
                operation_id,
                name,
                is_root,
                status,
                max_attempts,
                retryable_error_codes_json,
                retry_input_json,
                attempt,
                retry_of_operation_id,
                retry_of_task_id,
                created_at,
                started_at,
                finished_at,
                error_json
            FROM tasks AS tasks
            {where}
            ORDER BY tasks.created_at DESC, tasks.task_id DESC
            LIMIT ?
            """,
            parameters,
        ).fetchall()

    def _read_retry(self, task_id: str) -> sqlite3.Row | None:
        return self._require_connection().execute(
            """
            SELECT
                task_id,
                operation_id,
                name,
                is_root,
                status,
                max_attempts,
                retryable_error_codes_json,
                retry_input_json,
                attempt,
                retry_of_operation_id,
                retry_of_task_id,
                created_at,
                started_at,
                finished_at,
                error_json
            FROM tasks
            WHERE retry_of_task_id = ?
            """,
            (task_id,),
        ).fetchone()

    def _unfinished_tasks(self, operation_id: str) -> list[sqlite3.Row]:
        return self._require_connection().execute(
            """
            SELECT
                task_id,
                operation_id,
                name,
                is_root,
                status,
                max_attempts,
                retryable_error_codes_json,
                retry_input_json,
                attempt,
                retry_of_operation_id,
                retry_of_task_id,
                created_at,
                started_at,
                finished_at,
                error_json
            FROM tasks
            WHERE operation_id = ?
              AND status NOT IN ('completed', 'failed', 'cancelled')
            ORDER BY created_at
            """,
            (operation_id,),
        ).fetchall()

    def _operation_ids(self) -> list[sqlite3.Row]:
        return self._require_connection().execute(
            """
            SELECT operation_id
            FROM operations
            ORDER BY created_at, operation_id
            """
        ).fetchall()

    def _unfinished_operation_ids(self) -> list[sqlite3.Row]:
        return self._require_connection().execute(
            """
            SELECT operation_id
            FROM operations
            WHERE is_finished = 0
            ORDER BY created_at, operation_id
            """
        ).fetchall()

    def _expired_operation_ids(
        self,
        cutoff: str,
        limit: int,
    ) -> list[sqlite3.Row]:
        return self._require_connection().execute(
            """
            SELECT operation_id
            FROM operations
            WHERE is_finished = 1
              AND finished_at < ?
            ORDER BY finished_at, operation_id
            LIMIT ?
            """,
            (cutoff, limit),
        ).fetchall()

    def _delete_operation(self, operation_id: str) -> bool:
        connection = self._require_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                "DELETE FROM operations WHERE operation_id = ?",
                (operation_id,),
            )
            connection.commit()
            return cursor.rowcount == 1
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise

    def _append_event(
        self,
        event: Event,
        transition: LifecycleTransition | None,
    ) -> None:
        connection = self._require_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._append_event_rows(connection, event, transition)
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise

    def _append_event_rows(
        self,
        connection: sqlite3.Connection,
        event: Event,
        transition: LifecycleTransition | None,
    ) -> None:
        if event.operation_id is None or event.event_id is None:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "A persisted event requires operation_id and event_id.",
            )

        operation_id = str(event.operation_id)
        serialized = event.model_dump(mode="json")
        timestamp = str(serialized["timestamp"])
        operation_status = (
            transition.operation_status if transition is not None else None
        )
        operation_is_terminal = (
            operation_status is not None and operation_status.is_terminal
        )
        if event.is_final != operation_is_terminal:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "A final event and terminal operation transition must occur together.",
                details={"operation_id": operation_id},
            )
        if (
            transition is not None
            and transition.task_id is not None
            and event.task_id != transition.task_id
        ):
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "A task transition must target the event's task.",
                details={
                    "operation_id": operation_id,
                    "event_task_id": (
                        str(event.task_id) if event.task_id is not None else None
                    ),
                    "transition_task_id": str(transition.task_id),
                },
            )

        operation_error_json = self._serialize_error(
            transition.operation_error if transition is not None else None
        )
        task_error_json = self._serialize_error(
            transition.task_error if transition is not None else None
        )
        existing = connection.execute(
                """
                SELECT
                    name,
                    status,
                    last_event_id,
                    started_at,
                    finished_at,
                    is_finished,
                    error_json
                FROM operations
                WHERE operation_id = ?
                """,
                (operation_id,),
            ).fetchone()

        if existing is None:
            self._insert_initial_operation(
                connection,
                event,
                transition,
                timestamp,
                operation_error_json,
            )
        else:
            self._update_operation(
                connection,
                event,
                transition,
                existing,
                timestamp,
                operation_error_json,
            )

        if transition is not None and transition.task_status is not None:
            self._update_task(
                connection,
                operation_id,
                transition,
                timestamp,
                task_error_json,
            )

        connection.execute(
            """
                INSERT INTO events (
                    operation_id,
                    event_id,
                    type,
                    timestamp,
                    task_id,
                    task_name,
                    operation_status,
                    task_status,
                    is_final,
                    data_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                operation_id,
                event.event_id,
                event.type,
                timestamp,
                serialized["task_id"],
                event.task_name,
                (
                    transition.operation_status.value
                    if transition is not None
                    and transition.operation_status is not None
                    else None
                ),
                (
                    transition.task_status.value
                    if transition is not None
                    and transition.task_status is not None
                    else None
                ),
                int(event.is_final),
                json.dumps(
                    serialized["data"],
                    separators=(",", ":"),
                    allow_nan=False,
                ),
            ),
        )

    def _insert_initial_operation(
        self,
        connection: sqlite3.Connection,
        event: Event,
        transition: LifecycleTransition | None,
        timestamp: str,
        error_json: str | None,
    ) -> None:
        operation_id = str(event.operation_id)
        if event.event_id != 1:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "The first persisted event must have event_id 1.",
                details={"operation_id": operation_id},
            )
        if transition is None or transition.operation_status is None:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "The first event requires an operation lifecycle transition.",
                details={"operation_id": operation_id},
            )
        if transition.task_status is not None:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "The first operation event cannot transition a task.",
                details={"operation_id": operation_id},
            )

        name = transition.operation_name
        if name is None:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_NAME,
                "The first operation transition requires an operation name.",
                details={"operation_id": operation_id},
            )

        status = transition.operation_status
        is_terminal = status.is_terminal
        connection.execute(
            """
            INSERT INTO operations (
                operation_id,
                name,
                status,
                last_event_id,
                created_at,
                started_at,
                finished_at,
                is_finished,
                error_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                operation_id,
                name,
                status.value,
                event.event_id,
                timestamp,
                timestamp if status == LifecycleStatus.RUNNING else None,
                timestamp if is_terminal else None,
                int(is_terminal),
                error_json,
            ),
        )

    def _update_operation(
        self,
        connection: sqlite3.Connection,
        event: Event,
        transition: LifecycleTransition | None,
        existing: sqlite3.Row,
        timestamp: str,
        error_json: str | None,
    ) -> None:
        operation_id = str(event.operation_id)
        if bool(existing["is_finished"]):
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_EVENT_STREAM_FINISHED,
                f"Operation '{operation_id}' is already finished.",
            )

        expected_event_id = int(existing["last_event_id"]) + 1
        if event.event_id != expected_event_id:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "Event IDs must be contiguous within an operation.",
                details={
                    "operation_id": operation_id,
                    "expected_event_id": expected_event_id,
                    "received_event_id": event.event_id,
                },
            )

        if (
            transition is not None
            and transition.operation_name is not None
            and transition.operation_name != str(existing["name"])
        ):
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "An operation lifecycle transition cannot rename an operation.",
                details={"operation_id": operation_id},
            )

        status = (
            transition.operation_status
            if transition is not None and transition.operation_status is not None
            else LifecycleStatus(str(existing["status"]))
        )
        operation_changed = (
            transition is not None and transition.operation_status is not None
        )
        is_terminal = status.is_terminal
        connection.execute(
            """
            UPDATE operations
            SET status = ?,
                last_event_id = ?,
                started_at = ?,
                finished_at = ?,
                is_finished = ?,
                error_json = ?
            WHERE operation_id = ?
            """,
            (
                status.value,
                event.event_id,
                (
                    timestamp
                    if operation_changed and status == LifecycleStatus.RUNNING
                    else existing["started_at"]
                ),
                timestamp if is_terminal else existing["finished_at"],
                int(is_terminal),
                error_json if operation_changed else existing["error_json"],
                operation_id,
            ),
        )

    def _update_task(
        self,
        connection: sqlite3.Connection,
        operation_id: str,
        transition: LifecycleTransition,
        timestamp: str,
        error_json: str | None,
    ) -> None:
        task_status = transition.task_status
        task_id = transition.task_id
        if task_status is None or task_id is None:
            return
        is_terminal = task_status.is_terminal
        cursor = connection.execute(
            """
            UPDATE tasks
            SET status = ?,
                started_at = CASE
                    WHEN ? THEN COALESCE(started_at, ?)
                    ELSE started_at
                END,
                finished_at = CASE
                    WHEN ? THEN ?
                    ELSE finished_at
                END,
                error_json = ?
            WHERE task_id = ? AND operation_id = ?
            """,
            (
                task_status.value,
                int(task_status == LifecycleStatus.RUNNING),
                timestamp,
                int(is_terminal),
                timestamp,
                error_json,
                str(task_id),
                operation_id,
            ),
        )
        if cursor.rowcount != 1:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "A task lifecycle transition references an unknown operation task.",
                details={
                    "operation_id": operation_id,
                    "task_id": str(task_id),
                },
            )

    def _read_events(
        self,
        operation_id: str,
        after_event_id: int,
        limit: int | None,
    ) -> list[sqlite3.Row]:
        query = """
            SELECT
                operation_id,
                event_id,
                type,
                timestamp,
                task_id,
                task_name,
                is_final,
                data_json
            FROM events
            WHERE operation_id = ? AND event_id > ?
            ORDER BY event_id
        """
        parameters: tuple[Any, ...] = (operation_id, after_event_id)
        if limit is not None:
            query += " LIMIT ?"
            parameters += (limit,)
        return self._require_connection().execute(query, parameters).fetchall()

    def _read_event_stream_state(
        self,
        operation_id: str,
    ) -> sqlite3.Row | None:
        return self._require_connection().execute(
            """
            SELECT last_event_id, is_finished, finished_at
            FROM operations
            WHERE operation_id = ?
            """,
            (operation_id,),
        ).fetchone()

    def _checkpoint(self) -> None:
        row = self._require_connection().execute(
            "PRAGMA wal_checkpoint(FULL)"
        ).fetchone()
        if row is not None and int(row[0]) != 0:
            raise sqlite3.OperationalError("The WAL checkpoint remained busy")

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("Operation store is not open.")
        return self._connection

    @staticmethod
    def _serialize_error(error: dict[str, Any] | None) -> str | None:
        if error is None:
            return None
        return json.dumps(error, separators=(",", ":"), allow_nan=False)

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> Event:
        return Event.model_validate(
            {
                "operation_id": row["operation_id"],
                "event_id": row["event_id"],
                "type": row["type"],
                "timestamp": row["timestamp"],
                "task_id": row["task_id"],
                "task_name": row["task_name"],
                "is_final": bool(row["is_final"]),
                "data": json.loads(row["data_json"]),
            }
        )

    def _operation_from_row(
        self,
        row: sqlite3.Row,
        *,
        operation_id: UUID | None = None,
    ) -> OperationRecord:
        try:
            error = self._optional_object(row["error_json"])
            return OperationRecord.model_validate(
                {
                    "operation_id": row["operation_id"],
                    "name": row["name"],
                    "status": row["status"],
                    "last_event_id": row["last_event_id"],
                    "created_at": row["created_at"],
                    "started_at": row["started_at"],
                    "finished_at": row["finished_at"],
                    "error": error,
                }
            )
        except Exception as exc:
            details = (
                {"operation_id": str(operation_id)}
                if operation_id is not None
                else {}
            )
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "The operation store contains an invalid operation record.",
                details=details,
            ) from exc

    def _task_from_row(
        self,
        row: sqlite3.Row,
        *,
        task_id: UUID | None = None,
    ) -> OperationTaskRecord:
        try:
            retryable_error_codes = json.loads(
                str(row["retryable_error_codes_json"])
            )
            if not isinstance(retryable_error_codes, list) or not all(
                isinstance(code, str) for code in retryable_error_codes
            ):
                raise ValueError("retryable_error_codes_json must contain strings")
            retry_policy = RetryPolicy(
                max_attempts=int(row["max_attempts"]),
                retryable_error_codes=frozenset(retryable_error_codes),
            )
            return OperationTaskRecord.model_validate(
                {
                    "task_id": row["task_id"],
                    "operation_id": row["operation_id"],
                    "name": row["name"],
                    "is_root": bool(row["is_root"]),
                    "status": row["status"],
                    "retry_policy": retry_policy,
                    "retry_input": self._optional_object(row["retry_input_json"]),
                    "attempt": row["attempt"],
                    "retry_of_operation_id": row["retry_of_operation_id"],
                    "retry_of_task_id": row["retry_of_task_id"],
                    "created_at": row["created_at"],
                    "started_at": row["started_at"],
                    "finished_at": row["finished_at"],
                    "error": self._optional_object(row["error_json"]),
                }
            )
        except Exception as exc:
            details = {"task_id": str(task_id)} if task_id is not None else {}
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_DATABASE_CORRUPTED,
                "The operation store contains an invalid task record.",
                details=details,
            ) from exc

    @staticmethod
    def _optional_object(value: Any) -> dict[str, Any] | None:
        if value is None:
            return None
        parsed = json.loads(str(value))
        if not isinstance(parsed, dict):
            raise ValueError("Stored JSON value must be an object")
        return parsed

    def _validate_limit(self, limit: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise OperationError(
                self.error_code.OPERATION_RUNTIME_INVALID_PAGE_SIZE,
                "Page size must be a positive integer.",
            )

    def _database_error(self, message: str) -> OperationError:
        return OperationError(
            self.error_code.OPERATION_RUNTIME_DATABASE_FAILED,
            message,
        )
