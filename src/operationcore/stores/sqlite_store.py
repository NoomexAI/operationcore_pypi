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
from ..state import EventStreamState, LifecycleStatus
from .transitions import LifecycleTransition


_SCHEMA_VERSION = 1
_TERMINAL_STATUSES = frozenset(
    {
        LifecycleStatus.COMPLETED,
        LifecycleStatus.FAILED,
        LifecycleStatus.CANCELLED,
    }
)


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
                self.error_code.EVENT_STREAM_CLOSED,
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
                    self.error_code.OPERATION_DATABASE_FAILED,
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
                    self.error_code.OPERATION_DATABASE_FAILED,
                    "The event could not be persisted.",
                    details={"operation_id": str(event.operation_id)},
                ) from exc
            self._write_generation += 1
            return self._write_generation

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
                self.error_code.INVALID_EVENT_CURSOR,
                "after_event_id cannot be negative.",
            )
        if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0
        ):
            raise OperationError(
                self.error_code.INVALID_EVENT_PAGE_SIZE,
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
                self.error_code.OPERATION_DATABASE_CORRUPTED,
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
                self.error_code.OPERATION_DATABASE_CORRUPTED,
                "The operation store contains invalid event stream state.",
                details={"operation_id": str(operation_id)},
            ) from exc

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
                    self.error_code.OPERATION_SYNC_FAILED,
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
                self.error_code.OPERATION_DATABASE_IN_USE,
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
            if version not in {0, _SCHEMA_VERSION}:
                raise OperationError(
                    self.error_code.UNSUPPORTED_OPERATION_DATABASE_VERSION,
                    "The operation store uses an unsupported schema version.",
                    details={"schema_version": version},
                )
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA foreign_keys = ON")
            journal_mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()
            if journal_mode is None or str(journal_mode[0]).lower() != "wal":
                raise sqlite3.OperationalError("WAL mode could not be enabled")
            connection.execute("PRAGMA synchronous = NORMAL")
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

    def _append_event(
        self,
        event: Event,
        transition: LifecycleTransition | None,
    ) -> None:
        if event.operation_id is None or event.event_id is None:
            raise OperationError(
                self.error_code.OPERATION_DATABASE_CORRUPTED,
                "A persisted event requires operation_id and event_id.",
            )

        connection = self._require_connection()
        operation_id = str(event.operation_id)
        serialized = event.model_dump(mode="json")
        timestamp = str(serialized["timestamp"])
        operation_status = (
            transition.operation_status if transition is not None else None
        )
        operation_is_terminal = operation_status in _TERMINAL_STATUSES

        if event.is_final != operation_is_terminal:
            raise OperationError(
                self.error_code.OPERATION_DATABASE_CORRUPTED,
                "A final event and terminal operation transition must occur together.",
                details={"operation_id": operation_id},
            )
        if transition is not None and transition.task_id is not None:
            if event.task_id != transition.task_id:
                raise OperationError(
                    self.error_code.OPERATION_DATABASE_CORRUPTED,
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

        try:
            connection.execute("BEGIN IMMEDIATE")
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
                    is_final,
                    data_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    operation_id,
                    event.event_id,
                    event.type,
                    timestamp,
                    serialized["task_id"],
                    event.task_name,
                    int(event.is_final),
                    json.dumps(
                        serialized["data"],
                        separators=(",", ":"),
                        allow_nan=False,
                    ),
                ),
            )
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise

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
                self.error_code.OPERATION_DATABASE_CORRUPTED,
                "The first persisted event must have event_id 1.",
                details={"operation_id": operation_id},
            )
        if transition is None or transition.operation_status is None:
            raise OperationError(
                self.error_code.OPERATION_DATABASE_CORRUPTED,
                "The first event requires an operation lifecycle transition.",
                details={"operation_id": operation_id},
            )
        if transition.task_status is not None:
            raise OperationError(
                self.error_code.OPERATION_DATABASE_CORRUPTED,
                "The first operation event cannot transition a task.",
                details={"operation_id": operation_id},
            )

        name = transition.operation_name
        if name is None:
            raise OperationError(
                self.error_code.INVALID_OPERATION_NAME,
                "The first operation transition requires an operation name.",
                details={"operation_id": operation_id},
            )

        status = transition.operation_status
        is_terminal = status in _TERMINAL_STATUSES
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
                self.error_code.EVENT_STREAM_FINISHED,
                f"Operation '{operation_id}' is already finished.",
            )

        expected_event_id = int(existing["last_event_id"]) + 1
        if event.event_id != expected_event_id:
            raise OperationError(
                self.error_code.OPERATION_DATABASE_CORRUPTED,
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
                self.error_code.OPERATION_DATABASE_CORRUPTED,
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
        is_terminal = status in _TERMINAL_STATUSES
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
        is_terminal = task_status in _TERMINAL_STATUSES
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
                self.error_code.OPERATION_DATABASE_CORRUPTED,
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

    def _database_error(self, message: str) -> OperationError:
        return OperationError(
            self.error_code.OPERATION_DATABASE_FAILED,
            message,
        )
