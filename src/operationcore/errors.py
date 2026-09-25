"""Operation runtime errors and stable error codes."""

from __future__ import annotations

from typing import Any


class ErrorCode(str):
    """Stable machine-readable errors used by the operation runtime."""

    INVALID_OPERATION_NAME = "invalid_operation_name"
    OPERATION_NOT_FOUND = "operation_not_found"
    OPERATION_CANCELLED = "operation_cancelled"
    OPERATION_INTERRUPTED = "operation_interrupted"
    OPERATION_FINISHED = "operation_finished"
    INVALID_OPERATION_ID = "invalid_operation_id"
    INVALID_OPERATION_STATUS = "invalid_operation_status"
    INVALID_OPERATION_PAGE_SIZE = "invalid_operation_page_size"
    INVALID_OPERATION_CACHE_SIZE = "invalid_operation_cache_size"
    OPERATION_MANAGER_CLOSED = "operation_manager_closed"
    OPERATION_TASK_NOT_FOUND = "operation_task_not_found"
    OPERATION_TASK_NOT_RETRYABLE = "operation_task_not_retryable"
    OPERATION_TASK_ALREADY_RETRIED = "operation_task_already_retried"
    INVALID_RETRY_INPUT = "invalid_retry_input"

    EVENT_STREAM_CLOSED = "event_stream_closed"
    EVENT_STREAM_FINISHED = "event_stream_finished"
    INVALID_EVENT_CURSOR = "invalid_event_cursor"
    INVALID_EVENT_PAGE_SIZE = "invalid_event_page_size"
    EVENT_HISTORY_GAP = "event_history_gap"
    OPERATION_SYNC_FAILED = "operation_sync_failed"
    OPERATION_DATABASE_FAILED = "operation_database_failed"
    OPERATION_DATABASE_IN_USE = "operation_database_in_use"
    OPERATION_DATABASE_CORRUPTED = "operation_database_corrupted"
    UNSUPPORTED_OPERATION_DATABASE_VERSION = (
        "unsupported_operation_database_version"
    )
    INVALID_OPERATION_SYNC_INTERVAL = "invalid_operation_sync_interval"
    INVALID_RETENTION = "invalid_retention"
    INVALID_CLEANUP_BATCH_SIZE = "invalid_cleanup_batch_size"
    INTERNAL_ERROR = "internal_error"


class OperationError(Exception):
    """An expected operation runtime error safe to expose at a boundary."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.details = details or {}
        super().__init__(message)


    def as_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.details:
            payload["details"] = self.details
        return payload


def error_payload(
    error: BaseException,
    error_code: type[ErrorCode] | None = None,
) -> dict[str, Any]:
    """Return a safe event/API payload for an exception."""
    if isinstance(error, OperationError):
        return error.as_payload()
    return {
        "code": (error_code or ErrorCode).INTERNAL_ERROR,
        "message": "An unexpected internal error occurred.",
    }
