"""Operation runtime errors and stable error codes."""

from __future__ import annotations

from typing import Any


class ErrorCode(str):
    """Stable machine-readable errors used by the operation runtime."""

    OPERATION_RUNTIME_INVALID_NAME = "operation-runtime:invalid-name"
    OPERATION_RUNTIME_NOT_FOUND = "operation-runtime:not-found"
    OPERATION_RUNTIME_CANCELLED = "operation-runtime:cancelled"
    OPERATION_RUNTIME_INTERRUPTED = "operation-runtime:interrupted"
    OPERATION_RUNTIME_FINISHED = "operation-runtime:finished"
    OPERATION_RUNTIME_INVALID_ID = "operation-runtime:invalid-id"
    OPERATION_RUNTIME_INVALID_STATUS = "operation-runtime:invalid-status"
    OPERATION_RUNTIME_INVALID_PAGE_SIZE = "operation-runtime:invalid-page-size"
    OPERATION_RUNTIME_INVALID_CACHE_SIZE = "operation-runtime:invalid-cache-size"
    OPERATION_RUNTIME_MANAGER_CLOSED = "operation-runtime:manager:closed"
    OPERATION_RUNTIME_TASK_NOT_FOUND = "operation-runtime:task:not-found"
    OPERATION_RUNTIME_TASK_NOT_RETRYABLE = "operation-runtime:task:not-retryable"
    OPERATION_RUNTIME_TASK_ALREADY_RETRIED = "operation-runtime:task:already-retried"
    OPERATION_RUNTIME_INVALID_RETRY_INPUT = "operation-runtime:retry-input:invalid"

    OPERATION_RUNTIME_EVENT_STREAM_CLOSED = "operation-runtime:event-stream:closed"
    OPERATION_RUNTIME_EVENT_STREAM_FINISHED = "operation-runtime:event-stream:finished"
    OPERATION_RUNTIME_INVALID_EVENT_CURSOR = "operation-runtime:event:invalid-cursor"
    OPERATION_RUNTIME_INVALID_EVENT_PAGE_SIZE = "operation-runtime:event:invalid-page-size"
    OPERATION_RUNTIME_EVENT_HISTORY_GAP = "operation-runtime:event:history-gap"
    OPERATION_RUNTIME_SYNC_FAILED = "operation-runtime:sync:failed"
    OPERATION_RUNTIME_DATABASE_FAILED = "operation-runtime:database:failed"
    OPERATION_RUNTIME_DATABASE_IN_USE = "operation-runtime:database:in-use"
    OPERATION_RUNTIME_DATABASE_CORRUPTED = "operation-runtime:database:corrupted"
    OPERATION_RUNTIME_UNSUPPORTED_DATABASE_VERSION = "operation-runtime:database:unsupported-version"
    OPERATION_RUNTIME_INVALID_SYNC_INTERVAL = "operation-runtime:sync:invalid-interval"
    OPERATION_RUNTIME_INVALID_RETENTION = "operation-runtime:retention:invalid"
    OPERATION_RUNTIME_INVALID_CLEANUP_BATCH_SIZE = "operation-runtime:cleanup:invalid-batch-size"
    OPERATION_RUNTIME_INTERNAL_ERROR = "operation-runtime:internal-error"

    @classmethod
    def to_dict(cls) -> dict[str, str]:
        """Return the inherited and locally defined error codes."""
        attributes: dict[str, None] = {}
        for base in reversed(cls.__mro__):
            if not issubclass(base, ErrorCode):
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
        for attribute, expected in vars(ErrorCode).items():
            if not attribute.isupper() or not isinstance(expected, str):
                continue
            supplied = cls.__dict__.get(attribute, expected)
            if supplied != expected:
                raise TypeError(
                    f"{cls.__name__} cannot override "
                    f"ErrorCode.{attribute}; add a new error code instead."
                )


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
        "code": (error_code or ErrorCode).OPERATION_RUNTIME_INTERNAL_ERROR,
        "message": "An unexpected internal error occurred.",
    }
