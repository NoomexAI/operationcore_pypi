"""Validated snapshots of durable operation and task state."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from .retry import RetryPolicy
from .state import LifecycleStatus


class OperationRecord(BaseModel):
    """The durable state of one operation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operation_id: UUID
    name: str = Field(min_length=1)
    status: LifecycleStatus
    last_event_id: int = Field(ge=0)
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: dict[str, Any] | None = None

    @property
    def is_finished(self) -> bool:
        return self.status.is_terminal


class OperationTaskRecord(BaseModel):
    """The durable state and retry lineage of one operation task."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    task_id: UUID
    operation_id: UUID
    name: str = Field(min_length=1)
    is_root: bool
    status: LifecycleStatus
    retry_policy: RetryPolicy = RetryPolicy()
    retry_input: dict[str, Any] | None = None
    attempt: int = Field(default=1, ge=1)
    retry_of_operation_id: UUID | None = None
    retry_of_task_id: UUID | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: dict[str, Any] | None = None

    @property
    def max_attempts(self) -> int:
        return self.retry_policy.max_attempts

    @property
    def retryable_error_codes(self) -> frozenset[str]:
        return self.retry_policy.retryable_error_codes

    @property
    def attempts_remaining(self) -> int:
        return max(self.max_attempts - self.attempt, 0)

    @property
    def can_retry(self) -> bool:
        error_code = self.error.get("code") if self.error is not None else None
        return (
            self.retry_policy.enabled
            and self.retry_input is not None
            and self.status == LifecycleStatus.FAILED
            and self.attempt < self.max_attempts
            and error_code in self.retryable_error_codes
        )


class OperationCleanupResult(BaseModel):
    """The outcome of one bounded expired-operation cleanup pass."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    deleted_operation_ids: tuple[UUID, ...] = ()
    skipped_operation_ids: tuple[UUID, ...] = ()
    failures: dict[UUID, dict[str, Any]] = Field(default_factory=dict)
