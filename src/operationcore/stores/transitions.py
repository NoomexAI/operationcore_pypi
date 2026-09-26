"""Explicit lifecycle changes committed with operation events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from ..state import LifecycleStatus


@dataclass(frozen=True, slots=True)
class LifecycleTransition:
    """Projection changes committed atomically with a lifecycle event."""

    operation_name: str | None = None
    operation_status: LifecycleStatus | None = None
    operation_error: dict[str, Any] | None = None
    task_id: UUID | None = None
    task_status: LifecycleStatus | None = None
    task_error: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.operation_status is None and self.task_status is None:
            raise ValueError(
                "A lifecycle transition requires at least one projection change."
            )
        if self.task_status is not None and self.task_id is None:
            raise ValueError("A task transition requires task_id.")
        if self.task_status is None and self.task_id is not None:
            raise ValueError("task_id is only valid for a task transition.")
        if self.operation_status is None and self.operation_error is not None:
            raise ValueError(
                "operation_error requires an operation status change."
            )
        if self.operation_status is None and self.operation_name is not None:
            raise ValueError(
                "operation_name requires an operation status change."
            )
        if self.operation_name is not None and not self.operation_name.strip():
            raise ValueError("operation_name must be non-empty when provided.")
        if self.task_status is None and self.task_error is not None:
            raise ValueError("task_error requires a task status change.")
