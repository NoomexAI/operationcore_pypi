"""Configuration for the operation and event runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from .errors import ErrorCode
from .events import EventType


@dataclass(frozen=True, slots=True)
class EventConfig:
    """Event runtime types, defaults, and event-stream behavior."""

    event_type: type[EventType] = EventType
    error_code: type[ErrorCode] = ErrorCode
    replay_page_size: int = 256
    immediate_sync_event_types: frozenset[str] = field(
        init=False,
        hash=False,
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "immediate_sync_event_types",
            frozenset(
                {
                    self.event_type.OPERATION_QUEUED,
                    self.event_type.OPERATION_STARTED,
                    self.event_type.OPERATION_COMPLETED,
                    self.event_type.OPERATION_FAILED,
                    self.event_type.OPERATION_CANCELLED,
                }
            ),
        )


@dataclass(frozen=True, slots=True)
class OperationConfig:
    """Operation runtime types, defaults, and lifecycle lookup tables."""

    event_type: type[EventType] = EventType
    error_code: type[ErrorCode] = ErrorCode
    finished_operation_cache_size: int = 256
    cleanup_batch_size: int = 100
    page_size: int = 50
    sync_interval_seconds: float = 1.0
    terminal_statuses: frozenset[str] = frozenset(
        {"completed", "failed", "cancelled"}
    )
    operation_status_by_event: Mapping[str, str] = field(
        init=False,
        hash=False,
    )
    task_status_by_event: Mapping[str, str] = field(
        init=False,
        hash=False,
    )
    terminal_task_event_types: frozenset[str] = field(
        init=False,
        hash=False,
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "operation_status_by_event",
            MappingProxyType(
                {
                    self.event_type.OPERATION_QUEUED: "queued",
                    self.event_type.OPERATION_STARTED: "running",
                    self.event_type.OPERATION_COMPLETED: "completed",
                    self.event_type.OPERATION_FAILED: "failed",
                    self.event_type.OPERATION_CANCELLED: "cancelled",
                }
            ),
        )
        object.__setattr__(
            self,
            "task_status_by_event",
            MappingProxyType(
                {
                    self.event_type.OPERATION_TASK_QUEUED: "queued",
                    self.event_type.OPERATION_TASK_STARTED: "running",
                    self.event_type.OPERATION_TASK_COMPLETED: "completed",
                    self.event_type.OPERATION_TASK_FAILED: "failed",
                    self.event_type.OPERATION_TASK_CANCELLED: "cancelled",
                }
            ),
        )
        object.__setattr__(
            self,
            "terminal_task_event_types",
            frozenset(
                {
                    self.event_type.OPERATION_TASK_COMPLETED,
                    self.event_type.OPERATION_TASK_FAILED,
                    self.event_type.OPERATION_TASK_CANCELLED,
                }
            ),
        )
