"""Runtime tuning values for OperationCore."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class OperationSettings:
    """Validated non-vocabulary settings shared by the runtime."""

    event_replay_page_size: int = 256
    operation_page_size: int = 50
    sync_interval_seconds: float = 1.0
    finished_operation_cache_size: int = 256
    cleanup_batch_size: int = 100
    retention: timedelta | None = None

    def __post_init__(self) -> None:
        self._positive_int(
            self.event_replay_page_size,
            "event_replay_page_size",
        )
        self._positive_int(self.operation_page_size, "operation_page_size")
        if (
            isinstance(self.finished_operation_cache_size, bool)
            or not isinstance(self.finished_operation_cache_size, int)
            or self.finished_operation_cache_size < 0
        ):
            raise ValueError(
                "finished_operation_cache_size must be a non-negative integer"
            )
        self._positive_int(self.cleanup_batch_size, "cleanup_batch_size")
        if (
            isinstance(self.sync_interval_seconds, bool)
            or not isinstance(self.sync_interval_seconds, (int, float))
            or self.sync_interval_seconds <= 0
        ):
            raise ValueError("sync_interval_seconds must be positive")
        if self.retention is not None and self.retention < timedelta(0):
            raise ValueError("retention cannot be negative")

    @staticmethod
    def _positive_int(value: int, name: str) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
