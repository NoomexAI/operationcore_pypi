"""Composition root for the operation runtime."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from .config import EventConfig, OperationConfig
from .errors import ErrorCode
from .events import EventType
from .operations import OperationManager


class OperationCore:
    """Keep the configured operation machinery together in one place."""

    def __init__(
        self,
        storage_dir: str | Path,
        *,
        event_type: type[EventType] | None = None,
        error_code: type[ErrorCode] | None = None,
        operation_config: OperationConfig | None = None,
        sync_interval: float | None = None,
        max_cached_finished_operations: int | None = None,
        event_replay_page_size: int | None = None,
    ) -> None:
        configured_event_type = (
            event_type
            or (operation_config.event_type if operation_config is not None else None)
            or EventType
        )
        configured_error_code = (
            error_code
            or (operation_config.error_code if operation_config is not None else None)
            or ErrorCode
        )

        event_defaults = EventConfig(
            event_type=configured_event_type,
            error_code=configured_error_code,
        )
        self.event_config = replace(
            event_defaults,
            replay_page_size=(
                event_defaults.replay_page_size
                if event_replay_page_size is None
                else event_replay_page_size
            ),
        )

        operation_defaults = operation_config or OperationConfig(
            event_type=configured_event_type,
            error_code=configured_error_code,
        )
        self.operation_config = replace(
            operation_defaults,
            event_type=configured_event_type,
            error_code=configured_error_code,
            finished_operation_cache_size=(
                operation_defaults.finished_operation_cache_size
                if max_cached_finished_operations is None
                else max_cached_finished_operations
            ),
            sync_interval_seconds=(
                operation_defaults.sync_interval_seconds
                if sync_interval is None
                else sync_interval
            ),
        )

        self.event_type = configured_event_type
        self.error_code = configured_error_code
        self.manager = OperationManager(
            storage_dir,
            event_config=self.event_config,
            operation_config=self.operation_config,
        )
