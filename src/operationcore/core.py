"""Composition root for the operation runtime."""

from __future__ import annotations

from pathlib import Path

from .errors import ErrorCode
from .events import EventType
from .operations import OperationManager
from .settings import OperationSettings
from .stores.protocol import OperationStore
from .stores.sqlite_store import SQLiteOperationStore


class OperationCore:
    """Assemble configured operation machinery and expose its manager."""

    def __init__(
        self,
        storage_dir: str | Path | None = None,
        *,
        event_type: type[EventType] | None = None,
        error_code: type[ErrorCode] | None = None,
        settings: OperationSettings | None = None,
        store: OperationStore | None = None,
    ) -> None:
        self.settings = settings or OperationSettings()
        self.event_type = event_type or EventType
        self.error_code = error_code or ErrorCode
        self._validate_catalog(self.event_type, EventType)
        self._validate_catalog(self.error_code, ErrorCode)

        if store is not None and storage_dir is not None:
            raise ValueError("Pass either storage_dir or store, not both")
        if store is None:
            directory = Path(storage_dir or "operation_store").expanduser().resolve()
            directory.mkdir(parents=True, exist_ok=True)
            self.storage_dir: Path | None = directory
            self.database_path: Path | None = directory / "operations.sqlite3"
            store = SQLiteOperationStore(
                self.database_path,
                error_code=self.error_code,
            )
        else:
            self.storage_dir = None
            self.database_path = None

        self.store = store
        self.manager = OperationManager(
            store=self.store,
            event_type=self.event_type,
            error_code=self.error_code,
            settings=self.settings,
        )

    @staticmethod
    def _validate_catalog(
        catalog: type[EventType] | type[ErrorCode],
        default: type[EventType] | type[ErrorCode],
    ) -> None:
        catalog_name = getattr(catalog, "__name__", type(catalog).__name__)
        values: list[str] = []
        for attribute, default_value in vars(default).items():
            if not attribute.isupper() or not isinstance(default_value, str):
                continue
            value = getattr(catalog, attribute, None)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"{catalog_name}.{attribute} must be a non-empty string"
                )
            values.append(value)

        if default is EventType and len(set(values)) != len(values):
            raise ValueError("Required lifecycle event values must be unique")
