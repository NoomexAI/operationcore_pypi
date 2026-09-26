"""Persistence contracts and store implementations for OperationCore."""

from .protocol import OperationStore
from .sqlite_store import SQLiteOperationStore
from .transitions import LifecycleTransition

__all__ = [
    "LifecycleTransition",
    "OperationStore",
    "SQLiteOperationStore",
]
