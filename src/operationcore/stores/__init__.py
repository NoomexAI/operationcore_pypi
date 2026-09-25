"""Persistence contracts and store implementations for OperationCore."""

from .protocol import OperationStore
from .transitions import LifecycleTransition

__all__ = [
    "LifecycleTransition",
    "OperationStore",
]
