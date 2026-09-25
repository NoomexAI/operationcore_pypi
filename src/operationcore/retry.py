"""Task retry policy values."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Retry limits for one task invocation.

    Retries are requested explicitly by the caller. A task is eligible when it
    has more than one allowed attempt and its failure code is listed here.
    """

    max_attempts: int = 1
    retryable_error_codes: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts < 1
        ):
            raise ValueError("max_attempts must be a positive integer")
        object.__setattr__(
            self,
            "retryable_error_codes",
            frozenset(str(code) for code in self.retryable_error_codes),
        )

    @property
    def enabled(self) -> bool:
        """Whether this policy can permit at least one retry."""
        return self.max_attempts > 1 and bool(self.retryable_error_codes)
