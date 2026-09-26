"""Retry a failed task from its durable task record."""

import asyncio
from tempfile import TemporaryDirectory

from operationcore import (
    ErrorCode,
    Operation,
    OperationCore,
    OperationError,
    RetryPolicy,
)


class ApplicationErrorCode(ErrorCode):
    SOURCE_UNAVAILABLE = "document-source:unavailable"


async def unavailable_source(operation: Operation) -> None:
    raise OperationError(
        ApplicationErrorCode.SOURCE_UNAVAILABLE,
        "The document source is temporarily unavailable.",
    )


async def available_source(operation: Operation) -> dict[str, str]:
    return {"document_id": "doc-123", "status": "imported"}


async def main() -> None:
    with TemporaryDirectory(prefix="operationcore-retry-") as storage_dir:
        core = OperationCore(
            storage_dir,
            error_code=ApplicationErrorCode,
        )
        try:
            await core.manager.start()

            original = await core.manager.create("document.import")
            policy = RetryPolicy(
                max_attempts=3,
                retryable_error_codes=frozenset(
                    {ApplicationErrorCode.SOURCE_UNAVAILABLE}
                ),
            )
            original_task = await original.run(
                "document.import",
                unavailable_source,
                retry_policy=policy,
                retry_input={"document_id": "doc-123"},
            )

            try:
                await original_task.result()
            except OperationError as error:
                print(f"attempt 1 failed: {error.code}")

            retryable = await core.manager.list_retryable_tasks()
            failed_record = next(
                record
                for record in retryable
                if record.task_id == original_task.task_id
            )
            print("saved retry input:", failed_record.retry_input)

            retry_operation = await core.manager.create(failed_record.name)
            retry_task = await retry_operation.run(
                failed_record.name,
                available_source,
                retry_of=failed_record,
            )
            print(f"attempt {retry_task.attempt} result:", await retry_task.result())
        finally:
            await core.manager.close()


if __name__ == "__main__":
    asyncio.run(main())
