"""Run child tasks and inspect their correlated events."""

import asyncio
from tempfile import TemporaryDirectory

from operationcore import Event, EventType, Operation, OperationCore


class ApplicationEventType(EventType):
    PAGE_PROCESSED = "document.page.processed"


async def process_page(operation: Operation, page: int) -> str:
    await asyncio.sleep(0.1)
    await operation.publish(
        Event(
            type=ApplicationEventType.PAGE_PROCESSED,
            data={"page": page},
        )
    )
    return f"page-{page}"


async def import_document(operation: Operation) -> list[str]:
    tasks = []
    for page in range(1, 4):
        task = await operation.run(
            f"document.process-page-{page}",
            lambda active_operation, page=page: process_page(active_operation, page),
        )
        tasks.append(task)

    return [await task.result() for task in tasks]


async def main() -> None:
    with TemporaryDirectory(prefix="operationcore-children-") as storage_dir:
        core = OperationCore(
            storage_dir,
            event_type=ApplicationEventType,
        )
        try:
            await core.manager.start()
            operation = await core.manager.create("document.import")
            root_task = await operation.run("document.import", import_document)

            print("result:", await root_task.result())
            print("\npage events:")
            for event in await operation.read_events():
                if event.type == ApplicationEventType.PAGE_PROCESSED:
                    print(
                        f"  page={event.data['page']} "
                        f"task={event.task_name} task_id={event.task_id}"
                    )
        finally:
            await core.manager.close()


if __name__ == "__main__":
    asyncio.run(main())
