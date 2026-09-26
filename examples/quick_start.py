"""Create an operation, stream its progress, and await its result."""

import asyncio
from tempfile import TemporaryDirectory

from operationcore import Event, EventType, Operation, OperationCore


class ApplicationEventType(EventType):
    PROGRESS = "document.import.progress"


async def import_document(operation: Operation) -> dict[str, int | str]:
    for progress in range(20, 101, 20):
        await asyncio.sleep(0.2)
        await operation.publish(
            Event(
                type=ApplicationEventType.PROGRESS,
                data={"progress": progress},
            )
        )
    return {"status": "complete", "progress": 100}


async def print_events(operation: Operation) -> None:
    async for event in operation.events():
        if event.type == ApplicationEventType.PROGRESS:
            print(f"progress: {event.data['progress']}%")
        else:
            print(f"lifecycle: {event.type}")


async def main() -> None:
    with TemporaryDirectory(prefix="operationcore-progress-") as storage_dir:
        core = OperationCore(
            storage_dir,
            event_type=ApplicationEventType,
        )
        try:
            await core.manager.start()
            operation = await core.manager.create("document.import")
            event_consumer = asyncio.create_task(print_events(operation))
            task = await operation.run("document.import", import_document)

            result = await task.result()
            await event_consumer
            print("result:", result)
        finally:
            await core.manager.close()


if __name__ == "__main__":
    asyncio.run(main())
