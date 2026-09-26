# OperationCore

OperationCore provides durable asynchronous operations, tasks, event streams,
retries, cancellation, recovery, and replaceable storage behind one manager.

```python
from operationcore import OperationCore


core = OperationCore("operation_store")
operation = await core.manager.create("document.import")
task = await operation.run("document.import", worker)
result = await task.result()

await core.manager.close()
```

## Retention cleanup

Retention cleanup is startup maintenance. When retention is configured,
`OperationManager.start()` can automatically run one cleanup pass after
opening the store and before starting normal manager activity. Automatic
startup cleanup is disabled by default and must be explicitly enabled on
`OperationCore`.

```python
from datetime import timedelta

from operationcore import OperationCore, OperationSettings


core = OperationCore(
    "operation_store",
    enable_startup_cleanup=True,
    settings=OperationSettings(
        retention=timedelta(days=7),
        cleanup_batch_size=100,
    ),
)

await core.manager.start()
cleanup_result = core.manager.get_startup_cleanup_result()

if cleanup_result is not None:
    print("Deleted:", cleanup_result.deleted_operation_ids)
    print("Skipped:", cleanup_result.skipped_operation_ids)
    print("Failed:", cleanup_result.failures)

# Normal application work starts after cleanup finishes.
operation = await core.manager.create("document.import")
```

When `enable_startup_cleanup=False`, `start()` opens the store and starts
synchronization without running cleanup. This is the default.

`start()` is idempotent. With startup cleanup enabled, repeated or concurrent
calls run cleanup only once during a manager's lifetime. Manager methods also
call `start()` lazily, so the first operation API call triggers the same
startup sequence if the application does not call it explicitly. The stored
result is available through `manager.get_startup_cleanup_result()`. That method
returns `None` when cleanup has not run, automatic startup cleanup was disabled,
or no cleanup service was configured.

Do not call `manager.cleanup.run_once()` in the middle of normal application work. A
finished operation can leave the manager's in-memory cache while application
code still holds its `Operation` object. Cleanup cannot discover that external
reference after cache eviction and may delete the operation's stored task and
event history while the object is still being used.

The retention duration therefore means that a finished operation becomes
eligible for deletion after that duration. Its records are removed during a
later startup cleanup pass, rather than at the exact expiration time.

The startup pass processes at most `cleanup_batch_size` eligible operations.
