# SCC autostart scheduler fix

This patch fixes cyclic autostart semantics without requiring project nodes to change `autostart=True` to `False`.

## Problem

In a strongly connected component such as:

```text
explode -> explodeproof -> explode
explode -> explodedefinition -> explode
...
```

there must be only one scheduler owner for the whole communicating class. If each node is scheduled independently, or if same-component `autostart=True` recursively runs child jobs inside parent job threads, the workflow can reach a boundary where active worker slots are held by parents and no queued child jobs can make progress.

## Fix

- `autostart=True` inside the same cyclic SCC is now deferred: it creates a queued job and returns the `Job`; it does not call `run_job()` recursively.
- `run_concurrently()` now schedules strongly connected components as workflow units.
- Cyclic components are pumped by `run_component(...)` until no queued/running work remains in that component.
- Acyclic singleton nodes keep the original concurrent behavior.
- Normal DAG autostart behavior outside cyclic SCCs is preserved.

## Tests added

- `test_same_component_immediate_autostart_is_deferred_not_nested`
- `test_threaded_runfrom_pumps_cyclic_autostart_component_before_downstream`

## Checks run

```bash
python -m compileall -q micro_workflow_manager
PYTHONPATH=/mnt/data/mwf25_work pytest -q /mnt/data/mwf25_tests/tests
```

Result:

```text
31 passed, 4 warnings
```
