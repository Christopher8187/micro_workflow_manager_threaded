# SCC autostart stress test notes

Added two stress tests focused on `autostart=True` inside strongly connected components:

1. Branch cycle: `A -> B`, `A -> C`, `B -> A`, `C -> A`, with 1002 total jobs and deterministic random waits.
2. Chain cycle: `A -> B -> C -> A`, with 1000 total jobs and deterministic random waits.

The normal pytest suite excludes these by default because they are stress tests. Run them explicitly with:

```bash
pytest -m stress tests/test_autostart_cycle_stress.py -ra
```

The earlier four warnings were Python 3.13 `DeprecationWarning`s from `multiprocessing` using `fork` in a multi-threaded pytest process. The process runner now uses `mp.get_context("spawn")` for `ProcessPoolExecutor`, and `pyproject.toml` treats warnings as errors for the default test suite.

This package keeps `autostart=True` as the intended SCC behavior: same-component autostart enqueues/wakes the cyclic component rather than recursively running child jobs inside the parent job.
