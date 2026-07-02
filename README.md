# micro-workflow-manager

A small file-backed DAG workflow manager. Each node has inspectable `input/`, `output/`, and `jobs/` folders, one main task, optional fallbacks, explicit starter jobs, and APIRouter-style node modules.

## Important workflow rule

A finished job is not the same thing as a finished node. This matters for dynamically-created jobs: a downstream node can receive and finish one job before all of its predecessor nodes have completed and before those predecessors have finished creating every downstream job. The library only marks a node `done` during node-level finalization after its predecessors are complete.

## Cyclic autostart components

Graphs may contain self-loops and mutually reachable autostart nodes. The
workflow manager treats each strongly connected component as one communicating
class for readiness and completion. For example, if `A -> A`, `A -> B`,
`B -> A`, and `A -> D`, then `D` is not ready merely because one A job
finished. `D` waits until all queued/running jobs across the whole A/B
component are done and the component is finalized together.

See `examples/autostart_cycle_lab` for a runnable four-node experiment.

## Explicit jobs

`mwf run` and `mwf runfrom` no longer invent a default starter job. Declare default jobs in the respective node file:

```python
from micro_workflow_manager import NodeRouter

router = NodeRouter("split", max_threads=2)
router.create_job(number=2, params={"message": "hello"})

@router.task
def split(ctx, message):
    print(message, ctx.job_id)
```

`number=2` creates jobs 1 and 2 with the same params. Multiple `router.create_job(...)` calls are allocated deterministic job ids in the order they appear. These declarations are idempotent when the CLI imports node files repeatedly.

## Passing files forward

Output-folder-triggered job creation has been removed. Instead, a task can add files directly to the input folder of a downstream node:

```python
@router.task
def split(ctx):
    page = ctx.write("page_001.txt", "page text")
    ctx.node("tagify").add_input_file(page, filename="page_001.txt")
```

The downstream node can read those files with `ctx.input_files(...)` or `ctx.input_path(...)`. This keeps job creation explicit while still allowing upstream nodes to prepare the file inputs that later nodes consume.

## Runners

The default runner is `threaded`.

```bash
mwf graph src/graph.py --runner threaded
mwf runfrom start_node
```

`threaded` is dependency-free and uses Python's local thread pool. It runs:

- multiple queued jobs inside the same node at the same time, capped by that node's `max_threads`
- multiple ready nodes at the same time, while still respecting DAG predecessor completion
- newly-ready downstream nodes while unrelated nodes are still running

For CPU-heavy work, use the process-pool runner:

```bash
mwf graph src/graph.py --runner process
mwf runfrom start_node
```

`process` mirrors the threaded runner's workflow behavior, but jobs run in child Python processes through `ProcessPoolExecutor`. It still runs multiple ready nodes at the same time, streams large job queues lazily, respects DAG readiness, and uses each node's `max_threads` value as the process-worker cap for that node. `processes`, `process_pool`, and `processpool` are accepted aliases.

Process mode is meant for normal CLI/router projects where child processes can rebuild the workflow from `src/graph.py` and `src/node_behavior/*.py`. Keep process-run node code in importable files, and return pickleable values such as strings, numbers, lists, dicts, or `Path` objects. On Windows, use the CLI or put programmatic runs behind `if __name__ == "__main__":`.

A node can override the global runner:

```python
from micro_workflow_manager import NodeRouter

router = NodeRouter("ocr_pages", max_threads=4, runner="process")
router.create_job(number=8)

@router.task
def ocr_pages(ctx):
    # CPU-heavy page work here. With runner="process", up to 4 jobs for
    # this node run in separate Python processes.
    text = f"processed page job {ctx.job_id}"
    ctx.write(f"page_{ctx.job_id}.txt", text)
    return text
```

For step-by-step debugging, use the direct runner:

```bash
mwf graph src/graph.py --runner direct
mwf runfrom start_node
```


## Monitoring and live statistics

While a workflow is running, open a second terminal in the same project and run:

```bash
mwf monitor
```

`mwf monitor` reads the file-backed job and node state and prints a live dashboard with running nodes, queued/running/done/failed job counts, jobs left, progress, running job IDs, average completed job duration, and rough ETA. It does not run task code.

Useful forms:

```bash
mwf monitor --once          # one snapshot
mwf monitor A B             # monitor selected nodes only
mwf monitor --json --once   # machine-readable snapshot
```

You can also print compact status lines in the same terminal as the run:

```bash
mwf runfrom start_node --stats
mwf run start_node --stats --stats-interval 10
```

ETA is intentionally approximate. It is calculated from completed job durations and becomes more useful after at least one job in the relevant node has finished.


## Large-node performance note

For large nodes, `queued` is the implicit per-job status. A job with `job.json` and `input.json` but no `status.json` is treated as queued by the storage API. This avoids thousands of small JSON writes during reset/requeue and lets `node_state.json` switch to `running` before the runner loads every queued job. Explicit `status.json` files are still written for `running`, `done`, `failed`, `cancelled`, and `skipped` jobs.

## Install for development

```bash
pip install -e .[test]
```

## Run tests

```bash
pytest
```
