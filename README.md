# micro-workflow-manager

A small file-backed DAG workflow manager. Each node has inspectable `input/`, `output/`, and `jobs/` folders, one main task, optional fallbacks, explicit starter jobs, and APIRouter-style node modules.

## Important workflow rule

A finished job is not the same thing as a finished node. This matters for dynamically-created jobs: a downstream node can receive and finish one job before all of its predecessor nodes have completed and before those predecessors have finished creating every downstream job. The library only marks a node `done` during node-level finalization after its predecessors are complete.

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

For step-by-step debugging, use the direct runner:

```bash
mwf graph src/graph.py --runner direct
mwf runfrom start_node
```


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
