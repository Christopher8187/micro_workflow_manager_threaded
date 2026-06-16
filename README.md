# micro-workflow-manager

A small file-backed DAG workflow manager. Each node has inspectable `input/`, `output/`, and `jobs/` folders, one main task, optional fallbacks, and APIRouter-style node modules.

## Important workflow rule

A finished job is not the same thing as a finished node. This matters for autostarted jobs: a downstream node can receive and finish one job before all of its predecessor nodes have completed and before those predecessors have finished creating every downstream job. The library only marks a node `done` during node-level finalization after its predecessors are complete.

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

## Install for development

```bash
pip install -e .[test]
```

## Run tests

```bash
pytest
```

## Per-node sequential override

The workflow-level runner can stay `threaded`, but a single node can force its own jobs to run sequentially from its `node_behavior/<node-name>.py` file:

```python
from micro_workflow_manager import NodeRouter

router = NodeRouter("slow_writer", max_threads=5)
router.run_sequentially()

@router.task
def slow_writer(ctx, text):
    ctx.write_output(f"{ctx.job_id}.txt", text)
```

Equivalent forms are also supported:

```python
router = NodeRouter("slow_writer", sequential=True)

# or
@router.task(sequential=True)
def slow_writer(ctx, text):
    ...
```

This is a per-node override. Other nodes can still use the CLI/workflow runner.

## Deferred output-file job creation

Use `add_from_output_files(...)` when a downstream job should be created from files in the current node's `output/` folder. These jobs are created only after the source node has finished, because the node-level output folder may not be complete while jobs inside the node are still running.

```python
@router.task
def make_pages(ctx, text):
    ctx.write_output(f"page_{ctx.job_id}.txt", text)
    ctx.node("index_pages").add_from_output_files(
        "page_*.txt",
        file_param="output_file",
        autostart=True,
    )
```

Then the downstream node receives one job per matched file:

```python
router = NodeRouter("index_pages")

@router.task
def index_pages(ctx, output_file):
    text = Path(output_file).read_text(encoding="utf-8")
```

Normal `ctx.node("next").add(..., autostart=True)` behavior is unchanged: it still autostarts immediately in normal library use. Only `add_from_output_files(..., autostart=True)` is deferred until the source node completes.

The helper defaults to `file_param="output_file"`, `pattern="*"`, and absolute file paths. You can also use `recursive=True`, `path_mode="relative"`, or `path_mode="name"`.
