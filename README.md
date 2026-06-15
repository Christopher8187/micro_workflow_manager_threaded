# micro-workflow-manager

A small file-backed DAG workflow manager. Each node has inspectable `input/`, `output/`, and `jobs/` folders, one main task, optional fallbacks, and APIRouter-style node modules.

## Important workflow rule

A finished job is not the same thing as a finished node. This matters for autostarted jobs: a downstream node can receive and finish one job before all of its predecessor nodes have completed and before those predecessors have finished creating every downstream job. The library only marks a node `done` during node-level finalization after its predecessors are complete.

## Runners

The default runner is now `threaded`.

```bash
mwf graph src/graph.py --runner threaded
mwf runfrom start_node
```

`threaded` is dependency-free and uses Python's local thread pool. It runs:

- multiple queued jobs inside the same node at the same time, capped by that node's `max_threads`
- multiple ready nodes at the same time, while still respecting DAG predecessor completion
- newly-ready downstream nodes while unrelated nodes are still running

Other runners:

```bash
mwf graph src/graph.py --runner direct
```

`direct` is best for step-by-step debugging.

```bash
mwf graph src/graph.py --runner prefect
```

`prefect` is still available as an optional compatibility runner, but the local `threaded` runner is recommended for this package.

## Install for development

```bash
pip install -e .[test]
```

Optional Prefect compatibility:

```bash
pip install -e .[prefect]
```

## Run tests

```bash
pytest
```
