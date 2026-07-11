# micro-workflow-manager

A small file-backed DAG workflow manager. Each node has inspectable `input/`, `output/`, and `jobs/` folders, one main task, optional fallbacks, explicit starter jobs, and APIRouter-style node modules.

## Explicit graph synchronization

The graph definition and the top-level `node/` folders are synchronized only by
the `graph` command. Ordinary commands such as `run`, `runfrom`, `clean`, and
`monitor` do not silently add or remove node folders.

Set the graph the first time:

```bash
mwf graph src/graph.py
```

After editing edges or renaming, adding, or removing nodes, explicitly apply the
new graph state:

```bash
mwf graph --update
```

`mwf graph --update` uses the graph path already stored in `.mwf`. It creates
folders for new nodes and permanently deletes folders for nodes no longer in the
graph, including their inputs, outputs, jobs, and state. Back up or move any data
you need before updating. If an ordinary command detects changed edges, missing
new folders, or stale renamed folders, it exits with an instruction to run the
update and leaves the disk unchanged.

A leftover `node_behavior/*.py` file whose router name is no longer in the graph
is ignored; importing the project will not recreate that old node folder.

## Compact directed fans in `graph.py`

A lowercase name can represent one node and an uppercase variable can represent
a group. Put a collection on one side of an edge to express an `a-B` fan-out or
an `A-b` fan-in:

```python
A = ["extract_text", "extract_images"]
B = ["jsonify", "index"]

EDGES = [
    ("split", B),   # split -> jsonify, split -> index
    (A, "merge"),   # extract_text -> merge, extract_images -> merge
]
```

The explicit helper form is also supported:

```python
from micro_workflow_manager import fan

EDGES = [
    fan("split", ["jsonify", "index"]),
    fan(["extract_text", "extract_images"], "merge"),
]
```

A collection on both sides is rejected because that would describe a complete
bipartite graph rather than one directed fan.

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

### Same-component autostart scheduling

Inside a cyclic strongly connected component, `autostart=True` means "enqueue the child job and wake the component scheduler". It does not recursively run the child job inside the parent job thread. This avoids cyclic autostart deadlocks where every worker slot is held by parent jobs trying to synchronously run children from the same communicating class.

For acyclic edges, normal autostart behavior is preserved. For CLI `mwf run` / `mwf runfrom`, the selected run set is still protected: dynamic autostarts outside the approved nodes are blocked, while jobs created inside the selected cyclic component are pumped until the whole component is quiescent.

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

## Restart one running job from a second terminal

When an individual job is hung inside an active `mwf run` or `mwf runfrom`
sequence, keep the original terminal running and use the dedicated restart
command from a second terminal in the same project:

```bash
mwf restart explode job 42
```

Several currently running jobs may be selected with IDs and ranges:

```bash
mwf restart explode jobs 42 57 80-82
```

`mwf restart` does not start another scheduler and does not replace the active
`.mwf_run.json` record. It atomically advances the selected job's execution
generation before clearing job-local `output.json` and `files/`. The scheduler
that already owns the larger run sees the new generation and immediately starts
the replacement attempt. The node remains active throughout this handoff, so it
cannot be finalized merely because the abandoned attempt stopped being current.
The original `job.json` and `input.json` are preserved.

An older generation is fenced from committing its final status, returned files,
`ctx.write(...)`, `ctx.write_output(...)`, and `ctx.node(...).add(...)` effects.
If it finishes while the restart command is preparing the replacement, its stale
completion is discarded. The command only accepts a job that is still `running`,
belongs to a live active run, and has a live execution lease; it refuses rather
than creating an orphan queued job when the old attempt has already completed.
Ordinary `mwf run` and `mwf runfrom` commands also refuse to start a competing
sequence while another one owns the project.

Python cannot safely force-kill an arbitrary thread that is blocked inside a
third-party HTTP request or native library. From MWF's point of view the old
generation is invalid immediately and the replacement begins, but the underlying
old call may continue until its own timeout or return. External side effects and
direct filesystem writes performed outside MWF's context helpers cannot be
rolled back. Long custom loops may call `ctx.checkpoint()` between expensive
operations to exit promptly after a restart. Process-runner attempts are fenced
in the same way; an abandoned daemon thread disappears when its worker process
returns.

Use ordinary selected-job rerun syntax after the larger workflow has ended:

```bash
mwf run explode job 42
```

## Large-node performance note

For large nodes, `queued` is the implicit per-job status. A job with `job.json` and `input.json` but no `status.json` is treated as queued by the storage API. This avoids thousands of small JSON writes during reset/requeue and lets `node_state.json` switch to `running` before the runner loads every queued job. Explicit `status.json` files are still written for `running`, `done`, `failed`, `cancelled`, and `skipped` jobs.

### Job index design

`job_index.json` is a rebuildable per-node summary cache, not the source of truth.
The source of truth is still the file-backed job state:

- `jobs/<id>/job.json` and `input.json` prove that a job exists
- missing `status.json` means the job is queued
- explicit `status.json` stores running/done/failed/cancelled/skipped
- `queued/<id>.queued` is the cheap scheduler queue marker

The index stores fast monitor/scheduler summaries such as status counts,
`last_job_id`, running job IDs, and completed-duration totals. It is maintained
incrementally during normal runs, but if Windows or another process temporarily
blocks `job_index.json`, the workflow marks the index dirty and continues. The
next reader rebuilds it from the authoritative job folders/status files. This
keeps high-fan-in spawn nodes such as `zoning` from failing just because many
workers touched the same summary file at once.

## Install, uninstall, and persistence

Use a project-local virtual environment so the package can be removed without
changing the system Python installation:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[test]"
```

The package installs no Windows service, daemon, scheduled task, registry entry,
or background process. Runtime state stays in the project (`.mwf`, `node/`,
`.mwf_locks/`, and `.mwf_run.json`). Stop any active `mwf run`, `mwf runfrom`,
or `mwf monitor` process before uninstalling, especially on Windows where an
active `mwf.exe` launcher can be locked.

```powershell
python -m pip uninstall micro-workflow-manager
```

Deleting the project-local `.venv` removes the entire isolated installation as
an alternative. Deleting the Python package does not delete workflow project
data; remove `.mwf`, `node/`, `.mwf_locks/`, and `.mwf_run.json` separately only
when you intentionally want to remove that data.

If an older interrupted pip operation reports an invalid distribution such as
`~icro-workflow-manager`, close all Python/MWF processes and remove only the
stale temporary entries from that virtual environment, then reinstall or
uninstall normally:

```powershell
Get-ChildItem .\.venv\Lib\site-packages -Force |
  Where-Object { $_.Name -like "~icro*" } |
  Remove-Item -Recurse -Force
Remove-Item .\.venv\Scripts\mwf.exe -Force -ErrorAction SilentlyContinue
python -m pip install --force-reinstall .
python -m pip uninstall micro-workflow-manager
```

## Run tests

```bash
pytest
```

Run the marked long stress test explicitly:

```bash
pytest -m stress tests/test_markov_chain_stress.py
```
