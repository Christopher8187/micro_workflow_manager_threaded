# micro-workflow-manager 0.2.5

A small file-backed DAG workflow manager. Each node has inspectable `input/`, `output/`, and `jobs/` folders, one main task, optional fallbacks, explicit starter jobs, and APIRouter-style node modules.


## Client-facing filesystem architecture

MWF 0.2.5 encourages node behavior files to describe their filesystem contract
next to the router. A task should read like workflow logic, while reusable
filesystem objects hold the stable information about where data comes from,
where it is written, and which downstream node receives it.

The four standard objects are:

- `InputFileSystem`: the current node's read-only `input/` folder.
- `OutputFileSystem`: the current node's persistent `output/` folder.
- `JobFileSystem`: files returned by one job in `jobs/<id>/files/`.
- `NodeInputFileSystem`: another node's `input/` folder and job-creation route.

Each declaration has a human-readable label, an optional portable base-path
template, and an encoding. `NodeInputFileSystem` also records the destination
node. The object is only a declaration at import time; it resolves paths lazily
when bound to a `JobContext`, so the same node file works on Windows and Linux.

A representative node behavior file is:

```python
from micro_workflow_manager import (
    InputFileSystem,
    NodeInputFileSystem,
    NodeRouter,
    OutputFileSystem,
)

router = NodeRouter("add_numbers", max_threads=2)

INPUT = InputFileSystem("number input")
OUTPUT = OutputFileSystem("sum output", base="{batch}")
REVIEW_INPUT = NodeInputFileSystem(
    "review",
    "review input",
    base="{batch}",
)


@router.task(timeout=60)
def add_numbers(ctx, batch, source_file):
    # Load input through the declared filesystem contract.
    numbers = INPUT.file(ctx, source_file).read_json()

    ctx.checkpoint("numbers loaded", timeout=20, progress=0.25)

    total = sum(numbers)

    ctx.checkpoint("sum calculated", timeout=20, progress=0.75)

    # Write output and carry it forward through filesystem objects.
    result = OUTPUT.file(ctx, "sum.json", batch=batch)
    result.write_json({"total": total})

    review_copy = REVIEW_INPUT.file(ctx, "sum.json", batch=batch)
    review_copy.copy_from(result, overwrite=True)
    REVIEW_INPUT.add_job(
        ctx,
        batch=batch,
        result_file=review_copy.relative_path,
    )

    return {"total": total}
```

This structure is deliberate:

1. Imports state the external tools and MWF concepts used by the node.
2. The router and filesystem declarations state the node's execution and data
   contract before any task code.
3. The task loads named inputs, performs domain subtasks, reports optional
   checkpoints, then writes and routes named outputs.
4. Helper functions can accept `FileSystemEntry` objects instead of rebuilding
   project paths or calling low-level context methods repeatedly.

### Binding and templates

A filesystem object's `base` may use simple `str.format` placeholders:

```python
PAGES = OutputFileSystem("rendered pages", base="{book_name}")
page = PAGES.file(ctx, "page_001.png", book_name=book_name)
```

`page` is a `FileSystemEntry`. It is path-like, so it can be passed to most
libraries that accept `str`, `Path`, or `os.PathLike`, while also providing
workflow-aware methods:

```python
page.exists()
page.read_bytes()
page.write_bytes(data)
page.copy_to(destination, overwrite=True)
page.parent.mkdir()
PAGES.files(ctx, "*.png", book_name=book_name)
```

All relative paths are normalized to portable `/` form and reject absolute paths
and `..`. Managed writes through `write_text`, `write_bytes`, `write_json`,
`append_text`, `copy_from`, and `delete` use MWF's execution-generation guards.
This prevents a restarted or timed-out stale attempt from committing through the
framework.

The `.path` property and writable `.open()` are available for third-party
libraries that require an ordinary filesystem path. Direct writes made by such
a library cannot be rolled back or fenced for the full duration of the open
handle, so prefer the managed methods when possible and use checkpoints around
long external operations.

### Naming downstream filesystem types

For a frequently used destination, a project may give its route a domain name:

```python
class ReviewInputFileSystem(NodeInputFileSystem):
    def __init__(self):
        super().__init__("review", "review input")

REVIEW_INPUT = ReviewInputFileSystem()
```

This is optional. A plainly named instance such as
`REVIEW_INPUT = NodeInputFileSystem("review", "review input")` is usually the
smallest and clearest form.

A complete runnable version of the simple addition example is included in
`examples/filesystem_objects`.

### Compatibility and philosophy

The original `ctx.input_path()`, `ctx.write_output()`, `ctx.write()`, and
`ctx.node()` methods remain supported. Filesystem objects are the recommended
client-facing architecture, not a forced migration or a second storage system.
They are thin declarations over the same file-backed storage, scheduler guards,
transactions, and downstream job APIs, so they do not add project scans or
per-file background work.

## Explicit graph synchronization

The graph definition and the top-level `node/` folders are synchronized only by
the `graph` command. Ordinary commands such as `run`, `runfrom`, `clean`, and
`monitor` do not silently add or remove node folders.

Set the graph the first time:

```bash
mwf graph src/graph.py
```

After editing edges or renaming, adding, or removing nodes, preview and then
explicitly apply the new graph state:

```bash
mwf graph --update --dry-run
mwf graph --update
```

`mwf graph --update` uses the graph path already stored in `.mwf`. Relative graph
paths are stored with `/`, even on Windows. When reading an older or manually
edited project, MWF accepts both `src/graph.py` and `src\graph.py`, resolves the
path inside the project root, and rewrites it to the portable `/` form on the
next update. It creates folders for new nodes and permanently deletes folders
for nodes no longer in the graph, including their inputs, outputs, jobs, and state. Back up or move any data
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

Use a `NodeInputFileSystem` to make the destination visible at the top of the
node file and to route both files and job parameters:

```python
from micro_workflow_manager import NodeInputFileSystem, OutputFileSystem

OUTPUT = OutputFileSystem("split pages")
TAGIFY_INPUT = NodeInputFileSystem("tagify", "tagify page input")


@router.task
def split(ctx):
    page = OUTPUT.file(ctx, "page_001.txt")
    page.write_text("page text")

    incoming = TAGIFY_INPUT.file(ctx, page.name)
    incoming.copy_from(page, overwrite=True)
    TAGIFY_INPUT.add_job(ctx, page_file=incoming.relative_path)
```

The downstream node reads `page_file` with its own `InputFileSystem`. Job
creation remains explicit, so preparing a file never silently invents work.
`ctx.transaction()` and idempotency keys continue to work because
`NodeInputFileSystem.add_job()` delegates to the same guarded `NodeHandle.add()`
operation.

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


## Health checks, inspection, and job history

Run a read-only project check before a long workflow or after changing files:

```bash
mwf doctor
```

`mwf doctor` compares the graph, node folders, and router files; checks important
JSON state; reports stale active-run records and abandoned running jobs; and warns
about simple literal `ctx.node("B")` calls without a declared edge. It also warns
when MWF-owned metadata should be upgraded with `mwf migrate`. It does not repair
or modify the project. Errors produce a nonzero exit status.

Use `inspect` when you need an explanation rather than a raw directory listing:

```bash
mwf inspect A
mwf inspect A job 3
```

Node inspection explains readiness, blockers, status counts, strongly connected
component membership, runner, total timeout, checkpoint timeout, and fallbacks.
Job inspection additionally shows the current/last handler, named checkpoint,
checkpoint deadline, progress percentage, progress detail, execution generation,
child jobs, and chronological lifecycle events. Explicit checkpoints and
supervised handlers use a small job-local `runtime.json`; this is scheduler
diagnostic state, not task output or a provenance manifest.

Each job also has an append-only `events.jsonl` file containing small records
such as `created`, `started`, `fallback_started`, `timeout`, `restart_requested`,
and `done`. `output.json` and job-local output files remain the actual task
result.

## State schema migration and read-only previews

MWF-owned metadata includes an explicit `schema_version`. This applies to files
such as `.mwf`, `.mwf_run.json`, `node_state.json`, `schema.json`, `job.json`,
`status.json`, `execution.json`, `runtime.json`, and the rebuildable job index. It does not apply
to `input.json`, `output.json`, returned files, or `events.jsonl`.

Preview and apply an upgrade from an older project:

```bash
mwf migrate --dry-run
mwf migrate
```

Migration is additive and atomic per metadata file. MWF remains able to read
older unversioned state long enough to migrate it, but refuses state that claims
a newer schema than the installed package supports.

Several destructive commands support a read-only preview:

```bash
mwf graph --update --dry-run
mwf clean A --dry-run
mwf reset A --dry-run
mwf wipe A --dry-run
mwf recover --dry-run
mwf restart wait job 4 --dry-run
```

Execution commands provide `--plan` instead of pretending to run:

```bash
mwf run A --plan
mwf runfrom A --plan
mwf resume A --plan
mwf resumefrom A --plan
```

A plan prints the selected nodes and jobs, reset-versus-resume semantics, detected
static autostarts, external blockers, and current status counts. It does not claim
the active-run slot or change state. Dynamic jobs created by task functions are
reported as runtime-dependent rather than guessed.

## Resume and crash recovery

A CLI-owned run records its process ID, hostname, command, selected nodes, MWF
version, and a lightweight heartbeat in `.mwf_run.json`. The same single
scheduler-supervisor thread that manages timeout deadlines updates this run
heartbeat. Run liveness and job progress remain separate signals: the run
heartbeat proves the scheduler process is alive, while a job checkpoint proves
that one handler reached a progress boundary. Normal scheduling does not scan
the project for liveness.

If the owning process has crashed, recover abandoned `running` jobs without
resetting completed work:

```bash
mwf recover --dry-run
mwf recover
```

Recovery refuses to compete with a demonstrably live owner. For each abandoned
job it advances the execution generation before requeueing it, so a late stale
process cannot commit afterward. Jobs already marked `done`, `skipped`, or
`failed` are not reset by recovery.

Continue a failed partial run while preserving successful jobs:

```bash
mwf resume B
mwf resumefrom A
```

`resume` continues one node. `resumefrom` continues that node and its descendants.
Both preserve `done` and `skipped` jobs and their outputs, leave queued jobs
available, and requeue only failed, cancelled, or abandoned-running jobs. By
contrast, `run` and `runfrom` retain their fresh-reset behavior.

## Centralized checkpoint watchdog, progress, and total timeouts

MWF has a total handler timeout and dynamic checkpoint deadlines. Declare the
hard upper bound with `timeout=` on the task or fallback, then choose the maximum
allowed silence for each section in task code:

```python
from micro_workflow_manager import NodeRouter

router = NodeRouter("wait")

@router.task(timeout=300)
def wait(ctx):
    ctx.checkpoint(
        "preparing request",
        timeout=20,
        progress=0.1,
        detail="building parameters",
    )
    prepare()

    ctx.checkpoint(
        "waiting for service",
        timeout=90,
        progress=0.25,
    )
    call_service()

    ctx.checkpoint(
        "saving result",
        timeout=15,
        progress=0.8,
    )
    save_result()
    return "finished"
```

Each `timeout=` passed to `ctx.checkpoint()` means the handler must either finish
or reach another checkpoint before that many seconds pass. Reaching a checkpoint
refreshes the scheduler-owned deadline. The task/fallback `timeout=` is still the
hard upper bound for the whole attempt. The older router/task
`checkpoint_timeout=` default remains accepted for compatibility, but dynamic
checkpoint deadlines in task code are preferred.

`progress` is a fraction from `0` to `1`. `detail` and the checkpoint name are
optional human-readable values displayed by:

```bash
mwf inspect wait job 3
```

All configured total/checkpoint deadlines are managed by one workflow-owned
scheduler supervisor using a deadline heap. There is no timer thread per job and
no repeated scan of every job folder. Untimed handlers without checkpoints keep
the original direct invocation path. An explicit progress checkpoint on an
untimed handler writes only that job's `runtime.json` on demand.

When a watchdog deadline expires, MWF sets the attempt's cancellation fence,
records one timeout event, wakes the normal fallback/retry path, and prevents the
abandoned handler from using MWF-managed writes or downstream-job creation.
Python still cannot force-kill an arbitrary thread blocked inside an external
library, so external request timeouts remain useful and direct side effects made
outside `ctx` helpers cannot be rolled back. The process runner can isolate such
code more strongly.

`ctx.raise_if_cancelled()` checks restart/timeout state without reporting
progress. `ctx.sleep(seconds)` checks cancellation in short intervals but does
not fabricate progress checkpoints.

## Idempotent and transactional downstream jobs

For a single downstream creation that may be retried, provide an idempotency key:

```python
@router.task
def A(ctx):
    return ctx.node("B").add(value=4, idempotency_key=f"A:{ctx.job_id}:B")
```

The same target node and key return the existing job instead of creating a
duplicate. For several downstream jobs, stage them until a block succeeds:

```python
@router.task
def A(ctx):
    with ctx.transaction():
        first = ctx.node("B").add(value=1)
        second = ctx.node("B").add(value=2)
    return [first.job_id, second.job_id]
```

Only `ctx.node(...).add(...)` operations are staged. If the block raises, none are
created. Successful commits use deterministic per-parent-and-operation keys, so retries,
resume, and manual restart generations complete a partially committed transaction
without duplicate jobs. This is
opt-in; ordinary downstream creation retains its existing fast path.

## Cleanup previews

The cleanup commands support `--dry-run` and preserve their existing semantics:

```bash
mwf clean A --dry-run   # would remove jobs/output, keep input
mwf reset A --dry-run   # would keep jobs/input and requeue all jobs
mwf wipe A --dry-run    # would remove jobs/output/input
```

The preview resolves `*` and validates node names but does not remove files or
change statuses.

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
mwf restart wait job 42
```

Several currently running jobs may be selected with IDs and ranges:

```bash
mwf restart wait jobs 42 57 80-82
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
mwf run wait job 42
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
keeps high-fan-in spawn nodes such as `combine` from failing just because many
workers touched the same summary file at once.

## Install, uninstall, and persistence

Use a project-local virtual environment so the package can be removed without
changing the system Python installation.

### Install from source for development

From the framework source directory containing `pyproject.toml`, create and
activate a virtual environment, then install an editable development copy:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[test]"
```

On Linux or WSL, activate with `source .venv/bin/activate` instead.

An editable installation points Python at the source directory, so code changes
are visible immediately. Use this form while developing MWF itself.

### Build a wheel

A wheel is an installation-ready `.whl` package. Build one from the framework
source directory containing `pyproject.toml`:

```powershell
python -m pip install --upgrade build
python -m build --wheel
```

The wheel is written to `dist/`. For version 0.2.5 the expected filename is:

```text
micro_workflow_manager-0.2.5-py3-none-any.whl
```

`py3-none-any` means the package is pure Python, supports Python 3, and does not
contain operating-system-specific compiled code.

To build both a wheel and a source archive, run:

```powershell
python -m build
```

This creates the wheel and a `.tar.gz` source distribution under `dist/`.

### Install from a wheel

Install the wheel by giving pip its actual file path. From the framework source
directory after building:

```powershell
python -m pip install --force-reinstall .\dist\micro_workflow_manager-0.2.5-py3-none-any.whl
```

From Linux or WSL:

```bash
python -m pip install --force-reinstall ./dist/micro_workflow_manager-0.2.5-py3-none-any.whl
```

If the wheel is in Downloads or another directory, use its full path:

```powershell
python -m pip install --force-reinstall "C:\path\to\micro_workflow_manager-0.2.5-py3-none-any.whl"
```

Do not write `.micro-workflow-manager==0.2.5`; that is interpreted as a malformed
package requirement rather than a file path. On PowerShell, a file in the
current directory begins with `.\`, and the wheel filename uses underscores.

Verify the installed version, module location, and CLI:

```powershell
python -c "import micro_workflow_manager; print(micro_workflow_manager.__version__); print(micro_workflow_manager.__file__)"
mwf --help
```

A project can bundle the wheel in a directory such as `vendor/` and reference it
from `requirements.txt`:

```text
./vendor/micro_workflow_manager-0.2.5-py3-none-any.whl
```

Then users can install the project and its framework together from the project
root:

```powershell
python -m pip install -r requirements.txt
```

### Uninstall and persistence

The package installs no Windows service, daemon, scheduled task, registry entry,
or background process. Runtime state stays in the project (`.mwf`, `node/`,
`.mwf_locks/`, and `.mwf_run.json`). Stop any active `mwf run`, `mwf runfrom`, `mwf resume`, `mwf resumefrom`,
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

Run the ordinary suite without combining the timing-sensitive cyclic tests:

```bash
python -m pytest -q --ignore=tests/test_autostart_cycles.py
```

Run every cyclic-autostart test in its own process with an extended outer timeout:

```bash
python -m pytest -q tests/test_autostart_cycles.py::test_runfrom_supports_self_and_mutual_autostart_cycles_before_downstream
python -m pytest -q tests/test_autostart_cycles.py::test_threaded_diamond_cycle_spawns_100_seed_jobs_without_deadlock
python -m pytest -q tests/test_autostart_cycles.py::test_threaded_ring_cycle_spawns_100_seed_jobs_without_deadlock
python -m pytest -q tests/test_autostart_cycles.py::test_threaded_stochastic_game_engine_spawn_cycle_finishes
```

Run the marked long stress test explicitly:

```bash
python -m pytest -q -m stress tests/test_markov_chain_stress.py
```

## Checkpoint API

A supervised task can report progress and set the deadline for its next section:

```python
@router.task(timeout=300)
def work(ctx):
    ctx.checkpoint("request started", timeout=60, progress=0.2)
    result = call_service()
    ctx.checkpoint("response received", timeout=20, progress=0.8, detail="validating")
    return result
```

`JobContext.checkpoint()` accepts `name`, `timeout`, `progress`, and `detail`.
The `timeout` value means that the handler must either finish or reach another
checkpoint before that many seconds pass. Progress is a number from 0 through 1
and is shown by `mwf inspect NODE job ID`. The total task/fallback `timeout=`
keeps the handler on the centralized scheduler-supervised path; checkpoint
timeouts may then be chosen dynamically in task code.
