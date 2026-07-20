# micro-workflow-manager 0.3.16

A small hybrid file/SQLite DAG workflow manager. User payloads stay inspectable in `input/`, `output/`, and `jobs/<id>/`, while high-churn scheduler state is stored transactionally in `.mwf/state.sqlite3`. Each node has one main task, optional fallbacks, explicit starter jobs, and APIRouter-style node modules.


## Design requirement: durable result and debugging provenance

The `output/` folder is not only a place for the final value. A well-designed
node writes both the durable result that can be reused or reformatted **and**
user-owned provenance that makes the result easier to debug and improve. Useful
provenance includes the relevant inputs, algorithm/model/tool choice, attempt or
fallback, validation evidence, and important parameters. Framework diagnostics in `.mwf/state.sqlite3` explain scheduler behavior and are shown by `mwf inspect`; they do not replace domain provenance written by the project.

See [DESIGN.md](DESIGN.md) for design and code-architecture recommendations,
command workflows, provenance guidance, and runnable examples covering adapted
`src/` + `utils/` pipelines, five common agentic patterns, a database change
manager, and a Pygame state machine.

## What changed in 0.3.16

- `mwf clean`, `mwf reset`, and `mwf wipe` now treat a Hoeflein component as
  the indivisible cleanup unit. Naming one member expands to every member of
  that component; DAG nodes remain singleton components.
- Nodes may declare an intra-component waiting gate with `waiting=True` and
  `wait_for=...`. Queued jobs remain durably queued, but the node displays as
  `waiting` and no new node pump starts until the selected peers have no queued
  jobs left. A pump that already started continues normally.
- `wait_for=None` with `waiting=True` means all other vertices in the component.
  A list selects a subset. Waiting targets outside the component are rejected.
- Waiting on a singleton DAG component is allowed but has no effect; CLI loading
  prints a reminder that ordinary DAG predecessor readiness is the available
  queue-independent mechanism.
- Mutual waiting cycles use a deterministic one-pump bootstrap only when a
  resumed component has queued work on every side and no active pump can change
  the queues. Normal waiting gates resume immediately after that bootstrap.

### Waiting-node example

```python
from micro_workflow_manager import NodeRouter

router = NodeRouter(
    "router",
    runner="threaded",
    waiting=True,
    wait_for=["worker_a", "worker_b"],
)

# Equivalent fluent forms:
# router.wait_for_nodes("worker_a", "worker_b")
# router.wait_for_component()  # every other component member
```

Waiting is a node-pump admission rule, not a job-status rewrite. Jobs stay
`queued` in SQLite so reset/resume semantics remain unchanged; `mwf monitor`
shows the node lifecycle state as `waiting` and includes `waiting_on` in JSON.

## What changed in 0.3.15

- MWF now owns one process-wide pooled `httpx.AsyncClient`. Synchronous API tasks
  call `shared_http_transport` and suspend cooperatively on the existing fiber
  runtime; projects no longer need to embed an asyncio thread/client bridge.
- Framework HTTP waits are explicit scheduler states. A bounded live transport
  request suspends checkpoint-progress expiry, while the task total timeout and
  the HTTP transport timeout remain active. This prevents synchronized watchdog
  cancellation waves when hundreds of model requests are legitimately waiting.
- Timed `concurrent.futures.Future.result(timeout=...)` retains its periodic
  timeout behavior inside fibers, so heartbeat loops continue to run.
- Fiber admission occurs in bounded bursts with scheduler servicing between
  bursts. Starting thousands of jobs cannot starve earlier fibers for an entire
  checkpoint window.
- `ctx.sleep()` is cooperative in API fibers. Per-node API limits may be set into
  the thousands; there is no workflow-wide aggregate API cap.

### Framework-owned HTTP transport

```python
from micro_workflow_manager import shared_http_transport

payload = shared_http_transport.post_json(
    "https://api.example.com/v1/chat",
    headers={"Authorization": "Bearer ..."},
    json={"model": "...", "messages": [...]},
    timeout=(30, 1800),
    heartbeat_callback=lambda elapsed: ctx.checkpoint(
        f"model request active for {elapsed:.0f}s", timeout=90
    ),
    heartbeat_interval=15,
    wait_name="model request",
)
```

The transport uses `httpx` connection pooling and integrates directly with the
scheduler watchdog. The checkpoint lease is suspended only while the bounded
network operation is active.

## What changed in 0.3.12

- API-runner node pumps now refill from jobs committed after the pump starts. A
  typed handler that begins with one routed job can grow toward its configured
  concurrency while that first API request is still running, instead of waiting
  for the initial static queue snapshot to finish.
- Refreshable queued-job sources follow SQLite row insertion order, so concurrent
  batch producers that reserve lower job IDs but commit after a higher range are
  still discovered. Non-API runners retain the existing snapshot behavior and
  deterministic job-ID ordering.
- Added component and CLI `--monitor` regressions for gradual high-fanout routing,
  including the former state where a handler displayed hundreds queued but only
  one running.

## What changed in 0.3.11

- Hoeflein components now use live node pumps. While one member is still
  processing its queue, the scheduler polls idle sibling queues and starts them
  as soon as internal component jobs appear. A fast handler can drain and be
  restarted repeatedly while a long-running router continues producing work.
- Monitor rows now derive their display state from actual per-node job counts.
  Queued component work is not reported as running before execution, idle
  handlers remain queued while a router starts, and a handler with active jobs
  is shown running even if a concurrent component refresh wrote a broader node
  lifecycle state.
- The adaptive threaded runner now exits after the first worker failure even
  when its lazy source still contains unclaimed jobs. This fixes active runs
  that remained stuck at `status=running`, `running=0`, with queued jobs left.
- Windows safe-path validation now treats ordinary and extended-length
  (`\\?\\`) spellings of the same resolved path as equivalent while retaining
  the same descendant-only security check.
- Fresh producer-scoped cleanup rewinds the quiescent target's job allocator so
  deterministically recreated jobs keep their previous tail IDs.

## What changed in 0.3.10

- Added a high-fanout batch API: `NodeHandle.add_many`,
  `NodeInputFileSystem.add_jobs`, and `NodeInputFileSystem.write_jsons`. One
  downstream job is still created per parameter object, but payload files and
  SQLite metadata are registered in batches rather than through one global lock
  and transaction sequence per object.
- Added transactional per-node job-ID sequences. Existing 0.3.9 databases are
  migrated in place by initializing each sequence to `MAX(job_id) + 1`; existing
  jobs, statuses, events, and payload folders are preserved.
- Batch registration reserves IDs briefly, prepares disjoint payload files
  outside the database transaction, and commits jobs, creation events,
  idempotency rows, and node status in one transaction. Concurrent producers are
  safe and duplicate idempotency keys resolve to the existing jobs.
- Deterministic `overwrite=True` node-input batches use atomic replacement
  without a global advisory lock. `overwrite=False` retains locked unique-name
  allocation.
- Added separate-component high-fanout regressions proving that a producer can
  queue hundreds of jobs for a downstream Hoeflein component without autostarting
  it or merging the two components.

## What changed in 0.3.9

- Framework-created API, threaded-runner, Hoeflein node, scheduler-supervisor,
  and inline monitor threads now close their per-thread SQLite connection when
  the thread or job finishes. Dead thread identifiers cannot inherit an older
  connection if Python later reuses the numeric thread ID.
- Same-process SQLite writers are serialized before `BEGIN IMMEDIATE`. SQLite
  still provides cross-process coordination, but hundreds of local workers no
  longer enter `busy_timeout` together. Commit failures now always roll back so
  a persistent connection cannot retain a write transaction and poison later
  scheduler rounds.
- Checkpoint runtime persistence is one conditional `UPDATE` instead of a
  database advisory-lock acquire, runtime update, and advisory-lock release.
  Late checkpoints from a timed-out or restarted watch still cannot overwrite
  its terminal runtime state.
- Generation/restart-fenced file mutations now use per-job operating-system file
  locks in `.mwf/execution-fences/`. This preserves second-terminal `restart`
  ordering without writing SQLite advisory-lock rows around every file write.
- Repeated high-concurrency API rounds and repeated CLI `run --monitor` rounds
  now have regression coverage for connection cleanup, runtime writes, file
  fencing, database integrity, and progressive slowdown.

## What changed in 0.3.8

- `mwf run NODE` is again a true fresh run even when NODE's jobs were created by
  an external predecessor. Every remaining job in the explicitly selected
  Hoeflein start component is requeued and its generated output/files are
  cleared before scheduling.
- `mwf runfrom NODE` applies that full reset to the selected start component,
  then keeps producer-scoped cleanup for descendant merge components. Work from
  unselected incoming branches is still preserved exactly as before.
- `mwf resumefrom NODE` automatically generation-fences and requeues failed,
  cancelled, and abandoned-running jobs throughout the selected descendant set.
  A separate `mwf restart` step is not required after a completed partial run.
- `mwf restart` is now strictly a second-terminal control for a live `run`,
  `runfrom`, `resume`, or `resumefrom` sequence. It may replace a running attempt
  or requeue a failed/cancelled job owned by that active sequence, but it no
  longer edits failed jobs after the sequence has ended. Use `resume` or
  `resumefrom` for post-failure continuation.
- Fresh-run, branch-preservation, resumefrom, restart, and final run-state
  behavior are covered by repeated CLI regression tests using `--monitor`.

## What changed in 0.3.7

- SQLite advisory locks now record the owning host and process and immediately
  reclaim rows whose local owner process has exited. An interrupted or killed
  `mwf threads`/`mwf run` command therefore cannot strand `thread-overrides`
  behind its old 300-second lease and make the next run time out after 120
  seconds.
- A live local owner is no longer displaced merely because a long critical
  section exceeded the nominal lease. Unknown or remote owners still use the
  lease as the safe fallback.
- Run startup binds temporary thread overrides before publishing `run.json` as
  running. `mwf threads` serializes active-run discovery with that startup, so a
  concurrent command cannot accidentally scope its value to the following run.
  Run completion publishes its terminal state before best-effort override
  cleanup, so an override cleanup problem cannot leave a completed run falsely
  recorded as active.
- `mwf threads NODE VALUE` prints a resource warning above 256 in-flight jobs.
  CLI restart/timeout supervision can use roughly one controller and one handler
  thread per active job, so settings such as 750 can put severe pressure on
  Windows thread, memory, SQLite, socket, and API connection limits.

## What changed in 0.3.6

- Scheduling now uses **Hoeflein components**. Let `A ⊆ E` be the graph edges
  explicitly used with `autostart=True` in node behavior code. MWF constructs
  the augmented directed graph
  `G_H = (V, E ∪ {(v, u) : (u, v) ∈ A})` and takes its strongly connected
  components. The quotient keeps the direction of the original graph edges and
  is the scheduler DAG, `HDAG(G)`.
- Naming any node in a multi-node Hoeflein component with `mwf run`,
  `mwf runfrom`, `mwf resume`, or `mwf resumefrom` selects the whole component.
  MWF prints a reminder before execution. Every original graph edge whose ends
  lie in the same component is automatically treated as component-autostart,
  even when that particular `add(...)` call omits `autostart=True`.
- A Hoeflein component is one lifecycle unit: its nodes enter running together,
  become done together when quiescent, and become failed together if any job
  fails. Live node pumps stop accepting newly discovered sibling work after the
  first failure, and the component is published failed while active pumps wind
  down.
- Generated jobs retain their immediate parent node/job and also record a stable
  producer-component identity plus whether they are a `dag` or `component` job.
  Fresh runs remove only jobs produced by the selected Hoeflein components.
  Jobs produced by unselected branches are preserved with their status, input,
  output, returned files, and provenance.
- `mwf runfrom A` may process A's branch through a later merge component while
  another incoming branch is unfinished. A later `mwf runfrom B` removes and
  rebuilds only B-produced work; it does not delete A-produced jobs already in
  the shared descendant. By contrast, the *starting* component must have every
  external predecessor complete. For `A -> C` and `B autostarts C`, the quotient
  is `A -> {B,C}`, so `mwf run B` is refused until A has completed.
- `mwf inspect NODE job ID` now shows the producer component and the job kind.
  `--plan` explains component selection and producer-scoped cleanup.

## What changed in 0.3.5

- `mwf run NODE --monitor` and `mwf runfrom NODE --monitor` print the full
  timestamped monitor dashboard in the execution terminal. Inline snapshots do
  not clear task output; `--monitor-interval` controls their cadence. The same
  option is available on resume forms and selected-job runs.
- A terminal monitor snapshot reports `active run: none` after a sequence is
  done, blocked, incomplete, or failed, and separately identifies the last run.
- `AGENT.md` defines the required testing and failure-diagnosis protocol: focused
  reproduction, concurrency and timeout experiments, freeze classification,
  repeated command use, separate-process cyclic tests, test maintenance, and the
  rare stubborn-issue escalation format.
- Execution reporters are lifecycle-owned by the active run, preventing stale
  inline reporter threads or final snapshots that still describe a finished run
  as active.

## What changed in 0.3.4

- The synchronous execution stack is flatter. A runner worker is now the
  attempt controller and invokes retry/fallback orchestration directly. For a
  supervised or actively restartable attempt, it creates exactly one
  abandonable `mwf-handler-*` thread for the current user handler; the old
  worker -> attempt thread -> handler thread stack is gone.
- The new `api` runner is designed for blocking network/API/I/O jobs. It fills
  the requested concurrency immediately and intentionally keeps the familiar
  `max_threads` setting, where the value means maximum in-flight API jobs.
  `io` and `network` are accepted aliases.
- `mwf init` creates `.mwf/state.sqlite3`. Job identity and status, queue
  membership, node status, lifecycle events, retries/fallback diagnostics,
  execution generations, checkpoints, idempotency keys, default-job
  declarations, summary counts, and cross-process advisory locks now live in
  SQLite with WAL enabled.
- User data remains ordinary files: node `input/`, node `output/`, job
  `input.json`, job `output.json`, and returned `jobs/<id>/files/` are not moved
  into the database. Existing 0.3.3-and-earlier metadata is imported once and
  removed only after it is durable in SQLite.
- `mwf migrate --dry-run` remains read-only, while `doctor`, `monitor`,
  `inspect`, restart/recovery, cleanup, clipboard, deployment, process running,
  and filter-funnel inspection preserve their previous functionality against
  the new state backend.
- The generated Material Icon Theme settings no longer force the top-level
  `node` folder to use the `flow` icon. Exact graph-node folder names still use
  `flow`, and unrelated user associations remain untouched.


## What changed in 0.3.2

- When every fallback fails, the job output now records the **terminal fallback error** rather than the stale main-task error.
- This makes `mwf inspect <node-name> job <id>` agree with the final timeout/event that actually caused the job to fail.
- Existing task, retry, checkpoint, restart, deployment, and thread-override behavior is unchanged.


- `mwf inspect <node-name> failed` prints failed job IDs, concise errors, and the appropriate resume/resumefrom command; during a live sequence it also shows the second-terminal restart form.
- Extended CLI examples no longer use a node literally named `wait`; examples use neutral placeholders or simple operation names.

## What changed in 0.3.0

- Runtime `mwf threads` overrides are scoped to one workflow run. An override
  configured before a run is claimed by that next run and deleted when the run
  finishes; an override changed from a second terminal is deleted with the
  active run. Stale overrides from a crashed older run are ignored.
- `mwf restart` generation-fenced live attempts and originally allowed offline
  failed/cancelled requeueing. Version 0.3.8 reserves restart for a live
  second-terminal sequence; post-failure continuation now belongs directly to
  `mwf resume` and `mwf resumefrom`.
- `mwf deploy setup` explicitly prompts for the SSH port when `--port` is not
  supplied.
- `mwf init` merges Material Icon Theme settings into `.vscode/settings.json`
  and associates `.mwfignore` with the `routing` icon. Existing unrelated VS
  Code settings are preserved.


## Client-facing filesystem architecture

MWF 0.3.0 encourages node behavior files to describe their filesystem contract
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
They are thin declarations over the same hybrid storage, scheduler guards, transactions, and downstream job APIs. Payload paths remain files, while scheduler mutations use SQLite, so the declarations do not add project scans or per-file background work.

## Consolidated project runtime directory

Framework-owned project state is consolidated under `.mwf/`:

```text
.mwf/
  project.json       # graph path, stored edges, default runner, low-churn config
  run.json           # active/recent CLI ownership and scheduler heartbeat
  threads.json       # optional run-scoped node overrides and API total budget
  state.sqlite3      # jobs, queue, events, checkpoints, idempotency, advisory locks
  deploy/            # server setup and replaceable local deployment archive
```

SQLite uses WAL mode so `monitor` and `inspect` readers do not block the
scheduler's short writes. The database stores framework state only. Node
`input/`, node `output/`, each job's `input.json` and `output.json`, and returned
files remain ordinary files.

Projects from older releases are migrated automatically. Legacy `.mwf` root
JSON, `.mwf_run.json`, and `.mwf_threads.json` are consolidated; legacy
`.mwf_locks/`, `queued/`, `idempotency/`, `node_state.json`, `job.json`,
`status.json`, `execution.json`, `runtime.json`, `events.jsonl`, default-job
manifests, and job indexes are imported into SQLite when applicable. User
payload files are not rewritten. Migration is idempotent.

The generated `.gitignore` ignores `.mwf/` and legacy runtime-only paths under
node and clipboard snapshots. Direct files in
`clipboard/<node>/input/` and `clipboard/<node>/output/` remain trackable.

`mwf init` merges Material Icon Theme settings without replacing unrelated user
settings:

```json
{
  "workbench.iconTheme": "material-icon-theme",
  "material-icon-theme.files.associations": {
    ".mwfignore": "routing",
    "graph.py": "routing"
  },
  "material-icon-theme.folders.associations": {
    "clipboard": "archive",
    "input": "input",
    "output": "export",
    "jobs": "tasks",
    "queued": "queue",
    "idempotency": "keys"
  }
}
```

The top-level `node` folder deliberately keeps the icon theme's native icon.
After the graph is set, MWF associates each exact graph node name with `flow`.
Because Material Icon Theme associations are name-based, the same mapping styles
both `node/<name>/` and `clipboard/<name>/`. Install the Material Icon Theme VS
Code extension to see these associations.

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

`mwf graph --update` uses the graph path already stored in `.mwf/project.json`. Relative graph
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

## Hoeflein components and the quotient DAG

A finished job is not the same thing as a finished scheduling component. MWF
separates ordinary dependency direction from explicit autostart communication.
Let `G = (V, E)` be the project graph and let `A ⊆ E` contain exactly the edges
statically declared with `autostart=True`. Construct:

```text
G_H = (V, E union reverse(A))
reverse(A) = {(v, u) : (u, v) in A}
Hoeflein(G) = SCC(G_H)
HDAG(G) = G / Hoeflein(G)
```

The quotient edges come from the original `E`, not from the synthetic reverse
arcs. Therefore, with `A -> C` and `B autostarts C`, B and C form one component
and the quotient is:

```text
A -> {B, C}
```

There is no reverse dependency from `{B,C}` to A. Running B or C selects the
whole `{B,C}` component, but it is refused until A is complete.

Within one Hoeflein component, every original directed edge behaves as
component-autostart. Child work is queued and the component scheduler keeps one
live pump per active member node. It polls idle sibling queues while other pumps
are still running, so a newly routed handler job does not wait for the router's
entire queue to drain. Work is never executed recursively inside the parent
handler. Queued components remain queued before execution; once active, member
nodes quiesce and fail as one lifecycle unit. A node that has no jobs is
vacuously successful when the component's actual jobs all finish.

A partial `runfrom` deliberately permits a later merge component to process the
selected incoming branch while other incoming branches remain unfinished. That
component may reactivate when another producer creates new jobs later. Starting
component readiness is stricter: all external predecessors of the start
component must already be complete.

See `examples/autostart_cycle_lab` and the fan, K4, and C5 cyclic tests for
runnable communicating-component examples.

### Producer-component provenance and fresh-run cleanup

Every generated job records:

```text
parent.from_node
parent.from_job_id
producer_component
job_kind = dag | component
```

`component` jobs were generated inside their target Hoeflein component. `dag`
jobs crossed a quotient-DAG edge. Root/default jobs have no producer.

For a fresh `mwf run COMPONENT_MEMBER`, MWF first deletes every job produced by
that component, including internal component jobs and jobs it produced in later
components. It then requeues **every remaining job in the selected start
component**, including jobs created there by an external predecessor. For
`mwf runfrom`, that same full reset applies to the start component. Descendant
merge components rebuild selected-producer and root/default work while preserving
completed jobs produced by unselected incoming branches.

Example with ordinary edges `A -> C` and `B -> C`:

```bash
mwf runfrom A   # creates and completes A-produced jobs in C
mwf runfrom B   # preserves A-produced jobs; rebuilds only B-produced jobs
```

Node-level `output/` is preserved whenever a node still contains jobs from an
unselected producer, because that directory may contain shared debugging
provenance. Job-local outputs remain attributable and are removed only with the
job that owns them.

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

For high fan-out, batch publication without coarsening the downstream jobs:

```python
records = [(f"items/{i}.json", value) for i, value in enumerate(values, 1)]
TAGIFY_INPUT.write_jsons(ctx, records, overwrite=True)
TAGIFY_INPUT.add_jobs(
    ctx,
    [{"record_file": filename} for filename, _ in records],
    autostart=False,
    idempotency_keys=[f"record:{filename}" for filename, _ in records],
)
```

This still creates one `tagify` job per record. With `autostart=False`, the
producer and consumer remain separate Hoeflein components; the optimization is
only in file and SQLite registration.

## Deploying a filtered project copy

Deployment is explicit and uses a project-root `.mwfignore`, similar in spirit
to `.gitignore` and `.dockerignore`. Later rules override earlier rules and a
leading `!` re-includes a path. Server passwords are never stored.

Configure a server and create the default ignore file:

```powershell
mwf deploy setup
```

The default `.mwfignore` excludes Git/editor metadata, `.mwf/`, virtual
environments, Python caches, build output, and `.env` files. Review it before
every sensitive deployment. Password authentication uses PuTTY `pscp` and
`plink`; key authentication normally uses OpenSSH `scp` and `ssh`, while `.ppk`
keys use PuTTY. Setup stores connection metadata at `.mwf/deploy/server.json`.

Build a local deployment:

```powershell
mwf deploy local
```

This command deletes the previous `.mwf/deploy/local/` copy, filters the project
through `.mwfignore`, compresses every direct `node/<name>/` subfolder into its
own ZIP, and creates one outer `deployment.zip`. If a node subfolder contains no
ignored path, MWF zips it directly without staging every small file first. The
command prints ongoing copy/ZIP counts and final sizes. Rebuilding overwrites the
old local archive so repeated tests do not accumulate large deployments.

Upload and extract it on the configured server:

```powershell
mwf deploy remote
```

If no local deployment exists, MWF asks whether to build one. If one does exist,
it asks whether to deploy that archive or rebuild it first. It then asks for the
server destination path, uploads one compressed file, and uses remote Python to
extract the outer archive and each node archive. Files with matching paths are
overwritten; unrelated files already on the server are left in place.

Noninteractive setup fields are also available for scripts:

```powershell
mwf deploy setup --host 192.0.2.10 --user worker --port 22 --auth key --key C:\keys\server_key
mwf deploy remote --path /home/worker/simple_flow --yes
```

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

### API and blocking-I/O runner

Use `api` for blocking HTTP clients, SDK calls, database drivers, filesystem
waits, or other jobs whose wall time is mostly external latency:

```bash
mwf graph src/graph.py --runner api
mwf runfrom fetch_requests
```

```python
router = NodeRouter("fetch_requests", runner="api", max_threads=64)
```

For this runner, `max_threads=64` intentionally means at most 64 in-flight API
jobs **for that node**. API nodes also share a workflow-wide admission budget,
which defaults to 256. Therefore ten nodes declared at 100 do not create 1,000
simultaneous provider calls: they receive fair shares of the 256 slots and can
borrow capacity when sibling queues are light. The familiar `max_threads` name
is retained so router code, `mwf threads`, `monitor`, and `inspect` use one
concurrency vocabulary. Unlike the adaptive `threaded` runner, `api` fills its
available per-node and shared slots immediately. Executor threads are still
created lazily, and `io` and `network` are aliases.

### Synchronous controller and abandonable handler

The runner worker is the attempt controller. Retry, repeat, and fallback logic
runs synchronously in that controller. Normal untimed programmatic direct calls
execute the user handler in the caller thread. A timeout-supervised or
CLI-restartable attempt creates only one extra daemon handler thread:

```text
runner worker/controller -> one mwf-handler-* user thread
```

There is no intermediate `mwf-attempt-*` thread. If a timeout or manual restart
abandons the handler, generation fencing immediately prevents stale MWF-managed
writes and downstream job creation while the controller proceeds to the next
fallback, retry, or generation.

### Change a node's concurrency while testing

The router's `max_threads` value remains the readable source-code default. For
local testing, use `mwf threads` to apply a temporary project-local override
without editing the node behavior file or restarting an active workflow:

```bash
mwf threads                     # list declared, override, and effective values
mwf threads explode            # inspect one node
mwf threads explode 24         # set an absolute runtime limit
mwf threads explode +8         # add eight slots
mwf threads explode -4         # remove four slots
mwf threads explode reset      # return to the router declaration
```

The override is stored in `.mwf/threads.json`, which is ignored by the generated
`.gitignore`. It is deliberately temporary: an override set before execution
applies to the next run only, and an override changed during execution belongs
to that active run. MWF removes the override when the run finishes, including
failed runs. Stale values bound to an older crashed run are ignored and removed
when a new run claims the project.

For an active threaded or API node, an increase starts more queued jobs within
roughly 0.2 seconds. A decrease never cancels jobs already running; the runner
stops launching replacements until active concurrency falls to the new limit.

API values are cooperative fiber counts. They may be set into the thousands
without one controller or supervisor OS thread per request, and values from
multiple API nodes add together without an aggregate framework cap. Provider,
socket, memory, and rate limits still apply. Threaded and process runners retain
their OS-worker safety ceiling and warnings.

If a process is killed while changing the override, the next command detects
that the advisory-lock owner is no longer alive and immediately reclaims the
lock. It does not wait for the old five-minute lease to expire.

`mwf inspect NODE` shows the declared, overridden, and effective values.
`mwf monitor` shows the effective per-node value in its `threads` column, marks
runtime overrides with `*`, and prints cooperative API fiber totals with
`aggregate_limit=none`. Each API runner grows toward its own node limit; the
adaptive threaded runner grows toward its OS-worker limit. A process runner reads the override when its process pool is
created; an already-created process pool is not resized live. The direct runner
always remains at one job.

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
mwf inspect A filter
mwf inspect A failed
mwf inspect A job 3
```

`mwf inspect A filter` shows how many jobs entered, passed, and remained after
each main retry and fallback retry. It derives the funnel on demand from the
latest execution segment in each job's append-only events, so it adds no shared
provenance manifest or scheduler hot-path writes. The final section lists the
jobs that still failed.

Node inspection explains readiness, blockers, status counts, Hoeflein-component
membership, runner, total timeout, checkpoint timeout, and fallbacks.
Job inspection additionally shows the current/last handler, named checkpoint,
checkpoint deadline, progress percentage, progress detail, execution generation,
child jobs, and chronological lifecycle events. Checkpoint state and lifecycle events are stored in `.mwf/state.sqlite3`; they are scheduler diagnostics, not task output or a provenance manifest. `mwf inspect` renders records such as `created`, `started`, `fallback_started`, `timeout`, `restart_requested`, and `done`. `output.json` and job-local returned files remain the actual task result.

## State schema migration and read-only previews

Low-churn MWF JSON metadata such as `.mwf/project.json`, `.mwf/run.json`,
`.mwf/threads.json`, and node `schema.json` carries an explicit
`schema_version`. High-churn job and scheduler state has its own SQLite schema
version. Neither scheme applies to user `input.json`, `output.json`, returned
files, node `input/`, or node `output/`.

Preview and apply an upgrade from an older project:

```bash
mwf migrate --dry-run
mwf migrate
```

Migration upgrades low-churn JSON atomically and initializes/upgrades SQLite transactionally. A one-time importer reads legacy job metadata before deleting those framework-owned sidecars. `mwf migrate --dry-run` does not create the database or import/delete files. MWF refuses state that claims a newer incompatible schema.

Several destructive commands support a read-only preview:

```bash
mwf graph --update --dry-run
mwf clean A --dry-run
mwf reset A --dry-run
mwf wipe A --dry-run
mwf recover --dry-run
mwf restart <node-name> job 4 --dry-run
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
version, and a lightweight heartbeat in `.mwf/run.json`. The same single
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

router = NodeRouter("process_number")

@router.task(timeout=300)
def process_number(ctx):
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
mwf inspect process_number failed
mwf inspect process_number job 3
```

All configured total/checkpoint deadlines are managed by one workflow-owned
scheduler supervisor using a deadline heap. There is no timer thread per job and
no repeated scan of every job folder. Untimed handlers without checkpoints keep
the original direct invocation path. An explicit progress checkpoint updates only that job row in SQLite on demand.

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

Use the full dashboard in the same terminal as an execution command:

```bash
mwf run start_node --monitor
mwf runfrom start_node --monitor
mwf runfrom start_node --monitor --monitor-interval 0.5
```

Inline monitoring prints timestamped snapshots without clearing earlier task or
monitor output, making it suitable as a diagnostic timeline. The final snapshot
is emitted after the run record becomes terminal and therefore says
`active run: none`; the previous sequence is shown separately as the last run.

For an independent observer, open a second terminal and use:

```bash
mwf monitor
mwf monitor --once          # one snapshot
mwf monitor A B             # monitor selected nodes only
mwf monitor --json --once   # machine-readable snapshot
```

`mwf monitor` reads SQLite job/node summaries plus the low-churn run record. It
shows running nodes, queued/running/done/failed counts, jobs left, progress,
running job IDs, effective concurrency, average completed duration, and rough
ETA without calling task code. Compact same-terminal lines remain available:

```bash
mwf runfrom start_node --stats
mwf run start_node --stats --stats-interval 10
```

`--monitor` and `--stats` may be combined. ETA is intentionally approximate and
becomes more useful after at least one relevant job has finished. See
[AGENT.md](AGENT.md) for using snapshots to separate resource pressure, timeout
policy, test-code stalls, and scheduler defects.

## Restart inside an active sequence

When an individual job is hung inside an active `mwf run`, `runfrom`, `resume`,
or `resumefrom` sequence, keep the original terminal running and use the
dedicated restart command from a second terminal:

```bash
mwf restart <node-name> job 42
```

Several currently running jobs may be selected with IDs and ranges:

```bash
mwf restart <node-name> jobs 42 57 80-82
```

`mwf restart` does not start another scheduler and does not replace the active
`.mwf/run.json` record. It atomically advances the selected job's execution
generation before clearing job-local `output.json` and `files/`. The scheduler
that already owns the larger run sees the new generation and immediately starts
the replacement attempt. The node remains active throughout this handoff, so it
cannot be finalized merely because the abandoned attempt stopped being current.
The job row in SQLite and the original `input.json` are preserved.

An older generation is fenced from committing its final status, returned files,
`ctx.write(...)`, `ctx.write_output(...)`, and `ctx.node(...).add(...)` effects.
If it finishes while the restart command is preparing the replacement, its stale
completion is discarded. A live `running` attempt must belong to the active run
and have a live execution lease; MWF refuses rather than creating an orphan
queued job when the attempt has already completed. Ordinary `mwf run` and
`mwf runfrom` commands also refuse to start a competing sequence while another
one owns the project.

`mwf restart` requires that the owning workflow sequence is still live. It may
also requeue a job that has already reached `failed` or `cancelled` while that
sequence remains active, but it never creates an offline queued retry after the
run record becomes terminal.

After a partial run has ended, continue directly with:

```bash
mwf resume <node-name>
mwf resumefrom <start-node>
```

Those commands automatically advance the generation and clear old result/files
for failed, cancelled, or abandoned-running jobs while preserving `done` and
`skipped` jobs.

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
mwf run <node-name> job 42
```

## Large-node performance and SQLite state

MWF 0.3.4 no longer creates per-job queue markers, status files, execution
files, runtime files, event logs, or a shared `job_index.json`. High-churn state
is normalized into `.mwf/state.sqlite3`:

- `jobs` stores job identity, status, execution generation/lease, and checkpoint runtime
- `job_events` stores chronological lifecycle, retry, fallback, timeout, and restart records
- `idempotency` stores downstream creation keys
- `default_job_specs` stores idempotent router starter declarations
- `nodes` stores node-level state
- `advisory_locks` provides infrequent CLI-wide cross-process critical sections

Per-job execution/restart fences are intentionally not database advisory rows.
They use `.mwf/execution-fences/*.lock`, so a managed payload write does not add
an advisory-lock acquire and release around its normal work. Checkpoints update
their job runtime with one conditional SQLite statement. Framework-created
worker, handler, component, supervisor, and inline-monitor threads close their
connection at lifecycle end; same-process writers queue before entering SQLite,
while WAL keeps monitor/inspect readers concurrent.

The scheduler allocates dynamic job IDs and updates status counts with short
transactions. WAL mode allows concurrent monitoring and inspection. This avoids
thousands of tiny filesystem operations and removes contention on a shared
index file for high-fan-in nodes.

The user-visible source of payload truth stays on disk:

- `node/<name>/input/` and `node/<name>/output/`
- `node/<name>/jobs/<id>/input.json`
- `node/<name>/jobs/<id>/output.json`
- `node/<name>/jobs/<id>/files/` when the job returns or writes files

A job that never creates returned files does not need an empty `files/` folder.
Use `mwf inspect`, `mwf monitor`, and `mwf doctor` rather than querying SQLite
directly in application code.

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

The wheel is written to `dist/`. For version 0.3.16 the expected filename is:

```text
micro_workflow_manager-0.3.16-py3-none-any.whl
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
python -m pip install --force-reinstall .\dist\micro_workflow_manager-0.3.16-py3-none-any.whl
```

From Linux or WSL:

```bash
python -m pip install --force-reinstall ./dist/micro_workflow_manager-0.3.16-py3-none-any.whl
```

If the wheel is in Downloads or another directory, use its full path:

```powershell
python -m pip install --force-reinstall "C:\path\to\micro_workflow_manager-0.3.16-py3-none-any.whl"
```

Do not write `.micro-workflow-manager==0.3.16`; that is interpreted as a malformed
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
./vendor/micro_workflow_manager-0.3.16-py3-none-any.whl
```

Then users can install the project and its framework together from the project
root:

```powershell
python -m pip install -r requirements.txt
```

### Uninstall and persistence

The package installs no Windows service, daemon, scheduled task, registry entry,
or background process. Runtime state stays in the project under the consolidated `.mwf/` directory and `node/`. Stop any active `mwf run`, `mwf runfrom`, `mwf resume`, `mwf resumefrom`,
or `mwf monitor` process before uninstalling, especially on Windows where an
active `mwf.exe` launcher can be locked.

```powershell
python -m pip uninstall micro-workflow-manager
```

Deleting the project-local `.venv` removes the entire isolated installation as
an alternative. Deleting the Python package does not delete workflow project
data; remove `.mwf/` and `node/` separately only
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

[AGENT.md](AGENT.md) is the authoritative testing and diagnosis protocol for automated contributors. It requires focused reproduction, repeat-use testing, concurrency and timeout experiments, and explicit freeze analysis before changing scheduler semantics.

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

## Initialize from a deployment archive

A local or copied remote deployment may remain compressed as `deployment.zip`.
MWF can unpack the main archive and all independently compressed `node/<name>.zip`
folders during initialization:

```bash
mwf init deployment.zip
```

When no archive argument is supplied, `mwf init` checks these common locations:

- `./deployment.zip`
- `./mwf-deployment.zip`
- `./.mwf/deploy/local/deployment.zip`

Extraction rejects paths that escape the project directory. Initialization prints
each major step and each node archive as it is unpacked.

## Node clipboard

Save a node's payload folder and a cold SQLite state snapshot beside `node/`:

```bash
mwf copy preprocess
```

This replaces `clipboard/preprocess` while leaving other saved nodes intact.
Restore it later with:

```bash
mwf paste preprocess
```

Paste replaces `node/preprocess` and restores that node's jobs, statuses, events, idempotency keys, and default-job declarations from `clipboard/preprocess/.mwf-node-state.sqlite3`. Pre-0.3.4 clipboard copies without a snapshot remain supported as payload-only copies. The default
`.mwfignore` excludes `clipboard/`, `.mwf/`, `.venv/`, version-control metadata,
editor metadata, caches, and build output.

## Inspect a node debug log

```bash
mwf inspect preprocess debug
```

This prints the node's `output/debug.txt` path and contents, or explains that the
file does not exist yet.

### Refresh declared concurrency after editing node files

After changing `max_threads=` or a node-level `runner=` in `src/node_behavior/*.py`, refresh the mounted schemas without synchronizing the graph:

```bash
mwf threads --update
```

This reloads the synchronized node behavior files and updates their declared concurrency and runner values. It does not change graph edges, create/delete node folders, or clear runtime overrides. Use `mwf threads NODE reset` separately when an override should be removed.

### Clipboard restore consistency

`mwf paste NODE` now synchronizes the restored payload folders with SQLite before returning. Clipboard snapshots made before 0.3.4 have their numeric `jobs/<id>/input.json` payloads rebuilt as queued database jobs. A snapshot captured while a job was running is treated as a cold restore: stale running leases are cleared and those jobs are immediately queued, so the node can be run or resumed without another migration command.


## Component-level cleanup (0.3.16)

`mwf clean NODE`, `mwf reset NODE`, and `mwf wipe NODE` expand `NODE` to its
whole Hoeflein component. Use `--dry-run` to see the expanded component before
anything changes. Selecting several members of one component does not duplicate
work. `*` still selects every graph node.
