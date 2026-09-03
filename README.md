# micro-workflow-manager 0.6.1

Micro Workflow Manager (MWF) runs Python jobs through a directed graph while
keeping user data visible on disk and high-churn scheduling state in SQLite. It
supports ordinary acyclic workflows, bounded communicating components, direct
and parallel runners, retries and fallbacks, durable inspection, and controlled
restart and recovery.

## Documentation

This README is the current common-path guide. It is self-contained for the behavior a
project author needs to create, run, inspect, and recover a workflow. The linked
pages own deeper design or operating detail:

- [CONTEXT.md](CONTEXT.md): MWF language and release boundaries.
- [Graph architecture](docs/architecture/graph.md): raw graphs, Hoeflein
  components, quotient-DAG scheduling, circulation, and semantic pathing.
- [Node architecture](docs/architecture/node.md): tasks, fallbacks, validation,
  failure lineage, runners, and node README guidance.
- [Task architecture](docs/architecture/task.md): parameters, routing,
  filesystems, validation, output provenance, and idempotency.
- [Operations](docs/operations.md): command semantics, inspection, restart,
  recovery, cleanup, clipboard use, and deployment.
- [Installation](docs/installation.md): development installs, builds, wheels,
  uninstalling, and persistence.
- [Testing](docs/testing.md), [test modules](tests/README.md), and
  [benchmarks](benchmarks/README.md): isolated verification and performance
  programs.
- [Release history](docs/release-history.md): version-by-version change notes.
- [Provisional 0.6.4 planning](docs/plans/0.6.4.md): explicitly unsettled
  future work, not current behavior.

Repository procedures for agents live in [AGENTS.md](AGENTS.md) and the five
instruction-only skills under `.agents/skills/`.

## The model

An MWF project has three design scales:

1. The raw graph names nodes and directed routes.
2. A node owns one main task, optional fallbacks, validation, and one runner.
3. A task accepts one job's parameters and files, performs a transformation,
   validates it, writes useful output, and routes accepted work onward.

MWF contracts each communicating Hoeflein component into one vertex for
dependency scheduling. The resulting quotient graph is acyclic. Ordinary DAG
nodes are singleton components. Naming a node in a multi-node component for
`run`, `resume`, restart, or cleanup selects the whole component.

The filesystem and SQLite have separate responsibilities:

- `.mwf/state.sqlite3` is authoritative for jobs, statuses, execution
  generations, checkpoints, events, idempotency keys, default job declarations,
  node state, and queueing.
- `node/<name>/input/` is durable input visible to task code.
- `node/<name>/output/` is the node's single user-owned output prefix.
- `node/<name>/jobs/<id>/input.json` stores job parameters.
- `node/<name>/jobs/<id>/output.json` stores the concise terminal return or
  failure summary.

MWF 0.6.1 does not provide per-job file storage. Substantial results,
diagnostics, and intermediate files belong under the node output prefix, not
under `jobs/<id>/`.

## Recommended project layout

```text
project/
├── README.md
├── src/
│   ├── graph.py
│   ├── README.md                     # optional source organization guide
│   └── node_behavior/
│       └── <node-name>.py
├── node/
│   └── <node-name>/
│       ├── README.md
│       ├── input/
│       ├── output/
│       └── jobs/
│           └── <job-id>/
│               ├── input.json
│               └── output.json
├── .mwf/
│   ├── project.json
│   ├── run.json
│   ├── threads.json
│   └── state.sqlite3
├── .mwfignore
└── .gitignore
```

The root README should explain the project purpose, graph, component behavior,
setup, execution, inspection, and important operating boundaries. Each node
README should explain its role and Job Scope, task and fallback hierarchy,
parameters, file inputs and outputs, routing, validation hierarchy,
validator-fallback balancing, fallback context control, runner, concurrency,
timeouts, and idempotency. Add `src/README.md` only when the source layout needs
its own explanation.

These README files are documentation standards, not framework validity checks.
Current `mwf init` and `mwf graph` do not create them automatically.

## Quick start

From an MWF source checkout, create a virtual environment and install it in
editable mode. See the installation guide for wheel and source-archive
workflows.

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[test]"
```

Keep that environment active, then create a separate workflow project:

```powershell
cd ..
mkdir first_mwf_project
cd first_mwf_project
mwf init
New-Item -ItemType Directory -Force src\node_behavior
```

Create `src/graph.py` with a minimal runnable two-node graph:

```python
EDGES = [
    ("seed", "report"),
]
```

Create `src/node_behavior/seed.py` with the initial job and its route:

```python
from micro_workflow_manager import NodeRouter

router = NodeRouter("seed")
router.create_job(params={"question": "What changed?"})


@router.task
def seed(ctx, question: str):
    ctx.node("report").add(
        question=question,
        idempotency_key=f"seed:{ctx.job_id}:report",
    )
    return {"question": question}
```

Create `src/node_behavior/report.py` with the receiving task:

```python
from micro_workflow_manager import NodeRouter, OutputFileSystem

router = NodeRouter("report")
OUTPUT = OutputFileSystem("reports")


@router.task
def report(ctx, question: str):
    answer = f"Received: {question}"
    OUTPUT.file(ctx, "answer.txt").write_text(answer, overwrite=True)
    return answer
```

Preview and run:

```powershell
mwf graph src/graph.py
mwf doctor
mwf runfrom seed --plan
mwf runfrom seed --monitor
```

Graph synchronization is deliberate. After changing `src/graph.py`, use:

```powershell
mwf graph --update --dry-run
mwf graph --update
```

Removing a node during synchronization can remove its node directory. Preserve
needed data before applying the update.

## Graph and component behavior

MWF accepts an ordinary `(source, target)` edge, a one-to-many edge with a
collection on the target side, and `fan(sources, target)` for many-to-one. A
collection on both sides is rejected because it would silently imply a complete
bipartite graph.

Autostarting relationships make work available within one communicating
component instead of waiting for ordinary DAG completion. The raw edge still
keeps its declared direction; the reverse relationship is used only when MWF
builds component membership. The quotient DAG retains the original directions
between components.

Static component construction currently recognizes only the literal
`ctx.node("...").add(..., autostart=True)` form. The runtime also accepts
`add_many`, `add_job`, and `add_jobs`, but do not rely on those forms to declare
component membership until the scanner supports them.

A live Hoeflein component keeps ordinary threaded and API members available
while peer work can still arrive. An internal `waiting=True` declaration is an
exceptional admission gate. Waiting targets must be in the same component. A
singleton waiting node has no additional DAG effect.

`runfrom START` selects START's whole component and every quotient-DAG
descendant. Its fresh preparation removes work produced by selected components
while preserving merge work from unselected branches. `refuse BOUNDARY` stops
before the boundary component starts. `refuseafter BOUNDARY` lets it terminate,
then stops later admission. Already-running parallel components are joined and
later queued work can be continued with `resumefrom`.

## Jobs, parameters, and routing

The first task parameter is `ctx`. Remaining Python parameters define accepted
job parameters. Required parameters are required job inputs; Python defaults
make them optional.

The routing API reserves `job_id`, `autostart`, and `idempotency_key`. Task
execution reserves `error` for the immediately preceding exception and `errors`
for ordered failures from the current execution sequence. Do not use these
names for unrelated project data.

Create one child with `add()` and use an explicit key when a retry must reuse
the same child:

```python
ctx.node("review").add(
    document_id=document_id,
    idempotency_key=f"extract:{ctx.job_id}:review:{document_id}",
)
```

Create a same-node batch with `add_many()`:

```python
items = [{"section": section} for section in sections]
keys = [f"split:{ctx.job_id}:{section}" for section in sections]
ctx.node("process_section").add_many(items, idempotency_keys=keys)
```

For cross-node fan-out, precompute the child specifications and give each route
an explicit stable key:

```python
children = [
    ("research", {"question": question}, f"plan:{ctx.job_id}:research"),
    ("risk", {"question": question}, f"plan:{ctx.job_id}:risk"),
]
for node, params, key in children:
    ctx.node(node).add(idempotency_key=key, **params)
```

MWF 0.6.1 has no `ctx.transaction()` staging helper. Existing projects using it
must finish or clear affected partial runs before upgrading, then use the two
patterns above. The internal SQLite transaction machinery and generation fences
remain framework implementation details.

## Filesystems and output provenance

MWF exposes three filesystem objects for task code:

- `InputFileSystem` reads the current node's `input/` tree.
- `OutputFileSystem` reads and writes within the current node's one `output/`
  prefix.
- `NodeInputFileSystem` writes to a connected node's `input/` tree and can route
  jobs there.

They provide contained path resolution, text and byte operations, JSON helpers,
copying, listing, generation fencing, Windows extended-path handling, and trace
events where appropriate. `ctx.input_path()`, `ctx.output_path()`,
`ctx.write_output()`, and `ctx.write_output_bytes()` remain available for direct
node-scoped access. `ctx.write()`, `ctx.write_bytes()`, `ctx.files_dir`,
`ctx.storage_dir`, and `JobFileSystem` do not exist in 0.6.1.

Generation fencing applies to framework-managed write and copy methods. A
filesystem entry's `.path`, path-like conversion, `ctx.output_path()`, and a
writable `.open()` handle are deliberate escape hatches for third-party
libraries. MWF checks a writable handle before opening it, but cannot fence or
roll back later writes made through a retained raw path or handle.

Output provenance is the user-owned, navigable filesystem tree rooted at a node
output prefix. It retains the node's results and enough useful intermediate
information to inspect how those results developed. The tree may contain any
file types and may combine work from many jobs. Output provenance is not a
required file, record, manifest, schema, or filename.

There is exactly one framework output prefix per node:

```text
node/<node-name>/output/
```

An `OutputFileSystem(base="...")` selects a subtree within that prefix; it does
not create another framework output prefix. The tree may contain JSON, text,
PDFs, images, video, office documents, archives, directories, or domain-specific
formats. A task may organize stage results, diagnostics, and final results as
needed. `output/debug.txt` is a convenient lightweight diagnostic file, not the
definition of output provenance.

Prefer similar relative organization between output and the input tree that
receives it. Pass stable relative locators in job parameters instead of copying
large data into every `input.json`. The sender must still route the exact path
the receiver expects; node output is not transported automatically.

Returning a `Path`, `{"file": ...}`, or `{"files": ...}` is ordinary return
data. MWF does not copy the referenced file. On success, `output.json` records
status and a concise `result_type` and `result_repr`, plus the execution
generation when supervised. On failure, it records status, the terminal error,
and generation when applicable. Historical `jobs/<id>/files/` trees are left
untouched during upgrade but no new per-job file tree is created.

## Retries, fallbacks, validation, and failure lineage

A node's main task and fallbacks form its functional hierarchy. Retries repeat
the same implementation. Moving to a fallback is an escalation and should
change a meaningful implementation, model, configuration, or strategy.

Validators form a separate validation hierarchy. Tune validation strictness,
functional cost, retry counts, and fallback order together. A low pass rate is
only an observation; it does not by itself show whether acceptances or
rejections were correct.

During one job execution sequence, MWF maintains ordered live failure history:

- `ctx.error` and an optional `error` parameter receive the immediately
  preceding exception;
- `ctx.errors` and an optional `errors` parameter receive a fresh list of every
  earlier main, retry, repeated, and fallback failure in order;
- on every later attempt, `ctx.error == ctx.errors[-1]`;
- a fresh run, resume, or restarted execution begins with an empty live history.

The original exception objects are available only within that live sequence.
MWF also records a durable `task_failed` event for each failed attempt, including
task, role, attempt, repetition, rendered error, and time. Durable text is
diagnostic history; it does not reconstruct Python exception objects after a
restart.

Tasks should use finite client timeouts for external calls. `ctx.checkpoint()`
can report a name, progress, detail, and the deadline for the next section. A
task or fallback `timeout=` remains the total attempt limit. When a deadline or
restart replaces an execution generation, stale MWF-managed output, forwarded
input, state updates, and child creation are rejected.

## Runners and concurrency

- `direct` runs one job at a time and is useful for deterministic debugging.
- `threaded` handles blocking local I/O with bounded OS-thread concurrency.
- `api` runs cooperative fibers for high-latency external calls and uses MWF's
  shared HTTP transport.
- `process` isolates CPU work whose imports, parameters, and results can cross a
  process boundary.

`max_threads` is the node's requested job concurrency, not a promise about
provider, socket, database, or host capacity. Runtime overrides are run-scoped:

```powershell
mwf threads
mwf threads classify 8
mwf threads classify +2
mwf threads classify reset
mwf threads --api-total 256
mwf threads --api-total reset
mwf threads --update
```

For API nodes, `--api-total` is an aggregate admission budget allocated
proportionally across running API nodes. Per-node values remain weights and
upper bounds. Raising a live threaded or API limit is observed within roughly
0.2 seconds; lowering it does not cancel jobs already running. A process pool
reads the value when created, and the direct runner remains single-job.

## Commands and lifecycle

Use `mwf <command> --help` for syntax and `mwf --describe <command>` for a longer
explanation. Every executable command has both forms, including `copy`, `paste`,
`filter`, and `top`.

Current bootstrap behavior matters when interpreting observational commands and
previews. Before most commands, MWF may migrate an older runtime layout. Commands
that mount routers may also refresh schemas or create declared starter jobs.
Only `engine` and `migrate --dry-run` bypass those bootstrap paths. A preview or
observational command still avoids running jobs or applying its advertised
mutation; it is not a promise that no framework-owned byte changes.

| Intent | Commands | Current behavior |
| --- | --- | --- |
| Create and synchronize | `init`, `graph` | `init` creates framework state and sidecars. Only `graph` synchronizes edges and node folders. |
| Check and observe | `doctor`, `engine`, `inspect`, `trace`, `filter`, `monitor`, `top` | Read current graph or state without running jobs. `engine` is graph-only and loopback. |
| Execute fresh work | `run`, `runfrom` | Reset the selected component or selected descendant region before execution. |
| Continue work | `resume`, `resumefrom` | Preserve done and skipped jobs and continue queued, failed, cancelled, or abandoned work. |
| Control a live sequence | `restart`, `threads` | Fence selected active jobs or change run-scoped concurrency without starting another scheduler. |
| Prepare or delete | `reset`, `resetfrom`, `clean`, `cleanfrom`, `wipe`, `wipefrom` | Previewable destructive operations with component-aware scope. |
| Preserve node state | `copy`, `paste` | Save or restore a node tree with its SQLite node snapshot. |
| Maintain state and deployment | `recover`, `migrate`, `deploy` | Recover a dead owner, update MWF-owned metadata, or build and transfer a filtered archive. |

`run NODE` selects NODE's Hoeflein component. Explicit `job` or `jobs` selection
is supported for a singleton node. Deterministic `sample COUNT` runs are designed
to isolate a SHA-256-ranked subset. Current tests establish deterministic
selection, preservation of unselected work, and planning that does not apply the
sample run. They do not yet establish routed-descendant or component-circulation
isolation. Use `--plan` and `--expect-population` to review and guard a sample.

`resume` and `resumefrom` preserve successful work. They reconcile terminal
`output.json` records before requeueing eligible unsuccessful work. Use them
after a partial failure when completed jobs should remain completed.

`restart` is a second-terminal control for an active sequence. It advances the
execution generation of selected running or failed jobs and leaves the original
scheduler in control. It removes the selected job's terminal `output.json`, but
cannot infer which files under the shared node output prefix belong to that job.
Task design therefore owns stable output paths and idempotent replacement.

`recover` acts only when the recorded CLI owner is dead. It fences and requeues
abandoned running jobs while preserving done and failed jobs. Preview with
`mwf recover --dry-run`.

Fresh and destructive component operations clear their affected node output
trees. Selected-job reruns do not delete node output because it has no per-job
ownership. `clean` removes jobs and output but preserves input. `reset` preserves
job identities and input, removes terminal job results, and requeues. `wipe`
also removes input. The `*` and `from` variants expand these rules across their
selected components; use `--dry-run` before applying them.

## Inspection and diagnostics

```powershell
mwf doctor
mwf inspect classify
mwf inspect classify failed
mwf inspect classify job 17
mwf inspect classify debug
mwf trace classify job 17
mwf trace classify job 17 --errors
mwf filter classify
mwf filter classify stage 2
mwf monitor --once
mwf top --once
```

`inspect` combines graph, schema, job, checkpoint, and status data. `trace`
renders chronological origin, task and fallback starts, custom traces,
framework-aware file writes, forwarded inputs, child creation, failed attempts,
and terminal state. `--errors` displays only identity, origin, ordered failures,
attempt details, terminal state, and terminal error; recording is always on.

`filter` reconstructs the retry and fallback funnel from durable events. A
specific non-final stage lists jobs rejected there and accepted at the next
stage. The final stage lists terminal failures. `monitor` is the workflow status
view; `top` adds event rates, queue and terminal latency, process, SQLite, and
mutation-writer diagnostics.

## Clipboard and deployment

`mwf copy NODE` saves `node/NODE` under `clipboard/NODE` with a cold SQLite
snapshot. `mwf paste NODE` replaces the current node tree, restores that state,
and reconciles payloads and stale running leases. It does not copy Python
behavior or graph edges.

`mwf deploy setup` records connection metadata and creates `.mwfignore`.
`mwf deploy local` rebuilds a filtered local archive. `mwf deploy remote`
uploads and extracts it. Passwords are not stored. Review `.mwfignore`, output
size, credentials, and destination before deployment.

## Current capability and verification map

The following table makes the storage and lifecycle boundaries explicit. It is
not a substitute for the full test guide.

| Capability | Public surface | Primary regression area |
| --- | --- | --- |
| Read node input | `InputFileSystem`, `ctx.input_path()` | `test_filesystem_objects.py`, `test_file_entry_node_input_import.py` |
| Write node output | `OutputFileSystem`, `ctx.write_output*()` | `test_filesystem_objects.py`, timeout and restart fencing tests |
| Forward files and create jobs | `NodeInputFileSystem`, `ctx.node()` | filesystem, routing, and high-fan-out tests |
| Store job parameters and terminal summary | `input.json`, `output.json` | SQLite/API runner and filesystem tests |
| Reject per-job file storage | removed APIs and no automatic returned-file copy | `test_filesystem_objects.py` |
| Batch or idempotent fan-out | `add_many()`, explicit idempotency keys | high-fan-out and framework-improvement tests |
| Preserve failure lineage | `error`, `errors`, durable `task_failed` events | filter, trace, and framework-improvement tests |
| Fence stale writes | execution generations and managed filesystem calls | timeout, active restart, and process-runner tests |
| Schedule components | raw graph, Hoeflein components, quotient DAG | component, cycle, waiting, and reliability tests |
| Observe and recover | inspect, trace, filter, monitor, top, restart, recover | command, restart, recovery, and event-state tests |

## Install and test

Use [docs/installation.md](docs/installation.md) for editable installs, wheel
builds, source archives, uninstalling, and data persistence. MWF installs no
service, daemon, scheduled task, or registry entry. Removing the package does
not remove `.mwf/`, `node/`, or other project data.

Use [docs/testing.md](docs/testing.md), [tests/README.md](tests/README.md),
[benchmarks/README.md](benchmarks/README.md), and the `mwf-test` skill before
running verification. Test MWF framework changes from an exact copied source
tree under the sibling `test_area`, not from the durable development tree.

When a task-owned diff contains only Markdown or instruction-only `SKILL.md`
files, do not run pytest merely for ceremony. Check links, headings, paths,
terminology, and inventories; compare current-behavior claims with source and
tests; and ask an independent reviewer to look for mismatches when available.
Any executable or runtime-configuration change uses the focused, adjacent, and
release checks selected by the testing guide.

## 0.6.1 upgrade boundaries

- The public per-job file APIs and automatic copying of returned paths are
  removed. Old per-job file trees remain on disk until an already-authorized
  cleanup or manual migration.
- `ctx.transaction()` is removed without an alias or deprecation period. Use
  `add_many()` or explicit idempotency keys.
- Node-output layout is project-owned. MWF enforces one node output prefix and
  safe managed access, not a required result format.
- Root and node READMEs are required by the documentation standard but not by
  runtime validation, and MWF does not generate them in this release.
- Public Python API compatibility beyond the documented current surface remains
  unsettled. Check the installed version before relying on undocumented names.

See [the release history](docs/release-history.md) for older changes and
[the provisional 0.6.4 plan](docs/plans/0.6.4.md) for deferred work. Provisional
planning is not a promise about the final 0.6.3 versus 0.6.4 boundary.
