# Operations

This guide expands the command and lifecycle behavior summarized in the root
[README](../README.md). Read the graph, node, and task architecture pages before
changing workflow design. Use `mwf <command> --help` for exact syntax and
`mwf --describe <command>` for an extended command explanation.

## Bootstrap effects of observation and previews

Before most commands, MWF may migrate an older runtime layout. Commands that
mount routers may also refresh schemas or create declared starter jobs. Only
`mwf engine` and `mwf migrate --dry-run` bypass those bootstrap paths. A command
described as observational or a preview still avoids executing jobs or applying
its advertised mutation; it does not guarantee a byte-for-byte unchanged
framework state directory.

## Initialization and graph synchronization

`mwf init` creates `.mwf/project.json`, `.mwf/state.sqlite3`, and lightweight
editor and ignore-file support. It does not import `src/graph.py`, execute task
code, or create project and node README files.

Register the initial graph explicitly:

```powershell
mwf graph src/graph.py
```

Later graph changes remain explicit:

```powershell
mwf graph --update --dry-run
mwf graph --update
```

The preview does not apply the proposed graph metadata change or node-folder
addition/removal. The shared bootstrap caveat above still applies. The applied
form synchronizes stored edges and node folders, and can remove a node directory
when the graph no longer names it. Preserve needed node data first.

`mwf doctor` checks graph and behavior mismatches, malformed state, stale run
ownership, SQLite integrity, and undeclared literal `ctx.node("...")` routes.
It does not execute jobs or apply a requested repair, but startup may perform the
bootstrap work described above. Warnings do not force a failing exit status;
errors do.

`mwf engine` opens the synchronized graph as a loopback, graph-only browser
view. It collapses nontrivial Hoeflein components and reveals their members on
selection. It imports no project code, exposes no state mutation endpoint, and
loads no external assets.

## Fresh execution

`mwf run NODE` selects NODE's complete Hoeflein component. It performs fresh
preparation and then schedules the component if its external predecessors are
complete. For a singleton node, `job`, `jobs`, and `sample` modes can select a
subset.

`mwf runfrom START` selects START's component and quotient-DAG descendants. It
freshens the complete selected region, removes descendant work attributable to
selected producer components, preserves merge work from unselected branches,
then schedules in dependency order.

Both commands support `--plan`, and both can show an inline dashboard with
`--monitor`. A plan validates and displays the selection and reset effects
without creating a run record, applying the planned reset, or executing task
code. Loading and mounting routers may still perform the bootstrap effects
described above.

Fresh execution clears affected trace journals unless `--keeptrace` is used.
It also clears node output for whole-component preparation. A selected-job run
does not clear the shared node output prefix because MWF cannot infer which
files belong to one job.

### Refusal boundaries

`runfrom START refuse BOUNDARY` stops before the boundary component starts and
admits no other newly ready component once the boundary is reached.

`runfrom START refuseafter BOUNDARY` lets the boundary component finish or fail,
then stops later component admission. Components already running are joined.
Later work remains queued. The refusal boundary changes admission, not the
fresh-preparation scope.

## Deterministic sampled runs

`mwf run NODE sample COUNT` selects a stable SHA-256-ranked subset of existing
jobs. It can filter by status and accept a population digest:

```powershell
mwf run classify sample 100 --seed release-check --status failed --plan
mwf run classify sample 100 --seed release-check --status failed `
  --expect-population <sha256>
```

Sampling is designed to isolate selected jobs. Tests establish deterministic
selection, preservation of unselected work, and planning that does not apply the
sample run. They do not yet exercise routed descendants or Hoeflein-component
circulation. The active run record retains the selection manifest and digest.

## Resume

`mwf resume NODE` continues NODE's selected component without resetting `done`
or `skipped` jobs. `mwf resumefrom START` applies that behavior through the
selected descendant region. Existing queued work remains available, and failed,
cancelled, or abandoned-running work is fenced and requeued.

Before selection, resume reconciles terminal `output.json` records for jobs
still recorded as running and waits for those state updates to become durable.
The start component's trace is retained. Default `resumefrom` clears descendant
trace journals; `--keeptrace` retains them.

`resumefrom` supports the same `refuse` and `refuseafter` admission boundaries
as `runfrom`, but it does not perform fresh producer cleanup.

## Restart during a live sequence

Keep the original execution terminal running and use another terminal:

```powershell
mwf restart <node-name> --dry-run
mwf restart <node-name>
mwf restart <node-name> failed
mwf restart <node-name> job 42
mwf restart <node-name> jobs 42 57 80-82
```

Restart uses component membership stored in the active run record and does not
load project graph or task code. The default form selects running plus failed or
cancelled jobs in the component. `failed` excludes live-running attempts. Job
forms keep exact selection.

For each selected job, MWF advances its execution generation, removes its
terminal `output.json`, and leaves the existing scheduler in control. A stale
generation cannot commit MWF-managed output, input forwarding, status, or child
jobs. Restart does not remove files from the shared node output prefix. Tasks
must use stable paths and idempotent replacement when a rerun can rewrite output.

After the active sequence has ended, use resume instead of restart.

## Recover after a dead owner

Active runs store hostname, process ID, process-start identity, and scheduler
heartbeat in `.mwf/run.json`. `mwf recover` acts only when the recorded owner is
dead. It advances execution generations and requeues abandoned running jobs.
Done and failed jobs remain unchanged.

```powershell
mwf recover --dry-run
mwf recover
```

The checkpoint deadline of one job and the heartbeat of the scheduler are
different signals. Use inspection to distinguish a live scheduler with one
stalled job from a dead run owner.

## Inspection

`mwf inspect NODE` shows predecessors, successors, component membership, runner,
timeouts, status counts, and readiness. `failed`, `job ID`, and `debug` modes
show narrower information. Debug mode reads `node/NODE/output/debug.txt` when it
exists.

`mwf trace NODE job ID` renders chronological job origin, task and fallback
starts, custom `ctx.trace()` values, managed output writes, forwarded inputs,
child jobs, failed attempts, and terminal state. `--errors` narrows display to
identity, origin, ordered failures, attempt details, terminal state, and terminal
error. It does not change what MWF records.

`mwf filter NODE` reconstructs the retry and fallback funnel from durable
events. `stage X` lists the relevant jobs and error at one stage boundary.

`mwf monitor` shows current and recent run state, node counts, progress,
concurrency, durations, and approximate remaining time. It can watch in a
second terminal or print once, including JSON. `mwf top` adds event rates, queue
and terminal latency, process data, SQLite and WAL size, network data, and
mutation-writer diagnostics.

## Concurrency controls

`mwf threads NODE VALUE` stores a run-scoped override. A live threaded or API
node observes increases within roughly 0.2 seconds. A decrease does not cancel
work already running. Process pools read the value when created; direct remains
single-job.

`mwf threads --api-total VALUE` sets an aggregate API admission budget. MWF
allocates it proportionally across running API nodes using per-node requested
limits as weights and upper bounds. Both node overrides and the aggregate budget
can be reset and clear after their run scope.

`mwf threads --update` reloads node behavior declarations and refreshes mounted
runner and `max_threads` values. It does not synchronize graph edges or clear
runtime overrides.

## Destructive preparation

All destructive commands support `--dry-run`. Applied forms require typed
confirmation unless `--yes` is supplied.

| Command | Jobs | Node output | Node input | Executes tasks |
| --- | --- | --- | --- | --- |
| `reset` | keep identities and parameters; requeue selected work | clear for whole-component scope | keep | no |

The `resetfrom` command applies through quotient-DAG descendants. It uses the same
producer-aware freshening as `runfrom`. Naming any member expands to its whole
Hoeflein component, and `*` selects every graph node.

## Node clipboard

`mwf copy NODE` saves the node tree under `clipboard/NODE` and adds a cold
SQLite snapshot containing that node's jobs, statuses, events, idempotency keys,
and default job declarations. A new copy replaces an older saved copy of the
same node.

`mwf paste NODE` replaces the live node tree, restores the snapshot, and
reconciles payload jobs and stale running leases. Clipboard copies predating the
SQLite snapshot are restored as payload-only copies. Clipboard operations do
not copy graph edges or Python behavior.

## Migration

`mwf migrate --dry-run` reports changes to MWF-owned metadata without changing
it. `mwf migrate` updates low-churn JSON and SQLite schemas and can import older
framework-owned status, queue, event, execution, idempotency, default-job, and
node-summary records.

Applied migration checks both `.mwf_run.json` and `.mwf/run.json` before changing
layout, locks, JSON, or SQLite state. It refuses while either legacy run is
observed alive. Automatic conversion of an older runtime layout uses the same check.
Initialization checks before extracting a deployment archive. An unreadable or
non-object run file also prevents migration.
Wait for the recorded run to finish or become stale before migrating; a fresh
heartbeat from another host also counts as live.
This preflight does not prevent an older process from starting after the check.
Concurrent-start exclusion and safe session import remain unfinished.

Migration does not rewrite `input.json`, `output.json`, node input, node output,
or old per-job file trees. It refuses to downgrade state created by a newer,
incompatible schema.

## Deployment

```powershell
mwf deploy setup
mwf deploy local
mwf deploy remote
```

Setup stores server connection metadata and creates `.mwfignore`; it does not
store passwords. Local deployment replaces the previous local archive with a
filtered copy. Remote deployment confirms the archive, uploads it, and extracts
it at the selected destination, overwriting matching paths but leaving unrelated
remote files alone.

Review `.mwfignore` before each sensitive deployment. Check credentials,
environment files, large output trees, and the exact remote destination.

## Deployment archive initialization

`mwf init deployment.zip` extracts a local or copied deployment archive and its
independently compressed node archives. Without an argument, init checks
`deployment.zip`, `mwf-deployment.zip`, and
`.mwf/deploy/local/deployment.zip`. Extraction rejects paths that escape the
project directory.
