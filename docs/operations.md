# Operations

This guide expands the command and lifecycle behavior summarized in the root
[README](../README.md). Read the graph, node, and task architecture pages before
changing workflow design. Use `mwf <command> --help` for exact syntax and
`mwf --describe <command>` for an extended command explanation.

## Bootstrap effects of observation and previews

Graph execution plans, sample plans, reset and recovery dry runs, and `mwf
engine` bypass mutable runtime initialization. Graph command previews read
synchronized edges, literal autostart declarations, and persisted native state
without importing project graph or task code. They do not create starter jobs,
reserve sessions, prepare components, or change project files. When observing
an open WAL database, SQLite may update its existing shared-memory file.

Commands that mount routers can refresh runtime records and create declared
starter jobs. Graph synchronization and `doctor` still use that startup path.

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
addition/removal. The router-startup effects above still apply. The applied
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

`mwf run NODE` selects NODE's complete Hoeflein component. It checks the start
component's external predecessor results before fresh preparation and execution.
The `job`, `jobs`, and `sample` modes select exact starting jobs and their newly
produced causal work within that component.

`mwf runfrom START` selects START's component and quotient-DAG descendants. It
freshens the complete selected region, removes descendant work attributable to
selected producer components, preserves merge work from unselected branches,
then schedules in dependency order.

`mwf runbetween START END` selects the union of directed quotient paths from
START's component to END's component, excluding END's component. END must be a
strict directed descendant. A selected producer may publish to an excluded
receiver, but this command does not execute that receiver.

All three fresh commands support `--plan` and `--monitor`. Their full-component
plans show selected components and raw nodes, native prerequisite results, job counts, crossing
edges, publication receivers, and exact cleanup effects. Interval plans also
show the excluded end component. Busy state appears as a refusal reason;
damaged stored state makes the preview fail.

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

Sampling includes named component members and percentage counts. It preserves
unselected jobs and runs newly produced causal work within the selected
component. Admission stores the exact sample and population digest with its
session. Successful partial coverage leaves the component sampled; full
eligible coverage with no newer unprocessed work can complete it.

## Resume

`mwf resume NODE` continues one component while preserving successful jobs and
output. `mwf resumefrom START` continues through its quotient descendants.
`mwf resumebetween START END` continues the same half-open interval as
`runbetween`.
Queued jobs retain their generations. Repairable failed or cancelled jobs
receive a new job generation. Resume preserves the component's alignment generation.

All three commands preflight the whole selection before changing jobs, files, or
traces. Misalignment and incomplete or incompatible external parents block
resume. Misaligned `resume C` recommends `mwf run C`. Misaligned C inside
`resumefrom B` recommends `mwf resetfrom C`, then retrying `mwf resumefrom B`.
Misaligned C inside `resumebetween B E` recommends `mwf resetbetween C E`, then
retrying `mwf resumebetween B E`. Separate affected branches receive separate
repair commands.

Preparation requires exact session reservations, job instances, and execution
owners. Failed or cancelled attempts must retain their producing component
membership but may belong to an older shape or alignment. Execution locks precede
receiver locks. Requeue, component state, restart events, trace clearing, and
output recovery commit together. Failed SQL restores staged files.

Terminal-output recovery requires an abandoned running attempt in a terminal
session, with the current producing component, shape, and alignment. Output must
match its generation and execution ID. Recovery retains its start time and the
output file's finish time. Native running components and crashed sessions still
need the separate recovery work in the
[implementation progress](plans/0.6.2/producer-footprint-preparation-progress.md).

The start component's trace is retained. Default `resumefrom` clears descendant
traces after preflight; `--keeptrace` retains them. It supports the same `refuse`
and `refuseafter` admission boundaries as `runfrom` without fresh producer cleanup.
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

The `--api-total` option is deprecated in the 0.6.2 development branch. Help
marks the option deprecated, and using it prints a warning to standard error,
including when a supplied value is rejected. Setting and resetting the budget
remain functional. No removal date or session-specific form is introduced.

`mwf threads --update` reloads node behavior declarations and refreshes mounted
runner and `max_threads` values. It does not synchronize graph edges or clear
runtime overrides.

## Destructive preparation

All destructive commands support `--dry-run`. Applied forms require typed
confirmation unless `--yes` is supplied.

| Command | Jobs | Node output | Node input | Executes tasks |
| --- | --- | --- | --- | --- |
| `reset` | requeue retained jobs; remove selected-producer work | clear for whole-component scope | preserve outside input; remove selected-producer publications | no |

`resetfrom` prepares quotient-DAG descendants. `resetbetween START END` prepares
the same half-open interval as `runbetween`. Naming any member selects its whole
Hoeflein component. `reset '*'` and `resetfrom '*'` select every graph node.

Preparation can remove selected-producer files or jobs at excluded receivers
and mark their retained results misaligned. It preserves material from
unselected producers. Applied reset commands refuse before loading project
code while any main or interrupt session remains running, including an
abandoned session that needs recovery.

## Node clipboard

`mwf copy NODE` saves the node tree under `clipboard/NODE` and adds a cold
SQLite snapshot containing that node's jobs, statuses, events, idempotency keys,
and default job declarations. A new copy replaces an older saved copy of the
same node.

`mwf paste NODE` replaces the live node tree, restores the snapshot, and
reconciles payload jobs and stale running leases. Clipboard copies predating the
SQLite snapshot are restored as payload-only copies. Clipboard operations do
not copy graph edges or Python behavior.

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
