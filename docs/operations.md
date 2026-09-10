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
reserve sessions, prepare components, or change project files. Native database
observation uses a private snapshot, including when the project has an open WAL.

Commands that mount routers can refresh runtime records and create declared
starter jobs. `doctor` and compact lineage use persisted snapshots without
mounting routers.

## Initialization and graph synchronization

`mwf init` creates `.mwf/project.json`, `.mwf/state.sqlite3`, and lightweight
editor and ignore-file support. It does not import `src/graph.py`, execute task
code, or create project READMEs and node README/RUN pairs.

Register the initial graph explicitly:

```powershell
mwf graph src/graph.py
```

Later graph changes remain explicit:

```powershell
mwf graph --update --dry-run
mwf graph --update
```

The preview imports the graph module to read its proposed edges, so graph-module
side effects remain possible. It does not mount task routers, create declared
jobs, or apply graph metadata and node-folder changes. The applied
form synchronizes stored edges and node folders, and can remove a node directory
when the graph no longer names it. Preserve needed node data first.

`mwf doctor` checks graph and behavior mismatches, malformed state, stale session
ownership, SQLite integrity, and undeclared literal `ctx.node("...")` routes.
It imports no project code and changes no project state. Warnings do not force a failing exit status;
errors do.

`mwf engine` opens the synchronized graph as a loopback, graph-only browser
view. It collapses nontrivial Hoeflein components and reveals their members on
selection. It imports no project code, exposes no state mutation endpoint, and
loads no external assets.

## Fresh execution

| Operation | One component | Start and descendants | Half-open interval |
| --- | --- | --- | --- |
| Fresh preparation and execution | `run` | `runfrom` | `runbetween` |
| Continue retained work | `resume` | `resumefrom` | `resumebetween` |
| Fresh preparation only | `reset` | `resetfrom` | `resetbetween` |

Execution commands use `--plan`; reset commands use `--dry-run`.

`mwf run NODE` selects NODE's complete Hoeflein component. It checks the start
component's external predecessor results before fresh preparation and execution.
The `job`, `jobs`, and `sample` modes select exact starting jobs and their newly
produced causal work within that component.

`mwf runfrom START` selects START's component and quotient-DAG descendants. It
freshens the runnable selection after ordinary interrupt choices, removes
descendant work attributable to its producers, preserves stopped components
and merge work from unselected branches,
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

Use `sample 30` or `sample 10%` for the named raw node. Other component
members select zero unless named, for example `sample X=30 Y=10`.
Use counts or percentages consistently within one selection.
Status filtering happens before counting. Positive percentages round upward;
zero selects no work and changes no component state. A generated seed makes
each invocation independent. A supplied seed reproduces selection while the
population and relevant input remain unchanged.

Plans print selected IDs and a replay command with the seed and combined
population/input digest. `--expect-population` refuses drift before preparation.
Sampling is available only on `run`, including `run ... --interrupt`.
Selected-job preparation removes stale causal descendants of prior executions;
the invocation admits selected roots and newly created same-component work.
It preserves unrelated jobs and does not execute quotient descendants.

## Resume

`mwf resume NODE` continues one component while preserving successful jobs and
output. `mwf resumefrom START` continues through its quotient descendants.
`mwf resumebetween START END` continues the same half-open interval as
`runbetween`.
Queued jobs retain their generations. Repairable failed or cancelled jobs
receive a new job generation. Resume preserves the component's alignment generation.

All three commands preflight the whole selection before changing jobs, files, or
traces. Misalignment and incompatible external parents block resume. Ordinary
resume also requires completed external parents. Explicit interrupt resume can
continue aligned sampled work with unfinished parents under the retained-origin
rules below. Misaligned `resume C` recommends `mwf run C`. Misaligned C inside
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
output file's finish time. Applied native recovery also settles abandoned
running components and sessions, their reservations, and their holds.

The start component's trace is retained. Default `resumefrom` clears descendant
traces after preflight; `--keeptrace` retains them. It supports the same `refuse`
and `refuseafter` admission boundaries as `runfrom` without fresh producer cleanup.
## Interrupt choices and execution

One `NodeRouter(..., interrupt=True)` declaration classifies its whole component.
Ordinary execution still uses normal readiness. Before mutation, all six
execution commands resolve reachable interrupt choices. Interactive use offers
run-all, stop-all, or individual choices. Automation uses:

```text
--interrupt-policy run-all|stop-all|individual
--interrupt-choice I1=run --interrupt-choice I2=stop
```

Choice keys name raw nodes. Plans use the first sorted member as the canonical
component key. Conflicting choices refuse. Individual policy requires every
reachable choice; unreachable choices produce a notice.

Stopping creates no skipped state. Other selected work may finish. Alternate
paths to a merge still require ordinary readiness and valid retained results.
When no selected work remains runnable, the session ends `stopped` and releases
its ownership.

All six execution commands accept `--interrupt` for an interrupt-classified
start component. Only that start receives special readiness treatment. Later
components retain ordinary choices and readiness. Misalignment always refuses
resume, including interrupt resume.

The interrupt session reserves its selection, stops direct-predecessor admission,
and waits for their exact active attempts to acknowledge safe checkpoints or
finish. MWF cannot suspend arbitrary Python instructions or external requests.
Target input and sample population freeze after acknowledgement. The target
executes against that input, then releases predecessor holds even if selected
descendants are still running.

An already ready target retains ordinary stability. An override can establish
an unstable result with the new interrupt session as origin. Aligned retained
sampled work keeps its compatible origin when a later interrupt session resumes
it. Execution identity and instability origin may therefore differ.

Effective readiness override or a sampled result creates a post-interrupt fence.
Earlier admitted commands cannot cross it automatically. A later ordinary
command supplies new authority, while still requiring readiness, compatible
stability, ownership, and selection. A sampled parent remains incomplete.
Full coverage with ordinary readiness creates no partial-result fence.

The target never reruns automatically after release. New managed input or jobs
make its retained result misaligned. Fresh execution establishes new input;
resume remains available only while retained work is aligned.

## Session ownership

A project permits one live main session and several interrupt sessions.
Independent sessions require disjoint execution scopes and compatible complete
preparation effects, including changed receivers outside their selections.
Overlap refuses with the owning session and component names.

Defined parent-child interruption can transfer ownership temporarily. The
parent stops claiming transferred work; the child records its own execution
owners. A main session may also yield future admission when it has no active
claim in that scope. That transfer alone does not make it a paused parent.
Remaining scope returns after the child ends; completed child work is retained.

Monitoring identifies exact sessions. A main-session reader returns the main
session or no result. Multi-session readers list sessions. No generic
single-session fallback guesses an owner.

## Restart during a live sequence

Keep the original execution terminal running and use another terminal:

```powershell
mwf restart <node-name> --dry-run
mwf restart <node-name>
mwf restart <node-name> failed
mwf restart <node-name> job 42
mwf restart <node-name> jobs 42 57 80-82
```

Restart uses the selected jobs' native execution owners and their stored
component membership. It does not load project graph or task code. The default form selects running plus failed or
cancelled jobs in the component. `failed` excludes live-running attempts. Job
forms keep exact selection.

For each selected job, MWF advances its execution generation, removes its
terminal `output.json`, and leaves the existing scheduler in control. A stale
generation cannot commit MWF-managed output, input forwarding, status, or child
jobs. Restart does not remove files from the shared node output prefix. Tasks
must use stable paths and idempotent replacement when a rerun can rewrite output.

After the active sequence has ended, use resume instead of restart.

## Recover after a dead owner

Each execution session stores its hostname, process ID, process-start identity,
and scheduler heartbeat in SQLite. `mwf recover` examines each exact owner and
leaves live sessions active. For an abandoned execution, it retains a valid
terminal result or advances the generation and requeues unfinished work. It
then settles that session's component results, reservations, and holds. Recovery
refuses damaged or ambiguous ownership before changing the affected work.

```powershell
mwf recover --dry-run
mwf recover
```

The checkpoint deadline of one job and the heartbeat of the scheduler are
different signals. Use inspection to distinguish a live scheduler with one
stalled job from a dead run owner.

`mwf doctor` reads native session and job ownership from a database snapshot. It
reports stale owners, missing payloads, invalid runtime settings, and overdue
checkpoints. It uses the synchronized graph without importing project code or
changing project state.

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

`mwf trace NODE job ID --lineage` reads a compact persisted view without loading
project code. It separates job status from component state, stability, origin,
and misalignment. It includes sample and interrupt IDs when present, the
creating job, and directly created jobs.

Add `--json` for schema version 1. Its fields are `schema_version`, `node`,
`job_id`, `job_status`, `component`, `sample_id`, `interrupt_session_id`,
`created_by`, and `created_jobs`. Component fields are `members`, `state`,
`stability`, `instability_origin`, `misaligned`, and `misalignment_causes`.
Members and jobs sort deterministically. Causes include every receiving node's
first cause for the current alignment generation. Parameters, trace objects,
outputs, errors, retries, and timing bodies are omitted. Traverse to a related
job with another explicit lineage command.

`mwf filter NODE` reconstructs the retry and fallback funnel from durable
events. `stage X` lists the relevant jobs and error at one stage boundary.

`mwf monitor` shows current and recent run state, node counts, progress,
concurrency, durations, and approximate remaining time. It can watch in a
second terminal or print once, including JSON. `mwf top` adds event rates, queue
and terminal latency, process data, SQLite and WAL size, network data, and
mutation-writer diagnostics.

## Concurrency controls

`mwf threads NODE VALUE` stores an override for the exact session owning that
node, or a pending override when the node is idle. A live threaded or API
node observes increases within roughly 0.2 seconds. A decrease does not cancel
work already running. Process pools read the value when created; direct remains
single-job.

`mwf threads --api-total VALUE` limits active API executions across all sessions
in the project. Local node allocation uses requested limits as weights and
upper bounds. Active API work holds capacity while waiting for a provider.
Installing or lowering the limit leaves that work running and delays further
admission until capacity is available. A parent yields capacity at its
cooperative interrupt checkpoint and reacquires it before its handler continues.

Resetting the aggregate budget removes the project limit. Ending one session
clears only its node overrides and execution permits. The aggregate budget
clears when the final session ends.

The `--api-total` option is deprecated in 0.6.2. Help
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
unselected producers. Applied reset commands first recover abandoned sessions
whose ownership can be resolved safely. They refuse before loading project
code if recovery finds damaged or ambiguous ownership, or if any live main or
interrupt session remains. Reset preparation starts only after these checks.

## Node clipboard

`mwf copy NODE` saves the node tree under `clipboard/NODE`, including native
job identities, supporting execution history, and the retained component result.
A new copy replaces the previous saved copy of that node. The component must be
idle; copy does not pause active work. Recover an abandoned session before
copying its component.

`mwf paste NODE` restores only that node within the originating project. It
requires the same active component membership and validates the combined saved
node and current peer state before changing files or rows. Peer files and jobs
remain intact. Completed jobs retain the history supporting their result;
remaining queued jobs stay queued. Paste advances the component's alignment
generation and retains the restored result's stability and instability origin.

Paste refuses a snapshot from another project, missing supporting history,
inconsistent peer work, or payload jobs without native identities. Copies
without a native snapshot cannot be pasted. Graph edges and Python behavior
are outside the saved node state.

If copy or paste stops during file replacement, use `mwf recover --dry-run` to
inspect the recorded operation. `mwf recover` restores an undecided replacement
or finishes cleanup after a recorded terminal decision. `mwf doctor` also reports
pending clipboard work and damaged recovery state.

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
