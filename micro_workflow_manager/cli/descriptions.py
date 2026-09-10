HELP_EPILOG = """
Command help:
  mwf reset --help
  mwf run --help
  mwf resumefrom --help
Extended command descriptions:
  mwf --describe run
  mwf --describe runfrom
  mwf --describe resumefrom
Common flow:
  mwf init
  mwf graph src/graph.py
  mwf engine
  mwf doctor
  mwf run A --plan
  mwf run A --monitor
  mwf restart <node-name>
  mwf restart <node-name> failed
  mwf restart <node-name> job 42
  mwf threads <node-name> +2
  mwf deploy setup
  mwf deploy local
  mwf deploy remote
  mwf resumefrom A
  mwf monitor
Destructive preparation and cleanup:
  mwf reset A --dry-run
  mwf reset * --yes
  mwf resetfrom A --dry-run
Use 'mwf <command> --help' for syntax. Use 'mwf --describe <command>' for a longer essay explaining behavior, file effects, and abstract examples.
"""
COMMAND_HELP_DESCRIPTIONS = {
    "init": "Initialize the current folder as an MWF project. This creates .mwf/project.json, .mwf/state.sqlite3, and lightweight editor/git sidecars but does not load task code.",
    "copy": "Save an idle node component's node files, native job identities, and supporting history under clipboard/<node>.",
    "paste": "Restore one node and its native history from this project's clipboard after validating current component peers.",
    "graph": "Set or explicitly synchronize the graph file. Graph paths are stored with '/' and paths containing either '/' or '\\' are accepted on Linux and Windows.",
    "engine": "Open the synchronized workflow as a read-only, graph-only local browser view. Hoeflein components are collapsed into scheduling units.",
    "doctor": "Check the synchronized graph, payload files, native session owners, and runtime settings without importing project code or changing project state.",
    "inspect": "Inspect a node/job, list failed job IDs, or show node debug output.",
    "trace": "Render one job's chronological origin, task/fallback starts, ctx.trace objects, outputs, forwarded inputs, downstream jobs, and terminal state.",
    "filter": "Show the retry/fallback funnel, or list jobs at one stage boundary.",
    "recover": "Fence and requeue jobs left in running state by a dead CLI process. Done and failed jobs are not reset.",
    "reset": "Perform the same fresh preparation as mwf run, including selected-job mode, but do not execute task code.",
    "resetfrom": "Perform the same producer-aware fresh descendant preparation as mwf runfrom, but do not execute task code.",
    "run": "Reset and run the ready Hoeflein component selected by one node, or selected roots and their same-component causal work; --monitor prints the full timestamped dashboard in the same terminal.",
    "restart": "Second-terminal control that restarts running and failed/cancelled jobs in the selected active Hoeflein component; it never starts another scheduler.",
    "threads": "View or change session-owned per-node max_threads overrides and the deprecated aggregate API admission budget; active threaded and API nodes scale live.",
    "deploy": "Create .mwfignore, build an overwrite-in-place local deployment archive, and upload/extract it on a configured server.",
    "resume": "Register output-backed finished jobs, then continue unsuccessful or queued work for the Hoeflein component selected by one node without resetting done or skipped jobs.",
    "runfrom": "Reset and run the Hoeflein component selected by one node and its quotient-DAG descendants; optional refuse stops before a boundary and refuseafter stops after it.",
    "resumefrom": "Continue a selected Hoeflein component and its quotient-DAG descendants; optional refuse stops before a boundary and refuseafter stops after it, retaining queued work.",
    "monitor": "Show live or one-shot node/job statistics without running task code; normal CLI bootstrap and router mounting may still update framework state.",
    "top": "Show event-driven htop-style scheduler, latency, process, database, and mutation-writer diagnostics.",
}
COMMAND_DESCRIPTIONS = {
    "init": """
The help text tells you that init creates an MWF project. In practical terms,
this command creates `.mwf/project.json`, initializes the transactional `.mwf/state.sqlite3` scheduler database, and writes editor/git sidecars. Later commands can find the project root from any subfolder. It does not import graph.py, create
workflow nodes, or execute functions. That separation is useful because you can
prepare a clean project shell before deciding what the graph should contain.
A minimal beginning is:
  mkdir simple_flow
  cd simple_flow
  mwf init

Afterward, create a graph file and register it with mwf graph. Repeated init on
a native 0.6.2 project preserves its configuration and stored work and refreshes
editor sidecars. Older project formats are refused without modification. Use
the external migration.md guide to prepare a separate revamped project with
fresh native state. Deployment archives initialize a separate fresh directory.
""",
    "copy": """
`mwf copy classify` saves `node/classify` and its native job identities,
supporting execution history, and retained component result under
`clipboard/classify`. Its component must be idle. Copy replaces the previous
saved copy without changing the live node. Recover abandoned ownership first.
""",
    "paste": """
`mwf paste classify` restores only `classify` within the originating project.
It validates native identities, supporting history, active membership, and the
combined state of the saved node and its current component peers before mutation.
Peer files and jobs remain intact. Completed and queued work retain their saved
state. Paste advances the component generation and restores the saved result.
Use `mwf recover --dry-run` to inspect interrupted clipboard file work, then
`mwf recover` to restore an undecided replacement or finish terminal cleanup.
""",
    "graph": """
The help text describes graph as the explicit synchronization point. This means
ordinary run, monitor, inspect, and cleanup commands will not silently add or
remove top-level node folders. Only graph changes the stored edge list and makes
the node directory match the Python graph definition.

For a very small graph, src/graph.py could contain:
  EDGES = [("make_number", "double_number")]

Register it with:
  mwf graph src/graph.py

After renaming a node or changing an edge, preview and then apply the change deliberately:
  mwf graph --update --dry-run
  mwf graph --update

MWF stores the relative path as src/graph.py even on Windows. Older or manually
edited configurations containing src\\graph.py are also accepted, so the same
project folder can move between Linux and Windows without rewriting .mwf/project.json first.
Deleting or renaming a graph node during --update deletes that node's folder, so
copy any data you still need before synchronizing.
""",
    "engine": """
`mwf engine` opens the synchronized workflow graph in a pan-and-zoom local browser; select a collapsed Hoeflein component to reveal its members. The
read-only loopback viewer has no external assets or mutation endpoints and does not
import/run project code or modify project state. Stop it with Ctrl+C.
""",
    "doctor": """
Doctor reads a database snapshot and the synchronized graph without importing
project code or changing project state. It compares node folders and
node_behavior filenames, checks SQLite integrity and payload JSON, reports
exact live or stale session owners, and warns about undeclared literal
ctx.node("B") calls.

Run it after editing the graph or moving the project between machines:
  mwf doctor

For example, if the synchronized graph contains A -> B but src/node_behavior/B.py is missing,
doctor reports that mismatch without creating the file or changing job status.
A warning does not necessarily make the project unusable, while an ERROR causes
a nonzero exit status suitable for a simple test script.
""",
    "inspect": """
Inspect turns the hybrid file/SQLite state into a readable explanation. Node inspection
shows predecessors, successors, component state, stability, interrupt origin,
first misalignment causes, raw-node job counts, runner,
total timeout, checkpoint timeout, and why the node is ready, blocked, complete,
or failed. Job inspection additionally shows the active or last task, named
checkpoint, checkpoint deadline, progress percentage/detail, input, output,
execution generation, and chronological lifecycle events stored in SQLite.

Examples:
  mwf inspect process_number
  mwf inspect process_number failed
  mwf inspect process_number job 3
  mwf inspect process_number debug

A simple process_number task might report checkpoint "number chosen" with
progress 50%, then call ctx.sleep(1) before doubling the number. The job view
displays that live progress from SQLite without executing or retrying anything.
The failed view gives copyable failed job IDs and concise errors. Retry/fallback
funnel inspection is a separate command: `mwf filter process_number`, with
`mwf filter process_number stage 2` for one stage boundary. If the checkpoint
deadline expires, inspect shows the timeout reason and the event history shows
which fallback ran afterward.
""",
    "trace": """
Trace renders the append-only event journal for one job as a chronological,
human-readable execution transcript. Node code can append arbitrary structured
records with `ctx.trace(name, content=..., input=..., output=..., status=...)`.
MWF records task/fallback starts, failed attempts, framework-aware output writes,
forwarded inputs, downstream jobs, and terminal state.

Example: `mwf trace classify_document job 17 [--errors]`
Use `--errors` for identity, origin, ordered failures, attempt details, terminal state, and terminal error.
Use `--lineage [--json]` for persisted component state, sample and interrupt IDs,
the creating job, and directly created jobs. This view loads no project code
and omits task bodies and recursive descendants. JSON uses schema version 1.
`--errors` and `--lineage` are mutually exclusive; `--json` requires `--lineage`.

Trace values are serialized defensively, and displayed file contents are
truncated so a diagnostic command cannot flood the terminal. Event ordering is
the order in which the task called each operation; there is no fixed section
ordering for output, forwarding, job creation, and custom traces.
""",
    "filter": """
Filter reconstructs the current execution funnel from append-only lifecycle
events without extra scheduler writes or a separate output manifest. It
shows entry, success, and remaining counts after each retry. Stage errors use
durable failed-attempt events; older terminal journals fall back to stored data.

Examples:
  mwf filter process_number
  mwf filter process_number stage 1

The plain command prints only the funnel and current terminal counts. For a
non-final stage, `stage X` lists jobs that failed at X and then completed
successfully at X+1. For the final stage, it lists terminally failed jobs. Both
stage forms use compact `job_id: error` rows.
""",
    "recover": """
Recover examines native execution sessions and exact job owners in SQLite.
It leaves live sessions active. For each abandoned execution it retains a
valid terminal result, or advances the generation and requeues unfinished
work. It then settles that session's component results, reservations, and
holds. Damaged or ambiguous ownership refuses before the affected work changes.
A scheduler heartbeat and an individual job checkpoint describe different
kinds of progress.

Preview or apply recovery:
  mwf recover --dry-run
  mwf recover

If A finished and B's owner crashed, recovery preserves A and recovers B's
unfinished work. An unrelated live session stays active. Use mwf resume B or
mwf resumefrom B after recovery. The preview changes no project state.

""",
    "reset": """
Reset is `mwf run` without execution. Whole-node mode performs the exact fresh-run
preparation for the selected Hoeflein component: retained jobs are requeued,
generated output is cleared, and jobs produced by that component are removed so
they can be recreated deterministically later. Selected-job mode resets only the
named jobs, matching `mwf run NODE job ...` preparation.

Examples:
  mwf reset transform --dry-run
  mwf reset transform
  mwf reset transform job 2
  mwf reset transform jobs 1 3-5 --yes

A typed `reset` confirmation is required unless `--yes` is supplied. No task,
fallback, HTTP request, or downstream scheduler is started.
""",
    "resetfrom": """
Resetfrom is `mwf runfrom` without execution. It applies the same fresh,
producer-aware preparation to the start Hoeflein component and all descendants:
selected-producer jobs are removed, the start component is fully reset, and merge
components preserve jobs originating from unselected branches.

Examples:
  mwf resetfrom ingest --dry-run
  mwf resetfrom ingest
  mwf resetfrom ingest refuseafter publish --yes

`refuseafter` is accepted for command symmetry and validated against the selected
branch, but no admission occurs, so it never shrinks the reset scope.
""",
    "run": """
Run deliberately starts fresh work for one node. In normal node mode it resets
the selected run set before scheduling it. In job-selection mode it resets only
the named job IDs, leaving the other jobs in that node untouched.

Examples:
  mwf run make_number --plan
  mwf run make_number --monitor
  mwf run double_number job 2 --monitor
  mwf run process_number jobs 1 3-5
  mwf run process_number sample 100 --seed 20260817 --plan
  mwf run process_number sample 100 --seed 20260817
  mwf run process_number --keeptrace

`--monitor` prints the full timestamped dashboard in this terminal without
clearing prior task output. `--monitor-interval` controls the cadence. The final
snapshot is emitted after its session becomes terminal. It preserves that
session's result and shows other sessions separately. `--stats` is compact.

A basic task might choose a random integer, double it, or call ctx.sleep(1). Run
uses the configured threaded, API, process, or direct runner and refuses to start if
another main session owns the main slot. Explicit interrupt sessions have their
own selection and ownership checks. To preserve completed work after
a failure, use resume rather than run. Fresh runs clear affected trace journals
unless `--keeptrace` is supplied.

Sample mode provides deterministic SHA-256 ranking of existing jobs and preservation
of unselected jobs. Use `--status` to narrow candidates and `--expect-population`
to refuse changed eligible work or inputs. A count or percentage applies after
status filtering. Named assignments select raw members of the same component.
The selected roots and their new same-component causal work run; unrelated
existing jobs and quotient descendants do not execute. `--plan` is read-only
and does not import user code or create an execution session.
""",
    "restart": """
Restart is a second-terminal control for the workflow sequence that is currently
active. Naming a node selects its persisted active Hoeflein component; a DAG node
is a singleton component. The command never launches another scheduler.

Examples:
  mwf restart <node-name> --dry-run
  mwf restart <node-name>
  mwf restart <node-name> failed
  mwf restart <node-name> job 4
  mwf restart <node-name> jobs 4 7-8

The default form advances the generation of every live-running and
failed/cancelled job in the component. The `failed` form leaves running attempts
alone. Explicit job/job ranges remain available. Already queued, done, and
skipped work is not reset. The active scheduler remains in control and observes
the replacement generations through the normal durable queue.

A Python thread blocked in an outside library cannot always be force-killed, but
its old generation immediately loses permission to commit MWF-managed status,
files, or downstream jobs. After the active sequence has ended, use `mwf resume
NODE` or `mwf resumefrom START`; resume first registers terminal output files
that were still recorded as running, then requeues the remaining unsuccessful
work.
""",
    "threads": """
Threads is a lightweight second-terminal control for testing node concurrency.
The max_threads value declared in the node router remains the durable default;
this command stores an override against its exact native session owner in SQLite. It does not
edit node_behavior source or restart the workflow.

Examples:
  mwf threads
  mwf threads <node-name>
  mwf threads <node-name> 8
  mwf threads <node-name> +2
  mwf threads <node-name> -1
  mwf threads <node-name> reset

For an active threaded or API node, increasing the value starts additional queued jobs
within roughly 0.2 seconds. Decreasing it never kills jobs already running; MWF
stops launching replacements until active concurrency falls to the new limit.
For example, a node declared with `max_threads=2` can be raised to 5 during a test.
API node values are cooperative fiber counts. They may be set into the thousands
without one OS thread per job. `mwf threads --api-total 500` sets a project-wide aggregate API admission budget across API sessions; `reset` restores the default.
The --api-total option is deprecated and remains functional. No removal date or
session-specific form is introduced in 0.6.2.
Per-node overrides belong to the exact live owner, or remain pending for the
next owner of that node. Settlement clears only the ending session's overrides. Process pools read overrides when created, while a direct runner
always executes one job at a time.
""",
    "deploy": """
Deploy is an explicit two-stage copy workflow for testing code on another machine.
The setup action stores only connection metadata under .mwf/deploy/server.json and
creates .mwfignore. Passwords are never written to disk. Password authentication
uses PuTTY pscp/plink; key authentication normally uses OpenSSH unless the key is
a .ppk file.

Typical Windows flow:
  mwf deploy setup
  mwf deploy local
  mwf deploy remote

The local action deletes the previous .mwf/deploy/local deployment, copies only
paths allowed by .mwfignore, compresses each direct node subfolder independently,
and creates one deployment.zip. The remote action confirms which local archive to
use, asks for a destination path, uploads the single archive, and extracts both the
project archive and per-node archives on the server. Existing files with matching
paths are overwritten, while unrelated remote files are left alone. Review
.mwfignore before every sensitive deployment, especially when the project contains
.env files, API keys, large node outputs, or local credentials.
""",
    "resume": """
Resume continues the Hoeflein component selected by one node without erasing successful work. Before selecting any
retries, it reconciles terminal `output.json` files for the selected Hoeflein
component and waits for those SQLite updates to become durable. It then fences
and requeues failed, cancelled, or genuinely stale-running jobs; already queued
jobs remain queued, while done and skipped jobs and their files remain untouched.

Examples:
  mwf resume double_number --plan
  mwf resume double_number

Suppose double_number has jobs 1 and 2 done, job 3 failed, and job 4 wrote a done
output just before the previous process exited. Resume registers job 4 first and
runs only job 3. This differs from `mwf run double_number`, which is a fresh node
rerun. Resume always preserves the selected current component's trace journal;
`--keeptrace` is accepted for consistency but is redundant for this command.
""",
    "runfrom": """
Runfrom is the fresh-run form for one Hoeflein component and its quotient-DAG
descendants. Naming any member selects the whole component. It deletes only jobs
produced by selected components, preserves jobs produced by other branches, and
then schedules the selected branch in dependency order.

For A -> B -> C:
  mwf runfrom A --plan
  mwf runfrom A --monitor
  mwf runfrom A refuse B
  mwf runfrom A refuseafter B
  mwf runfrom A refuseafter B --keeptrace

The inline dashboard observes the complete descendant set and retains every
timestamped snapshot in the terminal, which is useful for diagnosing readiness
or cyclic-component stalls. A simple A task might generate a number, B might add
one, and C might write the answer after a short ctx.sleep(1) delay. Runfrom
rebuilds only work attributable to the selected producer components. A later
runfrom from another incoming branch keeps this branch's completed descendant
jobs. Use resumefrom when unsuccessful jobs should continue without fresh
producer cleanup. Both refusal modes keep the ordinary full descendant
freshening. `refuse B` stops globally as soon as B's component is ready, before
B or any other newly ready component starts. `refuseafter B` instead lets B's
component complete or fail and then stops later admission. Already-running
parallel components are joined and refused jobs remain queued. Trace journals
clear by default and are retained with `--keeptrace`.
""",
    "resumefrom": """
Resumefrom mirrors runfrom's graph selection but uses resume semantics. It keeps
done and skipped jobs throughout the descendant set, requeues only unsuccessful
or abandoned work, and leaves existing queued jobs available. This makes it the
normal command after a partial runfrom failure.

For A -> B -> C:
  mwf resumefrom A --plan
  mwf resumefrom A
  mwf resumefrom A refuse B
  mwf resumefrom A refuseafter B
  mwf resumefrom A refuseafter B --keeptrace

If A is done, one B job failed, and C has not run yet, resumefrom preserves A,
reruns the failed B job, and then allows C to continue when B completes. With
`refuse B`, B's whole component is not started; if it is already terminal, the
boundary is treated as already reached. With `refuseafter B`, B's component may
terminate, but no new later component is admitted afterward. Both forms join
already-running parallel components and retain later work for a future resume.
Resumefrom does not perform producer-component cleanup and therefore preserves
every existing successful descendant job. The start Hoeflein component's trace
is always preserved. Descendant trace journals are cleared before the resume
unless `--keeptrace` is supplied.
""",
    "top": """
Top is an event-driven, htop-style diagnostic view for a running or completed
workflow. Durable SQLite commits wake the display immediately; --interval is a
maximum redraw and defensive fallback cadence, not the normal state-discovery
mechanism.

It displays per-node queue/running/terminal counts, effective concurrency,
starts and finishes per second, queue wait and terminal publication p95, recent
lifecycle events, process RSS/thread count, SQLite/WAL size, and the active
process's mutation-writer queue, active batch, and durability backlog.

Examples:
  mwf top
  mwf top api_a api_b
  mwf top --once
  mwf top --once --json
""",
    "monitor": """
Monitor displays component state, stability, exact interrupt origin, first
misalignment causes, raw-node job counts, and native execution sessions,
including exact parent session IDs. It does not execute jobs or claim a session. Normal CLI
bootstrap and router mounting may still update framework state.

Examples:
  mwf monitor
  mwf monitor A B --once
  mwf monitor --json --once
  mwf runfrom A --monitor

During a task that waits for several seconds, monitor shows the observation time,
running job ID, queued and completed counts, effective `max_threads`, average
duration, and approximate remaining time. Each session shows its own running or terminal status, selected components,
parent sessions, and result. Multiple interrupts remain separately visible. Use inspect when you need the detailed lifecycle,
checkpoint, retry, or fallback history of one specific node or job.
""",

}


COMMAND_HELP_DESCRIPTIONS.update({
    'runbetween': 'Freshly prepare and run the half-open quotient interval from the start component to the excluded end component.',
    'resumebetween': 'Preserve successful work and resume the half-open quotient interval before the excluded end component.',
    'resetbetween': 'Apply runbetween preparation without executing tasks; preview with --dry-run.',
})

COMMAND_DESCRIPTIONS.update({
    'runbetween': """
`mwf runbetween A B` prepares and executes every component on a directed quotient
path from A to B, including A's component and excluding B's whole component.
The end must be a strict directed descendant of the start. Other branches stay
unselected. Published jobs or input can reach an excluded receiver, but that
receiver does not execute. The start component's incoming input is preserved.
Use `--plan` to inspect selected components, crossing edges, prerequisites, and
preparation effects without importing user code or changing durable state.
""",
    'resumebetween': """
`mwf resumebetween A B` uses the same half-open selection as runbetween. It
preserves done and skipped jobs and repairs eligible unsuccessful work. Aligned
sampled results retain compatible lineage while the remaining work runs. The
end component and unrelated branches do not execute. Use `--plan` for a
read-only preview and `--keeptrace` to preserve descendant traces.
""",
    'resetbetween': """
`mwf resetbetween A B` performs runbetween's fresh preparation without task
execution. The included start and excluded end select the same half-open
quotient interval. Preparation can remove selected producers' prior material
from excluded receivers and mark those receivers misaligned. Any live execution
session prevents reset. Use `--dry-run` to inspect the effects; apply with a
typed resetbetween confirmation or `--yes`.
""",
})

for _command in ('run', 'runfrom', 'runbetween', 'resume', 'resumefrom', 'resumebetween'):
    COMMAND_DESCRIPTIONS[_command] += """
Ordinary commands resolve reachable interrupt choices before changing state.
For unattended execution, use --interrupt-policy run-all, stop-all, or
individual with explicit --interrupt-choice NODE=run|stop values. Stopped
components retain their work. Use --interrupt at an interrupt-classified start
to pause active direct-parent attempts and freeze input before execution.
Partial samples and effective readiness overrides fence earlier commands.
Aligned sampled interrupt resume keeps its compatible original lineage.
"""
