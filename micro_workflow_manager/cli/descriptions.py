HELP_EPILOG = """
Command help:
  mwf clean --help
  mwf run --help
  mwf resumefrom --help

Extended command descriptions:
  mwf --describe run
  mwf --describe runfrom
  mwf --describe resumefrom

Common flow:
  mwf init
  mwf graph src/graph.py
  mwf doctor
  mwf migrate --dry-run
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

Cleaning all nodes:
  mwf clean *
  mwf reset *
  mwf wipe *

Use 'mwf <command> --help' for syntax. Use 'mwf --describe <command>'
for a longer essay explaining behavior, file effects, and abstract examples.
"""

COMMAND_HELP_DESCRIPTIONS = {
    "init": "Initialize the current folder as an MWF project. This creates .mwf/project.json, .mwf/state.sqlite3, and lightweight editor/git sidecars but does not load task code.",
    "graph": "Set or explicitly synchronize the graph file. Graph paths are stored with '/' and paths containing either '/' or '\\' are accepted on Linux and Windows.",
    "doctor": "Run read-only project health checks for graph/router mismatches, malformed state, stale runs, and undeclared literal ctx.node(...) edges.",
    "migrate": "Upgrade MWF-owned JSON and SQLite state schemas. User inputs, outputs, returned files, and provenance are never rewritten.",
    "inspect": "Inspect a node/job, list failed job IDs, or show node debug output.",
    "trace": "Render one job's chronological origin, task/fallback starts, ctx.trace objects, outputs, forwarded inputs, downstream jobs, and terminal state.",
    "filter": "Show the retry/fallback funnel, or list jobs at one stage boundary.",
    "recover": "Fence and requeue jobs left in running state by a dead CLI process. Done and failed jobs are not reset.",
    "clean": "Delete jobs and output for selected Hoeflein components while keeping node input files.",
    "reset": "Requeue every existing job for one selected DAG node or the selected node's whole Hoeflein component, while keeping job definitions and inputs.",
    "wipe": "Like component-level clean, but remove the selected components' input files as well.",
    "run": "Reset and run one ready node or selected jobs; --monitor prints the full timestamped dashboard in the same terminal.",
    "restart": "Second-terminal control that restarts running and failed/cancelled jobs in the selected active Hoeflein component; it never starts another scheduler.",
    "threads": "View or change run-scoped per-node max_threads overrides. API values are cooperative fiber counts with no aggregate framework cap; active nodes scale live.",
    "deploy": "Create .mwfignore, build an overwrite-in-place local deployment archive, and upload/extract it on a configured server.",
    "resume": "Register output-backed finished jobs, then continue unsuccessful or queued work for one node without resetting done or skipped jobs.",
    "runfrom": "Reset and run one node and its descendants; --monitor retains a timestamped dashboard timeline in the same terminal.",
    "resumefrom": "Continue unsuccessful or queued work from one node through its descendants without resetting completed jobs.",
    "monitor": "Show live or one-shot node/job statistics without running task code; completed sequences report active run: none.",
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

Afterward, create a graph file and register it with mwf graph. If `.mwf/project.json` already exists, init preserves its configuration, upgrades sidecars, and ensures the SQLite schema is ready.
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
    "doctor": """
Doctor is a read-only diagnostic pass. It builds on ordinary help by explaining
why a project may fail before you spend time running it. It compares graph nodes,
node folders, and node_behavior filenames; checks SQLite integrity and on-disk payload/config JSON; checks whether a recorded run is live or stale; and warns about literal
ctx.node("B") calls whose A -> B edge is absent.

Run it after editing the graph or moving the project between machines:
  mwf doctor

For example, if graph.py contains A -> B but src/node_behavior/B.py is missing,
doctor reports that mismatch without creating the file or changing any status.
A warning does not necessarily make the project unusable, while an ERROR causes
a nonzero exit status suitable for a simple test script.
""",
    "migrate": """
Migrate upgrades low-churn MWF JSON metadata and the SQLite scheduler schema.
On the first 0.3.4 migration it imports legacy job identity/status, queue markers,
events, checkpoints, execution generations, idempotency keys, default-job
manifests, and summary indexes into `.mwf/state.sqlite3`. User `input.json`,
`output.json`, returned files, node input/output folders, and project provenance
are never moved into the framework database.

Preview without creating the database or deleting legacy sidecars:
  mwf migrate --dry-run

Then apply the migration:
  mwf migrate

The importer removes only framework-owned legacy metadata after the transaction
is durable. If JSON or SQLite state claims a newer incompatible schema, MWF
refuses to downgrade it and asks you to install a compatible newer package.
""",

    "inspect": """
Inspect turns the hybrid file/SQLite state into a readable explanation. Node inspection
shows predecessors, successors, component membership, status counts, runner,
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
MWF automatically records task and fallback starts, framework-aware output
writes, forwarded node inputs, downstream job creation, and terminal state.

Example:
  mwf trace classify_document job 17

Trace values are serialized defensively, and displayed file contents are
truncated so a diagnostic command cannot flood the terminal. Event ordering is
the order in which the task called each operation; there is no fixed section
ordering for output, forwarding, job creation, and custom traces.
""",
    "filter": """
Filter reconstructs the current execution funnel from each job's append-only
lifecycle events. It shows how many jobs entered, succeeded at, and remained
after every main retry and fallback retry without adding scheduler hot-path
writes or a separate provenance manifest.

Examples:
  mwf filter process_number
  mwf filter process_number stage 1

The plain command prints only the funnel and current terminal counts. For a
non-final stage, `stage X` lists jobs that failed at X and then completed
successfully at X+1. For the final stage, it lists terminally failed jobs. Both
stage forms use compact `job_id: error` rows.
""",
    "recover": """
Recover is for an interrupted command whose owning process is definitely gone.
Active runs write a hostname, process ID, and scheduler heartbeat to
.mwf/run.json. The scheduler supervisor also manages job checkpoint deadlines,
but the two signals remain separate: a fresh run heartbeat proves the scheduler
is alive, while the job runtime row describes one job's latest progress. Recover uses
run ownership and each running job's execution record before it acts. It advances
the execution generation first, then requeues only abandoned running jobs, which
prevents a late stale process from committing afterward.

Preview or apply recovery:
  mwf recover --dry-run
  mwf recover

Suppose A finished, B was running a short calculation, and the terminal process crashed.
Recover leaves A done, requeues B, and records that the old run was recovered.
You can then use mwf resume B or mwf resumefrom B. Recover refuses to operate
while the recorded owner is still live.
""",
    "clean": """
Code context:
Clean loads the configured graph and routers only to validate node names; it does
not execute a task function. Naming any node selects its entire Hoeflein component.
Clean removes every selected component member's job folders and output while
preserving their input folders.

File-system context:
The jobs and output directories are recreated empty, while input remains in place. It is the broad reset to use when existing job definitions are no
longer useful and should be recreated from router.create_job(...) or by an
upstream node on the next run.

Examples:
  mwf clean make_number --dry-run
  mwf clean make_number
  mwf clean A B
  mwf clean "*"

If make_number previously created five random-number jobs, clean removes those
five job records. It does not run the function and does not delete files you put
in node/make_number/input/. Use reset when you want to keep the same jobs.
""",
    "reset": """
Reset preserves each SQLite job identity and `input.json`, but clears status/result
state and job-local returned files so every existing job becomes queued again. A
normal quotient-DAG node resets only itself. Naming a member of a nontrivial
Hoeflein component resets every member because that component is one scheduling
and lifecycle unit. The reset is batched across the selected scope.

Examples:
  mwf reset double_number --dry-run
  mwf reset double_number
  mwf reset A B

If jobs 1 and 2 were done, both are requeued. If you only want to continue the
failed job while preserving the done one, use mwf resume double_number instead.
""",
    "wipe": """
Wipe performs the same component-level cleanup as clean and also recreates every
selected component member's input folder empty. It is intended for a complete
local restart of a Hoeflein component's stored material, not for ordinary failure
recovery.

Examples:
  mwf wipe temporary_result --dry-run
  mwf wipe temporary_result
  mwf wipe "*"

A node function is not executed by wipe. Because input files are removed, use
this command only when those files can be recreated or are no longer needed.
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

`--monitor` prints the full timestamped dashboard in this terminal without
clearing prior task output. `--monitor-interval` controls the cadence. The final
snapshot is emitted after the run record becomes terminal, so it reports
`active run: none`. `--stats` remains the compact alternative.

A basic task might choose a random integer, double it, or call ctx.sleep(1). Run
uses the configured threaded, API, process, or direct runner and refuses to start if
another CLI sequence already owns the project. To preserve completed work after
a failure, use resume rather than run.
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
this command stores a local runtime override in .mwf/threads.json. It does not
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
without one OS thread per job, and there is no workflow-wide aggregate API cap.
Per-node overrides are scoped to the active or next run and are cleared when that
run finishes. Process pools read overrides when created, while a direct runner
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
Resume continues one node without erasing successful work. Before selecting any
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
rerun.
""",
    "runfrom": """
Runfrom is the fresh-run form for one Hoeflein component and its quotient-DAG
descendants. Naming any member selects the whole component. It deletes only jobs
produced by selected components, preserves jobs produced by other branches, and
then schedules the selected branch in dependency order.

For A -> B -> C:
  mwf runfrom A --plan
  mwf runfrom A --monitor

The inline dashboard observes the complete descendant set and retains every
timestamped snapshot in the terminal, which is useful for diagnosing readiness
or cyclic-component stalls. A simple A task might generate a number, B might add
one, and C might write the answer after a short ctx.sleep(1) delay. Runfrom
rebuilds only work attributable to the selected producer components. A later
runfrom from another incoming branch keeps this branch's completed descendant
jobs. Use resumefrom when unsuccessful jobs should continue without fresh
producer cleanup.
""",
    "resumefrom": """
Resumefrom mirrors runfrom's graph selection but uses resume semantics. It keeps
done and skipped jobs throughout the descendant set, requeues only unsuccessful
or abandoned work, and leaves existing queued jobs available. This makes it the
normal command after a partial runfrom failure.

For A -> B -> C:
  mwf resumefrom A --plan
  mwf resumefrom A

If A is done, one B job failed, and C has not run yet, resumefrom preserves A,
reruns the failed B job, and then allows C to continue when B completes. It does not perform producer-component cleanup and therefore preserves every
existing successful descendant job.
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
  mwf top explodeclaim explodecontext
  mwf top --once
  mwf top --once --json
""",
    "monitor": """
Monitor is a read-only live view over SQLite node/job summaries plus the
low-churn `.mwf/run.json` ownership record. It is safe to run in another
terminal because WAL readers do not claim the run slot or call node functions.

Examples:
  mwf monitor
  mwf monitor A B --once
  mwf monitor --json --once
  mwf runfrom A --monitor

During a task that waits for several seconds, monitor shows the observation time,
running job ID, queued and completed counts, effective `max_threads`, average
duration, and approximate remaining time. A run record is called active only
while its status is `running`; terminal records are shown as `active run: none`
plus a separate last-run line. Use inspect when you need the detailed lifecycle,
checkpoint, retry, or fallback history of one specific node or job.
""",

}
