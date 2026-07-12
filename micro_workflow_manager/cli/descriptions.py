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
  mwf run A
  mwf restart wait job 42
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
    "init": "Initialize the current folder as an MWF project. This creates .mwf and lightweight editor/git sidecars but does not load task code.",
    "graph": "Set or explicitly synchronize the graph file. Graph paths are stored with '/' and paths containing either '/' or '\\' are accepted on Linux and Windows.",
    "doctor": "Run read-only project health checks for graph/router mismatches, malformed state, stale runs, and undeclared literal ctx.node(...) edges.",
    "migrate": "Upgrade only MWF-owned metadata to the current state schema. User inputs, outputs, returned files, and event logs are never rewritten.",
    "inspect": "Explain one node or one job, including readiness, status counts, execution generation, input/output, and append-only job events.",
    "recover": "Fence and requeue jobs left in running state by a dead CLI process. Done and failed jobs are not reset.",
    "clean": "Delete jobs and output for selected nodes while keeping node input files.",
    "reset": "Requeue every existing job for selected nodes while keeping job definitions and node input files.",
    "wipe": "Like clean, but remove selected nodes' input files as well.",
    "run": "Reset and run one ready node, or reset and run explicitly selected job IDs.",
    "restart": "From a second terminal, safely replace running jobs inside an active run/runfrom/resume sequence without starting another scheduler.",
    "resume": "Continue unsuccessful or queued work for one node without resetting jobs that are already done or skipped.",
    "runfrom": "Reset and run one node and its descendants while respecting dependency readiness.",
    "resumefrom": "Continue unsuccessful or queued work from one node through its descendants without resetting completed jobs.",
    "monitor": "Show live or one-shot node/job statistics without running task code.",
}

COMMAND_DESCRIPTIONS = {
    "init": """
The help text tells you that init creates an MWF project. In practical terms,
this command places a small .mwf marker in the current folder so later commands
can find the project root from any subfolder. It does not import graph.py, create
workflow nodes, or execute functions. That separation is useful because you can
prepare a clean project shell before deciding what the graph should contain.

A minimal beginning is:
  mkdir simple_flow
  cd simple_flow
  mwf init

Afterward, create a graph file and register it with mwf graph. If .mwf already
exists, init leaves the existing project configuration intact.
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
project folder can move between Linux and Windows without rewriting .mwf first.
Deleting or renaming a graph node during --update deletes that node's folder, so
copy any data you still need before synchronizing.
""",
    "doctor": """
Doctor is a read-only diagnostic pass. It builds on ordinary help by explaining
why a project may fail before you spend time running it. It compares graph nodes,
node folders, and node_behavior filenames; parses important JSON state files;
checks whether a recorded run is live or stale; and warns about literal
ctx.node("B") calls whose A -> B edge is absent.

Run it after editing the graph or moving the project between machines:
  mwf doctor

For example, if graph.py contains A -> B but src/node_behavior/B.py is missing,
doctor reports that mismatch without creating the file or changing any status.
A warning does not necessarily make the project unusable, while an ERROR causes
a nonzero exit status suitable for a simple test script.
""",
    "migrate": """
Migrate updates the schema_version field on framework-owned metadata so a project
created by an older MWF release has an explicit, supported state format. It does
not change input.json, output.json, returned files, or events.jsonl, because those
contain user data or task results rather than scheduler metadata.

Preview the exact files first:
  mwf migrate --dry-run

Then apply the migration:
  mwf migrate

For a simple A -> B workflow, this may update .mwf, node_state.json, schema.json,
job.json, status.json, execution.json, and the rebuildable job index. If a file
claims a newer schema than the installed package supports, MWF refuses to
downgrade it and asks you to use a compatible newer package.
""",
    "inspect": """
Inspect turns the file-backed state into a readable explanation. Node inspection
shows predecessors, successors, component membership, status counts, runner,
timeout, and a sentence explaining why the node is complete, ready, blocked, or
failed. Job inspection additionally shows its input, output, execution generation,
and chronological events.jsonl history.

Examples:
  mwf inspect wait
  mwf inspect wait job 3

Imagine node wait has one failed job. The node view explains that the failure is
preventing completion and suggests mwf resume wait. The job view then shows when
it started, which fallback ran, whether it timed out, and the final error. Inspect
only reads state and never retries work.
""",
    "recover": """
Recover is for an interrupted command whose owning process is definitely gone.
Active runs write a hostname, process ID, and heartbeat to .mwf_run.json. Recover
uses that ownership information and each running job's execution record before it
acts. It increments the execution generation first, then requeues only abandoned
running jobs, which prevents a late stale process from committing afterward.

Preview or apply recovery:
  mwf recover --dry-run
  mwf recover

Suppose A finished, B was running a short wait, and the terminal process crashed.
Recover leaves A done, requeues B, and records that the old run was recovered.
You can then use mwf resume B or mwf resumefrom B. Recover refuses to operate
while the recorded owner is still live.
""",
    "clean": """
Code context:
Clean loads the configured graph and routers only to validate node names; it does
not execute a task function. It removes the selected nodes' job folders and output
while preserving their input folders.

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
Reset preserves job.json and input.json but removes each selected job's status,
result metadata, and job-local files so every existing job becomes queued again.
It also clears node output. This is useful when the inputs are correct and you
simply want all jobs to execute again.

Examples:
  mwf reset double_number --dry-run
  mwf reset double_number
  mwf reset A B

If jobs 1 and 2 were done, both are requeued. If you only want to continue the
failed job while preserving the done one, use mwf resume double_number instead.
""",
    "wipe": """
Wipe performs the same cleanup as clean and also recreates the selected input
folders empty. It is intended for a complete local restart of a node's stored
material, not for ordinary failure recovery.

Examples:
  mwf wipe wait --dry-run
  mwf wipe wait
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
  mwf run make_number
  mwf run double_number job 2
  mwf run wait jobs 1 3-5

A basic task might choose a random integer, double it, or call ctx.sleep(1). Run
uses the configured threaded, process, or direct runner and refuses to start if
another CLI sequence already owns the project. To preserve completed work after
a failure, use resume rather than run.
""",
    "restart": """
Restart is the second-terminal control for one job that is currently running
inside a live sequence. It does not import graph.py and does not launch another
scheduler. Instead, it atomically advances the job's execution generation, clears
that job's local result/files, and lets the existing scheduler start the new
generation while the surrounding run remains intact.

Examples:
  mwf restart wait job 4 --dry-run
  mwf restart wait job 4
  mwf restart wait jobs 4 7-8

A Python thread blocked in an outside library cannot always be force-killed, but
its old generation immediately loses permission to commit MWF-managed status,
files, or downstream jobs. Cooperative code can call ctx.checkpoint() or use
ctx.sleep(...) so it notices replacement quickly.
""",
    "resume": """
Resume continues one node without erasing successful work. Failed, cancelled,
and stale-running jobs are fenced and requeued; already queued jobs remain queued;
done and skipped jobs, their output records, and their files remain untouched.
The command then schedules whatever work is still needed.

Examples:
  mwf resume double_number --plan
  mwf resume double_number

Suppose double_number has jobs 1 and 2 done and job 3 failed. Resume runs job 3
only. This differs from mwf run double_number, which is a fresh node rerun. The
append-only event history records the resume transition so inspect can explain
what happened later.
""",
    "runfrom": """
Runfrom is the fresh-run form for a node and all of its descendants. It resets
the selected path, checks external predecessors, and then schedules ready nodes
in dependency order while allowing independent work to overlap under a concurrent
runner.

For A -> B -> C:
  mwf runfrom A --plan
  mwf runfrom A

A simple A task might generate a number, B might add one, and C might wait briefly
before writing the answer. Runfrom resets that selected sequence. Use resumefrom
when some jobs are already done and should stay done.
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
reruns the failed B job, and then allows C to continue when B completes. It does
not delete parent-created jobs merely because they belong to a descendant node.
""",
    "monitor": """
Monitor is a read-only live view over node_state.json, job_index.json, and job
status files. It is safe to run in another terminal because it never claims the
run slot or calls node functions.

Examples:
  mwf monitor
  mwf monitor A B --once
  mwf monitor --json --once

During a task that waits for several seconds, monitor shows the running job ID,
queued and completed counts, average duration, and approximate remaining time.
Use inspect when you need the detailed history of one specific node or job.
""",
}
