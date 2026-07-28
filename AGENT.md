# Agent guide for micro-workflow-manager 0.5.1

This file is the first resource an AI coding agent should read after opening the
archive. It serves two purposes:

1. guide the construction of a new MWF project; and
2. protect the framework's scheduling, filesystem, and durability invariants
   when modifying MWF itself.

For a complete project-shaped example, open
`examples/agent_reference_architecture/` first. For focused patterns, use the
index in `examples/README.md`. Framework internals and rationale are documented
in `DESIGN.md`; command semantics are documented in `README.md` and
`mwf --describe <command>`.

## Build new projects with this layout

```text
project/
├── AGENT.md                     project-specific operating rules
├── README.md                    purpose, setup, runbook, recovery
├── src/
│   ├── graph.py                 static dependency graph only
│   ├── config.py                environment/config parsing
│   ├── node_behavior/           one thin module per node
│   │   ├── ingest.py
│   │   └── transform.py
│   └── utils/                   shared domain and integration code
│       ├── agent.py             provider request/response contract
│       ├── http_client.py       pooled HTTP, finite timeouts
│       ├── validation.py        deterministic local checks
│       └── provenance.py        durable user-owned diagnostics
├── node/
│   └── <node>/input/            prompts and static node resources
└── tests/
    ├── test_graph.py
    ├── test_node_contracts.py
    └── test_end_to_end.py
```

Do not put HTTP clients, giant prompts, cross-node orchestration, or reusable
validation logic directly in `src/graph.py`. Node modules should adapt MWF
inputs/outputs to small shared functions. Runtime artifacts belong under
`node/<node>/input`, `node/<node>/output`, and `.mwf`; source code belongs under
`src`.

## Architecture rules

### 1. Make the durable file boundary explicit

Use `InputFileSystem`, `OutputFileSystem`, and `NodeInputFileSystem` for files
owned by the workflow. These APIs provide generation fencing, event tracing,
Windows long-path handling, and safe cross-node writes. Avoid raw `Path.write_*`
for MWF-managed outputs or forwarded inputs.

A node should normally write:

- the reusable result;
- the normalized input or request envelope;
- the model/tool/algorithm decision;
- validation evidence; and
- fallback or retry information.

Scheduler state in `.mwf/state.sqlite3` explains execution. It does not replace
project provenance.

### 2. Use the API runner for external latency

Declare blocking HTTP/SDK/database nodes with `runner="api"` and a realistic
`max_threads`. Use `micro_workflow_manager.shared_http_transport` rather than
creating a new client per job. Every request needs finite connect/read timeouts.
Pass a meaningful `wait_name`; the scheduler can then distinguish external wait
from a frozen handler.

Keep transport failure separate from semantic failure:

- task retries handle transient transport/provider errors;
- local parsing and validation reject malformed responses;
- named fallbacks change model, prompt, algorithm, or source;
- the final fallback should be conservative and deterministic where possible.

Never implement an unbounded retry loop inside a task. MWF owns attempts,
fallbacks, total timeout, checkpoint timeout, and trace chronology.

### 3. Fan out transactionally

Create related child jobs in one `ctx.transaction()` so a parent retry cannot
leave a half-published fan-out. Transaction-generated idempotency keys are stable
across retries. For large homogeneous batches, use `ctx.node(...).add_many(...)`
with deterministic keys.

Store each child result in a deterministic path such as
`parts/<request_id>/<index>.json`. A worker should be safe to rerun.

### 4. Make fan-in a real node

A join is not an implicit in-memory list. Give it an explicit job and durable
inputs. Each worker writes its contribution into the join node's input tree. The
join reads all required files, sorts them deterministically, validates
completeness, and writes one assembled result. The graph predecessors provide the
readiness barrier.

### 5. Design Hoeflein components deliberately

MWF augments the ordinary graph with explicit/autostart communication and then
computes strongly connected components. Each SCC is one **Hoeflein component**.
The component is scheduled as a unit; the component quotient is a DAG.

Use a cycle only when nodes genuinely form one bounded protocol, such as
`review -> revise -> review`. Every cycle needs:

- a monotone termination field (`iteration`, remaining work, accepted flag);
- an explicit maximum;
- idempotent child creation;
- no hidden sleep/poll loop; and
- tests for success, terminal failure, restart, and repeated fresh run.

An internal add to another node in the same component is component-autostart.
A downstream component does not start until external predecessor components are
complete. Reason from component membership and producer component, not only raw
edges.

### 6. Keep fallbacks source-aware and inspectable

Use named `@router.fallback(...)` functions. Each fallback receives the prior
error and should preserve valid work rather than blindly start over. Record the
stage/model/tool and the validation failure in project output. Keep the final
fallback strict: it may simplify output, but it must not silently invent data or
bypass a safety contract.

### 7. Make fresh preparation and deletion unambiguous

MWF 0.5.1 separates execution from preparation:

```bash
mwf reset NODE --dry-run       # run preparation, no execution
mwf resetfrom NODE --dry-run   # runfrom preparation, no execution
mwf clean NODE --dry-run       # delete jobs/output, preserve input
mwf cleanfrom NODE --dry-run   # same for descendants
mwf wipe NODE --dry-run        # delete jobs/output/input
mwf wipefrom NODE --dry-run    # same for descendants
```

These commands require typed confirmation unless `--yes` is supplied. Prefer a
dry run and `mwf inspect` before destructive changes. `reset/resetfrom` preserve
the producer-aware fresh-run semantics; `clean/wipe` intentionally delete every
job in the selected nodes.

## New-project workflow

1. Read `examples/README.md` and copy the closest pattern.
2. Write the graph and identify Hoeflein components before node code.
3. Define each node's input, output, and provenance contract.
4. Put provider transport and response parsing in `src/utils`.
5. Add task retries and named fallbacks with finite timeouts.
6. Publish fan-out in transactions and fan-in through durable files.
7. Add deterministic idempotency keys wherever an operation may be replayed.
8. Test each node directly, then test a full `runfrom` twice.
9. Exercise failure, `resume`, `resetfrom --dry-run`, and one fallback path.
10. Document exact setup, environment variables, and recovery commands.

## Project acceptance checklist

A project is not ready merely because one happy-path run passes. Confirm:

- graph declarations and `src/node_behavior/*.py` match;
- no provider call lacks a finite timeout;
- no task contains an unbounded retry or polling loop;
- every model response is locally parsed and validated;
- fallback stages are visible in `mwf trace` and project provenance;
- fan-out is atomic/idempotent;
- fan-in detects missing and duplicate contributions;
- every Hoeflein cycle has a deterministic bound;
- rerunning `mwf runfrom` does not accumulate duplicate child jobs;
- `mwf resume` preserves completed work after a controlled failure;
- Windows-deep paths are exercised if filenames derive from user content; and
- `mwf doctor` reports no graph/router mismatch.

## Framework contributor invariants

When editing MWF itself, preserve these invariants:

- SQLite is the authority for high-churn scheduler state; user payloads remain
  inspectable files.
- Generation/execution fencing blocks stale handlers from publishing output or
  child jobs after timeout/restart.
- A component failure stops new admission but joins already-started work to a
  terminal boundary.
- Fresh run/runfrom deletes jobs by producer component and preserves unrelated
  merge-branch jobs.
- `reset/resetfrom` invoke the exact same preparation as run/runfrom but never
  create a scheduler run.
- `clean/cleanfrom` remove all jobs and output in selected nodes while preserving
  input; `wipe/wipefrom` also remove input.
- Hoeflein SCC selection is the unit for run, resume, restart, reset, clean, and
  wipe unless an explicit job selection is supported.
- Framework file APIs normalize Windows `\\?\` paths without weakening
  containment checks.
- Trace preservation is controlled by `--keeptrace`; destructive defaults must
  be explicit and tested.

## Required framework-change workflow

1. Read the closest tests and implementation.
2. State the affected invariants.
3. Add or update one focused regression test.
4. Make the smallest coherent implementation change.
5. Run focused tests first.
6. Run the ordinary suite separately from timing-sensitive cycle tests.
7. Exercise the CLI more than once and in dry-run/confirmation modes.
8. Update README, command help, examples, and this file when behavior changes.

Ordinary suite:

```bash
python -m pytest -q --ignore=tests/test_autostart_cycles.py
```

Run cyclic/autostart tests in fresh processes:

```bash
python -m pytest -q tests/test_autostart_cycles.py::test_runfrom_supports_self_and_mutual_autostart_cycles_before_downstream
python -m pytest -q tests/test_autostart_cycles.py::test_threaded_diamond_cycle_spawns_100_seed_jobs_without_deadlock
python -m pytest -q tests/test_autostart_cycles.py::test_threaded_ring_cycle_spawns_100_seed_jobs_without_deadlock
python -m pytest -q tests/test_autostart_cycles.py::test_threaded_stochastic_game_engine_spawn_cycle_finishes
```

Scheduling, fresh-preparation, cleanup, or provenance changes must also run:

```bash
python -m pytest -q tests/test_036_hoeflein_scheduling.py
python -m pytest -q tests/test_038_fresh_resume_restart_semantics.py
python -m pytest -q tests/test_cli_help_and_clean_wipe.py
```

A wall-clock timeout is evidence, not a diagnosis. Inspect the active run,
component, job event stream, checkpoint deadline, provider timeout, thread count,
and SQLite mutation state before changing scheduler semantics.

For an inline execution timeline while reproducing a project issue, use:

```bash
mwf run NODE --monitor
mwf runfrom NODE --monitor
mwf monitor --once
```

## Failure isolation reminders

Reduce concurrency first when a failure appears only under load, then compare the
same workload at one worker, a moderate value, and the declared value. Separate
test-code freezing from framework freezing by checking whether job events and the
run heartbeat continue to advance while the harness is blocked. For a genuinely
stubborn unresolved issue, record controlled experiments and evidence in a local
`STUBBORN_ISSUE.md`; do not use such a file as a substitute for a regression test.

## Repeat-use matrix

For stateful changes, repeat the same fresh run, rerun one selected job, run two
different nodes, exercise resume after failure, and compare reset/clean/wipe dry
runs. Verify that child jobs do not accumulate and unrelated producer branches
remain intact.
