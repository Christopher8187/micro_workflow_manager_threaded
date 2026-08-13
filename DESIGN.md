# MWF design recommendations and runnable patterns

## Advice first

1. **Design outputs for both reuse and diagnosis.** Every node should write the
   durable artifact needed by downstream work and enough user-owned provenance
   to explain how that artifact was produced. A result that can be reformatted
   but cannot be traced is incomplete for an iterative workflow.
2. **Give nodes domain names, not implementation names.** Prefer
   `validate_solution`, `classify_request`, or `apply_schema_change` over
   `step_3`, `worker`, or `process_data`.
3. **Keep one reason to retry per node.** Split model calls, validation, database
   mutation, and publication when they have different failure modes or fallback
   policies.
4. **Pass compact jobs; pass large data as files.** Job parameters should identify
   the work. Use `InputFileSystem`, `OutputFileSystem`, and
   `NodeInputFileSystem` for substantial artifacts.
5. **Use autostart for work created during execution.** Use explicit
   `router.create_job(...)` for known starter or join jobs. This makes the graph
   readable while preserving dynamic fan-out.
6. **Use fallbacks for alternate implementations, not hidden business stages.**
   A fallback should satisfy the same output contract as the main task and write
   which implementation ran to provenance.
7. **Make joins deterministic.** Name branch files by stable IDs, sort them before
   assembly, and record the expected set. Do not depend on thread completion
   order.
8. **Use idempotency at side-effect boundaries.** Downstream job creation,
   database mutations, uploads, and publication need stable keys or transactions.
9. **Validate independently from generation.** A generator should not be the only
   authority deciding whether its result is correct.
10. **Inspect the funnel before adding capacity.** `mwf filter NODE`
    distinguishes a slow stage from a low-quality main attempt or fallback.
11. **Batch high-fanout registration without changing job granularity.** When one
    task emits many independent downstream objects, use
    `NodeInputFileSystem.write_jsons(...)` followed by `add_jobs(...)`. Each object
    remains a separate downstream job, while file and SQLite registration avoid
    one global lock/transaction cycle per object. `autostart=False` keeps the
    producer and consumer in separate Hoeflein components.

## The output contract: artifact plus provenance

The `output/` folder is a deliberate debugging surface. A production node should
normally contain both:

- the **durable result**, in a stable machine-readable form that can be copied,
  indexed, reformatted, or passed downstream; and
- **provenance**, written by project code, that records the inputs, decisions,
  implementation or model, attempt/fallback, validation evidence, and relevant
  parameters that produced that result.

A practical layout is:

```text
node/validate_solution/output/
  validated_solution.json
  metrics.json
  provenance/
    job_17_validation.json
```

A useful provenance record often includes:

```json
{
  "node": "validate_solution",
  "job_id": 17,
  "task": "validate_solution",
  "attempt": 1,
  "inputs": {"solution_file": "candidate_17.json"},
  "decisions": {
    "validator": "independent residual suite",
    "tolerance": 1e-9,
    "coordinate_system": "cartesian"
  },
  "result": {"accepted": true, "max_residual": 2.1e-12}
}
```

MWF's SQLite lifecycle events, job statuses, execution generations, and checkpoint data explain scheduler behavior through `mwf inspect` and `mwf monitor`. They do not replace domain provenance. Project provenance should explain *why the result is defensible*; scheduler diagnostics explain *what the framework did while running it*.

For an agent node, retain the model/provider/version, prompt or prompt hash,
structured response, tool calls, validation failures, chosen fallback, and token
or latency measurements when available. Avoid storing secrets or unnecessary
personal data.

All runnable examples in `examples/` use `src/utils/provenance.py` to create a
small JSON record under each node's `output/provenance/` folder. Replace that
helper with a domain-specific schema in a real project.

## Recommended project architecture

```text
project/
  src/
    graph.py
    node_behavior/
      discover_sources.py
      normalize_sections.py
      publish_records.py
    utils/
      provenance.py
      schemas.py
      domain_helpers.py
  node/                  # inspectable runtime/data folders
  clipboard/             # saved node snapshots
  examples/ or fixtures/
  tests/
  README.md
```

A node behavior file should read in this order:

1. imports;
2. `NodeRouter` declaration and explicit starter jobs;
3. filesystem declarations;
4. small pure helper functions;
5. main task;
6. fallbacks that preserve the main output contract.

## Choose the runner by the dominant cost

- Use `direct` for debugging and small sequential work.
- Use `threaded` for mixed local I/O and modest parallelism where adaptive
  worker growth is useful.
- Use `api` for blocking model providers, HTTP SDKs, remote databases, and other
  high-latency calls. Its `max_threads` value intentionally means the node's
  maximum in-flight calls. Simultaneously runnable API nodes share a bounded
  controller-pump vector. One pump is guaranteed per node; the remaining pool is
  allocated from the full concurrency vector by marginal benefit. Pump limits
  are exact partitions of each node's `max_threads`: internal sharding improves
  admission and callback throughput but never changes configured concurrency.
  Independent node limits add together by default.
  When several nodes share one provider/account, `mwf threads --api-total N`
  adds a run-scoped aggregate admission budget and divides it proportionally by
  their requested limits before jobs are claimed.
- Use `process` for CPU-heavy, pickleable tasks that benefit from process
  isolation.

The CLI-restartable execution shape is one runner worker/controller plus one
abandonable handler thread for the current user call. Retry and fallback
orchestration remains synchronous in the controller, so there is no extra
per-attempt orchestration thread. Keep external client timeouts even when MWF
checkpoints are enabled because Python cannot force-kill an arbitrary blocked
thread.

## Design the quotient DAG, not only individual nodes

MWF 0.3.6 schedules **Hoeflein components**. Ordinary graph edges retain their
one-way dependency meaning. An edge explicitly used with `autostart=True` also
adds reverse reachability for component construction. Formally, if `A` is the
set of explicit autostart edges:

```text
G_H = (V, E union {(v, u) : (u, v) in A})
Hoeflein(G) = SCC(G_H)
HDAG(G) = G / Hoeflein(G)
```

Design each Hoeflein component as one communicating subsystem. Its member nodes
start, quiesce, and fail together. Every original edge between members is
implicitly component-autostart, so do not rely on a non-autostart internal edge
to create a later DAG barrier. Put a real barrier between separate components.

The component scheduler keeps one resident node runner active per member; an API
runner may own several cooperative controller pumps within its exact node limit.
It polls idle member queues while other runners are still active. This matters for router
patterns: a long-running router can continue creating handler jobs while those
handlers drain concurrently, and a handler whose current queue snapshot empties
is restarted when more work arrives. Queued work alone does not make a component
look active in `mwf monitor` before a run starts.

Use explicit autostart only when the child belongs to the same communicating
subsystem or must wake immediately as part of that subsystem. Keep ordinary
cross-component edges for directed dependency flow. A useful review question is:
"Should these nodes be independently rerunnable, or should naming any one of them
run the whole group?" If independently rerunnable, do not connect them into a
mutually reachable autostart structure.

For merge graphs, producer provenance is part of the design. With `A -> C` and
`B -> C`, jobs arriving at C carry producer components `{A}` or `{B}`. A fresh
`runfrom B` fully resets B's selected start component, removes B-produced jobs
in descendants, and preserves A-produced jobs in shared merge components. Therefore:

- use stable idempotency keys within each producer branch;
- keep job-local output/provenance attributable to one producer;
- avoid deleting shared node-level output blindly when other producer jobs remain;
- expect a merge component to reactivate as later branches produce new work.

The starting component of `run`/`runfrom` requires all external predecessor
components complete. Descendant components in a partial `runfrom` may process the
selected branch before other incoming branches finish. This is deliberate and is
what makes branch-by-branch testing and reruns safe.

## Treat SQLite as framework state, not an application database

`.mwf/state.sqlite3` holds scheduler-owned job rows, queue state, events,
checkpoints, execution leases, idempotency keys, and advisory locks. Do not make
domain code query or mutate it directly. Use MWF APIs and CLI inspection. Keep
application records, artifacts, and provenance in node input/output files or in
a separate domain database owned by the project. This separation keeps
clipboard, migration, restart fencing, and future schema upgrades reliable.

SQLite WAL permits concurrent readers, but SQLite still has one writer. MWF
therefore serializes same-process framework writes before opening an immediate
transaction, closes each framework worker's connection when that worker exits,
and rolls back failed commits. High-frequency checkpoint updates use one atomic
row update. Per-job restart fences use operating-system file locks rather than
database advisory rows, because wrapping every payload write in an advisory
lock would add two unnecessary SQLite writes to the critical path.

Use pure functions in `src/utils/` for parsing, scoring, geometry, SQL planning,
or state transitions. Keep MWF context calls at the node boundary. This makes
unit testing possible without creating a workflow project for every calculation.

## Commands used across designs

```bash
mwf init
mwf graph src/graph.py
mwf graph --update --dry-run
mwf graph --update
mwf doctor
mwf run START --plan
mwf runfrom START
mwf runfrom START refuseafter STOP
mwf monitor --once
mwf inspect NODE
mwf filter NODE
mwf inspect NODE failed
mwf inspect NODE job 1
mwf restart NODE job 1
mwf resume NODE
mwf resumefrom NODE
```

Use `run` for a fresh reset of one selected Hoeflein component and `runfrom`
for a fresh reset of that component plus producer-scoped rebuilding through its
quotient-DAG descendants. Use `resume`/`resumefrom` after a partial failure so
successful jobs and their outputs are preserved while failed/cancelled jobs are
requeued automatically. Use `restart` only from a second terminal to control a
specific running or failed job inside the active sequence.

Use `runfrom START refuseafter STOP` when the entire descendant reset must happen
up front but component admission must end at a durable checkpoint in the graph.
STOP selects its entire Hoeflein component. Once that component terminates, MWF
does not start another component; already-running parallel components finish and
newly produced downstream jobs remain queued.

Fresh/destructive CLI operations clear their affected trace journals by default.
Use `--keeptrace` when historical attempts must remain inspectable. `resume`
preserves the current component automatically; default `resumefrom` preserves
the start component while clearing descendant traces. Copy/paste snapshots carry
the journal, and a preserved job recreated under a different parent emits an
`ORIGIN CHANGED` trace subsection.

For a timestamped diagnostic timeline in the execution terminal, add `--monitor`:

```bash
mwf run NODE --monitor
mwf runfrom NODE --monitor --monitor-interval 1
```

Inline snapshots deliberately retain prior output and finish with `active run:
none`. This makes them useful for differentiating slow progress, resource
pressure, timeout escalation, and a genuine scheduler freeze. Automated
contributors must follow [AGENT.md](AGENT.md), including repeat-use tests and
controlled concurrency/timeout experiments.


## Design tests as experiments, not wall-clock guesses

A reliable workflow test should expose state transitions, not merely wait longer.
Use deterministic inputs and probabilities, inspect SQLite-backed status/events,
and capture inline monitor snapshots at known intervals. When a high-concurrency
test fails, compare lower `max_threads` values and runner modes before changing
scheduler code. When a timeout fires, identify whether it belongs to the harness,
task, checkpoint, or external client. Run cycle stress cases in separate
processes, then preserve both a small deterministic regression and an explicit
high-load test.

Stateful commands should be exercised repeatedly: rerun the same node, run a
different node, repeat `runfrom`, refresh declarations, copy/paste, and rebuild a
deployment archive. This catches stale caches, leaked lifecycle threads, and
second-use database bugs that a clean one-shot test misses. The complete required
protocol and rare escalation rules are in [AGENT.md](AGENT.md).


---

# Adapted source-and-utilities designs

These examples are renamed, self-contained adaptations of the kinds of parsing,
solver, validation, and content-processing pipelines that naturally live in a
`src/` plus `utils/` project. They are not copies of application-specific code;
they demonstrate cleaner node boundaries and stronger output provenance.

## 1. Document refinery

**Location:** `examples/document_refinery`

```text
discover_sources
  -> normalize_sections
  -> attach_assets
  -> publish_records
```

- `discover_sources` creates one job for each source record.
- `normalize_sections` converts source-specific content to a stable schema.
- `attach_assets` keeps image/asset relationships explicit.
- `publish_records` writes the final reusable record.
- Every node records its source, transformation decision, and result.

This design is preferable to one large “process document” node because discovery,
normalization, asset association, and publication have different retry and
validation policies.

```bash
cd examples/document_refinery
mwf init
mwf graph src/graph.py
mwf runfrom discover_sources
mwf monitor --once
mwf filter normalize_sections
mwf inspect publish_records job 1
```

## 2. Geometry solver lab

**Location:** `examples/geometry_solver_lab`

```text
parse_construction
  -> choose_seed
  -> solve_coordinates
  -> validate_solution
  -> format_coordinates
```

This separates parser, seed/autofix policy, numerical solving, independent
validation, and output formatting. The solver can retry without changing the
parser. Validation does not merely repeat the equations supplied to the solver.
The formatter retains the seed, coordinate system, tolerance, and accepted
residuals so a compact coordinate string remains debuggable.

```bash
cd examples/geometry_solver_lab
mwf init
mwf graph src/graph.py
mwf runfrom parse_construction
mwf filter solve_coordinates
mwf inspect validate_solution job 1
mwf inspect format_coordinates job 1
```

For a larger geometry system, split validators by invariant family only when they
need different concurrency or failure handling. Otherwise keep one validator and
return a structured list of all residual checks.

---

# Five common agentic workflow patterns

The following five examples follow the workflow taxonomy described in
Anthropic's *Building effective agents*: prompt chaining, routing,
parallelization, orchestrator-workers, and evaluator-optimizer. The included
projects are deterministic and offline so their architecture can be tested
without API credentials. Replace the demo functions with model/tool clients while
preserving the same node, filesystem, validation, and provenance contracts.

Reference: <https://www.anthropic.com/engineering/building-effective-agents>

## 3. Prompt chaining

**Location:** `examples/agent_prompt_chain`

```text
draft_brief -> extract_constraints -> compose_response
```

Use prompt chaining when one call can make the next call smaller and more
reliable. Each node emits a compact intermediate result rather than forwarding a
large conversational transcript. Add a validator or fallback at the stage whose
contract can fail.

```bash
cd examples/agent_prompt_chain
mwf init
mwf graph src/graph.py
mwf runfrom draft_brief
mwf inspect draft_brief job 1
mwf inspect extract_constraints job 1
mwf inspect compose_response job 1
```

**Production provenance:** prompt template/version, model, structured response,
constraint checks, and final formatting decisions.

## 4. Routing

**Location:** `examples/agent_router`

```text
classify_request -> answer_with_specialist
```

The classifier chooses a named specialist strategy and passes one downstream
job. The execution node maps that route to the selected implementation and has a
safe fallback satisfying the same answer contract. This avoids creating fake
jobs for unselected branches and allows `runfrom` to finish cleanly.

```bash
cd examples/agent_router
mwf init
mwf graph src/graph.py
mwf runfrom classify_request
mwf inspect classify_request job 1
mwf filter answer_with_specialist
mwf inspect answer_with_specialist job 1
```

**Production provenance:** route label, confidence, classifier/model version,
selected specialist, fallback, and the reason for overriding a low-confidence
route.

## 5. Parallelization

**Location:** `examples/agent_parallelization`

```text
                    -> collect_facts ----\
fan_out             -> generate_options ---> synthesize_answer
                    -> check_risks -----/
```

The fan-out node creates three jobs in one transaction. Branches run
independently and write stable named files into the join node's input. The join
has one explicit starter job and becomes ready only after its predecessors
complete. This uses concurrency without making the synthesis depend on completion
order.

```bash
cd examples/agent_parallelization
mwf init
mwf graph src/graph.py
mwf runfrom fan_out
mwf monitor --once
mwf filter collect_facts
mwf inspect synthesize_answer job 1
```

**Production provenance:** branch list, source/tool used by each branch, expected
join inputs, missing/late branch policy, and synthesis rubric.

## 6. Orchestrator-workers

**Location:** `examples/agent_orchestrator_workers`

```text
plan_work -> execute_work_item (many jobs) -> assemble_report
```

The orchestrator produces an explicit plan and dynamically creates worker jobs.
`execute_work_item` uses `max_threads=3`; each worker writes a section named by a
stable index. The assembly job sorts by that index, not completion time. Worker
retries are visible in `mwf filter execute_work_item`.

```bash
cd examples/agent_orchestrator_workers
mwf init
mwf graph src/graph.py
mwf runfrom plan_work
mwf inspect plan_work job 1
mwf filter execute_work_item
mwf inspect assemble_report job 1
```

**Production provenance:** decomposition prompt/version, work-item IDs,
dependencies, worker model/tool choice, attempt count, and assembly ordering.

## 7. Evaluator-optimizer

**Location:** `examples/agent_evaluator_optimizer`

```text
generate_candidate
  -> evaluate_candidate
  -> improve_candidate
  -> final_evaluation
  -> publish_candidate
```

Generation and evaluation are separate authorities. The evaluator returns a
structured score and actionable feedback; the optimizer applies that feedback;
a final evaluator checks the revised result before publication. For an open-ended
loop, use a bounded cycle and a stop condition, but retain every candidate and
score rather than overwriting history.

```bash
cd examples/agent_evaluator_optimizer
mwf init
mwf graph src/graph.py
mwf runfrom generate_candidate
mwf inspect evaluate_candidate job 1
mwf inspect final_evaluation job 1
mwf inspect publish_candidate job 1
```

**Production provenance:** candidate lineage, rubric version, evaluator model,
score components, applied feedback, stop reason, and acceptance threshold.

---

# Application designs

## 8. Database change manager

**Location:** `examples/database_change_manager`

```text
plan_schema_change
  -> apply_schema_change
  -> verify_database
  -> export_schema_report
```

The example manages a SQLite database:

- planning writes the intended DDL and safety assumptions without mutating data;
- applying creates a backup and performs the change in a transaction;
- verification independently inspects schema and expected rows;
- reporting emits a portable schema/change report.

The database file is a durable artifact, while checksums, SQL, backup location,
row counts, schema observations, and verification results are provenance. In a
real service, use a migration ID as the idempotency key and keep credentials out
of outputs.

```bash
cd examples/database_change_manager
mwf init
mwf graph src/graph.py
mwf runfrom plan_schema_change
mwf inspect plan_schema_change job 1
mwf inspect apply_schema_change job 1
mwf inspect verify_database failed
mwf inspect export_schema_report job 1
```

Useful extensions:

- a `request_approval` node between plan and apply;
- separate `backup_database` and `restore_database` nodes;
- one verification job per table for large databases;
- a fallback that restores only when the apply contract proves rollback-safe;
- a publication node that stores signed migration evidence.

## 9. Pygame state machine

**Location:** `examples/pygame_state_machine`

```text
load_game_session
  -> apply_game_event (ordered jobs)
  -> render_frame
```

The loader writes an initial state and creates an ordered event stream. The state
reducer is sequential so two transitions cannot race. Each transition records
previous state, event, guard result, and next state. The final renderer reads the
state and emits a textual frame; a real Pygame project can replace it with
`pygame.Surface` rendering while leaving state transitions testable as pure
functions.

```bash
cd examples/pygame_state_machine
mwf init
mwf graph src/graph.py
mwf runfrom load_game_session
mwf inspect apply_game_event job 1
mwf inspect apply_game_event job 3
mwf inspect render_frame job 1
```

Recommended production split:

```text
load_assets -> initialize_session -> reduce_events -> simulate_world -> render_frame
                                      \-> persist_checkpoint
```

Keep rendering separate from the authoritative game-state transition. Store RNG
seed, delta time, input event, previous state hash, next state hash, and guard
outcome so a bad frame can be replayed deterministically.

---

# Choosing retries, fallbacks, and nodes

Use a **retry** when the same implementation may succeed after a transient error:
network interruption, rate limit, temporary file lock, or nondeterministic model
formatting failure.

Use a **fallback** when an alternate implementation can satisfy the same contract:
a second model, a simpler parser, a cached source, a lower-resolution renderer,
or a safe read-only database report. Record the fallback name and previous error
in output provenance.

Use a **new node** when the stage has a different contract, owner, concurrency
limit, validation criterion, or side effect. Do not hide approval, validation, or
publication inside a fallback.

A useful post-failure workflow is:

```bash
mwf filter NODE
mwf inspect NODE failed
mwf inspect NODE job 42
mwf resume NODE
```

For a descendant sequence, use `mwf resumefrom START`; it automatically resets
failed/cancelled jobs throughout the selected scope. Use `mwf restart NODE job
42` only from a second terminal while the original sequence is active.

# Testing recommendations

Test at three levels:

1. **Pure utility tests** for parsing, state transitions, SQL planning, scoring,
   and validation.
2. **Node contract tests** that create a temporary MWF project, run one node, and
   assert durable output plus provenance.
3. **Workflow tests** that run from the starter node and assert every expected
   node completes, joins receive all files, and retries/fallbacks produce the
   expected filter funnel.

The repository test suite runs every project under `examples/` using the direct runner and verifies that provenance JSON is produced. Add API-runner contract tests for provider-facing nodes and assert peak in-flight work never exceeds `max_threads`. To exercise them manually:

```bash
python -m pytest -q tests/test_033_filter_icons_design.py
```

For large real projects, add a small deterministic fixture mode so architecture
and provenance tests do not depend on external APIs.


Monitor rows use actual per-node job counts for display state; component lifecycle state remains durable scheduler metadata.


## Refreshable API node pumps (0.3.12)

A component node pump remains unique per node, but API runners no longer treat
the queue as a one-time snapshot. `RefreshableQueuedJobSource` follows SQLite
row insertion order and exposes jobs committed after the pump starts. The API
runner polls that source whenever its in-flight count is below the effective
thread limit. Row insertion order is used rather than job ID order because
concurrent batch producers may reserve lower IDs and commit them after a higher
reserved range. Non-API runners retain the existing snapshot iterator and job-ID
ordering.

## Cooperative API networking and watchdog leases (0.3.15)

API nodes use each node's effective `max_threads` as a fiber request. Without an
aggregate runtime setting, those requests remain independent. With
`mwf threads --api-total N`, the workflow computes deterministic proportional
per-node shares whose sum is `N` and applies them at the execution-claim layer.
Each node pump hosts greenlet job controllers and one scheduler loop, while one
process-wide asyncio thread owns the framework HTTP client shards.

A framework HTTP request registers an external-wait lease on its attempt watch.
While that bounded lease is active, checkpoint-progress expiry is suspended;
the task total deadline and transport deadline are never suspended. On return,
checkpoint timing restarts from the completed network operation. This models
network waiting directly instead of requiring exact heartbeat timing.

New fibers are admitted in bounded bursts. Between bursts MWF processes future
completions, cancellation, sleepers, and watchdog deadlines. This avoids startup
starvation when thousands of jobs are claimed at once.


## Waiting-node phase gates and component cleanup (0.3.16)

A waiting declaration is an admission gate for a node pump inside one Hoeflein
component. `waiting=True, wait_for=None` resolves to all other component
vertices; an explicit list resolves to that subset. A dependency is drained
only when its durable queued, running, and failed job counts are all zero. Done,
skipped, and cancelled rows do not block the gate.

The gate is checked only before a pump starts. Once admitted, a pump continues
to refill and drain its live source even if a waited-for peer later receives
new work. Waiting declarations are strict: the scheduler does not bootstrap or
bypass a mutually blocked cycle. A restart, resume, or producer action must
clear the declared conditions before another pump is admitted.

Node status `waiting` is lifecycle metadata. Individual job rows remain `queued`,
so cleanup, resume, restart, and provenance semantics do not acquire a new job
status. Monitor computes `waiting_on` from current durable queues.

Clean, reset, and wipe operate on component-expanded selections. This aligns
destructive lifecycle changes with scheduling: no command may leave half of an
SCC reset while its peers retain incompatible jobs or outputs.


## Queue-state group commit and transport sharding (0.3.17)

The scheduler has separate control and durability planes. In-process queue
publication and node-pump completion emit wakeups to the component controller;
SQLite remains authoritative and the fallback poll catches changes made by a
different process. A wakeup never substitutes for a durable queue row.

High-churn SQLite mutations enter one project-local group-commit lane. Each
operation retains an independent savepoint, while simultaneous job starts,
status transitions, events, checkpoint runtime updates, ID reservations, and
single-job publications share commits. User payload files are still prepared
before a job becomes visible. Restart-fenced file publication remains protected
by its exact per-job execution lease.

Restart observation is also split from fencing. The supervisor reads all active
leases in one query per poll and wakes stale attempts; a side effect still checks
its exact generation and execution ID while holding the per-job filesystem lock.
This preserves second-terminal restart semantics without an O(active jobs)
database-query loop.

The network runtime has distinct job-admission and connection-sharding planes.
Node `max_threads` continues to define exactly how many jobs may be running.
Those jobs submit into the process-wide manager without a second aggregate
request gate. With HTTP/2 enabled, one client owns one connection and accepts at
most the smaller of `streams_per_connection` and
`http2_stream_safety_cap` assignments before another client is created.
HTTP/1.1 uses its configured connection-pool capacity per client.

The default 32 HTTP/2 streams per connection is a measured connection-local
safety boundary rather than a node-concurrency override. Shards are selected
round-robin and opened elastically, so large nodes do not monopolize the oldest
connection and every admitted request reaches socket dispatch. Network-state
high-water fields are overwritten when a new manager run starts so monitor does
not confuse a previous run's peaks with current activity.

JSON terminal recovery distinguishes connection failure from a poisoned stream.
TCP keepalive handles connection-wide half-open paths. A complete compressed JSON
entity is accepted only after its coding trailer/checksum and JSON document both
validate; if the stream then omits its terminal event, the connection is drained
and replaced. An incomplete stream is replayed only after it remains nonterminal
for five minutes while a configured same-shard cohort terminates after that
attempt began. This proof is monotonic through the quiet tail of a workflow and
the original scheduler/request lease continues measuring the entire operation.
No recovery changes node concurrency or provider request admission.


## High-fanout component admission and completion (0.3.18)

Fresh `run` preparation operates on component-sized database snapshots. Parent
and producer metadata is selected once, stale child jobs are deleted in grouped
mutations, retained jobs are reset per node in one mutation, and artifact
removal uses a bounded file-operation pool. This removes the previous
query/transaction cost for every existing explode job without changing clean
versus resume semantics.

Threaded and API node pumps preload queued jobs in 64-item bursts. One SQLite
metadata query replaces one query per job, and Windows reads the independent
payload files with bounded parallelism. Claims remain individual restart
leases, but arrive together at the grouped writer instead of being separated
by synchronous input-file reads.

Single-child publications stage their input before entering the writer. One
priority mutation then allocates the durable ID, moves the unpublished payload
to its final job directory, and inserts the job, event, idempotency, node, and
sequence state. No ID is cached and no row is visible before its payload.

The writer drains one priority class per commit. Queue publication outranks
local threaded execution, which outranks API-consumer state churn. This prevents
a live handler wave from consuming the producer's commit capacity while that
producer is still filling the component. Within each node, simultaneously
arriving claims and terminal outcomes use native batch operations; each entry
still receives its own generation/execution lease and stale entries fail
independently of valid peers.

The fiber scheduler's hot path is event and deadline driven. Future callbacks
enqueue completed waiters, sleeper and future deadlines live in heaps, and
cancellation fallback polling occurs at its configured cadence rather than
after every admission burst. Node `max_threads` still controls admission; these
indexes only make large declared limits practical.

Terminal publication is split into two restart-safe phases. Output files and
the result store are published under the exact generation/execution filesystem
fence. After releasing that OS lock handle, one conditional grouped database
mutation records the terminal status and event only if the execution lease is
still current. This prevents thousands of cooperative fibers from retaining
thousands of lock-file descriptors while their grouped commits yield.

Handlers that need several local publications to share one restart fence may
use `JobContext.side_effects()`. The scope is intentionally synchronous and
short; network waits and long computation remain outside it.


## Single-writer terminal publication and resume recovery (0.4.3)

Terminal status records enter the same project-local priority mutation writer as
all other SQLite changes. Related records for one node use a 5 ms bounded
collection window and one bulk lease-fenced operation. The writer owns grouping,
transaction boundaries, ordering, and durability barriers; no terminal-specific
daemon queue sits in front of it.

A handler still writes `output.json` under its exact execution fence before
submitting the terminal row/event mutation. Normal finalization waits for that
writer result. If a process exits between those phases, recovery is explicit:
`mwf resume` and `mwf resumefrom` first reconcile matching terminal outputs,
wait at the writer barrier, and only then restart genuinely unsuccessful or
abandoned work. Active failure handling never scans output files.

The first job failure stops new component admission. Threaded, process, and API
runners join jobs that already entered their handlers; API fibers continue
servicing futures and sleeps until all started fibers finish. The component is
marked failed only after those active jobs have reached their terminal boundary.

Active run records persist Hoeflein-component membership. `mwf restart NODE`
therefore fences all running and failed/cancelled jobs in the selected active
component without importing graph code. `mwf restart NODE failed` limits the
selection to failed/cancelled jobs, while explicit job IDs remain supported.

## Terminal recovery and storage boundaries (0.4.2, historical)

The failure-time coordinator and periodic reconciliation described in this
section were 0.4.2 behavior and are superseded by the 0.4.3 design above.

SQLite has one project-local mutation writer with explicit priorities, while
read connections remain thread-local. Schema management, advisory locking,
state transfer, queued-job storage, execution claims, terminal publication,
and restart recovery live in separate modules behind the stable `FileStorage`
facade. This keeps transaction policy centralized without turning one storage
module into a collection of unrelated APIs.

Successful and failed handlers first publish their durable `output.json` under
the execution fence, then submit a grouped conditional status/event mutation to
the project SQLite writer. Normal execution does not scan output directories.
`mwf resume` and `mwf resumefrom` explicitly reconcile terminal outputs before
requeueing stale work; reconciliation is idempotent with a late ordinary
finalizer for the same generation and terminal status.

On the first node failure, the failed terminal update is urgent. The coordinator
flushes it together with any pending successful completions, then the node pump
sets the component stop event before unwinding. Sibling pumps stop taking new
items; API items that were claimed in a burst but have not started are released
back to `queued`. Already-running handlers are allowed to reach their fenced
terminal boundary, after which `mwf resume` sees durable, non-running state.

## Source-aware dual-window API admission (0.4.7)

Fixed geometric admission ladders are a poor fit for uneven Hoeflein members:
a 53-job node, a 1,192-job node, and a 3,600-job node should not all pay the same
64-job probe cadence. The production `balanced` strategy therefore uses one
window below 128 queued jobs and two coordinated windows above it. The initial
window is derived from the source's remaining-row hint and a four-turn target,
with a 512-job per-window ceiling. Small tails are drained immediately, while a
dense node can overlap payload reads and claims without creating an unbounded
number of startup controllers.

The SQLite mutation writer limits ordinary claim transactions by **row weight**,
not only queued request count. At most 192 claimed jobs enter one non-preemptible
ordinary transaction. This is the durability backpressure boundary: terminal
updates retain higher priority and can take the writer after a bounded claim
slice, preserving monitor visibility even when several Hoeflein members claim
thousands of jobs together.

Simultaneous per-node claim requests share one grouped operation, reducing
transaction/savepoint overhead without merging them into an unlimited write.
Refreshable sources protect only row-ID reservation; payload loading and claim
submission can overlap. On Windows those payload reads use one bounded shared
pool rather than constructing a new pool for each burst.

Evaluate admission changes with `benchmarks/compare_job_loading_models.py`.
Include both the observed uneven explode shape and profiles with several nodes
above 3,000 jobs and several below 500. Reject any candidate with a missing
monitor row, queued/running residue, output-to-terminal p95 above 100 ms, or max
above 750 ms. Faster admission alone is not sufficient.

## Event-driven state and startup diagnostics (0.4.6)

`job_events` is both the durable audit journal and the cursor-based state stream.
A SQLite commit emits a coalescible local or loopback wakeup; consumers then read
all rows after their last `event_id`. Wakeups may merge or be lost without losing
state because the cursor, not the datagram, defines progress. Component scheduling,
active restart detection, and `mwf top` therefore react to commits immediately,
with a slow timeout retained only for defensive recovery.

API startup must remain bounded and terminal-aware. The default `adaptive`
strategy computes one vector for API nodes becoming runnable in the same wave,
whether they share a Hoeflein component or are sibling DAG components. Each node
gets one pump. The shared ceiling is
`min(sum(isolated_ceiling_i), max(k, max(12, logical_processors + 5)))`, where
`isolated_ceiling_i = min(12, ceil(n_i / 64))`. Extra pumps greedily maximize
`n_i / (p_i * (p_i + 1))`, the marginal reduction of evenly divided controller
load. Separate DAG waves retain already-running pumps in the same global
accounting; a newly ready component waits if there is not yet one pump for each
of its API members. Within a node, lane concurrency shares (not pump counts)
sum exactly to `n_i`. This is controller parallelism, not an aggregate
concurrency override.

Evaluate alternate budgets with `benchmarks/benchmark_explode_pump_function.py`.
The supplied ten-node shape measured 14/18/20/21/24 pump totals and peaked at 21
on a 16-logical-processor host. Require zero missing monitor rows, zero
queued/running residue, and bounded exact terminal publication before accepting
throughput gains.

API-fiber observability must not serialize the controller after a provider
response. Trace, output, and input-forwarding journal rows are appended through
the ordinary ordered group commit without waiting at each call. Their append is
generation/execution fenced, and the attempt waits for all its event futures
before it can enter fallback or publish terminal state. This preserves both
restart correctness and durable-before-terminal trace order while allowing a
pump to interleave post-response work from many jobs.

Checkpoint runtime JSON is advisory state, not execution ownership. Multiple
not-yet-executing asynchronous `running` snapshots for the same job attempt and
priority therefore share one replaceable slot; the SQLite writer freezes the
newest value only when its transaction starts. Timeout persistence remains
synchronous, and the terminal job row/event remains authoritative. Admission,
successful completion, and failed completion retain equal priority 5.

`mwf top` is the generic diagnostic surface for that evaluation. In addition to
job counts and rates, it reads a throttled heartbeat from the active process's
mutation writer so a second terminal sees the real queue, active priority/batch,
and durability backlog rather than the observer process's empty writer.

## Fair API admission and monitor visibility (0.4.5)

Refreshable API sources start with a 64-job probe and never admit more than 64
new jobs in one scheduler slice. Within that slice the runtime services future
callbacks every 16 starts. A partial or empty pull drops the next probe to 16,
keeping sparse and trickling Hoeflein producers responsive. The bounded dense
slice is deliberate: geometric 256/512/1024 pulls reduced claim count but could
starve already-completed provider futures, creating a long interval where a
result existed while `mwf monitor` had not yet observed terminal state.

API attempt-runtime observations use low-priority grouped, generation-fenced
writes. Terminal job status remains higher priority and is the monitor source of
truth. The scheduler also polls one restart revision row and scans active leases
only when that revision changes, so normal high-concurrency runs do not perform
a component-wide lease comparison every 50 ms.

## Monitor-visible terminal persistence (0.4.0)

Output publication and scheduler-state publication are separate restart-safe
phases. The output file remains protected by the exact execution fence, then a
conditional SQLite mutation publishes the terminal job status and event. Since
`mwf monitor` reads the SQLite job index, that second phase must not be starved
by a large node that is still claiming API work.

The mutation lane therefore orders durable queue publication first, terminal
status and local threaded execution second, and API admission/checkpoint churn
last. Terminal updates remain batched by node and independently validate each
generation/execution lease. This ordering changes only state visibility and
writer fairness; node `max_threads`, transport sharding, output durability, and
restart fencing are unchanged.
