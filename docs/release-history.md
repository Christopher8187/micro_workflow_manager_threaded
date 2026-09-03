# Release history

These notes record what changed in each MWF release through 0.6.1.
Later releases may supersede behavior described in an earlier entry. Use the
[current README](../README.md) and linked architecture and operations guides for
current behavior.

## What changed in 0.6.1

- Reorganized current documentation around the root README, authoritative
  glossary, graph, node, and task architecture guides, operations, installation,
  testing, test-module, benchmark, release-history, and provisional planning
  pages. Added AFSR routing in `AGENTS.md` and five instruction-only skills.
- Defined output provenance as the user-owned filesystem tree under the one
  `node/<node-name>/output/` prefix. Project and node README files are now
  documentation standards, but current setup commands do not generate or
  require them.
- Removed per-job file storage: `JobFileSystem`, `ctx.write()`,
  `ctx.write_bytes()`, `ctx.files_dir`, `ctx.storage_dir`, automatic returned
  file copying, and new `stored_files` output fields. Per-job storage now
  contains only `input.json` and `output.json`; existing older file trees remain
  untouched during upgrade.
- Removed `ctx.transaction()` without an alias. Same-node fan-out uses
  `add_many()`; cross-node fan-out uses precomputed child specifications with
  explicit idempotency keys.
- Exposed ordered live failure history through `ctx.errors` and the optional
  `errors` task parameter while keeping `ctx.error` as the latest failure.
  Every failed task attempt now writes a durable `task_failed` event. Trace has
  an error-focused view, and filter uses the same events for stage errors.
- Completed the extended description list for `copy`, `paste`, `filter`, and
  `top`; corrected component selection and aggregate API-budget help.
- Corrected `benchmark_hoeflein_sync.py` and
  `benchmark_explode_pump_function.py` to return a nonzero status when their
  reported work fails.
- Preserved worker exceptions that arrive immediately after component
  quiescence by inspecting joined node-worker results before publishing success.
- Accepted same-clock-tick sibling completions in HTTP cohort recovery when the
  completion counter establishes that the terminal events are new.

## What changed in 0.6.0

- `mwf engine` opens a strictly read-only, graph-only loopback view from the
  synchronized project metadata. It collapses nontrivial Hoeflein components
  into scheduling units, reveals their members on demand, imports no project
  code, exposes no mutation endpoint, and loads no external browser assets.
- `mwf run NODE sample COUNT` performs deterministic isolated partial-node
  runs. A SHA-256-ranked population, explicit seed/status filter, no-write plan,
  population-drift guard, selected-input digest, and active-run manifest make
  the sample reproducible. Unselected jobs, descendants, and Hoeflein
  circulation remain untouched.
- The root `AGENTS.md` then gave coding agents complete framework command,
  architecture, workflow-design, fallback/validation, and release-testing
  guidance. No transport, timeout, admission, or provider-networking behavior
  changed in this release.

Current note: 0.6.1 replaced that monolithic agent guidance with AFSR routes,
focused architecture and operations pages, and instruction-only skills. Current
tests do not establish the descendant and circulation-isolation parts of the
historical sampled-run claim.

## What changed in 0.5.11

- A run-scoped aggregate API budget is allocated over API nodes that are
  actually running, not every selected descendant. When one parallel node
  drains, its slots are redistributed by declared/overridden request weight to
  the remaining live nodes; every live node retains at least one slot. The
  scheduler, `mwf threads`, and `mwf monitor` now distinguish requested,
  active, and aggregate API capacity.
- DAG nodes and Hoeflein components use the same API transport; 0.5.11 does
  not add a second protocol circuit, specialize networking by graph shape, or
  change caller-configured timeout values. It fixes the boundary between the
  accepted Explode recovery layer and the scheduler watchdog: ingress wait no
  longer consumes a transport attempt's lease, and a bounded hidden
  cohort/connection replay receives a fresh lease with the same configured
  duration. The task's total timeout remains active across every replay.
- The shared transport now packs a draining workload onto the busiest healthy
  shard before reusing idle connections. At peak width, the configured
  same-shard terminal evidence remains 16; when a quiet tail contains fewer
  peers, MWF requires every available same-shard peer (and at least two) to
  terminate before declaring the remaining stream an outlier. A JSON response
  body that silently ignores HTTPX's timer is also checked against the caller's
  existing read timeout inside MWF and transparently replayed before the
  supervisor's unchanged cleanup-grace lease expires. No timeout is extended
  and no provider admission gate is added.
- Physical transport attempt, renewal count/reason, and OpenRouter's documented
  `X-Generation-Id` are observable in live runtime/network diagnostics. This
  allows a provider generation to be correlated without browser automation.
- Every ordinary member of a live Hoeflein component now constructs and
  subscribes its refreshable queue source before sibling handlers are released.
  This makes the existing "resident before first feedback" guarantee literal
  under a busy interpreter: a fast router cannot fan out before its API
  consumers are listening. The gate runs only once at component startup and
  changes neither job concurrency nor steady-state pump allocation.
- Active run records carry an OS process-start identity in addition to a PID.
  A recycled Windows PID can no longer make an abandoned run appear live; for
  legacy records without an identity, a stale heartbeat is not rescued merely
  because an unrelated process later receives the same PID.
- `FileSystemEntry.read_jsons(pattern)` supports large immutable fan-in
  frontiers without repeating the scheduler execution check for every file.
  MWF checks the execution generation before and after the bounded batch,
  resolves and containment-checks every candidate path (including symlinks),
  and returns deterministically sorted relative-path/value pairs. A bounded
  32-worker reader overlaps independent Windows security/VPN filter latency
  across small immutable files without retaining handles after the call. This
  keeps fan-in inside the framework filesystem contract while avoiding both
  thousands of redundant supervisor/database checks and serial per-file filter
  stalls.

## What changed in 0.5.10

- `mwf runfrom START refuse BOUNDARY` and `mwf resumefrom START refuse
  BOUNDARY` add a global exclusive Hoeflein-component admission boundary. When
  the named component first becomes ready, MWF starts neither that component
  nor any other newly ready component. Components already running are joined,
  and refused work remains queued for a later resume.
- `refuse` is deliberately different from `refuseafter`: `refuse B` stops
  before B's whole component starts, while `refuseafter B` lets B's component
  terminate and then stops later admission. A boundary already terminal when a
  resume begins is treated as already reached.
- Fresh `runfrom` preparation still covers the complete selected descendant
  set. The boundary limits execution admission, not reset scope, and its mode
  and node are recorded in `.mwf/run.json`.

## What changed in 0.5.9

- Cohort and connection-error replays reuse healthy existing HTTP/2 shards and
  share capacity-required replacement shards. A recovery wave no longer opens
  one `AsyncClient`, TLS pool, and socket pool per recovered stream. Provider
  request concurrency and the caller's transport lease are unchanged.
- Read, write, and protocol failures retire the exact affected shard before a
  bounded transparent replay. Diagnostics distinguish healthy-shard reuse from
  newly created recovery shards and report live, retiring, and idle clients.
- Network diagnostics group active requests by shard in linear time. SQLite's
  durability watermark now stores only genuinely pending serials, rather than
  retaining every completed serial behind an older low-priority mutation.

## What changed in 0.5.8

- HTTP/2 shards are selected round-robin and opened elastically after the
  connection-local 32-stream safety width. There is no second aggregate
  request gate: declared node concurrency and provider request pressure remain
  unchanged.
- JSON requests recover from two multiplexed HTTP/2 terminal defects without
  replacing the caller's timeout. A complete checksum-valid JSON entity that
  lacks `END_STREAM` is returned after a five-second terminal grace period. A
  stream that remains nonterminal for five minutes while at least 16 newer
  same-shard requests terminate is replayed on another shard; the cohort
  determination remains valid through the quiet tail of a run.
- Live network futures cancel their underlying socket coroutines. TCP keepalive
  detects connection-wide half-open VPN/TUN paths, while
  `.mwf/network_manager.json` exposes shard, stream, job, phase, retirement,
  terminal-recovery, and cohort-replay evidence.
- Default multi-job declarations publish one prepared SQLite batch and write
  disjoint payload files concurrently. `mwf monitor` bulk-reads all selected
  node summaries, while `mwf top` uses a bounded reverse journal walk and
  honors its redraw interval under terminal waves. Fan-out and observers no
  longer compete unnecessarily with dense API work as the done-job journal
  grows.

## What changed in 0.5.7

- Simultaneously runnable API nodes now receive a shared controller-pump vector.
  Every node is guaranteed one pump; the host-bounded remainder is allocated by
  marginal controller-load reduction. On the 16-logical-processor explode
  shape, 21 pumps are allocated as `1,2,3,2,4,2,2,1,2,2` while every node's
  configured concurrency remains exact and unchanged.
- API-fiber trace, output, and input-forwarding events are generation-fenced and
  enqueued asynchronously into the existing ordered SQLite group commit. Each
  attempt flushes its event futures before fallback or terminal publication, so
  provenance remains durable-before-terminal without one SQLite round trip per
  observability record.
- Not-yet-executing priority-20 checkpoint snapshots coalesce per job attempt.
  The latest checkpoint stays inspectable, timeouts remain durable, and
  admission, successful terminal publication, and failed terminal publication
  all remain in the same priority-5 runtime-critical class.
- API networking now has an explicit backend `NetworkManager`: one process-wide event loop owns persistent HTTPX client shards and all socket I/O. Node fibers enqueue lightweight requests; dense cross-thread submissions are coalesced before asyncio task creation instead of calling `run_coroutine_threadsafe` once per request. Existing `shared_http_transport` application code remains unchanged.
- Network-manager state is aggregated in memory and bulk-upserted into the new SQLite `network_state` table at most every two seconds. This observability path is low-priority and non-fatal. SQLite schema version is 4.
- Adds the requested 22-node skew A/B benchmark: two 2,000-job nodes plus twenty 100-job nodes with 512 proportionally allocated API slots. In the observed unlimited-bandwidth H2 sample, the manager improved runner throughput ~6.5%, durable workflow throughput ~6.4%, and the durable big:small ratio from 11.69:1 to 13.16:1. See `NETWORK_MANAGER_ARCHITECTURE_056.md`.
- Retains the 0.5.5 queue-scan and dense-refill optimizations: refreshable queues use monotonic direct rowid range scans without a temporary ORDER BY tree, queue hints are bounded, and dense API nodes avoid repeated tiny durable refills.
- The localhost delay server paces the final/only chunk correctly and no longer awaits H2 socket backpressure while holding the protocol-state lock. A 4 KiB response at 4 KiB/s now takes about one second.

Current note: 0.6.1 removed the historical network-manager design file. The
[benchmark guide](../benchmarks/README.md) now identifies the retained programs,
saved results, and limits on interpreting historical measurements.

## What changed in 0.5.4

- High-concurrency API admission records the first valid main-task `task_started` event in the existing grouped execution-claim transaction instead of submitting one extra synchronous SQLite mutation per job. Malformed jobs with missing required parameters retain the old trace semantics and are not falsely marked task-started.
- Wide DAG finalization now bulk-reads node status, skips already-terminal and in-flight sibling components, and gives component execution one owner for `RUNNING`/`DONE` publication. This removes repeated sibling status rewrites as fan-out width grows into tens of nodes.
- HTTP/1.1 shared transport now uses elastic 16-connection client shards by default; HTTP/2 keeps its stream-per-connection behavior unchanged. The localhost benchmark measured about 3.55x higher H1 runner throughput at 512 concurrent requests versus the previous 100-connection shards.
- `include_router()` retains router objects instead of remembering only `id(router)`, preventing CPython object-id reuse from silently skipping short-lived programmatically generated routers in wide fan-outs.
- Adds a real localhost HTTP delay/throttle service and a three-axis fan-out benchmark over concurrency, per-response transfer rate, and fan-out node count. See `HOW_TO_TEST.md` and `HTTP_FANOUT_BENCHMARKS_054.md`.
- Retains all 0.5.3 Hoeflein live-pump, clean failure/join, EMFILE, threaded prefetch, FD-limit and `resumefrom ... refuseafter ...` behavior.

Current note: 0.6.1 removed those historical testing and benchmark-note files.
Current procedures and retained benchmark material are in the
[testing guide](testing.md) and [benchmark guide](../benchmarks/README.md).

## What changed in 0.5.3

- Hoeflein components now keep every ordinary threaded/API member attached to a live, event-driven queue pump for the lifetime of the component. Temporary empty queues no longer tear down a member and turn internal feedback into a mini-DAG queue; explicit `wait_for` nodes remain phase-gated.
- Threaded source advancement no longer holds peer workers behind payload I/O, and payload-loader failures such as `EMFILE` propagate as their original exception instead of creating a phantom `None` job.
- Component failure is published only after all already-started member runners join. Output-backed terminal states are reconciled first and any remaining abandoned `running` leases are marked failed, so a failed Hoeflein component cannot retain ghost running jobs.
- Retains 0.5.2 `resumefrom ... refuseafter ...`, automatic FD-limit raising, and bounded threaded payload prefetch.

## What changed in 0.5.2

- `mwf resumefrom START refuseafter BOUNDARY` now has the same inclusive Hoeflein-component admission boundary as `runfrom ... refuseafter ...`: the named boundary component may finish or fail, no new later component is admitted afterward, already-running parallel components are joined, and queued later work is retained for a future resume.
- Retains the 0.5.1 threaded payload-prefetch and automatic open-file-limit improvements.

## What changed in 0.5.1

- `mwf reset` and `mwf resetfrom` now perform the exact fresh preparation
  used by `run` and `runfrom` without starting a scheduler or executing tasks.
- `mwf cleanfrom` deletes all jobs and generated output in the selected Hoeflein
  component and quotient-DAG descendants; `mwf wipefrom` additionally deletes
  their inputs. `clean` and `wipe` apply the same semantics to the current
  component. All six destructive preparation commands require a typed
  confirmation unless `--yes` is supplied, and all support `--dry-run`.
- `AGENT.md`, `examples/README.md`, and the new
  `examples/agent_reference_architecture` provide a standard project layout and
  a complete API/HTTP, fallback, transactional fan-out, durable fan-in, and
  bounded Hoeflein-component reference design.
- `mwf runfrom START refuseafter STOP` performs the same full freshening as an
  ordinary `runfrom`, but stops admitting new Hoeflein components as soon as
  STOP's component completes or fails. Components already running at that
  instant are joined; downstream jobs remain queued for a later command.
- Fresh and destructive commands now clear affected job trace journals by
  default. Add `--keeptrace` to `run`, `runfrom`, selected-job runs, `reset`,
  `clean`, or `wipe` to retain the prior transcript.
- `resume` always preserves the selected current component's trace. Default
  `resumefrom` preserves its start component and clears descendant traces;
  `resumefrom --keeptrace` retains the entire selected descendant history.
- Preserved event journals survive node copy/paste and can remain attached to a
  temporarily deleted job identity. When a recreated job keeps its node/job ID
  but receives a different parent, producer component, or job kind, MWF appends
  and renders a separate `ORIGIN CHANGED` subsection before the new execution
  history.
- Repeated CLI/storage churn no longer risks a same-thread SQLite registry
  deadlock when cyclic garbage collection invokes an old storage finalizer
  during connection setup.

Current note: 0.6.1 removed `AGENT.md` and the staging helper behind the
historical transactional fan-out example. Current guidance uses the
[architecture pages](architecture/graph.md) and precomputed cross-node additions
with explicit idempotency keys.

## What changed in 0.4.8

- API nodes use **fixed-limit adaptive admission sharding** by default. Dense
  nodes that become runnable together receive one shared pump vector. Every API
  node is guaranteed one pump. The total is bounded by the smaller of the nodes'
  isolated benefit ceilings and `max(12, logical_processors + 5)`; remaining
  pumps are assigned by marginal benefit `n / (p * (p + 1))`, where `n` is the
  declared concurrency and `p` is the node's current pump count. On the supplied
  16-logical-processor explode workload this gives 21 pumps across ten handlers.
  `_LaneCoordinator` keeps each node's lane-concurrency sum exactly equal to its
  declared limit, so controller sharding never reduces or increases job
  concurrency. Already-running pumps remain charged when later DAG branches
  become ready, preventing successive waves from each consuming a fresh host
  budget. Explicit `event`, `balanced`, `elastic`, and `lanes:N` strategies
  remain available for controlled comparisons.
- Simultaneous Hoeflein claim bursts are combined into one grouped SQLite
  operation. The mutation writer also caps ordinary claim transactions at 192
  job rows, so a multi-thousand-job admission wave cannot trap urgent terminal
  publication behind one non-preemptible transaction.
- Refreshable job sources reserve row IDs under a short lock while payload reads
  and claims overlap. Windows share a bounded payload-read pool on Windows,
  avoiding a fresh thread pool for every node and every admission slice.
- `benchmarks/compare_job_loading_models.py` compares fixed ladders, source-aware
  windows, elastic loading, and two/three/four-lane models on the supplied
  explode shape plus uneven 11k–12k-job graphs. A model is eligible only with no
  missing monitor rows, no final residue, and bounded output-to-terminal p95/max.
- A bounded regression test now samples SQLite during uneven high-concurrency
  execution and verifies exact output-write to durable-terminal latency. A
  separate 27-job-tail test prevents a small final queue from waiting for the
  next admission plateau.

## What changed in 0.4.6

- Durable `job_events` are now the workflow state stream. In-process schedulers
  receive commit callbacks immediately, while second-terminal commands and
  restart control use coalesced loopback wakeups plus an `event_id` cursor. A
  five-second timeout remains only as a defensive fallback; normal lifecycle
  progress is no longer discovered by frequent status polling.
- `mwf top` adds an event-driven htop-style dashboard with per-node queue/run
  counts, effective limits, starts/finishes per second, queue and terminal p95
  latency, recent lifecycle events, process RSS/thread data, SQLite/WAL size,
  and the active process's mutation-writer backlog and batch diagnostics.
- The former production API startup strategy was `single`. It remains available
  as `event` for controlled comparisons, but shared-budget adaptive sharding is
  the default after single-controller scaling tests exposed a large throughput
  penalty at high declared concurrency.
- Retry/fallback inspection moved from `mwf inspect NODE filter` to
  `mwf filter NODE`; `mwf filter NODE stage X` shows terminal failures at the
  final stage or failures at X that succeeded at X+1.

## What changed in 0.4.5

- Dense API sources now admit at most 64 jobs per scheduler slice and service
  completed futures every 16 starts. Fast provider responses can therefore
  publish output and terminal state while a large Hoeflein component is still
  filling, instead of waiting behind a 256/512/1024 start wave.
- Supervised API attempt metadata is generation/execution fenced, grouped by
  node, deduplicated within each writer batch, and written asynchronously below
  terminal priority. Startup inspection writes no longer serialize every job,
  and terminal rows remain the monitor source of truth.
- Active-restart supervision polls one project revision row. It materializes
  live execution leases only after an actual restart request, rather than
  rereading every active job every 50 ms at high concurrency.
- Hot filesystem and SQLite paths cache canonical project/node directories and
  avoid repeated path resolution for validated node names and integer job IDs.
  This removes substantial per-job queueing overhead without weakening path
  traversal validation or generation fences.
- `benchmarks/reproduce_explode_ghost.py` copies the ten-handler
  `pdftostructureddata` explode component and uses a variable-latency mock HTTP
  provider to measure provider completion, durable output, and monitor-visible
  terminal state separately.

## What changed in 0.4.3

- Terminal job publication now enters the existing priority SQLite writer
  directly. The writer groups related terminal records for at most 5 ms and
  applies one bulk lease-fenced update/event operation; the redundant terminal
  daemon queue has been removed.
- A node or Hoeflein-component failure stops new admission but waits for every
  already-started threaded, process, or API job to reach its terminal boundary.
  Failure handling no longer scans output files or performs a special terminal
  drain before marking the component failed.
- `mwf resume NODE` and `mwf resumefrom START` first reconcile terminal
  `output.json` files against stale `running` rows, cross the SQLite durability
  barrier, and only then generation-fence and requeue the remaining unsuccessful
  work. This preserves handlers that finished before a process interruption.
- `mwf restart NODE` restarts every live-running and failed/cancelled job in the
  node's active Hoeflein component (a DAG node is a singleton component).
  `mwf restart NODE failed` limits that component-wide selection to
  failed/cancelled jobs. The explicit `job` and `jobs` forms remain available.
- A waiting node now requires every selected peer to have zero queued, running,
  and failed jobs. The old queued-only cycle bootstrap has been removed, so the
  declared gate is never bypassed.

## What changed in 0.4.2

- SQLite state handling is separated into connection/mutation, schema,
  advisory-lock, and transfer modules. Job creation, batching, querying,
  cleanup, execution claims, terminal publication, and restart recovery are
  likewise isolated behind the existing `FileStorage` facade.
- Terminal job updates are published by a dedicated fixed-cadence coordinator
  and use batched conditional SQL updates/events. Hoeflein components also
  reconcile terminal `output.json` files every 250 ms, recovering a completed
  handler if its final SQLite mutation was interrupted.
- A node failure urgently flushes all already-written terminal outcomes before
  the shared Hoeflein stop signal is raised. Sibling queue admission stops at
  that point, and API jobs preclaimed but not started are returned to `queued`
  for a safe `mwf resume`.
- Multi-concern production modules over 500 lines were split into focused
  facades and implementation files. The remaining file above 500 lines is the
  cohesive cooperative fiber runtime.

## What changed in 0.4.1 (historical; dense growth superseded in 0.4.5)

- Refreshable API admission then adapted across a wider range. It starts at 64,
  drops to 16 after a partial or empty pull for sparse/trickling queues, and
  grows geometrically to 1024 while pulls remain full for dense fixed queues.
- At that release, a 2,000-job dense queue reached its final partial pull in six admission
  rounds (`64, 128, 256, 512, 1024, 16`) instead of repeatedly claiming fixed
  groups of 64.
- Terminal completion batching remains independent of the API fiber scheduler,
  so adaptive claims cannot prevent output-backed `done` updates from entering
  the higher-priority SQLite mutation lane.

## What changed in 0.4.0

- Terminal job status/event commits and execution claims now share runtime-
  critical priority 5. Success, failure, and admission therefore enter one FIFO
  class; bounded claim batches and cooperative callback servicing supply the
  fairness without making one outcome type outrank another.
- A monitor-shaped regression verifies that a large supervised API node reports
  every completion from SQLite and leaves no queued/running residue while its
  claims and terminal outcomes use that same priority.

## What changed in 0.3.18

- Fresh Hoeflein cleanup now reads producer provenance in one SQLite snapshot,
  deletes selected-producer jobs in node-sized batches, requeues retained jobs
  in one transaction per node, and removes independent job artifacts with
  bounded parallelism on Windows. Large components begin pumping instead of
  spending tens of seconds in per-job preparation.
- API fiber admission no longer scans every outstanding future and sleeper after
  each 64-job burst. Future deadlines use a heap, while restart/cancellation
  checks run at the configured polling cadence. Large typed nodes therefore
  progress through claim, watchdog setup, and HTTP dispatch at the same rate as
  small nodes.
- Threaded and API queue pumps load one metadata snapshot per 64-job burst and
  prefetch the independent payload files before execution claims. The grouped
  writer now receives dense claim bursts instead of claims separated by
  per-job metadata queries and file reads.
- A one-child route stages its tiny input before entering SQLite, then allocates
  the ID and publishes the payload, row, event, and node state in one mutation.
  Queue publication, local threaded execution, and API execution use separate
  writer priorities, and one commit never absorbs lower-priority consumer work.
- Concurrent execution claims and terminal updates coalesce by node. This keeps
  live API startup and large completion waves from paying one savepoint and one
  scheduler round trip per job while preserving an independent restart lease
  and outcome for every job.
- Terminal job publication releases the generation fence before waiting for the
  grouped SQLite commit, then uses a lease-conditional terminal update. A large
  completion wave no longer retains one lock-file handle per yielding fiber.
- Newly reserved, unpublished job inputs use a direct exclusive create instead
  of a temporary file plus rename. Short local routers can also group several
  file/queue mutations under `with ctx.side_effects():` to reuse one restart
  fence without holding it across a network request.

## What changed in 0.3.17

- Live Hoeflein components now wake on committed queue changes and completed
  node pumps. The one-second poll is only a cross-process recovery fallback, so
  a handler can start immediately when a running router creates work.
- Single-job routing no longer acquires and releases a SQLite advisory lock for
  every handoff. Auto-ID reservation and prepared-job publication use grouped
  commits; publication combines the job row, creation event, idempotency row,
  node status, and sequence advancement.
- Job start, completion, event, and checkpoint mutations share the same short
  group-commit lane. Bursts of completions no longer become one durable commit
  per state field per job.
- Active-restart detection moved from one SQLite query per waiting job per poll
  to one supervisor query for all active leases. Exact side-effect and final
  publication fences remain per job.
- API fibers receive future-completion callbacks and O(1) ready-queue pops.
  Progressive completion waves no longer rescan all outstanding futures or
  shift a growing list on every resume.
- The framework HTTP transport supports connection sharding with `http2=` and
  `streams_per_connection=`. Per-node `max_threads` remains the exact job
  concurrency control. The manager does not impose a second aggregate request
  limit: every admitted API request is dispatched, with connection-local width
  bounded by the HTTP/2 stream safety cap.

### HTTP/2 connection sharding

```python
from micro_workflow_manager import configure_shared_http_transport

configure_shared_http_transport(
    http2=True,
    streams_per_connection=80,
    # Optional explicit overrides of the measured safe defaults:
    http2_stream_safety_cap=32,
)
```

For HTTP/2, the effective per-connection assignment is
`min(streams_per_connection, http2_stream_safety_cap)`. The default safety cap
is 32 because high stream widths caused severe long-tail collapse under
thousand-request provider loads. Once a shard is full, another connection is
created immediately. This is a connection-local safety control, not a workflow
admission control: node concurrency, pump allocation, provider request pressure,
and job lifecycle remain unchanged.

The environment equivalent is `MWF_HTTP2_STREAM_SAFETY_CAP` (default `32`).
`MWF_JSON_TERMINAL_GRACE_SECONDS` (default `5`),
`MWF_HTTP2_COHORT_STALL_SECONDS` (default `300`),
`MWF_HTTP2_COHORT_TERMINALS` (default `16`), and
`MWF_HTTP2_COHORT_RETRIES` (default `2`) control terminal recovery without
replacing the caller's request timeout. `MWF_HTTP_TRANSPORT_RETRIES` (default
`2`) bounds transparent read/write/protocol recovery. Each physical attempt
gets the same caller-configured bounded lease; ingress wait does not consume
that lease, and the task's total timeout continues to bound the whole task.
At peak width, the cohort threshold is the configured value. If fewer peers
share a quiet tail shard, MWF waits for every available peer to terminate and
requires at least two terminals before replaying the remaining outlier. For
JSON response bodies, MWF independently enforces the caller's existing HTTPX
read-idle timeout between body chunks; this covers a poisoned HTTP/2 stream
whose underlying iterator never raises its own timeout. The resulting bounded
physical replay receives the same configured lease. Replays use available
capacity on previous healthy shards before opening another shared shard; the
poisoned source shard is excluded and drained. The manager snapshot reports
requested/effective stream width, the cap, client count, current in-flight
requests, active stream phases, physical-attempt counts, replay reason,
OpenRouter generation ID when supplied, healthy-shard reuse, new recovery
shards, and recovery/retirement evidence. HTTP/1.1 keeps
its independent `MWF_HTTP1_CONNECTIONS_PER_SHARD` setting.

## What changed in 0.3.16

- `mwf clean`, `mwf reset`, and `mwf wipe` now treat a Hoeflein component as
  the indivisible cleanup unit. Naming one member expands to every member of
  that component; DAG nodes remain singleton components.
- Nodes may declare an intra-component waiting gate with `waiting=True` and
  `wait_for=...`. Queued jobs remain durably queued, but the node displays as
  `waiting` and no new node pump starts until every selected peer has zero
  queued, running, and failed jobs. A pump that already started continues
  normally.
- `wait_for=None` with `waiting=True` means all other vertices in the component.
  A list selects a subset. Waiting targets outside the component are rejected.
- Waiting on a singleton DAG component is allowed but has no effect; CLI loading
  prints a reminder that ordinary DAG predecessor readiness is the available
  queue-independent mechanism.
- Waiting declarations are strict. A mutually waiting set with blocked work on
  every side remains waiting until a restart, resume, or producer action clears
  the declared queued/running/failed conditions.

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
scheduler watchdog. In 0.3.17 it may own several automatically selected client
shards. The checkpoint lease is suspended only while the bounded network
operation is active.

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

Current note: 0.6.1 removed `AGENT.md`; the current procedure lives in the
[testing guide](testing.md) and the `mwf-test` instruction-only skill.

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
