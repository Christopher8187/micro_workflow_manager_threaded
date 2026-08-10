# MWF 0.5.3 Hoeflein synchronization and transport benchmarks

This report records the regressions reproduced from MWF 0.5.2 and the benchmark
matrix used to validate 0.5.3.  The important distinction is between the
quotient-DAG queue (components may be queued) and a live Hoeflein/SCC component:
ordinary members of one active SCC must remain resident even when one member's
local durable queue is temporarily empty.  Only explicit `waiting=True` members
may be phase-gated inside the component.

## Reproduced 0.5.2 regressions

1. **Temporary-empty member restart / desynchronization.** In a two-node
   `A -> B -> A` cycle, A can drain its local queue before B feeds back. MWF
   0.5.2 tears down A's finite node pump and later constructs a second A pump.
   The deterministic regression observes `{'A': 2, 'B': 1}`. MWF 0.5.3 keeps
   one resident pump for each ordinary member: `{'A': 1, 'B': 1}`.
2. **Queued internal member while the SCC is active.** A sibling with no initial
   work did not get a node controller until another member first queued work for
   it. 0.5.3 creates ordinary API/threaded member controllers at component
   startup and their monitor status remains `running` across temporary emptiness.
3. **Terminal component with live jobs.** A scheduler/resource exception could
   publish a failed component while another member still had RUNNING rows.
   0.5.3 stops admission, wakes idle sources, joins every member controller,
   reconciles terminal output, clears any abandoned RUNNING leases, and only
   then publishes component failure.
4. **Payload-loader EMFILE propagation.** A threaded loader `OSError(24)` could
   escape on a worker and poison the source with an invalid/phantom item. 0.5.3
   propagates the original `OSError`, joins the SCC and leaves no stale RUNNING
   rows.
5. **Global wake thundering herd / SQLite lock order.** Keeping ten handlers
   resident initially caused all ten to wake for unrelated job-state changes.
   Queue wakeups are now node-scoped. Node-status writes also use the same single
   mutation lane as job-state writes, removing a stressed lock-order inversion.

## Hoeflein stress benchmark

`benchmarks/benchmark_hoeflein_sync.py` models the Kaicenat shape:
`explode <-> 10 handlers`, 50 explode threads and 100 API fibers per handler.
The following runs use one CPU to make local scheduler/I/O pressure visible.
`payload-delay-per-job=0.002` is an artificial loader delay, not a claim about a
specific VPS disk.

| Scenario | MWF 0.5.2 | MWF 0.5.3 | Result |
|---|---:|---:|---:|
| 200 seeds x 3 rounds, 10 handlers, 2 ms payload pressure | 210.3 jobs/s | **608.0 jobs/s** | 2.89x in this stressed run |
| longest post-start `explode Q>0,R=0` interval | 159.8 ms | **121.9 ms** | shorter internal starvation |
| same topology, no artificial payload delay | **615.6 jobs/s** | 592.7 jobs/s | ~3.7% throughput cost |
| post-start `explode Q>0,R=0`, no delay | 69.0 ms | **0 ms observed** | resident SCC invariant |

The no-delay result is intentionally included: 0.5.3 prioritizes correct live-SCC
semantics and eliminates the internal queued/not-running gap; on an idle local
filesystem this sample is slightly slower in aggregate throughput. Under the
VPS-like pressured case it is substantially faster because pumps are not torn
down/restarted and node-specific wakeups avoid a SQLite probe herd.

`benchmarks/benchmark_hoeflein_wait.py` keeps explicit waiting semantics. A
one-CPU 200-seed/3-round run completed at **913.7 jobs/s** on 0.5.3; waiting
nodes remain phase-gated and are the intended exception to always-resident
ordinary SCC members.

## Low/high file-descriptor pump

`benchmarks/benchmark_hoeflein_pump.py` continuously pumps a finite `A <-> B`
chain while optionally holding socket-like descriptors per active task. A final
48-seed x 16-hop, 64-thread/node, 1.5-KB, four-FD/job run produced:

- `RLIMIT_NOFILE=256`: deliberate `OSError(24)` at fd peak 255. The run failed
  cleanly and both A and B ended with **running=0**.
- `RLIMIT_NOFILE=16384`: all **768/768** jobs completed, fd peak 463, about
  **432 jobs/s**, and both nodes ended with running=0.

The sandbox hard limit is 16,384, so it cannot literally benchmark 65,536.
Normal CLI execution commands (`run`, `runfrom`, `resume`, `resumefrom`) still
attempt to raise the soft limit to 65,536 before project I/O and clamp only when
the OS hard limit is lower. The low-FD benchmark intentionally bypasses that
protection to reproduce the failure class.

## Ordinary DAG / idimage-like fan-out

`benchmarks/benchmark_dag_fanout.py` models an idimage-like four-way fan-out.
On one CPU with 800 jobs:

- no artificial read delay: repeated 0.5.3 runs were **313-341 jobs/s**; a
  directly adjacent 0.5.2 run was 329.6 jobs/s. This is effectively parity,
  with normal run-to-run filesystem/SQLite variance.
- 20 ms artificial payload-read pressure: directly adjacent runs were
  **95.8 jobs/s (0.5.2)** and **93.6 jobs/s (0.5.3)**, also effectively parity.

This guards against fixing live SCC semantics by penalizing the quotient-DAG
path. Refreshable/live queue behavior is enabled only when the scheduler marks a
node as a resident Hoeflein member; ordinary DAG nodes retain finite queue
semantics.

## Test invariants

`tests/test_057_hoeflein_live_sync_053.py` is the 0.5.3 regression suite. It
covers resident pumps across late feedback, monitor-visible running status for
an initially empty ordinary member, component-failure joining, stale RUNNING
cleanup, node-scoped queue wakeups, mutation-lane status writes and exact EMFILE
propagation. The 0.5.2 baseline deterministically fails the late-feedback pump
identity test by restarting A.
