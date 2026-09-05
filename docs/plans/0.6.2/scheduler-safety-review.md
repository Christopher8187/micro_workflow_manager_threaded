# Scheduler observation and completion-gate safety review

## Disposition

**Reviewer:** independent `gpt-5.6-sol`, `xhigh` reasoning

**Assigned slice:** scheduler source, lifecycle safety, and benchmark completion gate

**Selected tree:** `C:\Business\product\test_area\mwf-062-issue45-20260904\scheduler-fix`

**Immediate base:** accepted topology tree `24f584413619d1bbe94da2264032600b9b401105`

I found no remaining actionable defect in this assigned slice. The final
correction fixes two separate early-stop races in the threaded Hoeflein
coordinator and makes the waiting benchmark reject incomplete durable results.
I recommend accepting these scheduler and benchmark changes within this narrow
scope.

The six reviewed files in direct MWF are byte-identical to the selected tree:

- `micro_workflow_manager/storage/job_queries.py`
- `micro_workflow_manager/workflow/component_scheduler.py`
- `tests/test_057_hoeflein_live_sync_053.py`
- `tests/test_068_component_snapshot.py`
- `tests/test_benchmark_exit_codes.py`
- `benchmarks/benchmark_hoeflein_wait.py`

This is one assigned source-and-safety review, not the entire stage review. The
work crosses SQLite observation, worker admission, waiting semantics, resident
pumps, failure cleanup, and performance measurement. One reviewer for the
whole section would be inadequate under `45-REV-003`. A second independent
review has been assigned to compatibility, specification, tests, and
documentation. This report does not substitute for that review's disposition.

## Applicable requirements and scope

I reread `C:\Business\product\CONTEXT-MAP.md`, `mwf/AGENTS.md`, the complete
final resolution for [Settle the MWF workflow-management model for
0.6.2](https://github.com/Christopher8187/product/issues/44#issuecomment-5539997969),
the implementation issue and its full preparation record, and the earlier
approved decisions before assessing the correction.

The narrow repair preserves the settled component model rather than choosing a
new architecture:

| Requirement | Result in this slice |
| --- | --- |
| `44-CMP-001` | The exact Hoeflein component remains one execution unit. The coordinator no longer returns while internally published work is still pending. |
| `44-SCP-010`, `44-CMP-015` through `44-CMP-017` | Waiting remains a raw-node declaration and active display condition. Admission still blocks on queued, running, or failed jobs in declared waiting peers. |
| `44-REC-014` | Regressions execute real tasks and use real SQLite state. The timing seams delay observation or a real job transition without replacing the scheduler result. |
| `45-TDD-015` through `45-TDD-026` | Both discovered behaviors have expected RED results, corrected GREEN results, relevant surrounding checks, broad checks, and sensitivity checks. |
| `45-REV-011`, `45-REV-017` | Existing direct execution, resident ordinary pumps, finite waiting pumps, API execution, error cleanup, run callers, and benchmark behavior were inspected beyond the two runtime files. |

The correction does not change waiting declarations, deadlock override policy,
component topology, task routing, runner concurrency limits, session ownership,
or any approved lifecycle value. It does not claim to serialize component
shutdown with a new external process adding work after an empty observation.
That broader admission question is outside the interleavings repaired here.

## Coherent job-state observation

The first race came from three coordinator reads: queued nodes, running nodes,
and waiting blockers. A producer could be observed as running after the queued
read, then publish a receiver job and finish before the running read. The
coordinator combined “no queued work” from the earlier moment with “no running
work” from the later moment and stopped, even though the receiver job existed.

`nodes_by_job_status()` now obtains every requested `(node_name, status)` pair
with one SQLite statement. MWF creates scheduler connections with
`isolation_level=None`, so this statement receives one current statement
snapshot. The method validates all supplied names and states before executing,
deduplicates them, returns an empty set for every requested state, and performs
no writes. The threaded coordinator derives queued nodes, running nodes, and
the union of queued/running/failed blockers from that one result.

This closes the internal publication transition. Before the publication commits,
the producer is still running. After it commits, the receiver is queued. After
the producer reaches done, that queued receiver still exists. One statement
therefore observes at least one side of the transition. It cannot assemble the
impossible empty state produced by the old sequence.

The focused test exercises this exact seam. It captures actual SQLite rows on
the coordinator connection, delays only their delivery, then allows a real
producer task to publish and finish. Three baseline probe runs each left B
queued. The permanent regression also fails on the base coordinator and passes
after the single-statement change. It checks exact durable job counts and final
node state for both members.

## Finite-pump admission and true deadlock

The first correction exposed a second, independent race in the original
multi-seed workload. A coherent observation can legitimately contain queued A
and B jobs with no running row while an already admitted finite waiting pump is
between job claims. Treating that moment as a stable mutual wait stops the
component. Executor shutdown then lets the already admitted pump finish, but
its peer remains queued.

The final deadlock condition now also requires
`not (active_nodes - live_nodes)`. That distinction matches the worker
lifecycle:

- Explicit waiting nodes and nonresident runner overrides use finite Futures.
  If one remains active, it may claim another queued job or publish work that
  unblocks a peer. The coordinator must wait for its completion callback and
  observe again.
- Ordinary threaded and API members in `live_nodes` are deliberately resident
  for the component lifetime. They remain active while idle. Counting them as
  progress would hide a real waiting deadlock forever, so the condition removes
  them before deciding whether a finite pump remains.

The correction does not create a lost wake. Every Future callback sets the
coordinator event. A Future that has already completed is removed from
`active_nodes` at the start of the next loop before the next observation. A
resident-pump regression test places an idle ordinary C beside mutually waiting
A and B and confirms that the real deadlock still returns promptly with A and B
queued.

The admitted-pump regression pauses A immediately before its second real
queued-to-running transition. The coordinator observes exactly A and B queued,
no running job, and an active finite A Future. The coherent-only version stops
early and fails the expected B completion count. The final predicate completes
A=2 and B=2 with no queued, running, or failed row.

The same reasoning holds for the runner boundaries inspected:

- The direct runner remains synchronous inside the coordinator loop, so no
  local worker can publish and finish between its separate reads.
- A waiting threaded runner uses a finite source. A waiting API runner may use
  refreshable loading, but without the resident live-source wrapper its Future
  is still finite. Both remain in `active_nodes - live_nodes` while admitted.
- Ordinary threaded and API members use resident sources and stay in
  `live_nodes`. API preclaims appear as running rows, so the coherent observation
  sees admitted API work.
- A quiescent observation is safe for internal work: a task still capable of
  publishing has a running row, and a pulled but unclaimed item remains queued.
  The coordinator then stops resident sources, joins the executor, inspects all
  remaining Futures, and only publishes success if no worker error surfaced.

Programmatic component execution through `run_node()` and CLI execution through
`run_orchestration.py` both reach this same `run_component()` coordinator. The
new storage method has no public caller-facing signature change, and its only
runtime caller is the threaded component observation described above.

I also ran an API-wait probe against the final selected source. Two mutually
waiting API nodes completed the exact A=6/B=4 workload with no queued, running,
or failed rows. This probe is supporting evidence; the recorded focused,
adjacent, broad, cyclic, and stress checks are the acceptance checks.

## Failure handling

An error from the new observation method enters the existing coordinator error
path. It records the first error, sets the stop event, wakes resident sources,
cancels work that has not started, joins active workers, repairs any abandoned
running rows, marks the component failed, and raises the original error.

The existing failure regression now injects its `OSError` through
`nodes_by_job_status()`. The test waits until real B API work has begun, verifies
that the injection ran, expects the original error, and checks that both members
are failed with no running jobs. Reverting the coordinator to the former reads
would bypass this injection and fail the test. The change therefore preserves
error-path sensitivity rather than merely updating a mock name.

## Benchmark completion gate

The earlier benchmark counted handler entries in memory and returned zero when
`run_component()` returned without raising. That allowed the observed partial
execution to look successful. The corrected benchmark reads job counts from
SQLite after the run, computes exact expected A/B totals from seeds and rounds,
and treats every non-done state as unfinished. It returns zero only when there
was no raised error, both done totals match, and all other state counts are zero.

Three tests cover the distinct decisions:

1. An immediate scheduler return leaves the real seed queued and must return 1.
2. A deliberately shortened run leaves all rows terminal but below the expected
   A=2/B=1 totals and must return 1.
3. A real complete one-seed run reaches A=2/B=1 and must return 0.

The expected-count-only mutation removed only the done-total comparison. The
second case then returned 0 and failed, while all rows were terminal. Restoring
the final code produced eight passing benchmark and scheduler cases in 5.44
seconds. This establishes sensitivity for both halves of the completion gate.

## Verification

| Check | Result | Safety signal |
| --- | --- | --- |
| Three controlled baseline publication probes | All failed with B left queued | Reproduces the first race consistently with real SQLite state and task publication. |
| Permanent publication regression on the base | Expected failure | Detects the incoherent observation behavior. |
| Admitted finite-pump regression on coherent-only code | Expected failure | Detects the second early deadlock independently of the first. |
| Final focused scheduler and benchmark checks | 7 passed in 3.62 s; later final selection 8 passed in 5.44 s | Both corrections, true-deadlock preservation, benchmark failures, positive completion, and error cleanup. |
| Adjacent selection | 42 passed in 43.82 s | Waiting, resident pumps, lifecycle cleanup, and affected scheduler paths. |
| Ordinary suite | 395 passed, 1 deselected in 433.10 s | Wider retained behavior in the isolated scheduler tree. |
| Four fresh-process cyclic checks | Passed in 6.32, 11.92, 9.06, and 4.00 s | Repeated cyclic execution without process-state reuse. |
| Deterministic cyclic stress | 1 passed in 7.62 s | Higher-volume component scheduling. |
| Original failure workloads after both fixes | 2/1: A6/B4; 10/10: A30/B20; 200/100: A600/B400; all exit 0 with no residue | Confirms both fixes together on the workloads that exposed early return. |
| Combined accepted-guard and scheduler tree | 38 passed in 25.08 s, then 20 passed in 15.57 s | Startup, SQLite, active restart, migration guard, scheduler, benchmark, and module-boundary paths pass together. The six scheduler file hashes match the isolated review tree. |

The predeclared performance comparison used one seed, 50 rounds, one thread,
and a 0.001-second task delay. Baseline and candidate alternated three times,
used the same corrected benchmark program, verified their imported source, and
required exact A=50/B=49 durable results with no residue before timing could
count. All six runs met that correctness condition.

Baseline elapsed times were 24.838847, 24.250503, and 23.250496 seconds.
Candidate times were 20.039760, 24.330900, and 20.190315 seconds. The medians
were 24.2505025 and 20.1903154 seconds, for a candidate/baseline ratio of
0.832573. This passes the predeclared maximum of 1.20. The one-seed comparison
isolates repeated coordinator transitions on a workload the base can complete;
the separate multi-seed runs supply the higher-volume correctness evidence.

Raw RED/GREEN logs, workload manifests and results, source hashes, and all six
performance records remain in `testing_ground/issue-45` with the
`scheduler-` prefix. No unresolved architecture choice is needed for this
repair, and no wider implementation stage is accepted by this report.
