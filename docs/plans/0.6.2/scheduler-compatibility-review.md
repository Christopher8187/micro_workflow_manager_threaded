# Scheduler completion compatibility review

Status: **PASS for the assigned retained-behavior, specification,
test-sensitivity, documentation, and combined-source review.** This does not
accept the new component lifecycle, waiting display, session ownership, S2, or
MWF 0.6.2.

Reviewer: GPT-5.6 Sol with xhigh reasoning. I reviewed the isolated
`test_area/mwf-062-issue45-20260904/scheduler-fix` source based on accepted
topology commit `24f584413619d1bbe94da2264032600b9b401105`. The copy excludes
the migration preflight and unfinished preview work. I rechecked
[Settle the MWF workflow-management model for 0.6.2](https://github.com/Christopher8187/product/issues/44),
the complete preparation for
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45),
and the current waiting, queue, runner, terminal-publication, and component
cleanup paths. The complete preparation contains no unresolved architecture
choice needed for this repair.

Reviewed isolated-file hashes:

- `storage/job_queries.py`: `0D82C9D660D5F45B1E12FD057E91A60497E412E6D20759F0775ED197F9182D75`
- `workflow/component_scheduler.py`: `9AE176B57A891396C32348F621F261E94895AF47C22C1FA82B38D8C27ED4538F`
- `test_057_hoeflein_live_sync_053.py`: `E6BC6F279CFE9702D782F1B05BAC6ABDB8C5758FE59561F02B93BC22C8C42AD3`
- `test_068_component_snapshot.py`: `711CACEDB682352970CB695D1F3E04C8145DECC6A380E269128A2F32E5C7C14C`
- `benchmark_hoeflein_wait.py`: `4C7C48D20524CF0411A78EB0CA79ACA87E8E8F87854CB619B7FF98514F24AA62`
- `test_benchmark_exit_codes.py`: `981B63433B431E0FD557C6C5520DAB1DEE5EE91F03A689914B1316E623DBABDF`

Reviewed direct-document hashes:

- `stage-scheduler-completion.md`: `026E01C84B8E2AF0C6BC19792454EB4C5765441777A35A8B581E195593EE219D`
- `tests/README.md`: `CF63D5A3ED0D14F9575AF0A7835779D3B1E645DE38E28B16A7A2BB7A271AD8B1`
- `benchmarks/README.md`: `25D1BC22764A978095E803711FE0F76930174BC048085B110D6ED8247F8331B1`

## Requirement disposition

This repair supplies section evidence for 45-SRC-002, 45-PLN-006,
45-TDD-016, 45-TDD-023, 45-TDD-026, 45-REV-011, 45-REV-017, and
44-REC-014. The ledger correctly records partial progress only for
44-REC-014, 45-REV-011, and 45-REV-017. It restores retained 0.6.1 scheduling
behavior and exercises real scheduler and SQLite paths. It does not implement
the final component record, lifecycle, or active waiting display. Requirements
44-CMP-015 through 44-CMP-017, 44-REC-002, and 44-REC-022 therefore remain
pending.

The job outcomes in 44-CMP-019 are unchanged. Queued, running, failed,
cancelled, skipped, and done remain job states. This repair does not promote raw
waiting state into a component lifecycle or add a new state transition.

## Preserved behavior and source review

The prior coordinator could combine three observations that never coexisted:
queued nodes from before publication, running nodes from after producer
completion, and blockers from a third moment. One SQL query now returns the
requested queued, running, and failed node sets from one read. Stable-state
behavior is unchanged, while a producer publication and completion can no
longer create false quiescence or a false waiting deadlock.

The query validates and deduplicates node and status inputs, returns every
requested status key even when empty, and returns sets just like the former
helpers. It introduces no schema, mutation, ordering, or storage-construction
change. MWF has one storage implementation, and the method is present on that
implementation's existing job-query mixin.

The second change narrows deadlock classification. A finite worker that has
already been admitted can be between two claims while SQLite contains queued
rows and no running row. The coordinator now waits while such a worker remains
active. It excludes ordinary resident threaded and API workers from that
condition because those workers intentionally remain alive while idle. A true
cycle of waiting nodes therefore still returns when the only active worker is
an idle resident member.

Completion still requires one snapshot with no queued or running work, followed
by stopping and joining resident workers. A worker exception that becomes
visible during the join is still surfaced and finalizes the component as
failed. The existing failure-injection test now targets the replacement
observation method and retains its checks that live work is joined and no stale
running job remains.

Direct-runner scheduling does not enter the changed coordinator branch.
Threaded, API, process, and other finite workers use the same active-worker
condition. Existing API waiting/admission, direct waiting, late feedback,
failure cleanup, terminal recovery, cyclic execution, and Markov stress checks
remain green. No beyond-diff source regression was found.

## Test sensitivity

The accepted RED records establish two separate scheduler failures:

- `scheduler-red-01.log` delays delivery of a real SQLite observation while a
  real producer publishes and completes. The old coordinator returns with both
  jobs done but leaves component node status queued. The two retained
  comparison cases pass.
- `scheduler-admission-red-02.log` pauses the real second transition to
  running. The admitted finite worker remains live while both nodes have queued
  work and no running row. The old coordinator stops early and leaves consumer
  work unfinished; the coherent-observation and true-deadlock cases pass.

The earlier admission RED used a claim hook that the programmatic path does not
call and asserted an incidental return-list detail. It is correctly excluded as
test setup rather than behavioral evidence. The corrected test asserts durable
job outcomes. The true-deadlock case has a diagnostic deadline, leaves both
waiting jobs queued, and fails if either task executes.

The benchmark checks are also sensitive. The first RED proves that queued work
now produces a failing process result. The review-added RED marks the only
existing seed done, leaving no non-done rows but fewer completed rounds than the
workload requires. Removing only the expected-count condition makes the
benchmark return success, so this case independently protects the missing-round
gate. The real completed case protects the successful result.

The benchmark derives both completed and residue counts from SQLite after the
timed interval. For `seeds=S` and `rounds=R`, the alternating workload
requires `A=S*max(R,1)` and `B=S*max(R-1,0)` for nonnegative `S`; negative
seeds create no rows and are normalized to zero. Any unexpected non-done row or
missing expected completion causes a nonzero process result.

Recorded checks are green:

- Seven focused scheduler and initial benchmark cases passed in 3.62 seconds.
- The review mutation failed as intended; the unmodified eight-case selection
  passed in 5.44 seconds.
- Final adjacent checks passed 42 tests in 43.82 seconds.
- The ordinary suite passed 395 tests with one deliberate stress deselection in
  433.10 seconds.
- Four cyclic cases passed in separate processes in 6.32, 11.92, 9.06, and
  4.00 seconds.
- The selected Markov-chain stress case passed in 7.62 seconds.
- I independently ran the three new scheduler cases, all five benchmark-exit
  cases, the moved scheduler-I/O failure case, and the retained high-concurrency
  API waiting/admission case: **10 passed in 20.03 seconds**, with the exact
  isolated source, source virtual environment, disabled pytest cache, and a
  fresh base directory.

The original workload checks now finish exact durable A/B done counts of 6/4,
30/20, and 600/400 for the 2/1, 10/10, and 200/100 configurations, with zero
non-done residue.

## Measured comparison

The comparison used the same revised benchmark program against verified
topology-only and scheduler-fix source trees. The workload was one seed, 50
rounds, one thread, and a 0.001-second task delay. Each of the six processes
reported exact A50/B49 done counts and zero non-done residue.

Baseline elapsed times were 24.838847, 24.2505025, and 23.2504955 seconds.
Candidate elapsed times were 20.03976, 24.3308996, and 20.1903154 seconds. The
medians were 24.2505025 and 20.1903154 seconds. The candidate/baseline ratio was
0.832573, within the predeclared maximum of 1.20. Durable result queries ran
after the measured interval. The manifest records the interpreter, commands,
source hashes, workload, alternation order, and threshold.

## Documentation and combined-tree verification

The direct `tests/README.md` correctly describes the new scheduler module and
the expanded benchmark-exit cases. The isolated scheduler copy contains an
earlier test guide, so documentation hashes come from the direct MWF tree while
executable hashes remain tied to the isolated copy. The benchmark guide now
states that all expected rounds must finish without non-done residue, matching
the implemented treatment of failed, cancelled, and skipped jobs. The stage
record includes the RED/GREEN history, exact performance results, combined
checks, both independent reviews, and the narrow acceptance boundary.

The scheduler copy predates the accepted migration guard. An immutable combined
copy based on accepted commit `8bde626ff083840bf002cfe685d79bd8d9aa674d`
received only the six reviewed scheduler source and test files plus their three
documents. Its six executable hashes match the isolated scheduler review copy.
The accepted migration files differ from the earlier review copy only in line
ending representation; a content comparison ignoring end-of-line spacing is
empty.

The first combined selection passed **38 tests in 25.08 seconds** and the second
passed **20 tests in 15.57 seconds**. Together they cover the new scheduler
cases, the moved observation-failure case, retained waiting and terminal paths,
API admission, the migration guard, initialization and SQLite behavior, active
restart, benchmark failures, and module boundaries. This checks the material
integration seam: current project/storage initialization reaches the unchanged
scheduler storage mixin after the raw migration preflight.

The source files otherwise do not overlap. The CLI migration guard does not run
in the direct `MicroWorkflow` benchmark path, so combining it does not require
another performance comparison. The combined checks used cache-disabled fresh
test roots.

No source, test, compatibility, documentation, or architecture finding remains
in this assigned section. This PASS accepts the retained scheduler repair only.
