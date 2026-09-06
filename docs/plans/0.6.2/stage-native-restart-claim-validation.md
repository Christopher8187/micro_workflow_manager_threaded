# Native restart process results and claim validation

Status: partial implementation, bounded claim review passed. This continues the
[finite admission work](stage-native-finite-restart-admission.md). Atomic
session termination, API, live component execution, and cancelled-work admission
remain unfinished. No whole restart requirement is accepted.

## Implemented behavior

The process runner now reports each drained child outcome to the execution
group. It retains values, failed-attempt metadata, and unstarted submissions.
A second runner cycle
replaces the accepted failed attempt without rerunning a completed peer or
widening an explicit selection. Transport and serialization errors remain fatal.

Independent review identified a gap between observing an accepted restart,
loading its payload, and claiming it. A public execution test deleted and
recreated the target after payload reload. The earlier implementation claimed
the new incarnation using the stale item and failed it.

The group now captures the current incarnation, generation, recorded owner, and
restart timestamp. The claim transaction rechecks them together with queued
status, absence of an active execution, running session, exact selected
component, and reservation ownership before writing any claim. Process workers
receive the expectation and check it where the child claims the job.

The expected generation describes the current restart request. It need not be
one greater than the failed claim. Restart, public cancellation while queued,
and another restart may advance it twice without an intervening claim. A real
second-terminal check preserves that approved sequence.

## Verification

Root uses immutable Test Area copies, freezer revision 18, and runner revision
04. Parent Repo records use prefix `sample-calculations-native-owned-restart-`.

| Run | Result |
| --- | --- |
| `process-admission-red-01` | 1 failed, 2.36 s |
| `process-admission-green-01` | 9 passed, 11.61 s |
| `process-result-preservation-01` | 1 passed, 1.79 s, before process correction |
| `process-adjacent-01` | 141 passed, 148.50 s |
| `claim-race-red-01` | 1 failed, 3.43 s |
| `claim-race-green-01` | 11 passed, 18.77 s |
| `claim-race-green-02` | 3 passed, 6.72 s |
| `claim-adjacent-01` | 213 passed; eight obsolete version-four cases failed, 175.34 s |
| `native-claims-only-01` | 34 passed, eight obsolete cases excluded, 21.79 s |
| `claim-changes-red-01` | 4 failed, 6.31 s, changed generation, owner, status, restart marker |
| `claim-changes-green-01` | 68 passed, 64.13 s |

Latest freeze SHA-256:
`AE6C15946A5CD31AC3E408130D495026E7782B4E43534BAC1AEF73A621DE781B`.
It records 317 Python files and passed all 68 restart checks.
The eight obsolete cases were selected accidentally; their old-model obligations
are cancelled. Native behavior passed in that mixed run.

The [independent design review](../../../../testing_ground/issue-45/native-failed-restart-admission-design.md)
records the process extension and claim race. The
[claim correction review](../../../../testing_ground/issue-45/native-restart-claim-validation-review.md) passes.
The [complementary finite review](../../../../testing_ground/issue-45/native-finite-restart-admission-complementary-review.md)
leaves direct failed-job execution and session-finalization arbitration open.

The [session-exit continuation](stage-native-restart-session-exit.md) adds the
atomic decision for finite selected-job calls and remains under review.
