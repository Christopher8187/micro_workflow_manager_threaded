# Native finite restart admission

Status: partial implementation under review. This section follows the
[native restart ownership work](stage-native-owned-restart.md). It completes no
whole requirement and does not settle Q10. Later results and acceptance are
recorded in [bounded restart progress](native-restart-progress.md).

## Behavior and cause

A second terminal could accept a failed job's restart while its owning session
was running other work. The replacement became queued, but the runner retained
the first attempt's exception. After joining active peers, it raised that
exception and the session ended without executing the replacement.

The private `NodeExecutionGroup` retains the original source, including
prefetched items, and each completed task's actual Python
value. Runners still stop ordinary admission and join active work on failure.
The group checks recorded failed attempts against current native ownership.
Only a queued successor with the same execution owner, session, component, and
validated current job instance qualifies for another attempt.

The implementation connects finite direct and threaded node calls and explicit
job selections. Accepted successors reread payloads. Completed jobs do not run
again. Explicit selections retain caller order and exclude unselected work.
Ordinary finite sources continue from their existing position. A separately
accepted replacement runs even when another failed job remains unrepaired;
that unrepaired failure still prevents ordinary admission and fails the call.

`JobFailedError` carries the exact attempt address, generation, and execution
ID. Other exceptions retain their failure behavior. No task result is rebuilt
from `output.json` or its textual representation.

## Verification

Root runs copied source through freezer revision 18 and runner revision 04.
Records under Parent Repo `testing_ground/issue-45/` use the prefix
`sample-calculations-native-owned-restart-`.

| Run | Result | Observation |
| --- | --- | --- |
| `result-preservation-01` | 4 passed, 4.03 s | Python values, selection order, excluded jobs, unrepaired failure. |
| `failed-admission-green-01` | 5 passed, 8.23 s | Original failed restart and preservation. |
| `selected-admission-red-01` | 3 failed, 4.71 s | All three public forms fail with the unchanged scheduler. |
| `failed-admission-green-02` | 59 passed, 53.31 s | Full restart module, including the three corrected forms. |
| `partial-admission-red-01` | 1 failed, 2.09 s | Another unrepaired job prevented the accepted replacement. |
| `partial-admission-green-01` | 8 passed, 10.32 s | Replacement executes while the other failure remains reported. |
| `finite-adjacent-01` | 139 passed, 151.31 s | All 60 restart checks, native entry points, runners, and prefetch. |

Latest source freeze SHA-256:
`B527AA6E3CAE10E780A2F6FB468997F315734E4F9E5D402EBE96F1D8395C5FD5`.
Freezer 18 SHA-256:
`41F55A7E302331B17C09C3F28B37A004441FA51A68A1A64A0E61246E7EA8C872`.
The freeze records all 317 Python files and the exact changed-file hashes.
Two independent Sol xhigh reviews are recorded in the
[following process and claim section](stage-native-restart-claim-validation.md).

## Remaining work

API and live component failed-job admission remain unfinished. The
existing direct, threaded, and process running-generation replacement checks
cover a different path.
Direct failed-job admission is wired but lacks a deterministic behavioral check.

The outer execution driver must arbitrate accepted pending restarts and session
termination in one writer transaction. If restart commits first, the owning
session must run that work. If termination commits first, restart must refuse.
A stopped node pump alone does not justify refusing its still-live owner.
Results and continuations must survive that outer boundary as well.

Failure of an interrupt target component differs from an individual job failure
while the component is still running. Once the target establishes failure, the
specified new-session retry rule applies. No blanket main-session restriction
was added. Component lifecycle integration and full interrupt execution remain
pending.
