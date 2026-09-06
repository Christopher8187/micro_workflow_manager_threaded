# Native restart ownership and replacement

Status: in progress. Native ownership and rollback passed 52 checks. The
[finite admission correction](stage-native-finite-restart-admission.md) executes
accepted failed-job replacements and retains completed results. Other runners
and atomic session termination remain unfinished. No whole requirement or stage
is accepted.

## Implemented boundary

Job, explicit-list, and component forms resolve persisted native ownership.
They require the exact current-or-last owner, job instance, live session,
immutable selected component, and unambiguous reservation. Queued and done
targets refuse. Failed/cancelled targets retain Q8's valid older last owner.
No singleton-run fallback remains in this command.

The mutation takes sorted job fences and repeats ownership and eligible-state
checks under `BEGIN IMMEDIATE`. Component commands also rebuild the complete
selection, including an empty selection. Generation, queued status, cleared
active execution, restart metadata, runtime observation, events, affected node
status, and restart revision commit together. The last owner remains until a
replacement claim. Node notifications follow commit.

Existing canonical output moves to a unique same-directory name before the
database changes. Synchronous failures roll back the database and restore
staged output. Failed restoration preserves the original error and reports
every retained path, including without Python 3.11's `add_note`. Cleanup or
notification failure after commit reports attention without restoring old output.

## Verification

Root ran immutable Test Area copies through freezer revision 17 and runner
revision 04. Records in Parent Repo `testing_ground/issue-45/` use prefix
`sample-calculations-native-owned-restart-`.

| Run | Result | Observation |
| --- | --- | --- |
| `red-01` | 3 failed, 2.78 s | Fixture used a nonexistent JSON helper. |
| `red-02` | 3 failed, 3.34 s | Native owners refused through the singleton lookup. |
| `green-01` | 3 passed, 2.75 s | Running, failed, cancelled durable transitions. |
| `preservation-01` | 20 passed, 2 failed, 17.07 s | Two session fixtures omitted finish time. |
| `preservation-02` | 22 passed, 17.15 s | Refusals, races, batch rollback. |
| `compensation-red-01` | 5 passed, 2 failed, 6.35 s | Corrected notification-count and trigger-timing fixtures. |
| `compensation-red-02` | 2 failed, 2.98 s | Hidden restoration note and missing node wake. |
| `green-02` | 30 passed, 23.52 s | Notes and node notifications corrected. |
| `component-red-01` | 3 failed, 3.29 s | Node-wide singleton lookup. |
| `green-03` | 33 passed, 21.63 s | Native component selection. |
| `py310-red-01` | 1 failed, 0.88 s | Missing `add_note` masked the original failure. |
| `green-04` | 34 passed, 24.17 s | Compatible error reporting; simulated missing method. |
| `runners-01` | 3 passed, 7.63 s | Real second-terminal running replacement, direct/threaded/process. |
| `component-preservation-01` | 49 passed, 45.20 s | Selection races, mixed owners, dry-run stored-state preservation. |
| `component-refusal-red-01` | 3 failed, 2.11 s | Empty-plan races and missing active execution. |
| `green-05` | 52 passed, 45.93 s | Both component corrections. |
| `failed-admission-red-01/02` | 1 failed each, 2.13/1.79 s | Accepted retry stays queued; the session fails. |
| `retained-01` | 2 passed, 6.49 s | Checkpoint replacement and separate-process execution. |

Green 05 freeze SHA-256 is
`1C9192478B4F9AA25A3F658A27311A08B5AE21EEE2046C6DBD4857B91CA903C4`.
The later failing-admission freeze is
`4CB155BECF686111725FE38A891EA9E5E8DC5872533009FD6C38F42049575097`.
Freezer 17 SHA-256 is
`D7B913D95ECCE874022E8BE77988BB86E58EC108ED0751D7D1F79914589116F2`.

## Reviews and remaining work

Independent GPT-5.6 Sol xhigh reviews cover
[transaction, compensation, and execution](../../../../testing_ground/issue-45/native-owned-restart-source-review.md)
and [component selection](../../../../testing_ground/issue-45/native-component-restart-source-review.md).
The [component corrections](../../../../testing_ground/issue-45/native-component-restart-correction-review.md)
pass independent review. Finite admission has a separately recorded correction.

Wider restart admission and atomic session termination need repair. Adjacent
verification, strict filesystem read-only
bootstrap, crash recovery for staged output, component lifecycle, and remaining
native command reconciliation are pending. Dry-run tests compare stored rows
and job-directory contents; they do not establish complete sidecar immutability.
Q10 and interrupt transfers receive no credit.
