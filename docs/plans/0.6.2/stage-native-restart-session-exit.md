# Native restart and session exit

Status: partial implementation; selected-call review passed. This continues
[claim validation](stage-native-restart-claim-validation.md). The driver covers
finite `run_jobs` and `run_node_jobs` execution. The
[queued-node and selected CLI extension](stage-native-queued-restart-exit.md)
records subsequent work. Component/DAG and API/live continuation remain unfinished.

## Behavior

A deterministic public regression paused the last unsuccessful restart lookup.
The separate-terminal command accepted a successor, but its session ended and
left it queued. The correction retains the operation, Python values, caller
order, failed attempts, and unstarted selected jobs until session exit.

One writer transaction either returns accepted successors or publishes failed
node state, terminal session state, and reservation release. Continuation keeps
heartbeat, reporters, ownership, and limits active. Terminal cleanup follows
commit; restart afterward refuses unchanged. The old caller skips post-release
node writes, preserving a newer session's state.

After a stale claim, continuation requires a newer accepted request whose
durable event matches generation, incarnation, execution, session, component,
and timestamp. Changed rows alone do not authorize another attempt.

A failed terminal write retains its original outcome for retry. Continuation
clears that outcome. Reporter cleanup preserves the primary job error. A real
process serialization failure remains visible after another job's accepted
successor runs; ordinary admission stays stopped.

## Verification

Root uses immutable Test Area copies, freezer revision 18, and runner revision
04. Parent Repo records use prefix `sample-calculations-native-owned-restart-`.

| Run | Result |
| --- | --- |
| `session-exit-red-01` | 1 failed, 1 passed, 2.72 s |
| `session-exit-green-01` | 2 passed, 2.72 s |
| `session-exit-adjacent-01` | 138 passed; two old fault-injection locations failed, 131.70 s |
| `session-exit-later-red-01` | 2 behavioral failures, 1 passed after adapting those locations, 3.40 s |
| `session-exit-terminal-green-01` | 2 passed, 1.83 s |
| `session-exit-later-green-01` | 3 passed, 3.79 s |
| `session-claim-preservation-01` | 5 selected-call preservation checks passed before later-claim correction, 6.43 s |
| `session-exit-adjacent-02` | 146 passed, 122.69 s |
| `session-exit-selected-01` | 12 Direct/threaded selected-call cases passed, 16.30 s |
| `session-reporter-red-01` / `green-01` | 2 failed, 2.07 s / 19 passed, 19.41 s |
| `session-node-state-red-01` / `green-01` | 1 failed, 1.60 s / 20 passed, 21.52 s |
| `session-process-error-red-01` / `green-01` | 1 failed, 2.51 s / 6 passed, 8.66 s |
| `session-corrections-adjacent-01` | 159 passed, 158.32 s |
| `session-submit-red-01` / `green-01` | 1 failed, 2.26 s / 4 passed, 10.25 s |

Reviewed selected-call freeze SHA-256:
`3E698AB139262977762FD14263EC17497534AE01E9CB8ED63FEC9C4CF3C1C452`.

The [source preparation](../../../../testing_ground/issue-45/native-restart-session-arbitration-preparation.md)
maps remaining callers. The [initial review](../../../../testing_ground/issue-45/native-restart-session-exit-review.md)
blocked acceptance on reporter precedence, non-job errors, and stale node writes.
The [correction review](../../../../testing_ground/issue-45/native-restart-session-exit-correction-review.md)
passes the finite selected-call boundary, including process submission failure
and draining all submitted outcomes. No whole restart requirement is accepted.
