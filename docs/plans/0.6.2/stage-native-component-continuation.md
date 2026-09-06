# Native component continuation

Status: partial implementation, bounded setup retry correction reviewed. This extends
[finite session exit](stage-native-restart-session-exit.md) and
[queued-node continuation](stage-native-queued-restart-exit.md) through public
`run_component` and cyclic `run_node`. [Whole-DAG and CLI continuation](stage-native-dag-continuation.md)
and [API continuation](stage-native-api-continuation.md) have bounded reviews.
Mixed runners, wider API, nested-task, and interrupt continuation remain unfinished.
Q10 is unchanged. Current acceptance is recorded in
[bounded restart progress](native-restart-progress.md).

## Behavior

The [component preparation](../../../../testing_ground/issue-45/native-component-session-continuation-preparation.md)
requires retaining a component operation while replacing stopped pump epochs.
`ComponentExecutionOperation` keeps returned values, failed attempts, accepted
expectations, ordinary-admission state, and the public node list. Each epoch
constructs fresh stop/wake/start events, sources, and workers, then joins them.

Job success and failure remain durable immediately. Node failure and component
failure publication wait for the session's atomic terminal decision. Joined
abandoned-job settlement remains separate. Every remaining Future is inspected
so a secondary infrastructure error cannot disappear during cleanup.

Restart-first retains the same session and component reservation. Successful
peers retain output and events. Terminal-first refuses a later restart unchanged.
Every recovery epoch first executes only accepted successors. Ordinary queues
reopen after all failures are repaired. Partial recovery leaves other queued
jobs untouched. The stopping source supports finite iterables and pull sources.

An epoch cannot report success with an accepted successor still unclaimed.
Cancellation after the decision therefore remains visible as failure. A later
accepted generation requires the existing durable request-event checks before
another epoch begins.

## Verification

Root uses immutable Test Area copies, freezer revision 19, and runner revision
04. Parent Repo records use prefix `sample-calculations-native-component-continuation-`.

| Run | Result |
| --- | --- |
| `red-01` / `green-01` | Restart-first failed, terminal-first passed, 3.25 s / 2 passed, 2.89 s |
| `partial-01` / `partial-green-01` | Finite replacement source failed, 4 controls passed, 6.99 s / 5 passed, 6.40 s |
| `superseded-01` | 2 superseded-request failures, 5 passed, 9.87 s |
| `cancelled-red-01` | 2 cancellation-after-decision failures, 3.17 s |
| `cancelled-green-01` | All 9 component and cyclic-node cases passed, 14.70 s |

The [review corrections](native-component-review-corrections.md) and
[retry bound](native-component-retry-bound.md) record subsequent regressions,
native test adaptations, and current freezes. These checks cover fixed membership only and grant
no whole restart, component-lifecycle, recovery, or release acceptance.
