# Native whole-workflow continuation

Status: bounded threaded and serial corrections reviewed. This extends
[component continuation](stage-native-component-continuation.md).
[API continuation](stage-native-api-continuation.md) is under verification.
Nested calls, interrupts, lifecycle completion, recovery, and Q10 remain open.

`DagExecutionOperation` retains selected components, readiness checks, refusal
boundaries, accumulated public results, failed component operations, and pending
Futures. The outer session driver makes one durable continuation decision after
all started units join. Accepted successors use the retained component operation.
Successful peers remain complete and descendants start after recovery succeeds.

The operation also collects pending outcomes when worker submission or scheduler
bookkeeping fails outside the usual Future-result loop. The regression exposed
a failed job whose node remained marked running after session termination.
Retaining the pending Futures through executor cleanup allows the atomic terminal
decision to publish that node failure. The original submission error remains
the caller's exception, completed peers survive, and unstarted jobs stay queued.

## Verification

Parent Repo record prefix is `sample-calculations-native-dag-continuation-`.

| Run | Result |
| --- | --- |
| `red-01` | Public restart-first failed, terminal-first passed, 3.89 s |
| `green-01` | 5 whole-workflow continuation cases passed, 8.52 s |
| `submission-red-01` | Submission failure left a running node; 22 other cases passed, 30.80 s |
| `submission-green-01` | All 23 cases passed, 33.90 s |

The initial 320-file freeze is `submission-green-01`, SHA-256
`633162F7BA1F227A2F05C9A63739EEA5BA54AC04C78E2968C1E9FCE3564CEBDB`.
It includes the reviewed [component setup retry bound](native-component-retry-bound.md).
[Scheduling](../../../../testing_ground/issue-45/native-dag-continuation-scheduling-review.md)
and [ownership](../../../../testing_ground/issue-45/native-dag-continuation-ownership-review.md)
passed on the [CLI extension](native-dag-cli-and-branches.md).
[Serial execution](native-dag-serial-execution.md) and
[process corrections](native-dag-process-and-cleanup.md) have bounded review.
