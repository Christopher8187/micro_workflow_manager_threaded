# Native DAG serial execution

Status: bounded corrections reviewed. This extends
[threaded DAG continuation](stage-native-dag-continuation.md).

Runner `direct` now uses the retained DAG operation with serial component
admission. It creates no DAG executor and stops immediately when a component
raises. Existing monitored handler threads remain unchanged. The serial control
passed before and after this change.

Both public `workflow.run()` and CLI orchestration retain completed results,
accepted successors, readiness, and refusal state. The serial component operation
reports each executed member once across replacement and ordinary epochs.

The first direct test incorrectly required handler thread identity. Source
inspection showed existing monitored handler threads; that unsupported assertion
was removed before implementation. The corrected regression isolates the missing
restart continuation and preserves terminal-first refusal.

Two retained refusal tests also used the retired session reader. Both failed
on the pre-DAG source. They now check native terminal outcome, exact refusal
details and selected components, released reservations, and no live main session.
Their subsequent resume behavior remains checked.

## Verification

Parent Repo prefix is `sample-calculations-native-dag-continuation-`.

| Run | Result |
| --- | --- |
| `direct-red-01` | 2 failures including unsupported thread assertion; 2 controls passed, 14.59 s |
| `prior-refusal-01` | Both old assertions failed before DAG changes, 2.87 s |
| `direct-red-02` | Restart failed; terminal control and all 12 native refusal checks passed, 12.46 s |
| `direct-green-01` | 33 passed, 65.37 s |
| `direct-cli-red-01` | Both direct CLI restart cases failed, 9.15 s |
| `direct-cli-green-01` | 59 passed, 95.53 s |
| `independent-01` | Independent completed peer and custom readiness continuation passed, 4.25 s |

The latest freeze is `independent-01`, SHA-256
`E5FC4C990713747D6F17AF3F56C7689FA9D67563D7D7B7DADE976C9E03ADAB4F`.
[Process and cleanup corrections](native-dag-process-and-cleanup.md) follow this source.
