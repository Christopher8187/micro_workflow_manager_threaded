# Native DAG CLI and branch checks

Status: bounded source reviews passed. This supplements
[whole-workflow continuation](stage-native-dag-continuation.md).

CLI orchestration passes its session driver into the retained DAG operation.
Preparation, selection, and printing stay outside the continuation loop.

`runfrom A` pauses after component failure. A separate restart queues its
successor. The run completes ordinary queued work
and descendant `C`, preserving completed peer `B`, job incarnation, session,
selection details, and one final node list. With `refuseafter B`, the recovered
`A <-> B` component completes and `C` stays queued and unowned.

Independent branches have separate retained errors. When both `A/1` and `B/1`
fail and only `A/1` is restarted, its successor completes, `B/1` stays failed,
and ordinary jobs and descendants remain queued. A public readiness callback
error also preserves already-started component outcomes and its original error.

## Verification

Parent Repo prefix is `sample-calculations-native-dag-continuation-`.

| Run | Result |
| --- | --- |
| `cli-red-01` | CLI continuation failed; 2 scheduler-error cases passed, 6.30 s |
| `cli-green-01` | All 25 continuation cases passed, 38.00 s |
| `partial-01` | Component and independent-branch partial repair passed, 3.38 s |
| `refusal-01` | Both CLI cases passed, 7.85 s |
| `adjacent-02` | 266 passed, 2 old-format assertions failed, 298.85 s |

Current 320-file freeze SHA-256:
`9D5AEE4CE5597B89B91BCB80B4209B582149698E06C1E6E1EE736E5BF51F5F98`.
`adjacent-01` named a nonexistent module and ran no tests. Both failures also
occur before this DAG change. Native assertion updates and further checks are
recorded with [serial execution](native-dag-serial-execution.md).
