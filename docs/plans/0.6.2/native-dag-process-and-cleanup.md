# Native DAG process and cleanup corrections

Status: bounded corrections reviewed; 171 selected checks passed. This extends
[serial execution](native-dag-serial-execution.md).

A process CLI run exposed duplicate `A` entries after restart and `refuseafter`.
Normal Future completion now applies the retained-member check used by joined
cleanup and serial execution. Separate pump epochs report each member once.

A combined submission and admission-restoration failure exposed a second issue.
The restoration exception replaced the original submission exception during
executor cleanup. Exit handlers now retain the first infrastructure error before
executor cleanup and before admission restoration. All workers still join and
their outcomes are collected before the native terminal decision.

## Verification

Parent Repo prefix is `sample-calculations-native-dag-continuation-`.

| Run | Result |
| --- | --- |
| `cleanup-red-01` | Cleanup error identity and process result duplication failed; ordinary process continuation passed, 15.60 s |
| `cleanup-green-01` | All 3 scheduler-error cases passed, 3.40 s |
| `process-green-01` | Both real process CLI continuation/refusal cases passed, 14.32 s |
| `extended-adjacent-01` | All 171 selected cases passed, 213.65 s |

The current 320-file `process-green-01` freeze has SHA-256
`2D3EA40967DA95B7C86222772F5D0D0C9F449D092187E73BD43DDFC8C7B23BC4`.
The [correction review](../../../../testing_ground/issue-45/native-dag-direct-process-correction-review.md)
passed this boundary. No whole stage or requirement is accepted.
