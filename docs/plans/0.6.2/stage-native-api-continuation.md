# Native API continuation

Status: 175 selected checks passed; [bounded review](../../../../testing_ground/issue-45/native-api-component-dag-continuation-source-review.md) passed.
This extends [DAG continuation](stage-native-dag-continuation.md).

The [API preparation](../../../../testing_ground/issue-45/native-api-component-dag-continuation-preparation.md)
identified runner exclusions at both operation entry points. API components and
DAGs now use the same retained operations. Ordinary API admission still uses its
batch claims. Replacement epochs load exact accepted jobs and validate their
expected ownership in the existing per-item writer transaction.

Public checks preserve completed peers, incarnation, owning session, and
reservations through restart. The DAG case retains an independent completed
component and runs its descendant after recovery. Terminal-first restart is refused.

## Verification

Parent Repo prefix is `sample-calculations-native-api-continuation-`.

| Run | Result |
| --- | --- |
| `red-01` / `green-01` | Component restart failed, terminal control passed, 2.87 s / both passed, 3.47 s |
| `dag-red-01` / `dag-green-01` | DAG restart failed, terminal control passed, 3.13 s / all 4 API cases passed, 6.69 s |
| `adjacent-01` | All 175 selected cases passed, 207.18 s |

Current freeze SHA-256:
`659405F13B79CDAB7A01974279EAE77ED0BEDE7405E457AE9B205045EC2C9421`.
[Abandoned-claim cleanup](native-api-abandoned-claims.md) follows this source.
Sharding, mixed runners, and wider integration remain unfinished.
