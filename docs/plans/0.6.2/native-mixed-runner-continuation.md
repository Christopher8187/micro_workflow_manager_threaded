# Mixed runner continuation

Status: bounded process and [sequential API cleanup](../../../../testing_ground/issue-45/native-api-direct-cleanup-correction-review.md)
corrections reviewed. This extends [DAG execution](stage-native-dag-continuation.md)
and [API release handling](native-api-release-corrections.md).

Sequential workflow scheduling with process node overrides lost the component's
failed attempts and restart expectations. The generic finite-node branch took
precedence, and process workers reported outcomes to its temporary node operation.
Component-aware process execution now takes precedence. Standalone finite
process calls retain their existing operation. The [correction review](../../../../testing_ground/issue-45/native-direct-process-component-continuation-correction-review.md)
passed this boundary.

Sequential scheduling with an API node also skipped joined cleanup after a
failed release. Its failure path now invokes the existing component cleanup with
the retained abandoned attempts. The normal generation-checked writer settles
the old claim; unresolved active claims still trigger the typed exit refusal.

## Verification

Parent Repo run names start with `sample-calculations-native-`.

| Run suffix | Result |
| --- | --- |
| `dag-mixed-process-red-01` / `green-01` | Both new CLI cases failed / all 8 runner and refusal cases passed, 43.06 s |
| `api-direct-cleanup-red-01` / `green-01` | Failed release left a running claim, 1 failed and 1 passed / all 22 abandonment cases passed, 39.75 s |

Current freeze SHA-256:
`E490E5FC8FECD6575571C9823209DFF6508718595DB5B197F35959F22C8A7B1A`.
Broader checks include [multiple API lanes](native-api-multiple-lane-restarts.md)
and [later-wave budgets](native-api-later-wave-budget.md).
[Output publication cleanup](native-output-publication-cleanup.md) and wider
session integration remain unfinished. Q10 remains undecided.
