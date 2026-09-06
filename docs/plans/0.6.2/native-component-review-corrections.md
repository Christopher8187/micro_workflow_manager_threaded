# Native component review corrections

Status: bounded corrections reviewed. This supplements
[component continuation](stage-native-component-continuation.md).

The [scheduling review](../../../../testing_ground/issue-45/native-component-continuation-scheduling-review.md)
and [ownership review](../../../../testing_ground/issue-45/native-component-continuation-ownership-review.md)
blocked the initial source. Extending the stopping wrapper to finite lists
eagerly iterated API pull-only sources. It now creates an iterator only when
iteration is requested. Existing API execution and finite replacement admission
both remain covered.

Four older checks also failed before this component implementation. Three
instrumented the former public scheduler call; they now instrument the scoped
call. The fourth now checks exact native session outcome, released reservation,
liveness, node state, and final monitor output. Their underlying requirements
remain active. The late-worker case additionally preserves the exact error
object and checks terminal session state and absence of running jobs.

An unrelated preparation error in a replacement epoch previously hid its
unclaimed accepted successor. Pending expectations now retain their failed
attempt for writer revalidation. A one-shot preparation failure remains the
final exception after the accepted successor executes; ordinary admission stays
stopped.

Adding ordinary `A/2` behind failed `A/1` confirmed admission reopens after full
recovery, but exposed unwanted admission if the accepted successor was cancelled.
Each recovery now completes an exact replacement epoch before reopening ordinary
queues. Its claim-time validation still refuses cancellation and follows a later
accepted generation. The superseded-request test now pauses at the claim boundary,
which both finite and batched loading reach.

## Verification

Parent Repo record prefix is `sample-calculations-native-component-continuation-`.

| Run | Result |
| --- | --- |
| `adjacent-01` | 207 passed, 17 failed, 230.39 s |
| `pull-green-01` | 39 passed, 3 old assertions failed, 44.32 s |
| `prior-checks-01` / `adapted-prior-01` | 4 old checks failed on preceding source, 4.03 s / adapted checks passed, 3.56 s |
| `adapted-current-01` | 46 passed, 44.34 s |
| `setup-red-01` / `setup-green-01` | 2 failed, 2.83 s / 11 passed, 19.03 s |
| `admission-red-01` | 2 cancellation/admission failures, 9 passed, 18.37 s |
| `admission-green-01` | 9 passed; 2 supersession hooks missed the new finite source, 20.28 s |
| `admission-green-02` | 12 passed, 15.82 s |
| `adjacent-02` | 226 passed, 5 exact whole-DAG deselections, 231.30 s |

Current freeze SHA-256:
`F3688C1C9A0BFAA1F9CD40524DD3FFB38C164E8D673BCE5A033CFC84AA5978D7`.
The [ownership correction review](../../../../testing_ground/issue-45/native-component-continuation-ownership-correction-review.md)
passed this boundary. The [scheduling correction review](../../../../testing_ground/issue-45/native-component-continuation-scheduling-correction-review.md)
found a persistent setup loop. Its [retry correction](native-component-retry-bound.md)
passed both independent reviews. No whole requirement is accepted.
