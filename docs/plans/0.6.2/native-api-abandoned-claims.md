# Native API abandoned claims

Status: bounded exit correction reviewed. This extends
[API continuation](stage-native-api-continuation.md).

API failure publication can yield to the writer and start another handler.
After correcting that scheduling assumption, the test waits for real writes at
the public submission boundary to exercise unstarted claims.

Normal abandonment queues the unstarted job, clears active fields, and retains
its last owner under Q8. A subsequent accepted restart runs the
replacement first, then claims the abandoned job once. One failed release write
is compensated by joined cleanup, which marks that claim failed.

Two failed cleanup writes exposed terminal publication with a running orphan.
The session writer now checks active executions before either continuing accepted
restarts or publishing terminal state. It also checks reserved nodes when an
active-owner pointer is missing or contradictory. The decision preserves the
session and reservations when cleanup remains unresolved.

Only the typed active-claim refusal stops local reporters and heartbeat and
prevents another exit attempt. It preserves the handler cause and adds a
diagnostic. Transient writer failures retain their existing retry behavior.
Run-bound state remains available for recovery.

## Verification

Parent Repo names begin `sample-calculations-native-api-`.

| Run suffix | Result |
| --- | --- |
| `release-failure-control-01` | Both cases passed, 2.49 s |
| `double-cleanup-failure-red-01` / `green-01` | Orphan failed; 2 controls passed / 3 passed |
| `orphan-restart-red-01` / `green-01` | Restart hung; 3 controls passed / 4 passed |
| `damaged-active-owner-red-01` / `green-01` | Missing-owner case failed / all 5 passed, 10.95 s |
| `cleanup-adjacent-01` | 258 passed; transient-writer retry regressed, 302.80 s |
| `exit-refusal-correction-01` | All 28 passed, 33.03 s |
| `damaged-reservation-red-01` / `green-01` | Combined damage failed / 29 passed, 1 settlement-race deselection, 35.95 s |

Current freeze SHA-256:
`2D1903E46BB62398B533618C1BFC3BFCC38C34756412A145281B34FD7590160E`.
The [exit review](../../../../testing_ground/issue-45/native-api-active-claim-exit-correction-review.md)
passed this boundary. [Restart during cleanup](native-api-abandoned-restart.md) follows.
