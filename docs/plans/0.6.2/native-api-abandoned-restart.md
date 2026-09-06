# Restart of an abandoned API claim

Status: nine cases passed; [release corrections](native-api-release-corrections.md) extend this work.
This extends [active-claim exit handling](native-api-abandoned-claims.md).

A real CLI restart accepted generation 1 after cleanup read the old running
claim. The unconditional cleanup write then marked generation 1 failed.

Failed release now reports the abandoned generation and execution ID to the
retained component operation. A conditional release that finds a changed lease
reports the same information. The operation retains that attempt through its
session decision, while preserving the earlier handler failure.

Cleanup of a known abandoned claim uses the existing generation-checked terminal
writer. A changed lease cannot receive the old failure. The session driver can
then admit the accepted successor under the same session while unrelated failed
work keeps ordinary admission stopped.

Recording abandoned attempts also exposed an ordering issue. The active-claim
guard must precede interpretation of those attempts, so damaged native ownership
cannot replace the original handler cause with a reader error.

## Verification

Parent Repo names begin `sample-calculations-native-api-`.

| Run suffix | Result |
| --- | --- |
| `settlement-race-red-01` | Accepted generation overwritten; failed, 1.67 s |
| `settlement-race-green-01` | Race passed; 2 damaged-owner regressions, 5 passed, 10.83 s |
| `settlement-race-green-02` | All 7 cases passed, 15.44 s |
| `settlement-orders-01` | All 9 cases passed, 17.83 s |

Current freeze SHA-256:
`5A3FEEEF459D60DA976080AD1DAD16E16DD1765343AB09698707C6875DB1228E`.
Other abandoned-running cleanup, mixed runners, multiple API lanes, and broader
session integration remain open.
