# Released API claims and continuation

Status: eleven focused cases, 300 adjacent checks, and the [bounded correction review](../../../../testing_ground/issue-45/native-api-abandoned-claim-corrections-review.md)
passed. This follows [restart during cleanup](native-api-abandoned-restart.md).

A queue notification exception after committed release incorrectly created an
abandoned-job failure. Release now returns its committed result even when the
notification raises an ordinary exception. Writer failures still propagate.
The queue's existing bounded polling can observe the durable change.

A second test successfully released an unstarted claim, cancelled it through
the public API, then restarted it through the CLI before session exit. The
session originally missed that accepted replacement.

The operation now retains every abandoned attempt, including successful
releases. Only unsuccessful release adds a synthetic failure. The session writer
ignores unchanged released attempts and recognizes accepted later generations.
Continuation admits those exact replacements while other failures keep ordinary
admission stopped. Actual execution clears the retained abandoned attempt.
Q8 last ownership remains the committed preclaim owner throughout release.

## Verification

Parent Repo names begin `sample-calculations-native-api-`.

| Run suffix | Result |
| --- | --- |
| `abandonment-adjacent-01` | Previous nine-order source, 298 passed, 340.80 s |
| `release-notification-red-01` / `green-01` | Committed release misclassified, 1 failed / all 10 cases passed, 19.12 s |
| `released-cancel-restart-red-01` / `green-01` | Accepted replacement missed, 1 failed / all 11 cases passed, 19.53 s |
| `release-corrections-adjacent-01` | All 300 cases passed, 334.20 s |

Current freeze SHA-256:
`C781D8A9B3DB804228A65AE766B3A94F0A5AA0CA42D71468A496638E18531055`.
Unknown running-claim cleanup, [mixed runners](native-mixed-runner-continuation.md), multiple API lanes, and wider
session integration remain unfinished.
