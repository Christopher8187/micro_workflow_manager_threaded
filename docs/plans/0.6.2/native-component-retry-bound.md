# Native component setup retry bound

Status: bounded correction reviewed; surrounding checks passed. This follows the
[component review corrections](native-component-review-corrections.md).

A persistent replacement-source setup error repeatedly returned the same
unclaimed restart expectation to the writer. The operation never terminated.
The regression releases its injected fault during cleanup so its owned thread
cannot remain alive after the failed test.

The operation now tracks whether the same unclaimed expectation has already
received one setup retry. The writer rejects that unchanged request at the next
terminal decision. It still examines the failed attempt in that transaction,
so a genuine later restart generation can win. Matching incarnation, owner,
generation, restart event, and reservation remain required. A completed claim
consumes this retry state; a new failed execution has its own attempt.

Both outcomes preserve the original setup exception. An exhausted unclaimed
successor stays queued, the session ends failed, and reservations release.
A newer accepted successor executes in the same session before that original
exception is returned. Ordinary work remains queued and completed peers retain
their output and events.

## Verification

Parent Repo record prefix is `sample-calculations-native-component-continuation-`.

| Run | Result |
| --- | --- |
| `persistent-red-01` | Persistent setup loop exposed, 1 failed, 22.35 s |
| `persistent-green-01` | 13 component cases passed, 30.11 s |
| `later-request-01` | 2 newer-request cases passed, 5.63 s |
| `retry-adjacent-01` | 146 passed, 7 exact whole-DAG deselections, 177.50 s |

The final 319-file freeze is `later-request-01`, SHA-256
`2ABC33F4035CB5651098C40975FAD3DE704B759FDDDEE0E5FFAE22A9E0798DAB`.
The [ownership review](../../../../testing_ground/issue-45/native-component-continuation-persistent-setup-correction-review.md)
and [scheduling review](../../../../testing_ground/issue-45/native-component-continuation-scheduling-final-correction-review.md)
both passed this boundary. Whole-DAG continuation is excluded from this freeze.
No whole requirement or stage is accepted here.
