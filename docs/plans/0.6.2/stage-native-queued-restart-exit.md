# Native queued-node and selected CLI continuation

Status: partial implementation, bounded correction review passed. This extends
[finite session exit](stage-native-restart-session-exit.md) to public
`run_queued_node_jobs`, single-node acyclic `run_node`, and explicit or sampled
CLI job selections. Component/DAG, API/live, and nested-task continuation remain
unfinished.

## Behavior

The finite queued operation keeps its source and prefetched payloads alive
through the session's restart decision. Completed jobs retain their actual
Python values and order. Accepted successors use exact generation and owner
checks. The source closes after continuation ends.

The CLI passes its existing session driver into selected execution. Selection,
reset, payload loading, and final reporting remain outside continuation.
Real subprocess tests preserve peer output/events and excluded jobs. The sample
case starts with two queued jobs and one completed job, then samples both queued
jobs with a fixed seed.

The [queued-call review](../../../../testing_ground/issue-45/native-queued-session-exit-review.md)
found that runner/source preparation could leave a raw running node after its
session failed. Preparation now enters the existing protected block, including
source cleanup. The catch handles `BaseException`, preserving an interruption
and the unclaimed job while failing the node and session. The
[final correction review](../../../../testing_ground/issue-45/native-queued-session-exit-final-correction-review.md)
passes this boundary; partial-constructor and compound-cleanup failures remain
unverified.

## Verification

Root uses immutable Test Area copies, freezer revision 18, and runner revision
04. Parent Repo record prefix is `sample-calculations-native-owned-restart-`.

| Run | Result |
| --- | --- |
| `queued-session-exit-red-01` / `green-01` | 8 failed, 4 passed, 15.98 s / 24 passed, 34.58 s |
| `queued-prefetch-01` | 130 jobs across prefetch batches, 1 passed, 13.19 s |
| `queued-session-adjacent-01` | 180 passed, 1 heartbeat comparison failed, 212.49 s |
| `cli-selected-red-01` / `green-01` | 1 failed, 3.96 s / CLI and heartbeat correction passed, 9.05 s |
| `cli-sample-red-01` / `green-01` | 1 failed, 4.51 s / both CLI selections passed, 7.67 s |
| `queued-prepare-red-01` / `green-01` | 2 failed, 1.39 s / preparation and CLI cases passed, 11.14 s |
| `queued-session-adjacent-02` | Configuration-correction source, 185 passed, 197.15 s |
| `queued-interrupt-red-01` / `green-01` | 2 failed, 2 passed, 2.19 s / 30 passed, 50.90 s |

The heartbeat failure compared second-resolution timestamps with strict `>`.
The corrected test observes the held write's actual ordering and confirms a
late heartbeat cannot change the terminal session. Runtime timing and thresholds
were unchanged. The wider result precedes the final one-line interruption fix.

Reviewed final freeze SHA-256:
`19310C9A38670A0D28721B3F63BBE65908A60633B89CD063B15165C767B2D3A4`.
No whole restart requirement is accepted.
