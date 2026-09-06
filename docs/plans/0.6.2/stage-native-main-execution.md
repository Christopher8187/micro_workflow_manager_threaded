# Native ordinary execution

Status: in progress. No stage acceptance or requirement completion is claimed.

This group connects ordinary command orchestration to the accepted native
session, component reservation, and job-claim APIs. It depends on the pending
[native bootstrap work](stage-native-project-bootstrap.md). Both groups remain
unaccepted while their implementation and review continue.

The independent [production-session preparation](../../../../testing_ground/issue-45/native-production-session-integration-preparation.md)
was read in full. Its SHA-256 is
`36D2C0A24A98AF49C84C6FA050866E4AA2F0537DDDB59B23E85126BFA7E5F6C8`.
It identifies both claim paths, process-worker propagation, exact native
heartbeats, bound runtime limits, and cleanup after partial startup. No new
architecture decision was identified. Process workers now validate native
storage before graph import, rebuild static autostart relationships, compare
their full graph with the admitted graph, and then receive the parent's owner.

The first approved execution/persistence check runs two real tasks in one
Hoeflein component through ordinary orchestration. It observes the native main,
exact claim owners, held reservation during execution, persisted started events,
terminal session history, released scope, and absence of a singleton run file.

## Focused verification

Each run uses the sequential executable lane, runner revision 04, and an
immutable Test Area source copy. The prefix below is
`sample-calculations-native-main-`; executable results end in `-run`.
Complete logs, XML results, manifests, and source hashes remain in Parent Repo
`testing_ground/issue-45/`.

| Run | Result | Observation |
| --- | --- | --- |
| `ordinary-red-01` | 1 failed, 1.06 seconds | Ordinary single-job admission lacked explicit native ownership. |
| `ordinary-green-01` | 1 passed, 1.39 seconds | The native main reserves its component, owns real claims, finishes, and releases scope. |
| `batch-red-01` | 1 failed, 1 deselected, 1.24 seconds | The refreshable API source still omitted ownership. |
| `batch-green-01` | 2 passed, 2.55 seconds | Direct and API admission both forward the captured owner. |
| `process-red-01` | 1 failed, 2 deselected, 2.20 seconds | Reconstructed process workflows lacked the parent's ownership context. |
| `process-green-01` | 3 passed, 3.81 seconds | CLI process tasks now retain the parent-created session and full component. |
| `startup-preservation-01` | 5 passed, 6.17 seconds | Failure after reservation or runtime-limit binding leaves terminal history, releases scope, and allows retry. Pending limits survive a failure before binding. |
| `competing-preservation-01` | 7 passed, 9.90 seconds | Another process cannot begin a second main with overlapping or disjoint scope; its preparation never runs. |
| `heartbeat-preservation-01` | 1 passed, 7 deselected, 8.23 seconds | A real native heartbeat advances; a delayed heartbeat cannot mutate a terminal session. Bound limits remain effective, and workflow reuse receives a new owner. |
| `failure-preservation-01` | 1 passed, 8 deselected, 1.12 seconds | A handler failure retains claim history, records failed session history, and releases ownership. |
| `native-adjacent-01` | 129 passed, 40.58 seconds | All 66 identity, 54 bootstrap, and nine then-current ordinary-main cases pass together. |
| `error-repr-red-01` / `error-repr-green-01` | 1 failed, 9 deselected / 1 passed, 9 deselected | A broken exception representation previously recorded success. Safe error rendering now retains the original failure and failed outcome. |
| `create-response-red-01` / `create-response-green-01` | 1 failed, 10 deselected / 1 passed, 10 deselected | Reading the returned session record now occurs inside creation's transaction. A read failure rolls back instead of stranding a running main. |
| `terminal-retry-red-01` / `terminal-retry-green-01` | 1 failed, 11 deselected / 4 passed, 8 deselected | Retrying terminal publication retains the first outcome and failure details. |
| `reporter-stop-red-01` / `reporter-stop-green-01` | 2 failed, 12 deselected / 6 passed, 8 deselected | Reporter shutdown failures surface after durable session completion and reservation release. |
| `process-shape-red-01` / `process-shape-green-01` | 3 failed, 14 deselected / 4 passed, 13 deselected | Changed edges and changed autostarts now refuse before claims. The unchanged case also exposed missing worker autostart reconstruction; it now preserves the parent's component. |
| `teardown-red-01` / `teardown-green-01` | 4 failed, 17 deselected / 21 passed | Terminalization and ownership cleanup now hold the admission lock; missing scope is reported; stored selection follows scheduler order. The green run covers every current ordinary-main case. |
| `body-error-red-01` / `body-error-green-01` | 2 failed, 21 deselected / 5 passed, 18 deselected | Reporter shutdown no longer replaces an existing body failure. The primary exception retains the reporter failure as its cause. The heartbeat case now checks reservation release after both runs. |

The latest source freeze contains 309 Python files with SHA-256
`0F1A6C352801647A43A12C241A8F8188B65C1B2310FFCBFFC1CF2ABDDA2A5748`.
Its then-current 21 ordinary-main cases passed in 26.05 seconds. The later
body-error group raises the current count to 23 and passed its focused reporter
and heartbeat checks in 8.17 seconds, freeze SHA-256
`8C8E3019873F3A00989E0DF22DABBF8C76AEBB47B394976548BD77E4A5B16059`.
The combined native run subsequently passed all 66 identity, 78 bootstrap, and
23 ordinary-main cases in 52.71 seconds. It is recorded as
`sample-calculations-native-project-native-adjacent-02-run`, using the
`sample-calculations-native-project-creator-preservation-01` source freeze,
SHA-256 `137DE2C518D29D7D0B258D92C39C117ECB1031E42642E6AA3AD3B9C1D20E696F`.
Independent acceptance and wider runtime verification remain pending.

The current implementation uses one captured topology and immutable
node-to-component mapping. Session creation and complete-scope reservation occur
before command preparation. Both claim paths receive explicit ownership;
process initialization receives a serializable copy of the same mapping.
The supervisor writes exact SQLite heartbeats, and bound runtime limits read
the exact session. Ordinary orchestration no longer writes a singleton run file.
Other consumers of that old file remain pending conversion and removal.

The independent [initial source review](../../../../testing_ground/issue-45/native-main-initial-source-review.md)
was read in full, SHA-256
`E20F82D4D98B9E64C2C8D730318A788643F87BAE9391AD0B7E3D44A23D8E4686`.
Its creation-response, error-rendering, terminal retry, reporter teardown,
process graph, teardown race, component ordering, and missing release findings
have the focused observations above. These are root corrections awaiting
independent review. The ordering correction uses the actual quotient-DAG
scheduler order under the workflow lock. Sorting the captured components
lexicographically, as suggested in the review, would place some descendants
before their predecessors.

The [correction source review](../../../../testing_ground/issue-45/native-main-corrections-source-review.md)
was read in full, SHA-256
`DB4192D079598AD5FAE3C33053F2D6700575197644F56DF4C497A2DB281B1C0F`.
It found the preceding corrections coherent and identified the combined
body/reporter failure, NM-009. The later focused correction was checked by the
[bootstrap correction review](../../../../testing_ground/issue-45/native-bootstrap-corrections-source-review.md),
read in full at SHA-256
`66354B5AFFC0955E7282BC8FE9A51AA4660CF8D9BD444375D454EBD6205E8A6C`.
It found that correction coherent. A reservation-deletion exception can still leave terminal
history with a retained reservation; that is explicit evidence for the pending
recovery work, not silent successful cleanup.

The shared lifecycle now lives in `workflow/execution_session.py`, used by CLI
orchestration and the pending [programmatic execution group](stage-native-programmatic-execution.md).
Full component lifecycle publication and remaining old session readers still
need conversion. The singleton in-memory owner serves one ordinary main;
it does not yet represent concurrent interrupt execution.

Selected-job execution, sampling, interrupts, transfer/hold behavior, stale-owner
recovery, monitoring, restart and thread command ownership, and complete component
lifecycle integration remain separate pending work. The first ordinary slice
does not establish their correctness.
