# Native programmatic execution

Status: in progress. No stage acceptance or requirement completion is claimed.

This group connects programmatic execution to the shared native main-session
lifecycle used by [ordinary commands](stage-native-main-execution.md).
The [independent preparation](../../../../testing_ground/issue-45/native-programmatic-session-integration-preparation.md)
was read in full, including its corrected implementation order. Its SHA-256 is
`1D884E1B7BA46CE716CAC7033D5C73EE7ECC7ECD7C0E29B043E46C912822D9C1`.

The shared lifecycle now lives in `workflow/execution_session.py`.
`cli/run_session.py` imports that implementation. Independent public calls hold
a per-workflow entry lock through admission, execution, and cleanup. CLI run
controls change and restore inside that guard. Each outer call owns one main;
private scheduler methods, runner closures, and process workers receive its
explicit immutable token. Missing or foreign tokens refuse before scheduler
status changes. Cleanup removes the exact token it installed.

Task-side reuse checks the current job lease, stored owner, current reservation,
and each requested component's reservation. It validates selected job IDs before
dispatch. Known-component autostarts preserve queued child creation and parent
events. Top-level batch autostart uses one session. Native claims now govern
API preclaims and restartable handler execution; the separate
`active_job_restart_enabled` mode and `_run_job_unfenced` path are removed.

## Focused observations

All runs use the sequential executable lane, immutable Test Area copies, runner
revision 04, and freezer revision 10. The prefix below is
`sample-calculations-native-programmatic-`; executable records end in `-run`.
Logs, JUnit records, commands, and per-file hashes remain in Parent Repo
`testing_ground/issue-45/`.

| Run | Result | Observation |
| --- | --- | --- |
| `single-red-01` / `single-green-01` | 3 failed / 3 passed | Direct job, run-one, and immediate start obtain native ownership. |
| `schedulers-red-01` / `schedulers-green-01` | 15 failed / 18 passed | Node, component, whole-workflow, and selected-list calls share one main. |
| `main-adjacent-01` | 23 passed | Existing ordinary-main behavior survives the shared implementation. |
| `entry-red-01` / `entry-red-02` | 4 failed / 3 failed | The first reproduced stale-token restoration. Refined list fixtures expose wrong-node execution, missing-job session creation, and duplicate selection refusal through SQLite. |
| `entry-green-01` | 22 passed | Entry serialization, exact cleanup, and pre-admission selection checks. |
| `batch-restart-red-01` / `batch-restart-green-01` | 3 failed / 25 passed | One main per batch; a replacement handler progresses before the stale handler exits, whose late mutation is refused. |
| `process-red-01` / `process-green-01` | 1 failed, 6 passed / 7 passed | Known-component child work, disjoint components, and programmatic process ownership; missing worker token refuses before graph import. |
| `review-red-01` / `review-red-02` | 4 failed / 2 failed | Nested selection and reservation defects reproduced. Two initial CLI fixture errors were corrected before confirming transient control changes. |
| `selection-builder-red-01` | 1 failed | An early list copy lost IDs selected by the admission callback. |
| `review-green-01` | 37 passed | Reviewed defects corrected; the callback's exact ordered IDs are retained. |
| `token-red-01` / `token-green-01` | 4 failed, 6 passed / 10 passed | Missing/foreign private tokens refuse before status changes. Startup failures preserve single/batch creation and permit a new owner on retry. |

The latest focused source contains 312 Python files. Its freeze SHA-256 is
`E7A957CB8D62B8CC3A83ED50CDF84302E40283E4A0F683BE8CB9CE9FB24D6281`.
Its ten selected token/failure cases passed in 8.31 seconds. The previous full
37-case programmatic run passed in 36.43 seconds. Combined current-source
verification then passed all 214 checks in 97.66 seconds: 66 identity,
78 bootstrap, 23 ordinary-main, and 47 programmatic cases. This is
`sample-calculations-native-programmatic-native-adjacent-01-run` on the same
token-green source freeze. It also verifies the refined bootstrap assertions
for removal of the owned database beside a preserved foreign companion.

## Review and remaining work

The [first independent entry review](../../../../testing_ground/issue-45/native-programmatic-entry-source-review.md)
was read in full, SHA-256
`563E78CAF724055289FEBBFEF06204E7D5C8EAD4C33698CE4B729513D3BF0403`.
Its three defects now have the focused observations above. Duplicated session
setup has also been removed. Root read both subsequent reports in full.
The [correction review](../../../../testing_ground/issue-45/native-programmatic-entry-correction-review.md),
SHA-256 `F5A705E913DE7C3C2FE536F44A8821A02590AE45245DA4839E21D66424705804`,
passes the assigned corrections. The
[complementary review](../../../../testing_ground/issue-45/native-programmatic-complementary-source-review.md),
SHA-256 `F3EA48B71F9BDC307308E08F4D1FC49517FF44096CAA2EB35D2182FCD9D5C9A7`,
finds no source blocker in the assigned known-shape execution mechanisms and
reconciles the completed 214-case run against all 312 frozen source paths.
Both reviewers used GPT-5.6 Sol with xhigh reasoning and performed no executable
work. Neither report accepts the whole stage or completes a requirement.

The reports retain nonblocking coverage limits for API batch-preclaim route
sensitivity, active process-worker replacement, invalid private node/list
tokens, and preservation of sentinel CLI controls on invalid refusal arguments.
These are evidence gaps, not demonstrated source defects.

[Q10](architectural-questions.md#q10-dynamic-component-discovery-during-admitted-execution)
awaits Christopher's answer. A newly discovered relationship that merges
components during active execution receives no behavior or acceptance credit
here. Existing decisions remain settled; declaration-only restrictions and
live ownership relabeling have not been inferred.

Full component lifecycle publication, producer-aware persistence, causal
selection, sampling, interrupts, transfers, holds, recovery, ownership-dependent
commands, and obsolete runtime/test removal remain required. The successful
low-level restart checks do not establish command-level restart ownership.
The migration guide remains an end-of-work deliverable.
