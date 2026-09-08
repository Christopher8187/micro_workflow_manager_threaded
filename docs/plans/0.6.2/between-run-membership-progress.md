# Between-run membership progress

This increment follows `baf5a82f9fe55f5e9da6d5a79c087eaeca1190e9` for
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).
The final broad run passed all 2,167 checks across 93 complete modules in
2,406.71 seconds, with no failures, errors, or skips. No failing case is excluded.

Native format 6 records each session's admitted graph shape and membership
revision. Active regional membership, retained successful results, and immutable
execution ownership have separate identities. An unchanged component can retain
results from an earlier graph shape. A new execution uses its admitted shape
without relabelling earlier owners or results. An unrelated regional revision
does not invalidate a continuing interrupt.

Full reset or fresh run expands preparation through intersecting historical
and current memberships when reusable work exists. The active mapping changes
only after all preparation units commit and their observations still match.
Earlier successful cleanup survives a later unit failure; retry validates it
again. Preparation can expand beyond execution selection and removes affected
historical producers' publications while preserving unrelated work.

Regions without reusable work reconcile automatically. Unowned jobs and input
remain intact. A retired component that returns starts queued above its
overlapping historical generations. Entirely removed components lose only their
active membership when no reusable work remains. Their state, definitions,
results, and ownership history remain available. Historical reads can inspect
inactive component states; execution-facing reads require active membership.

Preview and resume share a read-only reconciliation calculation. Changed reusable
membership refuses non-fresh execution before initialization. An overlapping
admitted session, reservation, hold, pending execution, receiver guard, or active
job prevents membership replacement. Regression snapshots include every existing
project file.

The checks also exposed two notification-listener shutdown bugs. Each listener
thread now retains its own socket, stop event, and subscriber record. Shutdown
retries transient record-deletion failures. Deterministic regressions cover
delayed thread startup and a Windows sharing failure.

The [run history](../../../../testing_ground/issue-45/between-run-membership-test-history-revision11.json)
retains twenty-six runs and every earlier failure. The focused run passed 75
checks across 13 complete modules. The first broad run had 2,090 passes and 77
failures. Corrected native fixtures preserve the tested scenarios and assertions.
The final broad run covers every earlier failing case, including recorded renames.

The [repeated API benchmark](../../../../testing_ground/issue-45/membership-benchmark-verification-revision01.json)
validated 4,320 executions across three fresh processes and five rounds per
process. Exact completion, output, ownership, event, runtime, cleanup, and
SQLite checks passed. Later measured rounds had a 1.775 timing ratio to earlier
rounds and passed the existing workload-growth allowance.

The [integrated source closure](../../../../testing_ground/issue-45/between-run-membership-integrated-source-review-closure.md)
resolves the earlier membership review conditions. The
[listener](../../../../testing_ground/issue-45/state-event-listener-retirement-source-review.md),
[core fixtures](../../../../testing_ground/issue-45/membership-core-root-fixture-independent-source-review.md),
[broad fixtures](../../../../testing_ground/issue-45/membership-root-broad-fixtures-independent-source-review.md),
[arrival fixtures](../../../../testing_ground/issue-45/membership-arrivals-fixture-root-source-review.md), and
[history fixtures](../../../../testing_ground/issue-45/membership-test130-strict-history-source-review-closure.md)
have bounded source acceptance. Earlier review failures remain recorded.

Acceptance requires the [documentation closure](../../../../testing_ground/issue-45/membership-progress-docs-source-review-closure.md),
[source verification](../../../../testing_ground/issue-45/membership-source-verification.json), and
[commit verification](../../../../testing_ground/issue-45/membership-commit-verification.json)
to match this tested source. Interrupt integration, applied recovery, clipboard
atomicity, remaining native cleanup, final documentation, and whole-issue
acceptance remain unfinished.
