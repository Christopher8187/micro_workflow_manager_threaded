# Nine graph commands progress

This increment follows `7540bf347687d36dabb385c3299867a6f451be2b` for
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).
The final combined run passed all 2,017 checks across 78 complete modules in
2,145.24 seconds, without failures, errors, skips, or deselections.
The focused combined run passed 217 checks across 17 complete modules in
267.11 seconds. Both used the same immutable source.

One shared selector supplies all nine graph commands. `runbetween`,
`resumebetween`, and `resetbetween` select the half-open quotient interval,
including the start component and excluding the strict descendant end
component. Admission stores the selected order, preparation rechecks it, and
the scheduler executes that order. Publication can reach excluded receivers
without executing them.

Full graph previews run before mutable initialization. A narrow native reader
observes component lifecycle, raw-node job counts, exact owners, preparation
effects, and excluded receiver guards from the same read-only snapshot. Busy
state appears as a refusal reason; damaged stored links fail. Plans retain the
exact command, refusal scope, trace behavior, and interval repair guidance.
Explicit-job preview cleanup remains separate work.

Fresh commands check the start component's external parent results before
admission and compare their exact lineage and generation after reservation,
before preparation. A wide selection still prepares its interior components
when a separate external parent prevents an interior merge from executing.
Applied reset checks every native running session before loading project code,
including abandoned rows that require recovery.

The [run history](../../../../testing_ground/issue-45/nine-command-test-history-revision15.json)
retains fifteen runs and all earlier failures. The first broad run reported
2,008 passes and nine failures in WAL display and monitoring fixtures.
Corrected fixtures establish real native component results and observe exact
SQLite sessions. Their complete modules pass in both final combined runs;
every earlier test identity is included in the final 78-module selection.

The [repeated API benchmark](../../../../testing_ground/issue-45/nine-command-benchmark-verification-revision02.json)
validated 4,320 executions across three processes. Outputs, runtime records,
queue state, cleanup, and SQLite integrity all passed. Its measured growth ratio
was 1.82289, within the recorded threshold.

Bounded independent reviews returned PASS for the
[assembled runtime](../../../../testing_ground/issue-45/nine-command-runtime-revision03-source-review.md),
[command wiring](../../../../testing_ground/issue-45/nine-command-wiring-revision04-source-review.md),
[adjacent fixtures](../../../../testing_ground/issue-45/nine-command-adjacent-fixture-source-review.md),
[WAL fixtures](../../../../testing_ground/issue-45/nine-command-test119-native-display-source-review.md),
and [monitoring fixtures](../../../../testing_ground/issue-45/nine-command-monitor-fixture-independent-source-review.md).
Acceptance requires the [documentation review](../../../../testing_ground/issue-45/nine-command-progress-documentation-review.md),
[source verification](../../../../testing_ground/issue-45/nine-command-source-verification.json),
and [local commit verification](../../../../testing_ground/issue-45/nine-command-commit-verification.json)
to bind the reviewed files and completed checks to the local commit.

Between-run membership repair, interrupts, applied recovery, clipboard
atomicity, remaining native cleanup, and final issue-wide verification remain
unfinished. This increment does not implement runtime component merging and
does not complete any whole requirement row.
