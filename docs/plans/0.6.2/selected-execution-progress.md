# Selected execution progress

This work follows `979e26462e8a3768622f5461c9980d6e36e145b5` for
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).
Verification is complete; acceptance requires the source and commit records below.

Selected preparation follows immutable execution ancestry from the exact
admitted root instances. It removes their earlier same-component descendants
and producer-owned material while preserving unrelated jobs, other producers,
and user-owned files. Ordinary parent readiness applies before admission and
again before execution. Selected preparation does not realign the component.

Execution retains the supplied root jobs and their Python return values. It
joins each finite set of admitted work before discovering newly created causal
children, then repeats until that invocation has no unfinished causal work.
Waiting checks use the same selected set. Quotient publications remain allowed;
their receivers do not enter execution. CLI selected runs permit publication
throughout the admitted component.

The session driver retains accepted nested repairs and rechecks the original
selected operation after those repairs finish. A pending full-component repair
completes its own remainder first. Rechecking does not replay a completed root
or replace its return value with a child's value.

Component lifecycle now begins through a jobs-kind pending execution and ends
through the existing session-exit writer. Successful partial coverage becomes
sampled; complete cumulative coverage becomes done. An internal successful
result record preserves sampled or done history across failure and repair.
Current lifecycle remains authoritative in `component_states`. Selected work
preserves misalignment and alignment generation. Compatible parent lineage
determines successful stability, including the exact interrupt origin.

Active programmatic restart now uses the shared owned-restart writer. Generic
restart does the same for live owned work, and rechecks manual requests under
the execution fence. A request that encounters newly active work refuses
without changing its execution, events, output, or generation.

The [run history](../../../../testing_ground/issue-45/selected-preparation-test-history-revision19.json)
retains all twenty-seven runs and their failures. The complete 45-module repeat
passed 1,638 cases. The final test-only addition passed its full module and the
module-size check, 64 cases. These cover 1,639 distinct cases without exclusions.
The [final freeze](../../../../testing_ground/issue-45/sample-calculations-selected-lifecycle-settlement-final-01-freeze.json)
contains the same runtime as the full repeat and the added settlement regression.
The [repeated-use benchmark](../../../../test_area/mwf-062-issue45-20260904/selected-execution-repeated-api-01/result.json)
passed all 4,320 executions across three fresh processes, including durable
results, cleanup, and the existing growth allowance.

The [standards review](../../../../testing_ground/issue-45/selected-execution-bounded-standards-review.md)
and its [settlement closure](../../../../testing_ground/issue-45/selected-execution-settlement-review-closure.md)
found no remaining blocker. The
[restart review](../../../../testing_ground/issue-45/selected-lifecycle-abandoned-restart-review.md)
returned bounded PASS. The original missing-job exception and native
abandoned-owner recovery have passing regressions. The
[source verification](../../../../testing_ground/issue-45/selected-execution-source-verification-revision02.json)
and [commit verification](../../../../testing_ground/issue-45/selected-execution-commit-verification.json)
must bind these checks and the local commit before this increment is accepted.

Random sampling commands and durable replay metadata, interval integration,
sampled resume, interrupt execution, read-only plans, recovery, and final
issue-wide verification remain unfinished. This increment does not settle
dynamic component merging.
