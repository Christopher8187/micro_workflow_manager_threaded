# Native previews and sampling progress

This increment follows `f7c11a097f087f5f985b8e73e7c9f95fa024635a` for
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).
The final 60-module repeat passed all 1,833 checks in 1,898.26 seconds, with no
failures, errors, or skips.

Run and resume plans, reset and recovery dry runs, and empty samples use a
native read-only snapshot before runtime initialization. They do not import
user code or create jobs, reservations, sessions, or durable project data.
Closed databases retain their original file set. Active WAL capture follows
the approved existing-SHM allowance and reads a consistent committed snapshot.
Recovery preview validates exact native session identity and ownership before
reporting possible actions. It does not apply recovery.

Sampling accepts counts, percentages, named component members, and status
filters. The printed seed and digests support guarded replay. Admission freezes
the current population, inputs, selected native job instances, and replay
metadata in the same writer decision as the session and reservation. A status
change that leaves a job outside the filter preserves the replay guard; the
later observation still supplies the complete starting-job baseline.

Sample execution shares exact selected-root preparation and recursive causal
execution. Unrelated work remains preserved. Successful full eligible starting coverage
can publish done; partial coverage or newer unprocessed work remains sampled.
Interrupted admission waits for the actual writer outcome before cleanup,
including notification failure after COMMIT.

Aligned sampled resume retains its established lineage while running. Failure
preserves successful history, and later repair retains a compatible sampled
origin. Resume checks conflicting history and predicted selected-parent
lineage before admission. Preparation repeats the history check before and
after file staging. Start records missing current history atomically;
settlement rechecks it for success and failure.

The latest focused repeat passed 210 checks without failures, errors, or skips.
The [sampling run history](../../../../testing_ground/issue-45/native-sampling-test-history-revision20.json)
retains earlier failures and their corrections. The
[resume review](../../../../testing_ground/issue-45/native-sampled-resume-source-review-closure-revision02.md)
closes the retained-history findings. Separate bounded reviews cover
[preview storage](../../../../testing_ground/issue-45/native-preview-runtime-source-review-closure.md),
[sampling admission](../../../../testing_ground/issue-45/native-sampling-admission-source-review-closure.md),
[filtered acquisition](../../../../testing_ground/issue-45/filtered-sample-acquisition-source-review-closure.md),
and [interrupted admission](../../../../testing_ground/issue-45/admission-interruption-drain-revision03-review-closure.md).

Acceptance requires the [documentation review](../../../../testing_ground/issue-45/native-preview-sampling-documentation-review.md),
[source verification](../../../../testing_ground/issue-45/native-preview-sampling-source-verification.json),
and [local commit record](../../../../testing_ground/issue-45/native-preview-sampling-commit-verification.json)
to bind the reviewed files and checks to the local commit. The run includes all 55 modules
from the earlier broad failure and all five added regression modules. Shared graph-command planning, interval execution, interrupts,
applied recovery, native cleanup, and final issue verification remain separate
unfinished work. This increment does not implement runtime component merging.
