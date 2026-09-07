# Producer footprint preparation progress

This increment follows `8d408f48418f4ad3cc161745d170e22ca80944ee` and adds
full component preparation and native resume preparation. Selected-job and
interval integration remain pending.

## Implemented behavior

Preparation reads immutable producing executions and exact job instances.
Optional parent metadata and trace events do not determine ownership.
Whole-selection preflight observes component state, downstream jobs, managed
inputs, and ownership before moving files. Selected receiver changes belong to
that receiver's preparation. Earlier committed components remain applied if a
later component fails. Other producers' material, project-owned input,
unattributed directories, and excluded receiver outputs remain preserved.

Receiver guards refuse conflicting reservations, publications, job creation,
and native state replacement. Idempotent reuse continues without publication.
Durable receipts distinguish preparation, commit, and restoration. Interrupted
callers drain the exact writer decision. Failed SQL restores staged files;
committed SQL remains applied after notification failure. Unknown outcomes
retain recovery material and guards.

Removed managed inputs and jobs mark established excluded receivers misaligned
once per receiving node and alignment generation. Causes retain the initiating
operation, immutable producer, exact affected material, and committed receipt.
Only full preparation realigns selected components.

## Native resume preparation

`resume` and `resumefrom` preflight the whole selection, external parents, and
job owners before admission. Misalignment refusals name appropriate repair
commands. Execution locks precede receiver locks and terminal-output reads.
Exact output recovery, failed-job requeue, component transitions, trace removal,
and receipt completion share one database decision. Recovery preserves timing
and terminal history. Failed or cancelled attempts require unchanged component
membership; retained failures may belong to an older alignment. Abandoned
running attempts also require the current producing shape and alignment.

## Verification

The [recorded history](../../../../testing_ground/issue-45/preparation-test-history-revision19.json)
retains 34 pytest runs, including failures and exact source hashes.
[Native fixture corrections](../../../../testing_ground/issue-45/native-fixture-corrections.md)
record preserved behavioral checks and two renamed tests. The final 44-module
repeat passed all 1,347 cases in 1,692.10 seconds, with no failures, errors,
skips, or deselection. This includes all 21 benchmark validation cases.
The [three-process measurement](../../../../test_area/mwf-062-issue45-20260904/preparation-repeated-api-green-02/result.json)
passed 4,320 job executions and its unchanged growth allowance.

Both used the 364-file source freeze
`9BB6E57BBA110C00443CCCC00C7AA1860A52C44A883014C5C291F9C8DE9E0DAD`.
Acceptance records are the independent
[Standards review](../../../../testing_ground/issue-45/producer-footprint-preparation-standards-review.md),
[Spec review](../../../../testing_ground/issue-45/producer-footprint-preparation-spec-review.md),
[source verification](../../../../testing_ground/issue-45/producer-footprint-preparation-source-verification.json),
and [commit verification](../../../../testing_ground/issue-45/producer-footprint-preparation-commit-verification.json).

## Remaining work

Selected-root ancestry, interval commands, membership repair, sampled resume,
readonly plans, interrupt execution, automatic crash recovery, receipt
retirement, and final issue-wide verification remain required.
