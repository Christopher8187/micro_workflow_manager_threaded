# Managed job arrival progress

This increment follows `5db421a9530a059488852e57d7cc879edf71ddc5`
and extends the arrival portions of `44-MIS-001` through `44-MIS-019`.
It accepts no complete stage or release requirement.

## Implemented behavior

Auto-ID, explicit-ID, batch, default, and prepared-job creation share the component
flag and first-cause guard. Causes retain the receiving instance identity.
Execution ownership identifies task creators; project-created jobs have an unknown
producer. Task creation without a direct edge remains valid.

Current graph shape is checked before preparation or allocation and during
publication. Initial mounting without component definitions remains valid.
Reuse creates no arrival. Causes survive trace clearing, receiving-job deletion,
ID reuse, and reopening. Queued/running observations allow a later terminal
arrival in the same generation. Full preparation repairs alignment. The sampled
case uses a native stored-state fixture.

Explicit publication resolves keys and IDs before writing its input. Publishers
claim new job directories exclusively. Batch preparation cleans only directories
it created. Cleanup preserves committed inputs after notification failure,
drains queued decisions, and retains the original error if cleanup is uncertain.
Failed auto publication clears its cleanup reference after releasing a directory,
protecting a later prepared replacement.

## Verification

The [39-run history](../../../../testing_ground/issue-45/managed-job-test-history-revision06.json)
retains selections, source hashes, manifests, logs, JUnit counts, and all failures.
The first broad run passed 1,110 cases and failed six adjacent cases. Native
fixture corrections preserved their behavioral assertions and identities;
both affected modules then passed all 38 cases.

The final repeat passed all 1,116 cases across the same 30 complete modules in
1228.16 seconds, with zero failures, errors, skips, or exclusions.
It includes all 80 job-arrival cases and all six previously failing cases.
The corrected source is `sample-calculations-job-native-adjacent-01`,
351 Python files, freeze SHA-256
`423E89E0D960F658CE958E2EC3AB911A06F63736098900F54D755079C7FBBA6E`.

The [Standards review](../../../../testing_ground/issue-45/managed-job-arrivals-standards-review.md)
and [Spec review](../../../../testing_ground/issue-45/managed-job-arrivals-spec-review.md)
record bounded source PASS. Acceptance requires final documentation review,
the [source audit](../../../../testing_ground/issue-45/managed-job-arrivals-source-verification.json),
and the [local commit record](../../../../testing_ground/issue-45/managed-job-arrivals-commit-verification.json)
to match these files and results.

## Remaining work

Preparation causes, tracing, resume refusal, producer cleanup, membership repair,
crash recovery, and receipt retirement remain required within the same issue.
