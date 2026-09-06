# Verified native restart continuation

This progress boundary extends [native session integration](native-foundation-progress.md)
with exact-owner restart storage and CLI controls, finite execution, atomic
session-exit decisions, fixed-membership component and DAG continuation, and
API cleanup. Accepted replacements retain their session and job incarnation.
Completed peers retain their results; unrelated infrastructure errors keep
ordinary admission stopped while accepted replacements finish.

The immutable source is `sample-calculations-native-grouped-terminal-green-01`,
SHA-256 `266A2C406886BE873781122884297838D07BC2F2C3A3B3669EBCF3DB51F10697`.
Its [commit selection](../../../../testing_ground/issue-45/native-restart-progress-commit-candidates.json)
contains 17 runtime files and nine test modules. The unfinished strict-preview
test is excluded. The [boundary audit](../../../../testing_ground/issue-45/native-restart-commit-boundary-audit.md)
and the linked section reviews define the accepted partial behavior.

Detailed results appear in [owned restart](stage-native-owned-restart.md),
[finite admission](stage-native-finite-restart-admission.md),
[component continuation](stage-native-component-continuation.md),
[DAG continuation](stage-native-dag-continuation.md),
[API release](native-api-release-corrections.md),
[finite output failures](native-finite-output-restarts.md),
[terminal ownership](native-terminal-cleanup-ownership.md), and
[grouped terminal results](native-grouped-terminal-results.md).

The finite correction passed 168 surrounding checks. The preceding owner
correction passed 310. The final grouped correction passed 15 focused checks.
The exact-source surrounding selection passed all 330 checks in 422.92 seconds,
including every changed test module. Its correction reviews passed. This is
accepted local progress within the boundary above.

Generic recovery and native damage repair, lost-process and arbitrary-exception
transport, dynamic membership under Q10, full interrupts, lineage, component
lifecycle authority, readiness, sampling, thread controls, clipboard rollback,
and strict read-only previews remain unfinished. They receive no completion
credit from this partial result. No push or release is included.
