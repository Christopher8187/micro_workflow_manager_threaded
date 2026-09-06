# Verified native integration progress

This local progress boundary includes native project admission and reopening,
stable job identity, ordinary and programmatic main sessions for known graph
shapes, session monitoring, and current-owner inspection. It does not complete
the wider 0.6.2 implementation or its individual requirement groups.

The [independent boundary audit](../../../../testing_ground/issue-45/native-integration-commit-boundary-audit.md)
found no blocking correctness issue within these claims. The accepted source is
the immutable `sample-calculations-native-job-inspection-green-04` freeze,
SHA-256 `ABE9973DC97C22F8FAAC12CCCD5D6542079E79131622A227B5C6486C3F20BB47`.
Its [exact-source run](../../../../testing_ground/issue-45/sample-calculations-native-job-inspection-adjacent-01-run-manifest.json)
passed 389 checks in 182.24 seconds. Nine cancelled version-four cases were
deselected. Existing source reviews cover the included corrections; no new
performance gate applies to this commit.

The detailed records are [native identity](stage-job-instance-identity.md),
[project admission](stage-native-project-bootstrap.md),
[ordinary sessions](stage-native-main-execution.md),
[programmatic sessions](stage-native-programmatic-execution.md),
[monitoring](stage-native-session-monitoring.md),
[current owners](stage-native-current-job-owner.md), and
[inspection](stage-native-job-inspection.md). Their larger stages remain open.

The commit uses those frozen executable versions. Later restart continuation,
session-exit arbitration, component and DAG continuation, API abandonment, and
terminal cleanup remain in the working files for separate correction and
acceptance. The unfinished strict-preview test file is also excluded.

Strict read-only previews, complete clipboard transactions, recovery, component
lifecycle integration, thread controls, interrupts, sampling, and Q10 remain
unfinished. The separately found duplicate-terminal-event defect is recorded
in its [review](../../../../testing_ground/issue-45/native-terminal-owner-complementary-review.md).
It changes none of this commit's owner-inspection claims. No push or publication
is included.
