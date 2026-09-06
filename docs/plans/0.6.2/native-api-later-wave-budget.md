# Native API allocation across workflow waves

Status: all fourteen surrounding cases and the [bounded review](../../../../testing_ground/issue-45/native-api-later-wave-pump-budget-source-review.md) passed.
This extends [mixed runner verification](native-mixed-runner-continuation.md).

The broader selection passed 294 cases and failed one obsolete dummy-scheduler
fixture before native session admission. Its replacement uses a real workflow,
real API runners, and native storage. Events establish that C starts after A
finishes while independent B remains active.

The test retains both original 21-controller allocation assertions with each
node requesting 1,400 execution slots. A delegating observer records scheduler
allocations. This checks supplied budgets, not actual operating-system thread
counts or the separate aggregate API execution limit. It also checks completed
jobs, exact session ownership, cleared claims, and released reservations.

`sample-calculations-native-api-later-wave-native-01-run` passed all fourteen
cases in 11.80 seconds. The source freeze SHA-256 is
`E0EB0F5828E2B4A7287B3C7A4228CA053EB80A8CEC4419B6881578D27AC58F57`.
The later native selection passed 310 surrounding checks, including this fixture.
