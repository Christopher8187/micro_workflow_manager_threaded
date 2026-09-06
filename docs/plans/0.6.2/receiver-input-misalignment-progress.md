# Receiver input misalignment progress

This increment follows local commit `ef20f3bb0b2435976c0a2803cb96bc7179d0f1ec`.
It implements managed-file arrivals under `44-MIS-001` through `44-MIS-003`,
`44-MIS-005`, and the arrival portions of `44-MIS-010` through `44-MIS-019`.

## Implemented behavior

Managed input changes mark done, sampled, or failed receiver components misaligned.
Lifecycle, stability, origin, generation, jobs, and output remain unchanged.
Queued and running receivers stay aligned. Manual edits and unchanged missing-file
deletes create no cause. Deleting retained ownership counts even without bytes.

Ownership, events, receiver state, first cause, and receipt share one database
transaction. Synchronous compensation restores files after failure. Each receiving
raw node gets one first cause per alignment generation. Distinct receivers in one
component retain separate causes. The first actual changed path includes any
allocated suffix. Repeated arrivals preserve the first producer and path.

`read_component_misalignment_causes()` returns current-generation causes ordered
by receiver from one validated snapshot. It checks component, shape, producer,
edge, path, generation, and flag agreement. Trace clearing preserves causes.
Full preparation advances generation, hiding earlier causes without deleting them.

The positive cache includes receiver, process, shape, membership, and generation.
Only confirmed publication commits populate it. Queued and running observations
create no negative cache. Cold storage validates the durable first cause.

## Verification

Immutable Test Area results have matching manifests, logs, and source records.
Every run has zero errors and skips. Test 097 now
expects generation 1 after full preparation and 0 for selected jobs. Its other
owner, shape, session, and path assertions remain unchanged. The earlier wrapper
correction checks both `JobFailedError` and its original `ValueError`.

| Run | Result | JUnit time | Scope or finding |
| --- | --- | --- | --- |
| [arrivals RED](../../../../testing_ground/issue-45/sample-calculations-receiver-arrivals-red-01-run.xml) | 0 passed, 1 failed | 2.075 s | Missing receiver flag |
| [arrivals GREEN](../../../../testing_ground/issue-45/sample-calculations-receiver-arrivals-green-01-run.xml) | 52 passed | 48.663 s | Flag and surrounding input behavior |
| [causes RED](../../../../testing_ground/issue-45/sample-calculations-receiver-causes-red-01-run.xml) | 1 passed, 1 failed | 4.682 s | Missing required reader |
| [causes GREEN](../../../../testing_ground/issue-45/sample-calculations-receiver-causes-green-01-run.xml) | 121 passed | 71.503 s | First-cause and native storage |
| [boundaries 01](../../../../testing_ground/issue-45/sample-calculations-receiver-causes-boundaries-01-run.xml) | 9 passed, 1 failed | 24.492 s | Inner-error text expected from outer wrapper |
| [boundaries 02](../../../../testing_ground/issue-45/sample-calculations-receiver-causes-boundaries-02-run.xml) | 19 passed | 47.281 s | Terminal, repair, rollback, damage |
| [cache RED](../../../../testing_ground/issue-45/sample-calculations-receiver-causes-latch-red-01-run.xml) | 2 passed, 1 failed | 9.025 s | Repeated insertion failed; process races passed |
| [cache GREEN](../../../../testing_ground/issue-45/sample-calculations-receiver-causes-latch-green-01-run.xml) | 73 passed | 102.760 s | Committed-result cache and input checks |
| [final receiver cases](../../../../testing_ground/issue-45/sample-calculations-receiver-causes-final-boundaries-01-run.xml) | 27 passed | 65.246 s | Same-session changes, retry, deletes |
| [combined 01](../../../../testing_ground/issue-45/sample-calculations-receiver-causes-combined-01-run.xml) | 1029 passed, 3 failed | 1181.331 s | Stale full-preparation expectation in test 097 |
| [corrected identity checks](../../../../testing_ground/issue-45/sample-calculations-receiver-causes-final-02-boundary-run.xml) | 54 passed | 34.087 s | Full preparation uses generation 1 |
| [combined 02](../../../../testing_ground/issue-45/sample-calculations-receiver-causes-combined-02-run.xml) | 1032 passed | 1198.139 s | Same 28 complete modules |

Final source: `sample-calculations-receiver-causes-final-02`, 350 Python files.
Freeze SHA-256: `C0C18DD1F83ADE4AAB55D0808C0B71099A1B5882842B1FBB6F9BA5CCEA8B1778`.
All 1,032 checks passed; zero failures, errors, or skips.

## Review and remaining work

The [Standards review](../../../../testing_ground/issue-45/receiver-input-misalignment-standards-review.md)
and [Spec review](../../../../testing_ground/issue-45/receiver-input-misalignment-spec-review.md)
returned bounded PASS. Acceptance requires the [source audit](../../../../testing_ground/issue-45/receiver-input-misalignment-source-verification.json)
and [local commit record](../../../../testing_ground/issue-45/receiver-input-misalignment-commit-verification.json)
to match the reviewed files and checks.

Sampled and same-session state fixtures do not establish public sampling or
interrupt scheduling. Job-arrival and preparation causes, tracing, resume refusal,
producer cleanup, excluded-receiver guards, membership repair, crash recovery,
and receipt retirement remain required. This increment completes no whole stage
or release requirement.
