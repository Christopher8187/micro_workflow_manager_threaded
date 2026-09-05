# Private sampled-component resume

Status: accepted within the private sampled-to-running storage boundary.
Both independent Sol xhigh source reviews and the bounded acceptance review
pass. The corrected source has 346 focused and surrounding passes, 985 ordinary
passes with one retained nonblocking timing failure, four cyclic passes, and
the selected stress pass. The ordinary suite did not pass as a whole. Public
integration and whole-issue acceptance remain pending. The preceding
[private component-state storage](stage-component-state-storage.md) section
is committed locally as `ddf3c66dbfcdbfbe33a6a593799a772358d2833c`.

This section implements the approved sampled-to-running transition from the
final component and sampling requirements and approved AQ5. An aligned sampled
component retains its compatible stability and exact instability origin when
it resumes. Running remains incomplete and cannot satisfy downstream readiness.
The requirement portions are 44-CMP-004, 005, 007, 009, 010, and 025,
44-SMP-054 and 056, and 44-REC-021 and 026. This private transition alone will
not complete those requirements.

The [source-only preparation](../../../../testing_ground/issue-45/component-state-transitions-preparation.md)
specifies the existing storage dependencies and later public integration.
The method is `begin_sampled_component_resume(session_id, component,
expected_alignment_generation=...)`, with the generation supplied by keyword.
It must validate the component state, running acting session, selected scope,
exact reservation owner, alignment, and expected generation in one writer
transaction. A successful call changes only lifecycle to `running`. A repeat
for already-running work returns `False` after the same ownership, alignment,
and generation checks. Other invalid input states refuse without mutation.

The first test in `tests/test_076_component_state_transitions.py` uses an
unstable sampled result from a finished interrupt session and a distinct main
session that owns the resumed component. It requires exact lineage, producing
shape, and generation preservation across reopen. It also compares the other
component, session and ownership rows, job and raw-node records, events,
metadata, and established output. It seeds only the sampled result because
no completion transition currently creates that result.

The [first test review](../../../../testing_ground/issue-45/component-state-transition-first-red-review.md)
found an incorrect literal component key in the seed before any execution.
The test now uses the established encoder, requires one changed seed row,
checks the two session roles and reservation, and compares raw component-state
rows as well as their reader output. The earlier test version was never run
and receives no RED credit.

## Test-first results

The test-first and surrounding runs use the isolated runner
`testing_ground/issue-45/run_sample_calculations_check_revision04.py`.
Each name starts with `sample-calculations-sampled-resume-` and ends with
`-run`. Each uses its corresponding uniquely frozen source and disposable
SQLite projects.

| Run suffix | Result | Behavior |
| --- | --- | --- |
| `red-01` | 1 failed, 1.34 seconds | Setup succeeds and the absent transition method causes the expected failure. |
| `green-01` | 1 passed, 1.40 seconds | The initial update preserves established interrupt lineage and every other stored field across reopen. |
| `ownership-red-01` | 5 failed, 1 deselected, 3.54 seconds | Unknown or terminal actors, absent or foreign reservations, and missing selected scope were accepted. |
| `ownership-green-01` | 6 passed, 5.54 seconds | Ownership is checked inside the writer transaction before mutation. |
| `state-red-01` | 5 failed, 6 deselected, 4.76 seconds | Stale generation was accepted, invalid lifecycles returned a no-change result, and misalignment reached a SQLite constraint error instead of explicit pre-write refusal. |
| `state-green-01` | 11 passed, 8.23 seconds | The transition explicitly requires the expected generation and aligned sampled state. |
| `generation-red-01` | 6 failed, 11 deselected, 4.06 seconds | Boolean and floating-point generations were accepted; string, null, and negative values received the wrong refusal. |
| `generation-green-01` | 17 passed, 11.44 seconds | Generation arguments must be nonnegative exact integers. |
| `damage-red-01` | 7 failed, 17 deselected, 5.37 seconds | Six damaged records were accepted, including missing lineage, definition, and shape; a missing state received the generic lifecycle refusal. |
| `damage-green-01` | 24 passed, 11.62 seconds | Full stored state, producing-shape presence, and interrupt-origin validity are checked inside the writer transaction. |
| `rollback-red-01` | 1 failed, 1 passed, 24 deselected, 1.44 seconds | An aborted update already rolled back; a suppressed update incorrectly returned no change and committed a trigger side effect. |
| `rollback-green-01` | 26 passed, 12.48 seconds | An unexpected zero-row update refuses and rolls back; a later clean retry succeeds. |
| `preservation-01` | 34 passed, 14.57 seconds | Stable and live-interrupt lineage, exact padded origin, repeat checks, transaction-time owner/generation changes, concurrent calls, and version-4 preservation pass without further implementation changes. |

The 301-file preservation source is
`sample-calculations-sampled-resume-preservation-01`, freeze SHA-256
`C1651220FE4EA63E26B81B9B7D83BB66087234E80F4ABBBB8259DB23F76C8B5D`.
Its component-state module has SHA-256
`DD5A055008892274A5980B292515F6042DD5D9042FA6A7DA71C6B6DC8A97A95C`.
Its transition test module has SHA-256
`37C13D74A4054C6D4EBAC172F9E94640B7892F977A026C0721004B36B72AD19A`.
All completed native commands exited and drained before the next workload.

The waiting-call tests pause entry to the real write transaction while a
second storage object commits session completion, reservation transfer, or a
generation change. The delayed call must refuse and preserve the new state.
Six simultaneous calls produce exactly one successful transition and five
no-change results. Version-4 refusal compares the complete SQL dump and
established output before and after the call.

Two disposable negative-control sources change only `component_states.py`
relative to the 301-file preservation freeze. They leave Direct MWF unchanged.
`prepare_sampled_resume_negative_controls.py` records and verifies that boundary.

| Negative-control run suffix | Expected failure result | Fault detected |
| --- | --- | --- |
| `negative-prechecked-owner-01` | 2 failed, 1 passed, 31 deselected, 8.40 seconds | Moving session and reservation checks before transaction entry lets terminal and transferred owners resume. The generation case still refuses. |
| `negative-cleared-lineage-01` | 1 failed, 33 deselected, 6.78 seconds | Clearing lineage loses the exact established interrupt origin and unstable result. |

Their freeze hashes are
`D21A371A30E0BBEB3770920A2F031742D5E83796035BACEB98863208374DC6C3`
and `3D02FC05B6B5AAC3C141C617E6BE903DEB12B50CFE8CE9C817B5304F030F8DD5`,
respectively. These intentional failures establish test sensitivity and do
not describe failures of the working implementation.

The source reviewers examined state and ownership safety, compatibility,
requirement scope, test sensitivity, and the final update predicate.
This partial implementation remains unaccepted and has no production caller.

## Review refinement

Both independent reviewers confirmed that the real `BEGIN IMMEDIATE`
transaction prevents supported writers from changing lineage between the
validation and update. The compatibility reviewer requested final SQL
predicates matching observed stability and exact origin to follow the
transition preparation. The safety reviewer classified the same change as
defensive hardening. Neither found an observable interleaving failure in
the existing implementation.

The final update now includes `stability IS ?` and `instability_origin IS ?`,
bound from the validated row. This preserves null-origin matching for stable
results. It changes no intended behavior, and no artificial failing case is
claimed for this review-stage refinement. The earlier lineage and transaction
negative controls establish the observable guarantees. The class description
now says it persists private lifecycle records.

The resulting 301-file source is
`sample-calculations-sampled-resume-review-01`, freeze SHA-256
`FB3D4845B46B2330DB590094AE85138B7964EEE04CC349CAB8FE633D7C80CEF6`.
Its component-state module has SHA-256
`B0CD929697B586BF7891C61D7BA05394D5271737DEB37BC897CF6F156F5C9E82`.
The transition tests retain SHA-256
`37C13D74A4054C6D4EBAC172F9E94640B7892F977A026C0721004B36B72AD19A`.
The [safety review](../../../../testing_ground/issue-45/sampled-resume-safety-review.md)
has SHA-256
`00610B96B41739EDB1589F5276166BA2A199F71869773B0CF8F26F7F8BAABDA7`.
The [compatibility review](../../../../testing_ground/issue-45/sampled-resume-compatibility-review.md)
has SHA-256
`3B61B8D4BF74253291705E7C947B2D6F4A79CB428D1E506CD5CD8F4CA1E83258`.
Both give source PASS for the corrected private boundary and leave stage
acceptance pending required checks. Their initial versions remain alongside
them. Root required corrections to the safety report's stage-versus-final
review wording and to an incomplete compatibility passage and an overly broad
table-snapshot claim. Neither correction changed implementation findings.

## Corrected-source verification

Run names use the `sample-calculations-sampled-resume-` prefix and `-run`
suffix. Each manifest must match all 301 reviewed source hashes.

| Run suffix | Result | Scope |
| --- | --- | --- |
| `adjacent-01` | 346 passed, 267.81 seconds | All 34 transition cases plus state storage, topology, session, reservation, claims, and readiness regressions. |
| `ordinary-01` | 985 passed, 1 failed, 1 marked stress case deselected, 1,258.80 seconds | The failure is the existing high-concurrency terminal-update timing assertion described below. The ordinary suite did not pass. |
| `cycle-01-1` | 1 passed, 8.71 seconds | Self and mutual autostart cycles before downstream work, in a fresh process. |
| `cycle-01-2` | 1 passed, 59.17 seconds | Threaded diamond cycle with 100 seed jobs, in a fresh process. |
| `cycle-01-3` | 1 passed, 47.05 seconds | Threaded ring cycle with 100 seed jobs, in a fresh process. |
| `cycle-01-4` | 1 passed, 13.45 seconds | Stochastic game-engine spawn cycle, in a fresh process. |
| `stress-01` | 1 passed, 11.37 seconds | Separately selected marked cyclic stress case. |

The ordinary run uses `run_sample_calculations_check_revision05.py`, SHA-256
`6256EEB88FC6BE93E8A282A524057B877A989B0CF8E22F44DF5358F4EAB506EC`.
It preserves revision 04's source capture, isolation, pytest arguments, logs,
and JUnit output. Its outer process allowance is 3,600 seconds instead of
1,200 seconds and is recorded in the manifest. This gives the full functional
suite room to complete; it changes no test deadline, benchmark limit, or
historical result. All native commands have now exited and drained.

The [source and result verification record](../../../../testing_ground/issue-45/sampled-resume-final-source-verification.json),
SHA-256 `861C19AF8ABB384036CD34CACFA66AF0C39F0C86BE0C10C5CAB4E58BD6218B4F`,
matches all 301 frozen Python files, all seven final run manifests, both Direct
changed Python files, and the JUnit case counts. It preserves the ordinary
failure and its exact message. The six other final runs have zero failures,
errors, or skips. The checker is `verify_sampled_resume_final_source.py`.

The ordinary failure is
`test_balanced_high_concurrency_has_no_ghost_visibility_regression` at
`tests/test_048_ghost_free_admission.py:186`. Its observed 95th-percentile
output-to-terminal-event delay was `0.6483871936798096` seconds, above the
unchanged `0.250`-second limit. Every preceding functional assertion passed:
the worker completed within its bound with no errors, all 600 jobs were done,
none remained queued, running, or failed, the sampled missing-row count was
zero, and 600 terminal records were present.

The later `max(lags) < 0.750` assertion was not executed and receives no pass
credit. The failure output already includes a lag of `1.0134780406951904`
seconds. The run's JUnit record reports 986 tests, one failure, no errors, and
no skips. Its native process exited 1 and drained.

Christopher's performance direction prioritizes the added functionality and
permits expected performance costs. Root retains this failed timing result
without changing its limits or rerunning solely for timing. The independent
[timing disposition review](../../../../testing_ground/issue-45/sampled-resume-performance-disposition-review.md),
SHA-256 `6E853F8B2700D879E9C9E599D18D6897889606484B720848944C7A742A06D598`,
confirms this is a nonblocking timing observation for the private transition.
The failing test and its runtime path are unchanged from the accepted base,
and the new method has no production caller. The timing cause remains
unconfirmed. The separate cyclic and stress checks passed. The bounded acceptance
review retains this timing failure under the stated performance direction.

Public resume preflight, claim integration, readiness, terminal results,
fresh preparation, migration, recovery, and displays remain separate required
work. This section adds no second raw-node lifecycle authority.

## Bounded acceptance

The independent Sol xhigh [acceptance review](../../../../testing_ground/issue-45/sampled-resume-acceptance-review.md),
SHA-256 `54E38D6151827E9D111F59ED47A31BC6623A67ABBA4ED02178258DD5A386336D`,
accepts the exact 301-file source and this private transition's verification.
Root inspected the review and its source and result references. Acceptance
preserves the ordinary run's failed timing assertion and exit status. It adds
partial requirement evidence only. Public execution, result calculation,
completion, migration, recovery, and final Astra review remain required.
