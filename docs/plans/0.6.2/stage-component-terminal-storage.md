# Private component terminal storage

Status: accepted within the private component-terminal storage boundary.
The independent source reviews and bounded acceptance review pass. The ordinary
suite retains two nonblocking timing failures and did not pass as a whole.
This stage adds partial evidence only. Public integration and final review
remain pending. The preceding [sampled resume](stage-sampled-component-resume.md) stage
is accepted and committed locally as
`bba4be0d57825f93d6564eef0f8bf0c045ef0254`.

This stage groups private resumed-sample completion and running-to-failed
persistence because both require the same transaction-time ownership,
producing-shape, alignment, and generation checks. Each behavior receives its
own test-first sequence. Neither method calculates coverage or activates a
public execution path.

The [terminal preparation](../../../../testing_ground/issue-45/component-terminal-result-preparation.md)
defines the boundary. Resumed completion changes aligned running work with
retained lineage to done and preserves that exact lineage. Failure publication
clears successful lineage. Both retain ownership until the caller separately
releases it. Current job statuses cannot establish cumulative sample coverage.

The requirement portions are 44-CMP-004, 005, 007, 008, 009, 020, and 025;
44-SMP-055, 063, and 064; 44-INT-042 and 045; and 44-REC-021, 026, and 028.
Their public and integration requirements remain pending.

## Test-first progression

The [first completion test review](../../../../testing_ground/issue-45/component-resume-completion-first-red-review.md)
accepted the unrun candidate. It entered Direct only after the preceding stage
was accepted and committed. Completion copies contain 302 Python files and
use `freeze_component_terminal_revision01.py`. Failure copies add test 078,
contain 303 Python files, and use revision 02 of that helper. Run names have the
`sample-calculations-component-terminal-` prefix and `-run` suffix.

| Run suffix | Result | Meaning |
| --- | --- | --- |
| `red-02` | 1 failed, 1.05 seconds | Missing completion method after valid setup. |
| `green-01` | 1 passed, 1.71 seconds | Only lifecycle changes; every stored row, schema object, and established output is preserved after reopen. |
| `ownership-red-01` | 5 failed, 1 deselected, 3.53 seconds | Unknown or terminal actor, missing or transferred reservation, and missing selection were wrongly accepted. |
| `ownership-green-01` | 6 passed, 7.30 seconds | Ownership checks now run in the writer transaction. |
| `shape-red-01` | 1 failed, 6 deselected, 1.31 seconds | A different captured producing graph was wrongly accepted. |
| `shape-green-01` | 7 passed, 4.58 seconds | Wrong-shape completion refuses without mutation; the correct shape succeeds. |
| `state-red-01` | 7 failed, 7 deselected, 8.93 seconds | Stale generation and damaged alignment were accepted; other lifecycles returned a false no-change result. No-lineage running already failed a database constraint but raised the wrong exception type. |
| `state-green-01` | 14 passed, 13.62 seconds | Completion requires validated aligned running state with retained lineage at the exact generation. |
| `generation-red-01` | 6 failed, 14 deselected, 9.96 seconds | Boolean and floating-point values were accepted as integer generations; string, null, and negative values already refused with the wrong exception type. |
| `generation-green-01` | 20 passed, 22.26 seconds | Exact nonnegative integer input is required before mutation. |
| `preservation-01` | 37 passed, 33.91 seconds | Additional unchanged-code checks cover lineage, damaged records, rollback and retry, writer-entry races, concurrent attempts, and version-4 preservation. |
| `negative-prechecked-owner-01` | 2 expected failures, 1 pass, 34 deselected, 11.47 seconds | Moving ownership checks before the write transaction permits terminal and transferred owners; the generation case still refuses. |
| `negative-replaced-lineage-01` | 1 expected failure, 36 deselected, 6.15 seconds | Replacing retained interrupt lineage with stable/null breaks the exact state assertion. |
| `negative-ignored-zero-row-01` | 1 expected failure, 1 pass, 35 deselected, 2.85 seconds | Removing the zero-row refusal lets a suppressed update return successfully; the aborted-statement control still passes. |
| `failure-red-01` | 1 failed, 8.02 seconds | The reviewed failure candidate reaches the missing method after valid setup. |
| `failure-green-01` | 1 passed, 4.10 seconds | Failure clears lineage and preserves all other stored values, schema, ownership, and output after reopen. |
| `failure-ownership-red-01` | 5 failed, 1 deselected, 9.62 seconds | Unknown or terminal actor, missing or transferred reservation, and missing selection were wrongly accepted. |
| `failure-ownership-green-01` | 6 passed, 6.12 seconds | Failure checks ownership in the writer transaction. |
| `failure-state-red-01` | 7 failed, 6 deselected, 4.16 seconds | Wrong shape, stale generation, and damaged alignment were accepted; other lifecycles returned false instead of refusing. |
| `failure-state-green-01` | 13 passed, 10.17 seconds | Failure requires the captured producing shape and validated aligned running state at the exact generation. |
| `failure-generation-red-01` | 6 failed, 13 deselected, 6.85 seconds | Boolean and floating-point generations were wrongly accepted. String, null, and negative inputs already refused at the stored-generation comparison, with the wrong exception type. |
| `failure-generation-green-01` | 19 passed, 13.64 seconds | Failure requires an exact nonnegative integer generation before submitting a mutation. |
| `failure-preservation-01` | 37 passed, 33.36 seconds | Unchanged-code checks cover every valid lineage, damaged records, rollback and retry, writer-entry races, concurrent failures, and version-4 preservation. |
| `failure-negative-prechecked-owner-01` | 2 expected failures, 1 pass, 34 deselected, 6.43 seconds | Moving ownership checks outside the writer transaction wrongly accepts terminal and transferred owners. The generation case still refuses. |
| `failure-negative-ignored-zero-row-01` | 1 expected failure, 1 pass, 35 deselected, 4.21 seconds | Removing the zero-row refusal lets a suppressed update succeed. The aborted-statement case still passes. |

The earlier `red-01` attempt also reached the missing method, but root started
it before draining the freeze helper's native session. It receives no selected
RED credit. Root drained both commands, retained their records, and ran
`red-02` serially against the completed unchanged freeze before implementation.

The no-lineage state case and the string, null, and negative generation cases
establish exception consistency only. Their earlier failures receive no credit
for newly preventing a mutation. The state implementation uses the existing
full stored-row validator and repeats observed lineage and generation in its
update predicate. It raises on an unexpected zero-row update so a suppressed
statement cannot silently succeed.

The preservation source freeze is
`sample-calculations-component-terminal-preservation-01`, SHA-256
`0AC5A0EFD5337293F05F7AA30296561274BD7F1506751237D01AFDAC1E7F9C07`.
Its component-state source is
`79AF385F804357FEEACBFDC3EC605D245586C14991CAF2479F50D713C0E2F89E`
and test 077 is
`E409EBDF249D91B0142C92CFEDC20C2EBD9B8F8B0447CF46119694277956AC8F`.
The three controls change one method in separate verified copies; they do not
change Direct MWF. Their expected failures demonstrate test sensitivity and
are not implementation failures.

The independent [completion source review](../../../../testing_ground/issue-45/component-terminal-completion-compatibility-review.md),
SHA-256 `9F5254C3390DE2AF422EF596CDC1FFCE835DCF2125FBD0B966C55F6F7F510D14`,
passes only the frozen 302-file completion boundary. It retracts a preliminary
log-reading error after exact JUnit parsing. Its preserved initial report also
mislocated an earlier generation refusal at the update; the corrected record
places it at the pre-update comparison. Neither correction changes the source
disposition.

The [first failure test review](../../../../testing_ground/issue-45/component-running-failure-first-red-review.md),
SHA-256 `4635336AB50AD266113F8A3D99B0135C79956932FC0D05520D4707BD8B0622C6`,
accepts the separate candidate design. Root corrected its initial claim that
status setters were asynchronous. They wait for their mutations; the candidate's
additional barrier is harmless. The candidate entered Direct unchanged at
SHA-256 `4BF1DFCB67C660C7B95B6B380D08D28B6660FC8060C223B3C94B2A9A7B7C02B3`.

## Combined source and remaining verification

Failure preservation uses the 303-file freeze
`sample-calculations-component-terminal-failure-preservation-01`, SHA-256
`5C745B25F483B0C9648877B1DC66E799CFF8F6DC9F731ED6054EADA72DDB7597`.
Its failure test has SHA-256
`831FF2225864BC0DE3AE4055C1D94A655E84E3180D71E62B8A4FA3D02FD86FE9`.
The two failure controls change only `fail_running_component` in separate
verified copies. Their expected failures supply sensitivity evidence.

The combined review freeze is
`sample-calculations-component-terminal-review-01`, SHA-256
`E44A441C5338B9A059AFAC33A5B4C02CC480BDD35CD98B7DA980A9C76AD96497`.
It retains the failure-preservation runtime unchanged at component-state
SHA-256 `EEF2CBDBFE58FBAB43F28CD170C3B9FB926046FF038BFA502ABD0B5EE5437637`.
Completion test 077 is unchanged. Failure test 078 is
`6861DF3DCBA959967179FF3F23814D7DD2E80BFD9588469C0E8FF60B5406C7F4`.
Its additional preservation case synchronizes completion and failure against
one running component. Exactly one call must succeed and the other must refuse.
The sole changed row must contain the winner's settled terminal state, with
all other rows, ownership, producing shape, and generation preserved.

The combined focused run, surrounding checks, ordinary suite, separate cyclic
checks, applicable stress verification, and two independent Sol xhigh reviews
precede acceptance. The methods do not add production callers or calculate
coverage, choose terminal outcomes, release reservations, or change raw-node
status. Their caller must establish the outcome before publishing it.

The preparation's proposed removal of final lineage predicates cannot expose
an external supported-writer race inside `BEGIN IMMEDIATE`. Keep those
predicates and review them directly. A lineage-clearing negative control can
instead verify observable preservation sensitivity without inventing an
unsupported interleaving.

Public execution, readiness, sampling manifests, causal work, cumulative
coverage, migration, fresh preparation, recovery, diagnostics, and final Astra
review remain required later work.

## Combined source reviews

The independent Sol xhigh [safety review](../../../../testing_ground/issue-45/component-terminal-combined-safety-review.md),
SHA-256 `FB48C2C95A4FD0E80E3C484A7C28B846384D748EDB05F595CCE31BE46A4F54AE`,
and [compatibility review](../../../../testing_ground/issue-45/component-terminal-combined-compatibility-review.md),
SHA-256 `42CCCAB2275B1A23F8BF26FC7578358EBDE5FD8E30534E82F4971E612DED3C3B`,
pass the exact combined source. Root read both reports and their source
references. Neither review accepts the stage or completes a requirement.

The compatibility report preserves and corrects two initial wording errors.
Ordinary construction creates version 4 and can reopen a complete version 5.
Also, omitting failure's lineage clearing would violate the existing database
check before the later all-row assertion. No separate failure-lineage control
was run, and the record claims no such result. These corrections do not change
the source disposition.

## Final-source verification

Every run below uses the same 303-file combined review freeze. Root verified
all eight run maps, all 303 selected source files, and the three changed Direct
Python files. The [source/result reconciliation](../../../../testing_ground/issue-45/component-terminal-final-source-verification.json)
has SHA-256 `8C3AB91B8E174211FAA1B422DC7F5A09EBC9F2C115068959C16B89B4DD4FF86A`.
The ordinary suite did not pass as a whole.

| Run suffix | Result |
| --- | --- |
| `combined-focused-01` | 75 passed, 71.46 seconds. |
| `adjacent-01` | 421 passed, 237.83 seconds. |
| `ordinary-01` | 1,059 passed, 2 timing failures, 1 marked stress case deselected, 1,496.17 seconds. Exit status 1 is retained. |
| `cycle-01-1` | 1 passed, 4.03 seconds, self and mutual cycles. |
| `cycle-01-2` | 1 passed, 9.41 seconds, diamond cycle. |
| `cycle-01-3` | 1 passed, 21.32 seconds, ring cycle. |
| `cycle-01-4` | 1 passed, 5.05 seconds, stochastic cycle. |
| `stress-01` | 1 passed, 7.97 seconds, separately selected marked stress case. |

The focused total contains 37 completion cases and 38 failure cases. The
surrounding selection also checks sampled begin, component-state registration
and reading, session and claim ownership, shared topology, and readiness.

## Retained ordinary timing results

`test_live_component_queue_notification_wakes_before_poll_fallback` failed
its 0.5-second event wait at test 044 line 114. Its `finally` block released the
router and joined the run thread, but the later explicit thread-liveness
assertion was not reached. `test_balanced_high_concurrency_has_no_ghost_visibility_regression`
failed the p95 limit at test 048 line 186, measuring 0.3719136714935303 seconds
against 0.250. The traceback includes a lag of 0.8021740913391113 seconds.
The later maximum-lag limit of 0.750 seconds was not reached and receives no
pass credit.

Root inspected copied databases after draining the completed ordinary run.
The [stored-state record](../../../../testing_ground/issue-45/component-terminal-timing-state-inspection.json),
SHA-256 `19B8A1B7AA2C7574C152B8A96DFC2851DF23660CF7BA954065681F9FA292BDFB`,
shows both router and worker jobs done in the first case and all 600 jobs done
in the second. Both stores have zero active execution rows and the expected
created, started, task-started, and done events. Original database files and
all present companions retained their hashes; only separate disposable copies
were opened for inspection.

These observations establish eventual stored completion. They do not validate
the missed timing limits, establish the queue notification mechanism, or show
that the unexecuted later assertions passed. Both cases use version-4 storage.
Their test sources are unchanged, and the new private terminal methods have no
production caller. No cause for the timing misses is established.

Christopher's [performance direction](requirements-audit.md#performance-priority)
prioritizes correctness and completion and permits modest timing costs. This
stage retains both failed results and their original thresholds. The independent
timing review finds both failures nonblocking for this private storage boundary.
The bounded acceptance review accepts this disposition. No timing-only rerun or threshold
change has been made.

## Documentation and timing reviews

The independent Sol xhigh [documentation review](../../../../testing_ground/issue-45/component-terminal-documentation-review.md),
SHA-256 `D3647DC9D6D6B404C9A67CF96A641974187DB1BC4D4338DB9336465FEC0A29C8`,
checks the 15 partial requirement rows and the focused and surrounding results.
Its corrected wording distinguishes test coverage from the README descriptions.

The independent Sol xhigh [timing disposition](../../../../testing_ground/issue-45/component-terminal-performance-disposition-review.md),
SHA-256 `052769428E3FF1D57A1D589A9EEFAE2714A0F5C64B18EB4937B50B607EC99953`,
reconciles the final source and all eight run maps. Root inspected both reports.
The timing review preserves the failed assertions, unexecuted later assertions,
and unknown cause. Its corrected wall-clock paragraph does not infer polling
or notification behavior. Neither report alone accepts this stage or completes
a requirement.

## Bounded acceptance

The independent Sol xhigh [acceptance review](../../../../testing_ground/issue-45/component-terminal-acceptance-review.md),
SHA-256 `1D9D19B10CC1EAAE6409DA2E686D0BCB4BBC4292E6245BAE29E09C162433B353`,
accepts the exact 303-file source and its recorded verification within this
private storage boundary. Root inspected the full review and its supporting
references. Acceptance retains the ordinary run's failed assertions and exit
status. All 15 requirement portions remain partial. Public result calculation,
coverage, execution integration, migration, recovery, and final Astra review
remain required.
