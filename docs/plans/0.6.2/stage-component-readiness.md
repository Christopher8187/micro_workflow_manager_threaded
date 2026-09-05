# Component readiness calculation

Status: **accepted within the pure component-readiness calculation boundary**.
Focused and adjacent checks passed; the prior ordinary timing failure remains
recorded, and the final 298-file combined ordinary, cyclic, and selected-stress
checks passed under the updated performance priority below. Supported runtime,
storage, lifecycle, and command integration remain pending.
This section belongs to
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).

## Boundary

Calculate compatible successful-result lineage from caller-supplied direct-parent
component observations. Done stable parents permit stable work; done unstable
parents require the same exact origin. Incomplete parents and conflicting
origins block ordinary execution. A permitted explicit interrupt at the named
starting component may replace blocked input with its new origin. Ordinarily
ready input preserves its existing compatible result.

This calculation discovers no graph, reads no storage, and changes no component
or session. The caller establishes exact parent observations and verifies any
starting-component override. Runtime admission, lifecycle publication,
migration, fences, and diagnostics remain later work.

The preparation is `component-readiness-calculation-preparation.md` in the
Parent Repo's `testing_ground/issue-45`. It maps Q53, Q56, Q57, Q59, Q76, and
the later sampled-lineage correction to the final resolution and existing
callers. The first regression supplied two done stable parents and expected a
stable result without an origin or an override. It failed because the new
module was absent, then passed after the first implementation.

## Test-first checks

Parent Repo records use the `component-readiness-` prefix. Each run copies the
frozen `sample-calculations-revision-08` source, verifies its 204 mapped package,
test, and benchmark Python files, and adds only the new readiness module and
its test. Every run uses source-only imports, disabled bytecode and pytest
caches, unique Test Area temporary paths, and the existing Python 3.12.14
environment. Its manifest, full log, and JUnit result are retained.

| Loop | Expected RED | GREEN |
| --- | --- | --- |
| `01` | Missing calculation module, one failure | 1 passed, 0.10 seconds |
| `02` | Shared unstable origin returned stable, one failure; parentless preservation passed | 4 passed, 0.09 seconds |
| `03` | Six incomplete-parent observations incorrectly permitted work | 10 passed, 0.12 seconds |
| `04` | Three conflicting-parent sets incorrectly permitted work | 13 passed, 0.11 seconds |
| `05` | Six blocked starts ignored the authorized override; two ordinarily ready preservation cases passed | 21 passed, 0.09 seconds |
| `06` | Twelve invalid lifecycle cases were not rejected, including after an incomplete parent | 33 passed, 0.10 seconds |
| `07` | Fourteen impossible done-result cases were not rejected | 47 passed, 0.12 seconds |
| `08` | Six empty or non-string interrupt origins were not rejected; exact-string and iterable preservation passed | 54 passed, 0.14 seconds |

The six-module adjacent check passed 146 cases in 17.45 seconds. It includes
the readiness and sampling calculations, shared topology, retained engine and
sampling behavior, Hoeflein scheduling, and source-module boundaries. The
ordinary suite ran on a separate frozen copy. At that point cyclic verification
and independent Sol review remained pending. No benchmark is selected for the pure
readiness module because no
supported runtime code imports this new pure module and no measured path
changes. Release artifact checks remain outside this section.

## Interpretation and remaining integration

A done stable parent must have no origin. A done unstable parent must have one
nonempty exact string origin. Invalid lifecycle names and impossible done
results raise before any readiness result, including when another parent
blocks or an override is supplied. Valid queued, running, sampled, and failed
parents block without consulting their lineage. Both possible running-lineage
representations block, so this calculation chooses no answer to AQ5.

An authorized override supplies a nonempty exact string and is used only if
ordinary readiness fails. Ready unstable parents retain their earlier shared
origin. Parentless and all-stable inputs remain stable. The calculation does
not normalize IDs, allocate a session, discover parents, validate interrupt
classification, or publish successful lifecycle state. Tests also check
reversed input order, a conflicting third parent, unchanged input values, and
iterator input.

The supported CLI and scheduler still use their existing readiness readers.
Replacing them requires the authoritative component observation reader and
coordinated integration. Legacy conversion and activation must implement the
now-approved AQ2 and AQ3 decisions. Full sampling coverage, actual execution success, descendant admission,
holds, fences, tracing, and persisted component results remain pending. This
section currently assigns no completed requirement or runtime acceptance.

## Ordinary result and retained timing failure

`component-readiness-ordinary-01` passed 713 tests and failed one in
1086.54 seconds, with one stress case deselected. All 54 readiness cases passed.
The failing unchanged case was
`test_balanced_high_concurrency_has_no_ghost_visibility_regression` in
`test_048_ghost_free_admission.py`. Its 95th-percentile output-write-to-terminal
event lag was 0.28942298889160156 seconds against the fixed 0.250-second limit.
All preceding assertions passed: 600 done jobs, no queued, running, or failed
jobs, no missing visible row, no run exception, and completion within 20 seconds.

The selected source adds only the readiness module and its test to revision
08's mapped executable files. Supported code does not import the new module.
The observed timing failure does not establish a readiness defect or identify
the source of terminal delay. Its source copy, project, log, JUnit, and manifest
are preserved. The original threshold remains unchanged; a focused rerun alone
cannot turn this result into a pass. Source-only diagnosis in Parent Repo
`ghost-visibility-timing-diagnosis.md` found no readiness cause or functional
state failure. The metric uses the terminal event's application timestamp,
not the later commit instant.

Christopher's [updated performance priority](requirements-audit.md#performance-priority)
now permits modest feature costs without treating them as standalone blockers.
No expanded timing investigation is planned solely for this observation.
The failed record and threshold remain unchanged. A fresh combined-source
ordinary run will still report every functional or timing failure explicitly.

## Combined functional acceptance

The final combined source is
`sample-calculations-combined-functionality-01`. Its 298-file freeze has
SHA-256
`F688AC7E0B395AAC607D17B411145C543E63852D7E5ADD6DE6198E12E30412FF`.
The ordinary suite passed 884 cases with one marked stress case deselected in
581.88 seconds; all 54 readiness cases passed. The previously failing unchanged
ghost-visibility check also passed in 5.585 seconds. Four fresh-process cyclic
cases passed in 3.24, 7.33, 7.60, and 3.22 seconds, and the separately selected
stress case passed in 6.84 seconds. Every run manifest carries the exact same
298 source hashes; all JUnit files report zero failures, errors, and skips, and
all native child processes exited zero and drained.

The independent
[combined functionality acceptance review](../../../../testing_ground/issue-45/combined-functionality-acceptance-review.md),
SHA-256
`1BD24E4345935161A89D9BCAB55974CB08E317F6B815E55D397632FF56367C2F`,
found no functional interaction defect and confirmed that supported package
code still does not import or call `component_readiness.py`.

This accepts the pure calculation and its bounded partial evidence. The prior
timing failure and threshold retain their historical result. Authoritative
component observations, supported scheduler and CLI use, lifecycle
publication, sampling outcomes, holds, fences, recovery, and diagnostics remain
pending. The newer 300-file component-state work and
`tests/test_075_component_state_storage.py` are excluded. This is not
whole-issue, release, or final Astra acceptance.
