# Execution-sampling calculations

Status: **the pure calculation is accepted within its private calculation
boundary, and the final 298-file combined functional selection passed its Sol
xhigh review**. Supported sampling commands, persistence, replay, causal
execution, and lifecycle integration remain pending within
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).
The local reviewed base is `4ea961b8a894df2adfaba5fbf747a2962d4662de`.
The initial pure-calculation Test Area copy excludes unfinished previews and
private component ownership. The final combined selection is recorded below.
Only the calculation portions identified by the independent review receive
partial credit; no sampling requirement is complete.

## Boundary

Implement the settled sampling calculations independently of storage snapshots,
session activation, and execution. The calculation accepts exact component
membership, caller-supplied job populations and normalized status filters, and
an explicit seed. It returns selectors and selections without filesystem,
database, randomness, printing, or lifecycle effects. Existing supported
sampling commands retain their behavior until later integration.

The approved observable result is the sample selection required by the final
resolution. Q15 and Q72 supply the worked examples. A count or percentage
shorthand addresses the named raw member; named assignments must stay inside
the starting component; omitted members receive zero. Status filtering precedes
percentage rounding. Full starting coverage requires some selected work and
complete selection of every nonempty member population. This alone cannot
establish successful execution or component completion.

The initial regression addresses count shorthand in `{X,Y,Z}`. It requests
30 X jobs and expects zero selectors for Y and Z. It failed because the
calculation module was absent, then passed after implementation. Every following
behavior used a separate observed failure before correction. Tests execute
sequentially in the isolated `sample-calculations` source with fresh pytest,
`TEMP`, and `TMP` locations. The source preparation verified all 189 baseline
Python files against the immutable reviewed-base manifest.

## Test-first verification

Records in the Parent Repo's `testing_ground/issue-45` use the
`sample-calculations-` prefix. Every invocation retains its command, complete
source hashes, environment, log, and JUnit result.

| RED/GREEN suffix | Behavior | GREEN result |
| --- | --- | --- |
| `01` | Count shorthand and omitted members | 1 passed |
| `02` | Percentage shorthand | 2 passed |
| `03` | Named count assignments | 3 passed |
| `04` | Assignment and start-node membership refusal | 10 passed |
| `05` | Empty, extra, duplicate, and mixed-form refusal | 14 passed |
| `06` | Negative selectors and percentages above 100 | 23 passed |
| `07` | Five exact ceiling examples | 28 passed |
| `08` | Exact requested counts, including zero | 32 passed |
| `09` | Count above filtered population refusal | 34 passed |
| `10` | Status filtering before percentage calculation | 35 passed |
| `11`, `11b` | Direct count and population validation, including negative and non-integer values | 44 passed |
| `12` | Explicit refusal when a named request omits an assignment | 45 passed |
| `13` | Five independently calculated seeded selections | 50 passed |
| `14` | Invalid ranking counts and empty or NUL-containing seeds | 55 passed |
| `15` | Duplicate identity bytes and empty or NUL-containing node names | 58 passed |
| `16` | Some work plus complete coverage of every populated member | 61 passed |
| `17` | Coverage refuses mismatched members and invalid counts | 67 passed |

`preserve-01` passed eight cases, adding named percentages and explicit zero in
all four forms without a source change. `preserve-02` passed twenty, retaining
existing integer spellings and malformed-value refusals before range changes.
`preserve-03` passed 38 cases, including the three existing sampling cases.
`preserve-04` passed all 77 calculation cases. Its additions cover composed
per-member percentages, large-integer rounding, repeated overlapping samples,
unchanged caller data, zero and full selection, and empty filtered populations.

The ranking expectations come from an independent .NET SHA-256 calculation.
`sample-calculations-rank-vectors-01.json` retains all 48 digests for two seeds,
two nodes, and twelve candidates. The tests use fixed selected IDs and also
change returned job IDs while keeping identity bytes fixed. Ranking retains the
existing `mwf.sample.v1` byte prefix. Callers supply unique, stable identity
bytes separately from the returned numeric IDs; this section does not choose
the final durable job identity or implement algorithm-version persistence.

The independent GPT-5.6 Sol xhigh review found missing direct count validation
and an incidental tuple-unpacking diagnostic. Loops 11 and 12 reproduced and
corrected these findings. Its correction review and complete-module source
review passed. The latter is `sample-calculations-complete-review.md`, SHA-256
`177CF03493D243E1CD0E9BB5EA305FFDF1D1982547A2510463E75F68CAEEE441`.
Broader verification remains in progress.

The original two-file selection is `sample-calculations-selected-files-01.json`,
SHA-256 `C7928FEDEC0D19BC773649C05D30884B685D1807C8386CC57E086A44483AC35B`.
It rechecks both direct/exported files and all 189 unchanged baseline Python
files. `adjacent-01` passed 112 cases in 82.81 seconds across the new calculations,
existing engine/sampling, CLI help/reset, removed commands, shared topology,
and source-module boundaries. The ordinary suite and four fresh-process cyclic
cases remain required. No benchmark is selected because supported code never
imports the new module and no existing measured path changes. This section is
not release verification and does not select the marked stress workload.

## Retained timing-test corrections

`ordinary-01` finished with 627 passes, two failures, and one stress deselection
in 931.46 seconds. Both failures occurred in unchanged tests. The networking
case correctly recorded a total timeout at 0.06 seconds, but its assertion
included cleanup and measured 0.687 seconds against a 0.5-second bound. The
restart case requested generation 1 successfully, then that replacement
legitimately exceeded its own 0.4-second checkpoint budget. Its saved error
names generation 1 and `replacement generation`. Both isolated cases passed
in 8.91 seconds, so that rerun alone did not resolve the failures.

Separate controlled probes reproduced both failures without changing framework
code. One added 0.6 seconds after network cleanup. The other added 0.6 seconds
of work after the replacement checkpoint. Their `timing-network-red-01` and
`timing-restart-red-01` records retain the exact failures and source copies.

The corrected network fixture blocks its async response on an explicit release
event. It requires a started request, `JobFailedError`, exactly one total-timeout
event at one second, and failed job state before releasing that response.
Cleanup releases the response before joining and closing transport. The same
0.6-second cleanup delay now passes. A deliberately wrong source copy that
suppresses total deadlines during external waits fails the new test because
execution cannot finish before the response is released. That negative control
does not modify the candidate framework.

The restart fixture keeps generation 0's 0.4-second deadline and 0.8-second
sleep. Generation 1 gets its own five-second checkpoint budget and performs
the controlled 0.6-second work delay. A plain-path flag in `finally` establishes
that generation 0 attempted its stale write before final observations. The
test checks completed replacement runtime, checkpoint and progress, fresh A/B
files, no stale file, and no timeout event. Its name now describes those
observable guarantees without claiming a particular watch-removal mechanism.

`timing-focused-green-03` passed both final cases in 7.23 seconds. Revision 02
freezes these two test corrections plus the unchanged sampling module and its
tests. `sample-calculations-selected-files-02.json` has SHA-256
`8003A7D5F16654242CC5DDD5B1A27709E37D039273BB27DD48D577A64606882E`.
All four direct/exported files match; the other 187 Python files match the
reviewed base. The timing correction review, adjacent checks, repeated ordinary
suite, and separate cyclic cases remain required before acceptance.

## Earlier combined failures

Revision 02's timing corrections passed independent source review and all 100
adjacent cases in 31.13 seconds. Its second ordinary run finished with 626
passes, three failures, and one stress deselection in 655.38 seconds. Both
corrected tests passed. The failures concern repeated API-round speed,
checkpoint reporting before network dispatch, and the elapsed-time inference
in independent ready-node concurrency. All three passed together in 36.76
seconds; that observation does not resolve the ordinary failures.

The demonstrated checkpoint-reporting defect is tracked separately in
[retained deadlines](stage-retained-deadlines.md). At that point it changed a
measured runtime path and required separate review; the earlier no-benchmark
decision applied only to the unused pure sampling module. The combined
ordinary suite, separate cyclic cases, timing diagnosis, and required reviews
then remained outstanding. Those functional checks are completed below while
the historical failure remains recorded.

Routine validation and presentation choices fall within the task's advance
approval. They are recorded with their tests, without inventing architectural
blockers. The private grammar rejects mixed count/percentage requests and
duplicate assignments. It retains existing integer spellings such as `+3` and
`003`; zero is valid under the final no-positive-selector rule. Population
capture, persisted sampling records, guarded replay,
fresh preparation, readiness, same-component causal execution, lifecycle, and
public command wiring remain separate implementation work. Any material
architecture question still requires the full specification, source, and
preparation-history check before deferral.

## Final verification and review boundary

The final executable selection is `sample-calculations-revision-08`, based on
`4ea961b8a894df2adfaba5fbf747a2962d4662de`. Parent Repo
`sample-calculations-selected-files-08.json` has SHA-256
`FE280BB644A4DCB725BEC8C395EF1EDF3C5EFD676FA74CC6B79770A68A8D10DB`.
It records 204 package, test, and benchmark Python files, including 13 selected
changes. All selected direct files and all mapped frozen files still match
their recorded hashes. Unfinished
preview, ownership, and readiness work is excluded.

The final ordinary run passed 660 tests in 440.41 seconds, with the separately
selected stress case deselected. The stress run passed its one case in
7.04 seconds. Both run manifests match all 204 mapped package, test, and
benchmark Python files, and both
JUnit records report zero failures, errors, and skips. The ordinary manifest
has SHA-256 `D7630B4B5F27E5DC038E8B94F94DA58BE43AA003BCA3063044E3174701BC1B63`;
the stress manifest has SHA-256
`2372B09C501215E29AAE5D434BEBB22A7F4A673A49F25C4302C002BB34662ED2`.
Commands use the recorded Python 3.12.14 environment with source-only imports,
disabled bytecode and pytest caches, and unique Test Area temporary paths.

The maps exclude 86 example Python files. A read-only supplement taken after
these runs confirms that their source-07 and source-08 bytes are identical and
match base Git content after checkout newline normalization. It does not add
prelaunch observations retrospectively. Parent Repo
`sample-calculations-examples-supplement-08.json` has SHA-256
`8CAAE86748EE85D04F4ADCEFFBD971826ABC9C827EFB26434E8C1D58286BAA3E`.
No task-owned executable example change is included in this selection.

Four separate source-07 cyclic processes passed in 3.81, 6.98, 6.75, and
3.10 seconds. All 132 runtime package files and the cyclic test are identical
in source 08. The only executable differences are the repeated-API benchmark's
metadata reader and its regression. Independent review checked this carry
forward. These remain source-07 cyclic results.

The 77 pure calculation tests pass within the final ordinary run. The earlier
149-case adjacent run and the later changed-test selections establish the
retained behavior recorded in [retained deadlines](stage-retained-deadlines.md)
and [repeated API verification](stage-repeated-api-verification.md). Their
performance measurements receive no sampling requirement credit. The pure
module remains unused by supported runtime and CLI code.

Final review must distinguish the calculation from its future integration.
Count and percentage grammar, member scoping, zero/default selectors, filtering,
ceiling rounding, deterministic per-node seeded ranking, allowed overlap, and
starting-population coverage are implemented. Population capture, input and
population digests, algorithm-version persistence, seed generation, guarded
replay, readiness, causal execution, lifecycle transitions, and public command
wiring remain pending. No complete sampling requirement is accepted by the
calculation alone, and this is not release verification.

## Combined functional acceptance

The final combined source is
`sample-calculations-combined-functionality-01`. Its 298-file freeze has
SHA-256
`F688AC7E0B395AAC607D17B411145C543E63852D7E5ADD6DE6198E12E30412FF`.
The ordinary suite passed 884 cases with one marked stress case deselected in
581.88 seconds; all 77 pure sampling cases passed. Four fresh-process cyclic
cases passed in 3.24, 7.33, 7.60, and 3.22 seconds, and the separately selected
stress case passed in 6.84 seconds. All six manifests carry the exact same 298
source hashes, all JUnit files report zero failures, errors, and skips, and all
native child processes exited zero and drained.

The independent
[combined functionality acceptance review](../../../../testing_ground/issue-45/combined-functionality-acceptance-review.md),
SHA-256
`1BD24E4345935161A89D9BCAB55974CB08E317F6B815E55D397632FF56367C2F`,
found no functional interaction defect and confirmed that supported package
code still does not import or call `sample_selection.py`. The combined source
also includes the accepted retained-runtime corrections, private ownership
foundation, pure readiness calculation, and API-limit deprecation.

This accepts only the private pure calculation and its bounded partial
requirement evidence. Population and input capture, persistence, seed creation,
guarded replay, readiness, causal execution, component lifecycle, and public
command wiring remain pending. The newer 300-file component-state work and
`tests/test_075_component_state_storage.py` are excluded. This is not whole-
issue, release, or final Astra acceptance.
