# Private component identity and session ownership

Status: **private revision 03 and its final 298-file combined functional
selection are accepted within the identity, reservation, hold, and claim-time
ownership boundary** under Christopher's revised performance priority.
Supported runtime integration remains in progress within
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).
The local base is reviewed commit `4ea961b8a894df2adfaba5fbf747a2962d4662de`.
Its public push is awaiting the explicit approval requested after automatic
approval review rejected publication. The previous accepted public commit is
`a9167c79bebfadec64a73ae92fbd68d07c00e6d3`.

## Planned boundary

Extend the private version-5 storage foundation with exact component identity,
the producing graph shape, atomic session reservations, counted holds, and
explicit session ownership in the existing job-claim transaction. Ordinary
version-4 construction and supported runtime callers retain their behavior.
Current commands do not activate these records.

The source-only preparation is `next-s2-preparation.md` in the Parent Repo's
`testing_ground/issue-45`. Root originally narrowed its component-state portion
while [AQ5](architectural-questions.md#aq5-stability-while-sampled-work-resumes)
was unanswered. The approved decision now retains established stability and
origin while sampled work runs. This existing section still records
component identity and shape without lifecycle/result columns or a generic
state writer. Lifecycle transitions and their dependent displays remain later
work. AQ1 through AQ4 and the ownership follow-ups are now approved within
their recorded boundaries; their implementation remains separate.

The test boundaries are the approved persistence and ownership invariants,
exact topology calculations, and retained scheduler/storage outcomes. Existing
behavior is checked before each affected change. New behavior receives a
sensitive failure before implementation. All MWF workloads run sequentially
from the isolated `component-ownership` export under the existing Test Area
run, using the declared-test virtual environment and selected-only `PYTHONPATH`.
Each run also has fresh Test Area locations for pytest, `TEMP`, and `TMP`.

## Verification record

`component-ownership-preserve-01` passed 77 tests in 10.06 seconds before source
changes: the shared-topology and private-session modules plus three new
preservation cases for reversed and duplicate member order across session
creation and reopening. Commands, environment, complete Python source hashes,
JUnit results, and logs are retained by `run_component_ownership_check.py` in
the Parent Repo record directory.

The initial graph-shape cases failed because the calculation did not exist
(`component-ownership-red-01`: two failed, three preserved). The implementation
passed 19 new and surrounding topology cases in 7.34 seconds. Six subsequent
preservation cases passed in 0.50 seconds, including isolated-node changes and
the retained no-op for autostart pairs absent from raw edges.

`component-ownership-red-02` exposed the missing immutable topology snapshot.
The new capture preserves one shape/component observation when the caller later
changes its graph; ten cases passed in 1.32 seconds. Private definition
registration then failed as absent in `component-ownership-red-03`; its first
implementation passed 79 registration and retained session cases in 11.27
seconds. Version 5 now includes graph shapes and component definitions, with
exact declaration validation; ordinary creation remains version 4.

`component-ownership-red-04` demonstrated that conflicting registration wrongly
accepted another shape for an existing exact component. An injected second-row
failure already rolled back the whole registration. The transaction now checks
existing definitions before insertion; ten cases passed in 0.84 seconds.
`component-ownership-red-05` recorded sixteen missing refusals for malformed
graph shapes and incorrect component partitions. Registration now independently
reconstructs and validates the canonical snapshot before submitting a mutation.

The corrected validation passed 100 focused and retained cases in 11.88 seconds.
Further preservation checks passed 45 cases in 8.98 seconds, including ordinary
version-4 construction, new-process schema refusal, and separate split/merge
definitions.

## Reservations and holds

Reservation acquisition reads the complete persisted session selection and its
expected producing shape in one writer transaction. It refuses unknown or
terminal sessions, conflicting owners, and damaged same-owner scope. A complete
repeat is unchanged. Explicit release removes only that session's reservations.

Review found that exact keys alone miss raw-node overlap across historical split
and merged shapes. Two sensitive failures preceded the correction. Acquisition
now compares selected raw members with all persisted reservations in the same
transaction. The existing owned component and its session appear in each
conflict. A stale persisted owner remains reserved until explicit release or
later recovery.

Holds retain one positive count for each session/component. Acquiring requires
a persisted running session. Held predecessors may be outside its execution
selection. Each batch changes each requested component once; duplicate requests
are rejected before mutation. Releasing decrements counts, deletes at zero, and
preserves other sessions. Release remains available after session termination.
Reads join the owning session heartbeat without expiring stale rows.

| Record suffix | Observed failure or retained behavior | Result after correction |
| --- | --- | --- |
| `red-06`, `green-06` | Missing reservation operation; two existing cases also selected by `reserves` | 32 passed, 2.36 seconds |
| `red-07`, `green-07` | Four missing owner, session-status, and partial-scope refusals | 107 passed, 13.93 seconds |
| `preserve-04` | Expected shape, two-process scope race, injected rollback | 40 passed, 5.06 seconds |
| `red-08`, `green-08` | Missing exact release | 41 passed, 5.09 seconds |
| `red-09`, `green-09` | Missing shared predecessor holds | 42 passed, 5.19 seconds |
| `red-10`, `green-10b` | Both split/merge overlap directions wrongly accepted | 115 passed, 19.24 seconds |
| `red-11`, `green-11` | Four unknown/terminal hold-acquisition cases | 48 passed, 7.67 seconds |
| `red-12`, `green-12` | Missing counted release | 49 passed, 8.05 seconds |
| `red-13`, `green-13` | Duplicate hold batches wrongly accepted | 51 passed, 7.69 seconds |
| `preserve-05` | Hold rollback/retry and stale reads | 54 passed, 10.22 seconds |
| `preserve-06` | Registration races, empty graph, noncanonical shape rejection | 60 passed, 12.83 seconds |
| `red-14`, `green-14` | Damaged reservation outside immutable selection wrongly accepted | 61 passed, 11.59 seconds |

Every suffix has the `component-ownership-` prefix. `green-10` ran no tests
because its retained test filename was incorrect; the corrected invocation is
`green-10b`.

## Review correction and remaining checks

`preserve-07` passed three exact-byte association cases before centralizing
component-key encoding. Unicode, commas, and quoted names retain the established
JSON spelling across session selections, definitions, reservations, and holds.
The shared encoder/decoder correction then passed 138 focused, session, and
topology cases in 30.81 seconds, recorded as `review-green-01`.

Topology capture requires the caller to keep its inputs unchanged during the
capture call. Later changes cannot alter the returned snapshot. Runtime
activation and authoritative-input timing remain unfinished.

The second preclaim review requested explicit version-4 refusal checks for all
new operations, separate historical-shape hold counts, and malformed ownership
declarations. Those preservation additions passed together with retained claim
bytes: 69 cases in 15.03 seconds, recorded as `preserve-09`.

## Exact job claims

Both claim entry points now accept an explicit session and component. Private
version 5 requires both; version 4 retains calls that omit both and explicitly
refuses supplied ownership context. The existing local collection key includes
both values. Each grouped writer operation checks running session status, exact
reservation ownership, selected scope, and raw-node membership. It caches each
distinct session/component lookup within that transaction. Lease updates,
historical owner rows, and matching `started` event data commit together.

Owner rows use exact execution IDs and do not reference mutable job rows.
Unstarted release and deletion with retained events preserve history. Integrating
ownership-history cleanup and job-scoped current-owner resolution depend on
[AQ6](architectural-questions.md#aq6-execution-ownership-after-trace-clearing).
Existing detailed-event retention remains unchanged. This section does not
classify owner rows as removable trace or choose their lifetime.

| Record suffix | Sensitive failure or preservation | Result |
| --- | --- | --- |
| `preserve-08`, `preserve-08b` | 33 retained cases passed; two new cases initially misread the flattened event API, then passed after fixture correction | 2 passed, 0.83 seconds |
| `red-15`, `green-15` | Both entry points lacked ownership inputs | 142 passed, 30.61 seconds |
| `red-16`, `green-16` | Six missing-input cases | 10 passed, 5.97 seconds |
| `red-17`, `green-17` | Twelve running-session, reservation, selection, and membership cases | 94 passed, 26.63 seconds |
| `red-18`, `green-18` | Six version-4 context cases | 28 passed, 13.97 seconds |
| `red-19`, `green-19` | Six malformed session-ID cases | 34 passed, 22.09 seconds |
| `preserve-10` | Owner/event failure rollback, release and recreated-job history | 37 passed, 19.71 seconds |
| `preserve-11b` | 600 claims retained every owner/lease/start event with one owner lookup | 1 passed, 1.19 seconds |
| `preserve-12` | Controlled callers differing in session or component remain separate | 2 passed, 1.78 seconds |
| `preserve-13` | A second storage instance ends the session or releases ownership before the claim transaction starts | 2 passed, 2.93 seconds |

`preserve-11` initially attached SQL tracing to a writer connection that retired
before the measured transaction. The corrected observer wraps the real
transaction and is recorded separately as `preserve-11b`; no source change was
needed. The concurrency comparison export changes only the local collection
key back to node/priority. Both cases then wrongly succeeded, producing two
expected failures in 7.68 seconds in `coalescing-red-01`. The candidate retains
the corrected key.

## Frozen-source checks

The first eleven-file executable selection is recorded in
`component-ownership-selected-files-final.json`, SHA-256
`3A25D25D41D5975BBAC78497C12F778640BF03C4B6C283EBD7998A4D4236FDFA`.
Every selected file matches direct MWF. The ordinary and adjacent checks used
identical runtime files. Two later preservation cases separately damage each
required execution-owner foreign key and verify refusal in a fresh process.
Only `test_071_component_session_ownership.py` changed after those broader runs;
the full focused pair then passed again.

| Check | Result |
| --- | --- |
| `adjacent-01` | 217 passed, 135.72 seconds |
| `adjacent-02` | 44 passed, 73.07 seconds |
| `ordinary-01` | 661 passed, one marked stress case deselected, 495.05 seconds |
| `focused-final-01` | 111 passed, 40.63 seconds |
| Four separate fresh-process cyclic checks | One passed in each process, 3.76, 9.86, 7.71, and 4.08 seconds |
| `stress-01` | One passed, 8.50 seconds |

The marked stress test runs repeated threaded jobs through the CLI and exercises
the retained version-4 single-claim path. It was selected separately from the
ordinary suite. All checks use fresh Test Area locations and the declared-test
environment. Logs, JUnit results, commands, and source manifests remain in the
Parent Repo record directory.

The independent safety reviewer used the same declared-test environment and
selected source for modules 071, 072, 069, and 066. That selection passed 185
tests in 50.91 seconds. Its run manifest is
`component-ownership-safety-independent-01-manifest.json`, SHA-256
`FBF2E3D452FC8704AAE5B662B8DC953F0DA613B6F69CBB061020EC6E48C7277B`.
All eleven selected hashes match the final selection.

## Declared measurements

The fixed comparison runs 22 fresh processes sequentially. Version-4 claims
compare the reviewed base with the candidate in three alternating pairs, using
batch sizes 1, 64, and 512. Initialization compares 200 creations and 200 reopens
per process. Each cell's candidate/base median ratio must be at most 1.20.

Private version-5 claims compare zero and 2,048 unrelated reservations in three
alternating pairs, with a dense/sparse median ratio at most 1.20. Separate
reservation measurements use zero, 256, and 2,048 unrelated owners. The
2,048/256 repeat-median ratio must be at most 12.0, each dense repeat p95 at most
250 milliseconds, and the median first dense acquisition at most 250
milliseconds. Three 256-component counted-hold observations check both owners
and every count change.

Every timed cell checks exact jobs, leases, events, ownership where applicable,
unchanged unrelated reservations, an empty writer backlog, and database
integrity. Setup and these checks are outside the timed intervals. A separate
untimed SQL observer verifies one metadata read per batch, no version-4 owner
queries, and one version-5 owner lookup per batch. Both preliminary pilots
passed; they are excluded from the comparisons.

`component-ownership-benchmark-manifest.json` records the fixed gates, commands,
environment, dependency versions, source hashes, and both measurement-program
hashes before the first comparison. The programs also check for source drift
after completion. The first full run passed every correctness check but failed
two fixed timing gates: version-4 batch size 1 had a 1.209158658 candidate/base
median ratio, and version-5 dense/sparse claims had a 1.347734734 ratio. The
original manifest SHA-256 is
`ed3627b5e8ba2afdbc97154bd102b9e78606959a85c325019961f0432b8f436f`.
Its source exports and results are preserved. The following sections record
diagnosis and corrections; the gates remain unchanged. This section is not
accepted and gives no new requirement credit yet.

## Producing-shape storage correction

Minimized instrumented claims did not reproduce either timing gap. The 256
version-4 metadata reads consumed 0.03177 seconds within a 13.02465-second
candidate claim interval; the baseline took 13.57779 seconds. Private sparse
and dense claims took 3.93388 and 3.94109 seconds. These diagnostic observations
do not replace or invalidate the failed fixed comparisons.

Source inspection found a separate storage-scaling defect. Each component row
repeated the entire producing graph JSON. A new 1,024-component regression
failed at 9,134,080 database bytes against a 2 MiB bound. Before changing source,
69 existing component cases passed in 16.49 seconds. Graph JSON now lives once
in `graph_shapes`, keyed by an internal integer; component definitions reference
that row. Registration, exact lookups, and reservation checks preserve the same
external shape representation and transaction behavior.

The corrected database uses 241,664 bytes and preserves every component lookup.
The focused pair passed 112 cases in 42.52 seconds. Separate declaration checks
then passed all nine cases in 5.57 seconds, including missing shape uniqueness,
a missing shape identifier, and refusal of the previous private JSON-reference
layout in a fresh process. Existing version-4 storage remains unchanged.

The second eleven-file selection is
`component-ownership-selected-files-revision-02.json`, SHA-256
`CABD21B5BC3762F95A475002340DE6D7B9A4A2C373F5933017783771F0324CCA`.
It is preserved in `component-ownership-revision-02`. The original source export,
manifest, failed results, and diagnostics remain untouched. A new 22-process
comparison uses the same sample program, workloads, correctness conditions,
and timing limits, with new `r02` paths. No schema-capability cache was added because the diagnostics did not
establish repeated schema reads as the cause of the failed timing.

## Initialization measurement and import correction

The complete revision-02 comparison passed every correctness check, the three
version-4 claim gates, private claim density, reservation scale and latency,
holds, and exact SQL counts. Version-4 ratios were 0.9311632104, 0.9803233779,
and 0.8858509397; private dense/sparse claims measured 0.9434313419. Initialization
creation passed at 1.0156494757. Reopening alone failed at 1.4165398715, with
0.5497485000 candidate seconds against 0.3880925000 baseline seconds. These
results remain in `component-ownership-benchmark-revision-02-summary.json`.

Six diagnostic processes then compared unchanged revision-02 source using the
same 200-store workload and alternating labels. Reopening falsely exceeded the
same limit at 1.3940341299. Two constructor profiles followed the same call
graph, with 230,086 calls each. These observations establish that the short
measurement can reject unchanged code; they do not turn its failure into a pass.

A separate fresh-process check exposed an import regression. Importing storage
loaded NetworkX and topology, which the reviewed base avoided. The preservation
case passed on the base, failed on the candidate for that exact reason, and
passed after graph imports moved into private snapshot validation. Ordinary
version-4 creation also remains graph-free. The final ownership and active-job
restart selection passed 124 cases in 63.89 seconds as `import-green-01`.

The final eleven-file selection is
`component-ownership-selected-files-revision-03.json`, SHA-256
`67FE469D6D6A5A2C515A5847B7FE64CEBA47573E94A07051C7C2387E7B1246A0`.
All selected direct and exported files match. The revision-02 measured claim,
reservation, hold, and SQL paths are unchanged; imports occur during their
untimed setup. The safety reviewer accepted carrying those passing results
forward. The final section records completed broad correctness checks;
initialization measurement remains unresolved.

A source-reviewed replacement measurement increases each initialization phase
to 2,000 stores while retaining constructor-only timing and every correctness
check. Its fixed plan runs six unchanged-candidate processes first. Both
creation and reopening median ratios must lie within `[1/1.20, 1.20]`. Only a
passing control permits six baseline/candidate processes, with the original
candidate/base limit of 1.20 for each phase. Either control failure stops the
comparison; this is one declared comparison, with no retries. Frozen source,
program, controller, and selected-manifest hashes are checked before either
summary. Results use new `initialization-long-01` names.

The longer unchanged-code control also failed. Creation medians were
30.1143460001 and 23.2931332998 seconds, ratio 0.7734895953, below the declared
lower bound. Reopening passed its control at 0.8913638493. All six processes
passed their correctness checks, and the controller verified every frozen
source and program hash before writing the summary and exiting nonzero.
It stopped before the actual baseline/candidate comparison, exactly as planned.
The manifest SHA-256 is
`745ae141577d03167eab5f9cfebc855b45f2b59289f32a891bda566032ae63d3`.

Initialization measurement remained unresolved at that review point. No further retry or threshold
change is planned without a new diagnostic basis. This is a demonstrated
measurement limitation on unchanged code, not a newly inferred implementation
defect or architectural question. Earlier passing claim, scale, hold, and SQL
checks remain valid within their stated source boundary. Final correctness
verification subsequently completed below, but at that point the section
remained unaccepted and added no requirement credit until every required
verification condition was met.

## Final revision correctness checks

The independent safety reviewer ran modules 071, 072, 069, and 066 against
revision 03. All 190 cases passed in 42.80 seconds. The manifest is
`component-ownership-safety-independent-03-manifest.json`, SHA-256
`30E488031DF51542A793AF5451CAB515D864DFB55A421C2D4E67CC6F54EE4B21`.
It records the declared-test interpreter, selected-only source, exact commands,
complete source hashes, and fresh Test Area temporary paths.

`ordinary-03` passed 668 cases with one marked stress case deselected in 443.64
seconds. Four separate `cycle-03` processes each passed one case in 4.18, 9.10,
5.55, and 3.41 seconds. `stress-03` separately passed its one case in 7.45
seconds. Every record has the `component-ownership-` prefix and uses the frozen
revision-03 export. The targeted storage/scheduler companion selection,
`adjacent-03`, passed 78 cases in 130.35 seconds. Together with the independent
selection, it covers every final adjacent module required by the measurement
preparation. These results do not resolve the remaining initialization
measurement. At that point the section remained unaccepted and uncommitted.

Final GPT-5.6 Sol xhigh reviews in the Parent Repo's `testing_ground/issue-45`
agree on that disposition. `component-identity-safety-review.md` has SHA-256
`434497CC67C32339AC32812AE6A9DA5BBE405246343C8836FCEFB7F430B84ABF`;
`component-ownership-compatibility-review.md` has SHA-256
`BA4B0E659436EAF1E259793B8C14709EA7BC7CE9CEAC49305573AA78168ED658`.
Both accept the final source and correctness checks while leaving initialization
measurement unresolved. Independent sampling work uses a separate export based
on the reviewed commit and does not include this unaccepted section.

## Subsequent performance priority

Christopher's [2026-09-05 instruction](requirements-audit.md#performance-priority)
prioritizes functionality and correctness while accepting that added features
may cost time. The unchanged-source initialization instability is therefore
reported as a measurement limitation rather than a standalone reason to stop
this section. No additional initialization comparison is planned. Existing
source and correctness reviews remain evidence for their exact selection.
Combined-source verification remained required at that point and is recorded
below. The historical failed measurements are unchanged.

## Functional acceptance of revision 03

The [independent reassessment](../../../../testing_ground/issue-45/component-ownership-functional-acceptance-recommendation.md)
found no remaining correctness blocker. All eleven direct files and the same
eleven preserved export files match the reviewed selection exactly. Its
SHA-256 is `8980F68E61AFB0B9ACE88B5840921500BC23CB181DD3CAFE6496655295FBA03E`;
the selection manifest remains
`67FE469D6D6A5A2C515A5847B7FE64CEBA47573E94A07051C7C2387E7B1246A0`.
Root read that reassessment and the preparation record and completed the
current diff-whitespace check. The source and correctness results above support
acceptance of this private storage foundation under the new performance
priority. No extra initialization comparison is required.

Acceptance covers exact component identity and producing shapes, atomic
reservations, counted holds, claim-time ownership, and exact execution-owner
lookup. Ordinary version-4 behavior remains covered by the existing evidence.
Lifecycle state, public version-5 activation, topology capture in supported
execution, legacy migration, explicit fresh preparation, membership repair,
interrupt scheduling, current/last-owner selection and cleanup, recovery,
trace, and command integration remain separate work. No requirement becomes
complete through this private acceptance.

## Combined functional acceptance

The combined source is `sample-calculations-combined-functionality-01`. Its
298-file freeze has SHA-256
`F688AC7E0B395AAC607D17B411145C543E63852D7E5ADD6DE6198E12E30412FF`.
The ordinary suite passed 884 cases with one marked stress case deselected in
581.88 seconds; it includes all 74 component definition, reservation, and hold
cases and all 42 owned-claim cases. Four fresh-process cyclic cases passed in
3.24, 7.33, 7.60, and 3.22 seconds. The separately selected marked stress case
passed in 6.84 seconds. All six manifests contain the same 298 source hashes,
all JUnit files report zero failures, errors, and skips, and every native child
process exited zero and drained.

The independent
[combined functionality acceptance review](../../../../testing_ground/issue-45/combined-functionality-acceptance-review.md),
SHA-256
`1BD24E4345935161A89D9BCAB55974CB08E317F6B815E55D397632FF56367C2F`,
found no functional interaction defect. Automatic storage creation remains
version 4, while the complete version-5 path has no production caller. The
modified claim path therefore preserves supported version-4 execution and
requires exact context only in private version-5 storage.

This accepts exact component identity and shapes, atomic reservations, counted
holds, claim-time ownership, and exact owner lookup within the private storage
boundary. Public version-5 activation, component lifecycle and results,
topology capture in supported execution, migration, current or last owner
selection and cleanup, recovery, trace, and command integration remain
pending. The newer 300-file component-state source and
`tests/test_075_component_state_storage.py` are excluded. This is not
whole-issue, release, or final Astra acceptance.
