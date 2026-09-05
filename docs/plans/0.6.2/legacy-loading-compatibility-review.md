# Legacy-loading compatibility and specification review

Status: **PASS for the selected eleven-file legacy-loading and retained-runtime
section.** This report does not accept S2 or the release.

## Reviewed selection and role

This is the independent compatibility and specification review for the
legacy-loading section of [Implement and verify the agreed MWF 0.6.2
workflow-management changes](https://github.com/Christopher8187/product/issues/45).
I reviewed as GPT-5.6 Sol with xhigh reasoning. The selected base is accepted
commit `a9167c79bebfadec64a73ae92fbd68d07c00e6d3`.

The initially selected executable changes are:

- `micro_workflow_manager/legacy_runs.py`;
- `micro_workflow_manager/cli/active_run.py`;
- `micro_workflow_manager/cli/layout.py`;
- `micro_workflow_manager/cli/project.py`;
- `micro_workflow_manager/storage/filesystem.py`;
- `tests/test_070_legacy_run_preflight.py`.

Review expanded the final selection to include
`micro_workflow_manager/cli/node_clipboard.py` and
`micro_workflow_manager/runners/process.py` after focused RED cases exposed
late preflight in those callers. It also includes the narrow fixture-lifetime
correction in `tests/test_reliability.py` described below. The ordinary gate
then exposed an existing scheduler-watchdog clock error under a dense network
completion wave. Its narrow retained-runtime repair adds
`micro_workflow_manager/workflow/supervisor_attempts.py` and
`tests/test_043_watchdog_networking.py` to the selection.

The isolated selection is
`C:/Business/product/test_area/mwf-062-issue45-20260904/legacy-loading`.
I compared the changed source with the accepted session-foundation copy and
inspected callers outside the changed files. I also read the final resolution,
both complete preparation transcripts, `direct-loading-preparation.md`,
`state-migration-review.md`, the accepted migration and session stage records
and reviews, `requirements-audit.md`, `architectural-questions.md`, and the
current MWF architecture and operational paths. The full-history requirement
was satisfied before assessing whether any architectural decision remained.

The final eleven executable files match the direct MWF copy byte for byte:

| File | SHA-256 |
| --- | --- |
| `micro_workflow_manager/legacy_runs.py` | `3C5ABE36E6BC955D7DC80CAACB8EBEC425B69467AE794CB91BD8E9B5D6F68773` |
| `micro_workflow_manager/cli/active_run.py` | `915E511B70BA3791433F101377C32EFE3BDB2D6C1BACC3DCAEBD147AD115D7EF` |
| `micro_workflow_manager/cli/layout.py` | `4F19E7FA82B443C4BDB3AAC219430512AC1727C3AD8D5A444706D3D76DC5E76C` |
| `micro_workflow_manager/cli/project.py` | `475322F419B231390171F2E588676047497DA4E06E2E26A80B2DC5195285D44C` |
| `micro_workflow_manager/storage/filesystem.py` | `D635FF53070C3D31E46BFBBA15CDBB031DC8C5F2596B5FC5FA1DAFF9AC53CAD1` |
| `micro_workflow_manager/cli/node_clipboard.py` | `4B25489540AF67FEE5E3AD9527DC0641640DAE124F20956C357D9B3DC93EE223` |
| `micro_workflow_manager/runners/process.py` | `D33D2EE223C143904FFABD420FBB772A63F5F8DF75E20A651C4D6A73CD207591` |
| `micro_workflow_manager/workflow/supervisor_attempts.py` | `EF6DFC163E725DF35528B3323643292FD61675A23688386A4C90933F15D032A9` |
| `tests/test_043_watchdog_networking.py` | `FEC5938B78F599B425396E1B82B00450CE67E462C204C9581E32F967D1E38314` |
| `tests/test_070_legacy_run_preflight.py` | `F4D8D76A8F2C6B0C60EAFDEC833079E0EC7B4CF4881D6B9B5491AC7D4E60935D` |
| `tests/test_reliability.py` | `D9137ED89613A3CD6E72F45071A1499DD2153357D1FFF518D2012E070C765915` |

## Governing behavior

The final resolution for [Settle the MWF workflow-management model for
0.6.2](https://github.com/Christopher8187/product/issues/44#issuecomment-5539997969)
requires safe one-time legacy session import and forbids migration underneath a
live legacy process. This section implements another partial guard under
44-SES-034. It does not implement the import required by 44-SES-033.

The approved loading order requires observation of both raw run locations
before layout conversion, SQLite initialization, WAL creation, legacy metadata
deletion, graph synchronization, user graph or router effects, starter-job
creation, or recovery. The section may require only JSON objects. It may not
invent legacy fields, merge records, choose a preferred location, infer that
matching bytes represent one record, or remove either source when both exist.

The existing-database boundary is narrower. Both raw paths must still receive
filesystem structural validation, but a live-owner refusal is added only when
the `state.sqlite3` path does not resolve to a file. An existing database may be
complete and legitimately opened by the active project. It may instead be an
interrupted initialization. Distinguishing those cases without changing SQLite
coordination files remains under AQ1. AQ4 continues to cover an older process
starting after the observation.

## Source assessment

`legacy_runs.py` reads `.mwf_run.json` and `.mwf/run.json` in fixed order. It
uses `lstat`, rejects non-regular and reparse entries, decodes UTF-8, requires a
JSON object, and collects every path-level structural error before raising. The
order stabilizes diagnostics and gives neither location precedence.

`preflight_legacy_storage_creation()` runs structural validation before its
database decision. When the database path does not resolve to a file, it
applies the accepted session liveness classifier to every validated record and
reports all observed live owners. Terminal and recycled-PID records remain
non-live. A fresh heartbeat from another host remains live. A directory or
dangling link named `state.sqlite3` does not bypass the missing-database branch.
`Path.is_file()` follows a link whose target is a file, so this review does not
describe the predicate as entry-aware. The approved preparation does not settle
database-link policy, and this section adds none.

`FileStorage._initialize_storage()` invokes the guard before
`FileStorageBase.__init__()`, publisher and broker setup, SQLite access, legacy
metadata import, and old-lock cleanup. This covers direct `FileStorage`, direct
`MicroWorkflow`, and downstream callers that construct storage before changing
project data. The private version-5 fresh creator has already rejected all raw
runtime evidence before it reaches this call.

`ensure_runtime_layout()` performs unconditional structural reading. It keeps
the liveness and two-record refusal conditional on pending layout work, so a
complete active project with one valid current run record can still reopen.
When conversion is pending, the existing helper refuses every two-record case,
including matching bytes and matching IDs, before deletion or movement.

`load_workflow()` and `setup_graph()` run the missing-database guard before
`read_config()`, graph-module import, configuration writes, node-folder
synchronization, router mounting, or starter materialization. This corrects the
two graph paths that otherwise reached user or filesystem effects before their
later `MicroWorkflow` or `FileStorage` construction.

Both clipboard operations now validate their source and then run the same
preflight before their first write. Process workers run it before graph import.
Initialization keeps its pre-extraction check and repeats the layout-level live
and two-record refusal immediately after extraction. An archive may therefore
unpack its requested files, but MWF creates no sidecar, opens no database, and
performs no layout conversion after discovering the unsafe record.

`cli.active_run.run_state_liveness()` retains its locally imported process and
identity probes when it calls the shared classifier. Existing Windows process
safety monkeypatches therefore continue to exercise the same seam. The storage
guard calls that same unchanged classifier rather than implementing a second
liveness policy.

## Findings

### LL-COMP-001: clipboard commands mutated before the storage guard

Resolved test-first.

Previously, `cli/node_clipboard.py` constructed storage after it had changed the
project tree. `copy_node_to_clipboard()` creates the clipboard directory and
copies the node into a temporary tree before `FileStorage(root)`. The paste path
creates a temporary node tree, removes or replaces the destination, and only
then constructs storage.

For a project with one live `.mwf/run.json`, no database, and no pending layout
conversion, `ensure_runtime_layout()` correctly skips its layout liveness
refusal. Either clipboard command can therefore change files before the later
storage constructor refuses SQLite initialization. This violates the approved
loading order for a mutating command that would initialize and import storage.

Both clipboard commands now run the missing-database preflight after validating
the requested source and before their first write. The focused RED shows the
copy temporary tree and replaced paste destination. The corrected cases retain
both trees byte for byte. Completed-database preservation checks keep both
operations usable for an established project with a current live record.

### LL-COMP-004: process worker graph import ran before the guard

Resolved test-first.

`runners/process.py::_init_process_worker()` imported and executed the project
graph before its later `MicroWorkflow` construction reached storage preflight.
The RED records the graph sentinel written before refusal. The worker now runs
the missing-database preflight before graph import. Its preservation case keeps
the supported completed-database worker path and confirms its workflow still
loads.

### LL-COMP-005: initialization needed an archive-content recheck

Resolved test-first after the safety reviewer found the gap.

An archive could introduce only a current live run record. With no old-layout
artifact, the conditional layout check allowed initialization to create
sidecars before the later missing-database refusal, or to complete when the
archive contained a database. `init_project()` now repeats the layout-level
live and two-record refusal immediately after extraction. The matrix covers a
live current record with and without an archived database, and keeps the
already-protected two-terminal cases. Exact snapshots permit the requested
extraction and detect every later MWF effect.

### LL-COMP-002: mixed valid and invalid raw records

Resolved by test correction. Source behavior was already correct.

Every current two-path malformed-data case makes both paths invalid. The
approved preparation separately requires a valid terminal root record with a
malformed current record, a malformed root record with a valid live current
record, and a non-object on either side while the other side is valid. These
cases establish that a valid earlier record does not hide a later error and
that an early error does not prevent inspection of a later valid record.

The revised test uses all four mixed orientations with exact tree preservation
and path assertions. The existing dangling-link matrix exercises reader,
layout, migration, and initialization wiring. `legacy-loading-preserve-04.log`
records the resulting focused preservation run.

### LL-COMP-003: object-only validity on direct storage

Resolved by test correction. Source behavior was already correct.

The earlier `{}` preservation case called only the CLI layout helper. The
revised test now exercises direct `FileStorage` with `{}` in each raw location,
creates the missing database, and preserves the raw bytes. This detects any
later attempt by the storage guard to require fields that the deferred import
must validate instead. Together with LL-COMP-002, the additions produce 60
focused cases passing in 4.05 seconds in
`legacy-loading-preserve-04.log`.

### Pre-stage database-entry correction

Resolved before this stage review began. The initial storage guard used
`lexists(state.sqlite3)` and treated a directory or dangling link as an existing
database. The directory reached SQLite and the dangling link allowed creation
at its missing target. `legacy-loading-review-red-01.log` records both failures.
The selected source now requires `state_database_file(root).is_file()`.
`legacy-loading-review-green-01.log` records 144 passing cases in 14.31 seconds.

### LL-COMP-006: external-wait completion used a stale clock sample

Resolved test-first during the required ordinary gate.

The first ordinary run produced one real `JobTimeoutError` in the retained
100-request watchdog test. The persisted job output and traceback show a
checkpoint timeout after `model request completed`; this excludes initial
checkpoint, transport-lease, and total-timeout paths. The complete focused
watchdog module then passed, so the dense failure was intermittent.

Source review found that `end_external_wait()` sampled monotonic time before
acquiring the supervisor condition. Contention for that shared condition could
therefore consume the renewed checkpoint interval and publish an already-old
deadline. A controlled real-lock interleaving advanced only an injected clock
while the end-wait worker was blocked. The old source recorded a deadline
exactly one second earlier than expected and failed the new assertion in 0.68
seconds in `legacy-loading-network-lock-red-01.log`.

The correction samples monotonic time while holding the condition and only
when the outermost wait ends. It does not change the checkpoint interval,
transport lease, total deadline, timeout policy, nested-wait behavior, or
completion rule. The test retains the real lock and verifies that the handler
receives the full 0.02-second interval after acquisition. The separate
check-to-handler-complete scheduling race was not isolated by the failure, so
no new completion policy was added.

The new interleaving, the complete watchdog module, checkpoint keyword API,
and retained queue/transport scaling checks then passed all 28 cases in 25.15
seconds in `legacy-loading-network-lock-green-01.log`.

## Recorded test relevance

The recorded RED/GREEN sequence is behavior-sensitive for aggregated structural
errors, two-record preservation, direct storage and workflow construction,
same-host and other-host liveness, reporting every live owner, graph-import
ordering, current-only invalid records, existing-database validation before
SQLite access, recycled PIDs, and non-file database entries. The tests compare
whole managed trees around refusals and verify the graph sentinel or
configuration write did not occur.

The first established-workflow fixture failure and the first adjacent selection
error are setup failures and supply no behavioral evidence. They must stay
excluded from accepted verification.

Root reports 54 cases in the initial new module, 144 passing cases in the
latest combined focused run, and 60 passing cases after the LL-COMP-002 and
LL-COMP-003 additions. `legacy-loading-review-red-02.log` then records five
expected late-preflight failures and two retained passes. The corrected test
070 plus retained migration module pass 84 cases in 6.86 seconds in
`legacy-loading-review-green-02.log`. I inspected those sources and records.

My first independent selection recorded 104 passes and one failure in 35.21
seconds. Windows refused to remove a temporary mutation-writer diagnostics file
after an existing reliability test had otherwise completed. The test explicitly
closed database connections, but that close does not join the writer. The writer
resolves the awaited mutation before its final diagnostics publication, leaving
a short cleanup race. Exact accepted-base and candidate copies each passed the
same case in three separate processes. The new preflight does not enter that
writer path. This first selection also let the test's direct
`TemporaryDirectory` use the system temporary folder, so it cannot serve as the
required isolated Test Area result. It remains recorded as a classified failed
attempt.

The narrow harness correction replaces that test's immediately deleted
`TemporaryDirectory` with pytest's `tmp_path`. It retains every status and
streaming assertion and the explicit connection close. The test does not depend
on a private writer-thread join or imply a new shutdown guarantee. This change
gets no runtime requirement credit.

The corrected independent selection passed all 105 cases in 31.00 seconds. It
ran test 070, retained migration and Windows liveness, ordinary clipboard, and
the complete reliability module. `legacy-loading-compatibility-02-manifest.json`
records the native declared-test interpreter, exact source directory, the nine
then-selected file hashes, and the test selection. Its companion command and
environment records show
selected-only `PYTHONPATH`, disabled pytest cache and bytecode, and unique Test
Area locations for pytest, `TEMP`, and `TMP`.

## Verification and disposition

After LL-COMP-006, the complete ordinary suite passed 551 tests with the marked
stress test deselected in 452.87 seconds. The four required cycle cases then
passed in separate processes in 3.96, 9.81, 8.41, and 3.98 seconds. These runs
used the native declared-test interpreter, selected-only `PYTHONPATH`, disabled
pytest cache and bytecode, and separate Test Area locations for pytest,
`TEMP`, and `TMP`. The eleven final hashes are recorded in
`legacy-loading-selected-files-final.json` and match the direct MWF files.

The bounded network comparison used the selected source against the same source
with only the watchdog clock correction reverted. All six runs completed 100
jobs with no residual work and an intact database. Baseline and candidate
median times were 10.7975345 and 9.3287964 seconds, a candidate ratio of
0.863974679. The immutable manifest SHA-256 is
`6BDBCCF0DEF939B686989F5D6D301322C25024F0258F065B127D39B3C3702085`.

The bounded initializer comparison completed all 2,400 store checks. Creation
medians were 2.2472286 seconds for the accepted base and 2.3871443 seconds for
the candidate, a ratio of 1.062261445. Reopen medians were 0.6803688 and
0.6846174 seconds, a ratio of 1.006244554. The immutable manifest SHA-256 is
`9B230783345385624A73ECA3C06957DA360BDA03A6AE4F574E5E07ABED22D417`.

The stage record, operations guide, test-module description, and AQ1/AQ4 text
match the selected behavior and its limits. The requirement ledger must retain
44-SES-033 as pending and may give 44-SES-034 only additional partial credit for
this stage. Its disposition must continue to name AQ1 for an existing but
incomplete database and AQ4 for an older process starting after observation.

AQ1 remains the exact boundary for an existing but incomplete database.
AQ4 remains the exact check/start race. No additional architectural question is
needed for the source and caller findings above.

Current disposition: **PASS for the selected legacy-loading and
retained-runtime section only**. The source observes both raw run locations at
the first affected boundaries, preserves ambiguous records, refuses every
observed live owner before missing-store creation, and retains completed-store
behavior. The controlled failures are sensitive to the corrected source, and
the focused, independent, ordinary, cyclic, network, and constructor checks
pass.

This disposition gives no completion credit to 44-SES-033, does not complete
44-SES-034, and does not accept S2 or the release. AQ1 and AQ4 remain the exact
unresolved boundaries described above.
