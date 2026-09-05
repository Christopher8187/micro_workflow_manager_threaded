# Legacy-loading safety review

Status: **PASS for the selected eleven-file legacy-loading and retained-runtime section.**

I reviewed the accepted base `a9167c79bebfadec64a73ae92fbd68d07c00e6d3`, the complete preparation record, the final behavioral history, the selected source beyond the changed lines, the corrected tests, and the current stage and architecture records. This is the independent safety and data-preservation review assigned under [Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45). It covers only the filesystem-observable part of 44-SES-034 resolved by [Settle the MWF workflow-management model for 0.6.2](https://github.com/Christopher8187/product/issues/44): inspect both raw run locations, refuse a live older owner before creating a missing store, and preserve both records when layout work would collapse them.

## Source disposition

I found no remaining source blocker in the selected boundary.

- `read_legacy_run_records()` examines the root and current paths in stable order without choosing one. It uses `lstat`, rejects directories and link/reparse entries, attempts both reads before raising, and aggregates I/O, UTF-8, JSON, and non-object failures. It adds no legacy field requirements and makes no filesystem change.
- Layout inspection validates both paths on every call. When old layout work is pending, it refuses every two-record combination and every observed live record before a move, deletion, directory creation, or lock cleanup. The conditional liveness call preserves ordinary use of an established current-layout project whose live run is already backed by a database.
- `FileStorage._initialize_storage()` invokes the neutral preflight before base initialization, publisher/broker setup, SQLite access, legacy import, or lock cleanup. `MicroWorkflow` reaches storage first, so direct workflow construction inherits that ordering. When the database path resolves to a file, this section makes no additional liveness decision; malformed raw records still refuse structurally before SQLite access.
- `load_workflow()` and `setup_graph()` preflight before reading configuration, executing the project graph, updating `sys.path`, rewriting configuration, synchronizing node folders, or constructing a workflow. Their sentinel cases make the before-user-code boundary observable.
- Clipboard copy validates the requested source and then preflights before creating a temporary tree, copying files, opening storage, replacing a prior clipboard tree, or updating editor settings. Paste follows the same order before creating its temporary tree, replacing the live node, importing SQLite state, or updating settings. Invalid source requests retain their prior error ordering.
- A process worker resolves its supplied paths and then preflights before graph import, `sys.path` changes, graph side effects, or `MicroWorkflow` construction. Its established-database case still reconstructs the worker normally.
- Initialization checks the destination before archive resolution and extraction. After an authorized archive extraction, it checks again immediately, before layout conversion, sidecars, configuration writes, or storage. Thus an archive can contribute the state that causes refusal without permitting later generated effects.
- `active_run.run_state_liveness()` retains its module-level process and identity probes when it delegates to the shared liveness calculation. The existing monkeypatch seam and recycled-PID behavior therefore remain intact.
- The stage record, operations guide, and test matrix describe the same boundary: post-extraction refusal, preservation of dual raw records, checks before clipboard and process-worker effects, the existing-database compatibility case, and the unresolved AQ1/AQ4 cases. They do not claim complete legacy-session migration.

The stage adds no new loading authority and no alternate persisted state. Every decision comes from the two exact raw JSON paths, the established liveness algorithm, and whether the database path resolves to a file.

## Resolved review finding

The first reviewed source checked `init_project()` only before archive extraction. A deployment containing a consolidated live current record could therefore create sidecars before the later storage refusal, or complete initialization when it also contained a database. Clipboard copy and paste likewise changed trees before their eventual `FileStorage` preflight, and a spawned process worker executed graph code before constructing `MicroWorkflow`.

`legacy-loading-review-red-02` recorded five sensitive failures: two live-archive forms, copy, paste, and process-worker graph import. The two terminal-record archive forms already refused and preserved their records. The corrections placed the shared check at the three required boundaries. `legacy-loading-review-green-02` then passed all 70 cases in `test_070_legacy_run_preflight.py` plus the 14 accepted migration cases: **84 passed in 6.86 seconds**. The archive snapshot assertions permit the requested extracted content and require no generated sidecars, SQLite files, layout changes, or metadata import afterward. Clipboard snapshots cover both a new temporary tree and replacement of existing node content. The worker test observes graph-file and process-global effects.

The neutral diagnostic was also corrected to say that legacy records cannot be inspected, rather than attributing every caller to migration.

## Broad-gate runtime correction

The first ordinary run passed 549 cases and failed the existing 100-job framework-network-wait stress case when one job received a false 20 ms checkpoint timeout. A deterministic test then held the real supervisor condition while `end_external_wait()` tried to acquire it and advanced an injected clock during that wait. The previous code sampled the clock before acquiring the condition, so the renewed deadline was exactly one second stale in `legacy-loading-network-lock-red-01`.

The correction samples the monotonic clock while holding the condition at the deadline assignment. It excludes only the demonstrated framework lock wait from the handler's renewed checkpoint interval. It leaves total timeouts, checkpoint thresholds, external-wait nesting, state transitions, scheduling, and completion rules unchanged. The focused correction run passed 28 networking, checkpoint, and watchdog cases in 25.15 seconds. I reviewed the lock and state ordering and find the correction sound. The quiet broad rerun and the original uninstrumented 100-job comparison both passed.

## Source identity

The selected export and direct MWF copies matched byte for byte at this review point:

| File | SHA-256 |
| --- | --- |
| `micro_workflow_manager/legacy_runs.py` | `3C5ABE36E6BC955D7DC80CAACB8EBEC425B69467AE794CB91BD8E9B5D6F68773` |
| `micro_workflow_manager/cli/active_run.py` | `915E511B70BA3791433F101377C32EFE3BDB2D6C1BACC3DCAEBD147AD115D7EF` |
| `micro_workflow_manager/cli/layout.py` | `4F19E7FA82B443C4BDB3AAC219430512AC1727C3AD8D5A444706D3D76DC5E76C` |
| `micro_workflow_manager/cli/project.py` | `475322F419B231390171F2E588676047497DA4E06E2E26A80B2DC5195285D44C` |
| `micro_workflow_manager/storage/filesystem.py` | `D635FF53070C3D31E46BFBBA15CDBB031DC8C5F2596B5FC5FA1DAFF9AC53CAD1` |
| `micro_workflow_manager/cli/node_clipboard.py` | `4B25489540AF67FEE5E3AD9527DC0641640DAE124F20956C357D9B3DC93EE223` |
| `micro_workflow_manager/runners/process.py` | `D33D2EE223C143904FFABD420FBB772A63F5F8DF75E20A651C4D6A73CD207591` |
| `tests/test_070_legacy_run_preflight.py` | `F4D8D76A8F2C6B0C60EAFDEC833079E0EC7B4CF4881D6B9B5491AC7D4E60935D` |
| `tests/test_reliability.py` | `D9137ED89613A3CD6E72F45071A1499DD2153357D1FFF518D2012E070C765915` |
| `micro_workflow_manager/workflow/supervisor_attempts.py` | `EF6DFC163E725DF35528B3323643292FD61675A23688386A4C90933F15D032A9` |
| `tests/test_043_watchdog_networking.py` | `FEC5938B78F599B425396E1B82B00450CE67E462C204C9581E32F967D1E38314` |

## Evidence reviewed so far

The valid test-first record covers aggregated malformed inputs, invalid UTF-8, directories and dangling links, the full dual-record layout matrix, direct storage and workflow creation, both raw locations, same-host and other-host liveness, recycled process identity, graph effects, archive effects, clipboard effects, process-worker effects, and non-file database entries. The latest earlier combined run passed 144 cases in 14.31 seconds. The corrected adjacent selection passed 192 in 78.37 seconds. The first adjacent command selected no tests and remains excluded. The latest focused correction run is the 84-case result above.

My independent selection used a unique Test Area location for `TEMP`, `TMP`, and pytest's base directory. It passed the full new and migration files, the Windows process-signal checks, deployment initialization and clipboard behavior, the real child-process runner, and its checkpoint watchdog: **93 passed in 13.96 seconds**. The command, environment, source hashes, log, and JUnit record are stored as `legacy-loading-safety-independent-01-*`.

The first 105-case compatibility run had one Windows cleanup race after every behavioral assertion passed: the test used a system temporary directory, and its asynchronous diagnostic writer still held a temporary file while `TemporaryDirectory` removed the tree. The test-only correction replaced that directory with pytest's `tmp_path` while retaining the assertions and explicit connection close. It does not alter MWF runtime code. The accepted base and candidate each passed three isolated runs, and the final combined compatibility run passed; the combined result was **105 passed in 31.00 seconds** with Test Area `TEMP` and `TMP`. I reviewed the correction and find it appropriate.

The corrected ordinary run passed **551 cases with one designated stress test deselected in 452.87 seconds**. Four required fresh-process cyclic runs passed in **3.96, 9.81, 8.41, and 3.98 seconds**. The ordinary result includes the new deterministic watchdog case.

The network comparison used the complete selected tree for both sources and reverted only `supervisor_attempts.py` in the baseline. All six fresh processes finished exactly 100 A jobs with no queued, running, failed, skipped, or cancelled residue and an intact database. Baseline samples were **11.516265, 10.797534, and 9.574008 seconds**; candidate samples were **9.333935, 9.328796, and 9.026665 seconds**. The median ratio was **0.863974679**, below the predeclared 1.20 limit.

The separate constructor comparison exercised 200 real version-4 stores per sample in three alternating pairs for both creation and reopening. The create median ratio was **1.062261445** and the reopen ratio was **1.006244554**, both below 1.20. All **2,400** store checks retained version 4, database integrity, source content, absence of session tables, and absence of raw run state. The immutable network and initialization manifests have SHA-256 values `6BDBCCF0DEF939B686989F5D6D301322C25024F0258F065B127D39B3C3702085` and `9B230783345385624A73ECA3C06957DA360BDA03A6AE4F574E5E07ABED22D417`.

## Disposition

The selected section passes independent safety review. Its checks precede the first affected mutation or user-code effect, retain established active-project use, preserve ambiguous dual raw records, and add no second state authority. The test-first failures are sensitive to the corrected ordering, and focused, independent, adjacent, ordinary, cyclic, networking, and constructor checks all pass.

This PASS is limited to the eleven files and the partial 44-SES-034 behavior described above. It does not accept the rest of S2 or the release.

## Deliberate limits

This section does not inspect an existing database for completed initialization. AQ1 records that opening live WAL state through ordinary read-only SQLite can create sidecars or change shared-memory coordination before a refusal. A present but incomplete database beside a live raw owner therefore remains unresolved; database-file presence is preserved only as the narrow compatibility boundary for established projects.

AQ4 continues to cover an older process starting or replacing a raw record after observation. Repeated checks narrow that interval but do not exclude it. The reader creates no execution-session row, migration result, reservation, hold, or ownership record and does not decide how two valid records later map to sessions. Full 44-SES-034 remains pending on those architectural decisions.
