# Legacy loading preflight and checkpoint renewal

Status: accepted within the boundary below as part of
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).
The base is accepted commit `a9167c79bebfadec64a73ae92fbd68d07c00e6d3`.

## Boundary

This extends the [accepted migration preflight](stage-migration-guard.md) under
44-SES-034. A neutral filesystem reader inspects both exact raw run paths before
reporting structural errors. It accepts JSON objects without inventing required
legacy fields. Invalid UTF-8, malformed JSON, non-objects, directories, and
links refuse with the affected paths. Neither source takes precedence.

Applied migration and initialization, and layout conversion when changes are
pending, refuse whenever both run records exist. This includes identical bytes.
The operation preserves both records; it does not choose an import result.
Ordinary layout inspection always validates both paths, but only invokes the
live-owner/layout refusal when layout work is pending. An established live
project with one valid current record therefore remains loadable.

Direct storage creation validates both raw records before base initialization,
broker setup, or SQLite access. When no database file exists, it refuses every
observed live owner using the retained host, process-instance, and heartbeat
rules. A directory or dangling database link does not bypass that guard; the
database-path predicate means the path resolves to a file. Direct workflow
construction shares this entry. Graph loading, setup, process-worker imports,
and clipboard copy/paste invoke the same preflight before configuration,
user-code, or filesystem effects. Initialization repeats the unconditional
migration check immediately after deployment extraction and before later
initialization writes, including when the archive supplies a database. The
requested extraction itself remains visible. Direct creation may preserve two
valid non-live records because it does not collapse the paths.

Existing SQLite completeness remains outside this section. A database file's
presence does not establish that schema setup or legacy import finished.
Inspecting that fact can itself alter SQLite coordination files before refusal.
After checking source and specification, root re-read both complete preparation
transcripts as the final check and recorded this dependency under
[AQ1](architectural-questions.md#aq1-sqlite-coordination-during-read-only-previews).
No probe or substitute completion rule was added. [AQ4](architectural-questions.md#aq4-excluding-older-processes-during-migration)
still covers an older process starting after observation. Complete 44-SES-034
and legacy session import remain pending.

The required ordinary checks also exposed a retained checkpoint-timing defect.
The network completion path sampled its renewed deadline before acquiring the
shared supervisor condition. Contention could therefore spend the handler's
next checkpoint interval while the framework still held up its return. The
correction samples the clock inside the owned condition, at deadline assignment.
It preserves the configured interval, nested waits, total deadlines, and existing
timeout/completion decisions. This gate repair adds `supervisor_attempts.py`
and its watchdog regression to the loading section; it adds no session behavior.

## Test-first record

Tests run in the isolated `legacy-loading` export under
`C:/Business/product/test_area/mwf-062-issue45-20260904`, using the existing
declared-test `source/.venv/Scripts/python.exe`. `PYTHONPATH` names only that
export; pytest cache and bytecode writes are disabled. Each command has a fresh
temporary directory. The local helper `run_legacy_loading_check.py` records
commands, logs, and JUnit files in `testing_ground/issue-45` in the Parent Repo.
Final independent and broad runs also set `TEMP` and `TMP` to their own Test
Area directory, keeping tests that use Python's `tempfile` library there.

| Behavior | Recorded result |
| --- | --- |
| Existing storage and one-record layout behavior | `legacy-loading-preserve-01`: 20 passed before implementation, including the accepted migration module. |
| Both invalid paths | `legacy-loading-red-01`: five failures exposed early diagnostic termination, invalid UTF-8 escape, and omitted directory/dangling entries. The shared reader produced 30 passes with migration and Windows identity checks in `legacy-loading-green-01`. |
| Preserve both valid records | `legacy-loading-red-02`: six cases wrongly proceeded and nine already-refused cases omitted one path. The matrix covers layout, applied migration, and archive initialization. `legacy-loading-green-02`: 45 passed. |
| Established workflow opens | The first added fixture omitted the required working directory and is excluded as setup failure. Corrected `legacy-loading-preserve-02b`: 29 passed, including established live workflow loading and two non-live records preserved by direct creation. |
| Direct creation | `legacy-loading-red-03`: ten missing refusals across storage/workflow, both raw paths, another host, and two live owners. `legacy-loading-green-03`: 129 passed including session foundations and retained guards. |
| Before graph effects | `legacy-loading-red-04`: graph loading wrote a sentinel, graph setup also rewrote configuration, and layout missed three invalid-path cases. Three other applied structural cases already passed. Correct call ordering produced 136 passes in `legacy-loading-green-04`. |
| Caller sensitivity | `legacy-loading-preserve-03`: 52 passed, adding existing-database structural refusal before SQLite access and direct-creation recycled-PID behavior. |
| Non-file database entry | `legacy-loading-review-red-01`: a directory reached SQLite and a dangling link allowed creation at its missing target. The guard now requires a path that resolves to a database file; `legacy-loading-review-green-01`: 144 passed in 14.31 seconds. |
| Mixed records and minimal objects | `legacy-loading-preserve-04`: 60 passed in 4.05 seconds, including four valid/invalid pairs and direct creation preserving `{}` at either raw location. |
| Established caller behavior | Before caller corrections, `legacy-loading-preserve-05`: three passed in 0.81 seconds. Both clipboard commands and process-worker initialization still operate with a complete database and valid live current record. |
| Archive, clipboard, and process effects | `legacy-loading-review-red-02`: five sensitive failures in 1.03 seconds. Init wrote sidecars after extraction without a database, and proceeded with an archived database; copy/paste changed existing trees; process initialization executed the graph sentinel. Two terminal-record archive cases already passed and are preservation coverage. Guards at the first affected boundary produced 84 passes in 6.86 seconds in `legacy-loading-review-green-02`. |
| Checkpoint renewal under lock contention | `legacy-loading-network-lock-red-01`: one failure in 0.68 seconds. The test blocks a real condition acquisition and advances an injected monotonic clock; the old deadline is exactly one second stale. Sampling after lock acquisition yields 28 passes in 25.15 seconds in `legacy-loading-network-lock-green-01`, including network waits, total timeouts, checkpoint keywords, and queue scaling. |

## Verification and review

The adjacent selection before the final caller fixes passed 192 cases in
78.37 seconds (`legacy-loading-adjacent-02`). Its first attempted selection
named two absent test files, collected nothing, and is excluded as setup error.
The first independent compatibility run passed 104 tests but failed while the
reliability test immediately deleted its temporary project. Its explicit
connection close does not wait for the writer's final advisory diagnostic
publication. The observed Windows file-handle error matched that existing
ordering; the accepted base and candidate each passed three isolated repeats.
Those repeats inherited system `TEMP` and are diagnostic observations only.
The affected test now uses pytest's `tmp_path` lifetime and retains every
behavior assertion and explicit connection close. This is a test-lifetime
correction, with no new runtime shutdown guarantee or requirement credit.
The first ordinary run passed 549 tests, deselected one stress case, and failed
the retained 100-job networking test after a request completed
(`legacy-loading-ordinary-01`, 449.89 seconds). The complete network module then
passed nine cases in 11.46 seconds without a source change. Instrumented versions
at 1, 10, and 100 jobs also passed; the 100-job sample observed an external-wait
completion call taking 15 milliseconds and leaving five milliseconds of the
20-millisecond interval. That observation does not isolate lock time from
scheduling time. The controlled RED above establishes the stale-clock defect
without relying on another incidental timeout.

The complete repeated gates use `legacy_loading_final_checks.py 02`. The
manifest `legacy-loading-final-checks-manifest-02.json` records each command,
working directory, and environment. All checks ran sequentially.

| Check | Result |
| --- | --- |
| Ordinary suite, excluding the cyclic module | 551 passed, one stress case deselected, 452.87 seconds. |
| Self and mutual autostart cycle | One passed, 3.96 seconds. |
| Threaded diamond cycle | One passed, 9.81 seconds. |
| Threaded ring cycle | One passed, 8.41 seconds. |
| Stochastic game-engine cycle | One passed, 3.98 seconds. |
| Independent compatibility selection | 105 passed, 31.00 seconds, including the corrected reliability fixture. |
| Independent safety selection | 93 passed, 13.96 seconds, including process-runner and process-checkpoint cases. |
| Checkpoint correction and adjacent cases | 28 passed, 25.15 seconds; both reviewers also examined the controlled regression and correction. |

The sustained scheduler stress case was not selected for these loading checks
and the bounded checkpoint-renewal correction. Packaging and example execution
remain outside this section.

## Bounded comparisons

Both comparisons passed their predeclared candidate-to-baseline median limit
of 1.20. The source remained frozen through the ordinary, cyclic, and measured
runs. Commands, interpreter and dependency versions, full Python source hashes,
program hashes, raw samples, and correctness fields are recorded in the Parent
Repo under `testing_ground/issue-45`.

The network comparison uses `legacy_loading_network_benchmark.py` and the
common `network_completion_sample.py`. It calls the original 100-job test with
its unchanged 20-millisecond checkpoint interval. The baseline is the selected
loading tree with only `supervisor_attempts.py` reverted to the accepted base;
every other file matches. Six fresh processes alternate three pairs. Imports
and the additional integrity check are outside the measured workload.

| 100-job network workload | Seconds |
| --- | --- |
| Baseline samples | 11.516265, 10.797534, 9.574008 |
| Candidate samples | 9.333935, 9.328796, 9.026665 |
| Baseline and candidate medians | 10.797534, 9.328796 |
| Candidate-to-baseline median ratio | 0.863975 |

All six samples have exactly 100 done jobs, no other job outcomes, and SQLite
integrity `ok`. These are local comparison results, not a general throughput
claim. The immutable `legacy-loading-network-manifest.json` has SHA-256
`6BDBCCF0DEF939B686989F5D6D301322C25024F0258F065B127D39B3C3702085`.
The full values are in `legacy-loading-network-summary.json` and six raw logs.

The constructor comparison uses `legacy_loading_initialization_benchmark.py`
and the unchanged `session_initialization_sample.py`: 200 real version-4
constructors per creation/reopening sample, three alternating pairs, with
checks and finalization outside timing. Its baseline is an exact archive of
the accepted base. Every candidate file outside the eleven selected runtime
and test files plus the updated test README matches that archive.

| Phase, 200 stores per sample | Baseline seconds | Candidate seconds | Median ratio |
| --- | --- | --- | --- |
| Create | 2.247229, 2.394627, 2.064218 | 2.499121, 2.387144, 2.172169 | 1.062261 |
| Reopen | 0.670175, 0.692646, 0.680369 | 0.688382, 0.683862, 0.684617 | 1.006245 |

The creation medians are 2.247229 and 2.387144 seconds; reopening medians are
0.680369 and 0.684617 seconds. All 2,400 store checks passed: schema version 4,
none of the three private session tables, integrity `ok`, unchanged source
file, and no run JSON. The immutable
`legacy-loading-initialization-manifest.json` has SHA-256
`9B230783345385624A73ECA3C06957DA360BDA03A6AE4F574E5E07ABED22D417`.
Its summary and twelve raw sample logs retain the exact values.

## Review coverage

Two GPT-5.6 Sol reviewers with xhigh reasoning cover data preservation and
initialization ordering, and retained liveness, caller compatibility,
specification, and test sensitivity. Both expanded their review to the
checkpoint correction, the controlled regression, the measured programs,
the final eleven-file selection, and the recorded AQ1/AQ4 limits. Both the
[safety review](legacy-loading-safety-review.md) and
[compatibility and specification review](legacy-loading-compatibility-review.md)
are PASS for this section. Full S2, migration, runtime activation, and the
final release review remain pending.
