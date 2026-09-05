# Fresh-project SQLite session storage

Status: accepted within the boundary below. This is an independent storage
section of [Implement and verify the agreed MWF 0.6.2 workflow-management
changes](https://github.com/Christopher8187/product/issues/45).

## Boundary

The selected base is accepted commit
`a5de6873f2bc687d9c7274149108307677fc518c`. The section adds durable session
creation, exact reads, conditional heartbeat and completion, parent identity,
selected components and jobs, failures, and named live readers. The main-slot
constraint applies to persisted running rows. A stale row stays recorded until
recovery gives it a terminal outcome. Multiple interrupts may have records;
these records alone are not reservations or authority to execute.

Schema version 5 stores sessions. Ordinary storage construction still creates
or upgrades only through version 4. It recognizes an existing version 5 store.
Only the internal fresh initializer creates version 5. It rejects existing MWF
runtime entries, including legacy node metadata, before creating state. It
writes no project configuration or run JSON. Source and user output files may
already exist.

Preparation initially proposed a public factory. Before implementation, source
review showed that existing CLI and workflow callers could then ignore the
registry and write a second JSON session source. The initial missing-public-API
RED was replaced by a missing-internal-initializer RED. The creation entry stays
internal until runtime activation updates admission, heartbeat, completion, and
dependent callers before router or preparation writes. This follows the settled
SQLite-only requirement; it introduces no new architectural decision.

The section does not activate CLI sessions, import older projects, reserve
components, transfer ownership, create holds, or change job claiming. AQ1 through
AQ4 and their dependent integration remain in
[architectural-questions.md](architectural-questions.md). Full `44-SES-031` and
`44-SES-032` remain pending, along with runtime ownership and recovery.

## Test-first record

The isolated copy is
`C:/Business/product/test_area/mwf-062-issue45-20260904/session-foundation`.
It contains the accepted base plus this section's selected executable changes.
Unfinished preview changes are excluded. The interpreter is the declared-test
environment at `source/.venv/Scripts/python.exe` in the same Test Area run,
Python 3.12.14 with SQLite 3.53.1. `PYTHONPATH` names the selected copy and pytest
cache is disabled. Each command uses a fresh `--basetemp`.

All logs below are under `testing_ground/issue-45` in the Parent Repo.

| Section | Expected failure and correction |
| --- | --- |
| Preservation | `session-preserve-01.log`: ordinary version 4 behavior, legacy payload migration, future-version refusal, and connection finalization passed 4 cases before implementation. |
| Creation and exact reopening | `session-red-01b.log`: missing internal fresh initializer, 1 failed and 1 passed. `session-green-01.log`: 2 passed. Main and interrupt fields, parent links, component and job order, and nested details survive reopening. |
| Conditional lifecycle | `session-red-02.log`: missing heartbeat update, 1 failed and 2 passed. `session-green-02.log`: 3 passed. A terminal record survives late heartbeat and completion attempts without changing another session. |
| One running main | `session-red-03.log`: two separate processes both created main sessions. The SQLite uniqueness rule made `session-green-03.log` pass all 4 cases. Staleness does not silently free the main slot. |
| Freshness refusal | The first placement of the new test accidentally entered a child-process script; that setup failure is excluded. Corrected `session-red-04b.log` records 16 missing refusals and 7 passes. After the guard, `session-green-04.log` records 23 passes with exact before/after filesystem checks. |
| Named readers | `session-red-05.log`: missing live-main reader, 1 failed and 28 passed including retained Windows identity checks. `session-green-05.log`: 43 passed including legacy migration. Main precedence, sole interrupt, no live session, sorted ambiguity, stale and terminal history, and recycled process identity are covered. |
| Creation validation | `session-red-06.log`: 17 invalid-input cases failed. Sixteen accepted malformed data; the kind constraint already rejected one through a database exception. Explicit validation made `session-green-06.log` pass 41 cases. |
| Existing atomicity | `session-preserve-02.log`: 46 passed after adding mutation-lane, partial-child rollback, parent foreign-key, interrupted-DDL, version 4 refusal, and user-file preservation checks. Review later found that reopening failed fresh state as version 4 did not establish the required fresh retry. The corrected checks are below. |
| Update validation | `session-red-07.log`: malformed heartbeat, finish time, and failure-object cases failed; empty outcome already refused. `session-green-07.log`: 50 passed after validation. |

The focused command is `python -m pytest -q tests/test_069_execution_sessions.py`
with the environment and fresh base directory above. The Windows identity and
legacy migration modules join the named-reader check. Before the later review
corrections, the surrounding SQLite, contention, fan-out, live-component,
Windows identity, finalizer, and migration selection passed 109 tests in 113.88
seconds. The final broader checks are recorded below.

## Review corrections

Both reviewers inspected code and tested behavior beyond the changed storage
module. Their findings produced these further checks and corrections:

| Finding | Failure, correction, and focused result |
| --- | --- |
| Retry after failed DDL | `session-review-red-01.log` found the attempt-owned state directory still present. Cleanup now closes its handles, checks directory identity and database ownership, removes its failed state, and permits a fresh version 5 retry. The full selection passed 50 cases in `session-review-green-01.log`. |
| Incomplete version 5 | `session-review-red-02b.log` rejected none of four damaged schemas. Validation now checks exact session declarations and both metadata markers before any legacy import. Missing uniqueness, malformed child shape, missing legacy marker, and missing version marker all refuse. An extra rule allowing only one interrupt was then caught by `session-review-red-08.log`; exact declaration equality fixed that omission. |
| Supported Python methods and real links | `session-review-red-03.log` recorded four failures and two retained passes with newer APIs removed. Creation and cleanup now use the older `lstat` reparse information and exception chaining. Real dangling and node-directory junction cases passed. These are compatibility simulations under Python 3.12, not a complete Python 3.10 interpreter run. |
| Actual parents | `session-review-red-04.log` accepted self-parenting and a parent on a main session. Only an interrupt may have a parent, and that parent must be a different session. API validation, a SQLite constraint, and the existing parent foreign key enforce this. |
| A competing ordinary initializer | `session-review-red-05.log` exposed both a cached same-process version 4 and a separate-process version 4. Fresh initialization now bypasses the ordinary initialization cache, refuses existing database state under the transaction, and preserves the other initializer's database. |
| Stale schema decisions | The first `session-review-red-06.log` passed on an insensitive interleaving and is excluded. The corrected `session-review-red-06b.log` pauses the ordinary initializer before its first write transaction, lets fresh version 5 commit, then releases the ordinary stale version 4 plan. It failed with session tables marked as version 4. Initialization now acquires its write transaction before schema reads and retains it through declarations and version publication. The combined focused selection passed 65 cases in `session-review-green-05.log`. |
| Deterministic selected order | `session-review-red-07.log` accepted four unordered outer collections. The store now requires ordered component and job sequences while canonicalizing each component's member names. The full selection passed 69 cases. |
| Failure after schema commit | `session-review-red-09.log` left the new state directory after object setup failed. Cleanup now recognizes its own committed version 5 as well as an empty rolled-back database, while preserving a competing non-v5 database. `session-review-green-08.log` passed all 71 focused cases in 8.67 seconds. |

The platform correction follows the documented availability of
[Windows file attributes in Python 3.10](https://docs.python.org/3.10/library/stat.html)
and [junction inspection in Python 3.12](https://docs.python.org/3.12/library/pathlib.html#pathlib.Path.is_junction).

The private fresh creator requires an initialization directory that has not
been handed to runtime callers. The tested transaction prevents stale schema
publication and preserves an initializer that wins first. Cleanup inspects
committed state before deleting its own failed attempt. It does not establish
safe public activation while an unrelated process begins using that private
directory between cleanup inspection and file removal. Runtime activation must
settle and enforce its complete entry conditions before exposing this creator.
This section makes no broader concurrent filesystem-cleanup or older-process
exclusion claim.

## Verification

The final selected executable files match the direct MWF files. The ordinary
suite uses `session_final_checks.py` in the Parent Repo records directory.
It starts each cyclic case in a separate process after the ordinary suite.
`session-final-checks-manifest.json` records every command and fresh temporary
directory. The corresponding logs and JUnit files record:

| Check | Result |
| --- | --- |
| Ordinary suite, excluding the cyclic module | 480 passed, 1 stress case deselected, 467.66 seconds. |
| Self and mutual autostart cycle | 1 passed, 3.42 seconds. |
| Threaded diamond cycle | 1 passed, 11.30 seconds. |
| Threaded ring cycle | 1 passed, 8.08 seconds. |
| Stochastic game-engine cycle | 1 passed, 4.47 seconds. |
| Independent compatibility selection | 95 passed, 14.75 seconds; session, Windows identity, legacy migration, and advisory-lock recovery modules. |
| Independent storage-safety selection | 78 passed, 44.47 seconds; session, SQLite contention recovery, and finalizer-reentrancy modules. |

The marked sustained-scheduler stress case was not selected for this storage
initialization and inactive session-API section. The ordinary and fresh-process
cyclic checks exercise retained execution. Packaging and example execution
remain outside this section.

The bounded version 4 initialization comparison passed. Its common program
times 200 real `FileStorage` constructors per sample for creation and reopening,
with three alternating pairs. The predeclared limit is a candidate-to-baseline
median ratio no greater than 1.20 in each phase. Every store must retain version
4, omit all three session tables, pass SQLite integrity checking, preserve its
source file, and create no run JSON. Import paths, full source hashes, interpreter,
dependencies, commands, individual durations, and durable checks are recorded.
The repository workflow benchmarks begin timing after storage initialization,
so this comparison measures the affected path directly.

| Phase, 200 stores per sample | Baseline seconds | Candidate seconds | Median ratio |
| --- | --- | --- | --- |
| Create | 3.188310, 2.743379, 2.604501 | 2.695745, 2.731189, 2.481881 | 0.982637 |
| Reopen | 0.775060, 0.681781, 0.733148 | 0.664824, 0.757771, 0.696472 | 0.949975 |

The create medians were 2.743379 and 2.695745 seconds. The reopen medians were
0.733148 and 0.696472 seconds. All 2,400 per-store checks passed. These local
measurements establish this comparison's limit; they are not a general
throughput claim. `session-initialization-manifest.json` and
`session-initialization-summary.json` retain the exact values and source hashes,
with twelve raw sample logs. The baseline is an exact archive of the accepted
base. Every candidate file outside the seven selected runtime/test files and
the updated test README matched that archive, excluding interpreter caches.

## Review coverage

Two independent GPT-5.6 Sol reviewers with xhigh reasoning passed this section.
The [storage safety review](session-storage-safety-review.md) covers schema
installation, persistence, concurrency, and failure recovery. The
[compatibility review](session-foundation-compatibility-review.md) covers named
APIs, liveness, retained behavior, specification, and test sensitivity. Both
examined the internal initialization boundary and full preparation history.
All findings are resolved within the recorded scope. The compatibility review
lists the exact seven executable hashes. The immutable initialization manifest
has SHA-256
`316A0C28F13B89FCE506894D6A8425C1F680C2831BB4E1471452EF5B8F5F3175`.
The final staged whitespace check removed one trailing blank line from
`session_liveness.py`. Its Python AST is identical to the tested file. The
manifest retains the tested bytes; the compatibility review records the final
file hash. No behavior changed, so the executable checks were not repeated.
Full S2, runtime activation, migration, and the final release review remain pending.
