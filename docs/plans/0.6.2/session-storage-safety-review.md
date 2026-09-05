# SQLite execution-session storage safety review

Status: **PASS for the private storage foundation** in the assigned schema,
initialization, persistence, concurrency, and failure-recovery slice. I recommend
accepting this bounded section. This review does not accept runtime activation or
the complete session behavior in
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).

## Scope and sources

I reviewed the isolated executable copy at
`C:/Business/product/test_area/mwf-062-issue45-20260904/session-foundation`, based
on accepted commit `a5de6873f2bc687d9c7274149108307677fc518c`. The assigned files
are `storage/filesystem.py`, `storage/sqlite/connection.py`,
`storage/sqlite/schema.py`, `storage/execution_sessions.py`,
`session_liveness.py`, `cli/active_run.py`, and
`tests/test_069_execution_sessions.py`. SHA-256 comparison showed that all seven
selected files match the current direct MWF files.

Before making findings, I read the Parent Repo context map, MWF agent instructions,
the complete requirements and history for
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45)
identified by the preparation,
`session-foundation-preparation.md`, the four open architecture questions, and the
current operations and stage records. The applicable settled behavior is in
44-SES-001, 44-SES-002, 44-SES-030, the record portion of 44-SES-031, and the
SQLite-only portion of 44-SES-032.

## Disposition of source findings

The final selected source resolves every acceptance blocker found in this slice:

- Fresh creation now removes attempt-owned SQLite files and its empty `.mwf`
  directory after rolled-back DDL. A second creation can then build a valid v5.
- Reopening v5 requires both metadata markers and exact declarations for the three
  session tables and their associated schema objects. Missing, altered, and extra
  session rules refuse before ordinary legacy import can run.
- Session creation rejects invalid identity, parent, scope, order, and timestamp
  inputs before mutation. Only an interrupt can name an actual, distinct parent.
- Creation, heartbeat, and completion use synchronous priority-zero mutation-writer
  requests. A session and all component/job rows execute inside one request
  savepoint and one SQLite write transaction. A child-row failure therefore leaves
  no partial session. Heartbeat and completion update only the exact running ID.
- A partial SQLite uniqueness rule permits one persisted running main and several
  running interrupts. Because each process reaches it inside SQLite's write
  serialization, competing processes cannot both publish a running main.
- Schema choice is now made after `BEGIN IMMEDIATE`, and all fixed DDL statements
  plus both markers commit before the transaction is released. An ordinary opener
  waiting behind fresh creation reads the committed v5 instead of publishing a
  stale v4 decision.
- Fresh intent bypasses the process-local initialization cache. It refuses any
  database that another initializer published first. Failure cleanup distinguishes
  its own empty or v5 database from a recognized competing v4 and preserves the
  latter.
- If v5 commits but later in-memory object setup fails, cleanup closes registered
  and inspection handles, removes the attempt-owned v5 files, and permits a clean
  retry. Unexpected entries remain in place and make the directory removal refuse.
- The compatibility implementation uses `lstat` and Windows reparse attributes
  available in the supported Python range, and preserves the original initialization
  exception through cleanup failure.
- Mutable session fields are read in one row query. Component and job scope rows are
  immutable after their atomic insertion, so attaching them with later reads cannot
  mix two scope generations. Ordered outer collections are required; component
  members are canonicalized.

I found no further source blocker. The ordinary constructor still creates or
upgrades only through v4 and can validate and reopen an existing v5. Session methods
refuse on v4. The internal creator writes neither configuration nor run JSON, so it
does not expose two writable session sources through a supported entry point.

## Test sensitivity reviewed

The failure records exercise real missing behavior rather than only helper return
values. The most important review corrections are:

- `session-review-red-01.log`: failed DDL stranded attempt state.
- `session-review-red-02b.log` and `session-review-red-08.log`: damaged or extra v5
  declarations reopened.
- `session-review-red-03.log`: supported-Python and link handling failed.
- `session-review-red-04.log`: invalid parent relationships persisted.
- `session-review-red-05.log`: same-process cache and separate-process v4 races
  mishandled fresh intent or cleanup ownership.
- `session-review-red-06b.log`: the corrected deterministic interleaving published
  v5 tables as v4. `session-review-red-06.log` is excluded because its interleaving
  was insensitive and passed before the correction.
- `session-review-red-07.log`: unordered scopes were accepted.
- `session-review-red-09.log`: failure after schema commit retained v5 and prevented
  retry.

The accumulated focused result after these corrections is
`session-review-green-08.log`: 71 passed in 8.67 seconds. I then independently ran
the complete session module with the existing SQLite contention-recovery and
finalizer-reentrancy modules. `session-safety-independent-01.log` records 78 passed
in 44.47 seconds.

## Final verification

The completed records agree with the final source and support the PASS:

- The other independent reviewer ran the session, Windows process-identity,
  legacy-migration, and advisory-lock-recovery selection: 95 passed in 14.75
  seconds.
- The ordinary suite passed 480 tests with one sustained stress case deselected in
  467.66 seconds. The four cyclic modules then passed in fresh processes in 3.42,
  11.30, 8.08, and 4.47 seconds.
- The initialization comparison used three alternating baseline/candidate pairs
  and 200 stores per sample for both creation and reopening. All 2,400 durable
  checks retained v4, omitted all three session tables, passed SQLite integrity,
  preserved the source sentinel, and created no run JSON.
- The candidate-to-baseline median ratio was 0.982637 for creation and 0.949975
  for reopening, within the predeclared maximum of 1.20 for each phase. Constructor
  time alone was measured; imports, checks, and finalization were outside the timed
  interval.
- `session-initialization-manifest.json` has SHA-256
  `316A0C28F13B89FCE506894D6A8425C1F680C2831BB4E1471452EF5B8F5F3175`. It records
  the exact `a5de6873f2bc687d9c7274149108307677fc518c` baseline, all Python source
  hashes, selected-file hashes, helper hash, environment, dependencies, order, and
  threshold. Every candidate file outside the selected seven and the updated test
  README matched the baseline archive, excluding interpreter caches.

After the executable gates, the staged whitespace check exposed one trailing blank
line in the previously untracked `session_liveness.py`. Removing only that blank
line produced identical parsed AST dumps before and after. The direct and selected
files still match byte for byte at SHA-256
`B362224E3033A507E73ED9C705BED5CFEE7283DCE97A213A245F8881D91652A5`. The immutable
benchmark manifest correctly retains the tested pre-format hash. This formatting-only
change does not alter the PASS and does not require another executable run.

The deselected sustained scheduler case does not exercise this inactive storage
creation path. The ordinary and fresh-process cyclic results cover retained workflow
execution, while the targeted selections directly cover initialization, persistence,
writer rollback, contention, and finalization.

## Deliberate boundary

The private fresh creator assumes its newly claimed directory has not been handed to
runtime callers. Its transaction prevents stale schema publication and preserves a
recognized initializer that wins first. Cleanup cannot make inspection followed by
file removal race-free if an unrelated process begins using the newly committed v5
in that interval. Solving that requires the later activation and admission work,
including complete entry conditions and older-process exclusion. The creator remains
private and unused by CLI or workflow execution, so this is a documented stage limit
and does not justify partial runtime locking here.

This section establishes durable session records only. It does not reserve component
scope, transfer ownership, persist holds, assign jobs to owners, import old run state,
recover stale sessions, or migrate current CLI readers and writers. AQ1 through AQ4
remain open. Consequently this review supports only the assigned storage foundation;
it makes no claim that 44-SES-031 or 44-SES-032 is complete.
