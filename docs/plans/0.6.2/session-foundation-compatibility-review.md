# Fresh SQLite session foundation compatibility review

Status: **PASS for the private fresh-project SQLite session-storage section.**
All source findings from this review are resolved in the selected copy, the
matching documentation preserves the section boundary, and the final gates
passed. This review does not accept public project initialization, runtime
activation, legacy session migration, reservations, holds, ownership, S2, or
MWF 0.6.2.

Reviewer: GPT-5.6 Sol with xhigh reasoning. I reviewed the isolated
`test_area/mwf-062-issue45-20260904/session-foundation` source based on
`a5de6873f2bc687d9c7274149108307677fc518c`. I rechecked
[Settle the MWF workflow-management model for 0.6.2](https://github.com/Christopher8187/product/issues/44),
the complete preparation for
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45),
the final session-foundation preparation audit, the current storage and
initialization paths, and every current caller of the legacy run-state API. No
new architectural decision is needed for this private foundation.

## Requirement boundary

The section can supply partial implementation and verification evidence for
44-SES-001, 44-SES-002, 44-SES-030 through 44-SES-032, and 44-SES-035 through
44-SES-041. The exact record shape covers IDs, kind, actual parent references,
command, canonical starting and selected components, selected jobs, heartbeat,
terminal outcome, ordered failures, and opaque details. It deliberately has no
reservation, hold, transfer, or job-owner data, so 44-SES-003 through
44-SES-029 and the omitted parts of 44-SES-031 remain pending.

This code does not replace the authoritative `.mwf/run.json` path used by
ordinary execution. It therefore cannot complete 44-SES-030 or 44-SES-032.
The generic SQLite reader implements the approved main/single-interrupt/none/
ambiguity matrix, but `cli.active_run.live_active_run()` still reads legacy
JSON. Requirements 44-SES-036 through 44-SES-040 receive storage-foundation
evidence only. The exact multi-session list is not wired into monitoring, so
44-SES-041 also remains partial.

The section imports no legacy session, performs no version 4 to version 5
conversion, and makes no live-process exclusion claim. Requirements 44-SES-033
and the remaining part of 44-SES-034 stay behind AQ4. It adds no public
initialization path and makes no preview immutability claim.

## Storage and initialization review

Ordinary `FileStorage(project_dir)` keeps the established opening behavior. A
missing database and versions 1 through 3 reach schema version 4; version 4
stays at version 4; a complete version 5 reopens as version 5; and a later
version refuses. Session operations check for version 5 and do not add tables
to an ordinary version 4 store.

The only fresh creator is `_create_new_project_state`. No MWF source caller
uses it. The exported `FileStorage` class has no documented public fresh
factory, and ordinary `MicroWorkflow`, CLI, monitor, recovery, restart, thread,
and supervisor paths remain on their current JSON behavior. This avoids
creating a supported project whose runtime ignores the SQLite sessions.

Freshness classification runs before the base initializer creates a directory.
It rejects `.mwf`, root-level legacy run/thread/lock entries, node schema and
state metadata, queues, idempotency data, and job trees. It allows user source
and output files. The code uses entry-aware checks for `.mwf` and root legacy
paths, and refuses a linked node root or node directory instead of following
it. Successful version 5 creation writes the
`legacy_file_metadata_imported` marker in the same transaction, so ordinary
reopen does not invoke legacy-node import or deletion.

The schema DDL begins an immediate transaction. Session tables, the partial
unique main-row rule, the legacy-import marker, and schema version 5 commit
together, with the version marker written last. A failure after session-table
creation rolls the whole schema transaction back. The new table relations and
unique constraints make parent existence, session identity, component and job
deduplication, positional order, and one persisted running main row SQLite
invariants.

## Session API and liveness review

Create validates nonempty identity fields, process identity, positive integer
PID, ISO start time, nonoverlapping selected components, inclusion of the
starting component, and selected-job membership. Component members are stored
as exact sorted keys. The parent foreign key rejects an absent parent. Session
and child rows are one synchronous priority-writer request, so an insert error
rolls the whole request back.

Heartbeat and finish target one exact session ID and update only a running row.
A terminal session cannot be changed by a late heartbeat or second finish.
Create, heartbeat, and finish all enter the existing SQLite mutation lane with
a synchronous wait. Several separate processes rely on the database partial
unique rule rather than a process-local check when they compete to create a
main row. Several interrupt rows remain allowed.

The actor-neutral liveness function is behaviorally the former active-run
classifier. The legacy wrapper still obtains its probes from
`cli.active_run`, preserving existing monkeypatch and Windows process-identity
behavior. A matching local process instance is live even when a heartbeat is
old; a recycled local PID is rejected; a live local PID without a process
identity needs a fresh or absent heartbeat; and a different host needs a fresh
heartbeat. Terminal records are never live. SQLite readers retain stale and
terminal rows for exact-ID lookup while excluding them from live selections.

The named readers ask distinct questions: exact ID, all sessions, all live
sessions, live main, and the temporary generic compatibility selection. The
generic reader chooses the live main when present, returns one sole live
interrupt when no main exists, returns no result when no session is live, and
raises with sorted IDs for several live interrupts. It never chooses among
several interrupts heuristically.

## Retained behavior and caller inspection

The schema-version change does not alter any caller-facing constructor
signature. `DATABASE_SCHEMA_VERSION` now identifies the highest understood
shape and the separate automatic ceiling remains 4. No source outside the
SQLite facade consumes that constant. The existing storage implementation is
still the only composed backend, and the new mixin introduces no alternative
connection or mutation worker.

Every ordinary run-state consumer remains on the legacy methods:
`run_session`, `supervisor_core`, run commands, reset admission, inspect,
monitor metrics, top, recovery, restart, threads, and runtime configuration.
That is the required boundary for this section. The private creator is not
called by `mwf init`, archive extraction, layout conversion, or direct
`MicroWorkflow` construction.

## Documentation compatibility

The current user-facing storage and operations documents still describe
`.mwf/run.json` as the active-run source because that remains true for every
ordinary caller. This section should not revise those documents to describe
SQLite session activation. `CONTEXT.md` still defines a run session broadly as
one framework-owned execution record or sequence; the final several-session
terminology remains S6 work. The test README describes `test_069` as internal
fresh SQLite storage and does not advertise a public factory.

The stage record states the narrower behavior: ordinary storage creates or
upgrades only through version 4, complete version 5 is understood, only the
private fresh initializer creates version 5, and no CLI, migration,
reservation, ownership, or recovery behavior is credited. Its parent wording
must say that only an interrupt may have a parent and that the parent is a
different persisted session. That matches both API validation and the SQLite
check without implying that this section settles later parent admission or
scope transfer.

## Test sensitivity reviewed so far

The recorded RED/GREEN sequence independently exposes the missing private
creator, session lifecycle operations, cross-process main uniqueness,
freshness refusal, named-reader behavior, validation, parent integrity, writer
rollback, and ordinary version-4 refusal. The original 41-case green selection
passed in 2.68 seconds, and the first 46-case retained selection passed in 4.14
seconds. Review-driven cases then exercised invalid updates, retry cleanup,
incomplete version-5 shapes, supported Python versions, invalid parent shapes,
competing initializers, transaction ordering, deterministic outer scope order,
extra uniqueness rules, and post-commit object-setup failure. The final focused
selection records 71 passes in 8.67 seconds.

The process competition is substantive: two native Python processes wait on a
shared start signal, then the database permits exactly one main insertion. The
mutation-lane test is sensitive to direct writes and verifies that a duplicate
child job rolls back its session and every child row. The schema failure probe
raises after creating all three session tables and verifies that none survive.
Freshness preservation compares all existing files before and after each of 19
contradictory runtime-evidence cases.

The initial freshness suite did not exercise the explicit dangling-link and
node-link branches. The later review selection adds two dangling root-entry
cases and two real Windows junction cases for the node root and a child node.
The dangling cases already passed; the node cases exposed the supported-version
problem described in SF-COMP-004 and now pass.

I also ran one independent combined selection against the isolated source:

```powershell
$env:PYTHONPATH='C:\Business\product\test_area\mwf-062-issue45-20260904\session-foundation'
$env:PYTEST_ADDOPTS='-p no:cacheprovider'
$env:PYTHONDONTWRITEBYTECODE='1'
& 'C:\Business\product\test_area\mwf-062-issue45-20260904\source\.venv\Scripts\python.exe' -m pytest `
  tests/test_069_execution_sessions.py `
  tests/test_050_windows_process_signal_safety.py `
  tests/test_067_live_legacy_migration.py `
  tests/test_037_advisory_lock_recovery.py `
  --basetemp='C:\Business\product\test_area\mwf-062-issue45-20260904\session-sol-compat-01' `
  --junitxml='C:\Business\product\testing_ground\issue-45\session-sol-compat-01.xml' -q
```

The result was 95 passed in 14.75 seconds. The durable output is
`session-sol-compat-01.log` with matching JUnit XML. This selection overlaps
the new lifecycle and fresh-initialization behavior with the retained Windows
process classifier, live-legacy migration guard, and advisory-lock recovery.

## Review findings and corrections

### SF-COMP-001: failed fresh-creation recovery, resolved in selected source

`_create_new_project_state` claims `.mwf` before initialization but does not
recover attempt-owned artifacts when `_initialize_storage` raises. The schema
transaction correctly rolls back its tables, yet the empty database and
`.mwf` directory remain. A second private fresh creation then refuses because
`.mwf` exists. Falling back to ordinary `FileStorage` creates version 4 and
loses the caller's explicit fresh-session intent.

The preparation requires closing connections, removing only database files and
sidecars created by this attempt, removing `.mwf` only when it remains empty,
and then allowing the same private fresh operation to retry successfully. It
must preserve every pre-existing user file and any unowned file that appears
during the failed attempt.

`session-review-red-01` failed on the surviving `.mwf` directory. The revised
creator invokes its registered connection finalizer, clears the unsuccessful
initialization cache entry, checks that the claimed directory itself was not
replaced, removes only the known SQLite database and sidecars, and removes the
directory only if it is otherwise empty. It then rethrows the original error.
The permanent case preserves a pre-existing source file and successfully
retries the private version-5 creation. The 50-case green passed in 3.34
seconds.

A later injection failed after the complete version-5 schema committed but
before process-local job-execution setup completed. `session-review-red-09`
showed that the attempt-owned `.mwf` directory survived and prevented retry.
The final cleanup opens the attempt-owned database read-only after closing its
connections. It removes the database and sidecars only when no schema objects
survived rollback or the attempt's complete version-5 marker is present; a
competing committed version-4 database remains untouched. It still lets
`rmdir` preserve and report any unexpected entry. `session-review-green-08`
records all 71 focused cases passing in 8.67 seconds. This finding is resolved
in the selected source. The cleanup behavior supplies no public initialization
or cross-process runtime-admission guarantee.

### SF-COMP-002: update validation, resolved in selected source

The selected copy validates only the terminal outcome during heartbeat and
finish. It accepts a non-time heartbeat, a non-time finish value, and a mapping
instead of the required ordered failure list. Those values become durable and
can make later liveness or recovery classification depend on malformed data.

Heartbeat now validates its session ID and ISO time. Finish validates its
session ID, ISO time, nonempty outcome, and list-shaped failures before entering
the writer. Invalid input must leave the running record byte-for-byte
equivalent at the API level. The focused RED exposed the three previously
accepted malformed values, and the revised selected source passed all 50 cases
in 3.37 seconds. This finding is resolved.

### SF-COMP-003: recorded version 5 does not establish a complete session shape

The selected schema recognizes version 5 as complete when the three session
table names and the established base table names exist. It does not verify the
partial unique rule that enforces one running main, the required child-table
columns, or the successful-freshness marker. A damaged database can therefore
reopen as version 5 without the one-main invariant, or it can proceed to the
legacy import path and mutate node metadata before any session operation fails.

`session-review-red-02` independently removes the unique main rule, replaces a
child table with a wrong shape, or deletes the legacy-import marker. All three
cases are incorrectly accepted by the selected source. A recorded version 5
must validate these required structural facts and the marker before invoking
any legacy import or cleanup path, and must refuse the incomplete database
without changing the sentinel legacy metadata.

The corrected schema detects session tables without a version-5 marker and,
for a recorded version 5, compares the table and unique-rule declarations
against a fresh in-memory reference before legacy import. It also requires the
successful-freshness marker. A review follow-up added an extra partial unique
rule allowing only one interrupt; the subset comparison accepted that
incompatible extra constraint. Exact equality for the session-owned schema
objects closes that hole. `session-review-green-02` passed the first 54 cases
in 4.54 seconds, and `session-review-green-07` passed all 70 cases in 7.39
seconds after the extra-constraint RED. This finding is resolved in the
selected source.

### SF-COMP-004: the fresh path uses Python 3.11 and 3.12-only APIs

MWF declares Python 3.10 or later. The selected fresh creator directly calls
`Path.is_junction`, added in Python 3.12, during ordinary fresh creation,
freshness refusal, and cleanup. It also unconditionally calls
`BaseException.add_note`, added in Python 3.11, if cleanup cannot remove the
new directory because an unexpected file appeared. Python 3.10 would therefore
fail before creating a valid fresh store and could replace the original
initialization failure with `AttributeError` during cleanup.

`session-review-red-03` simulates the supported older runtime, injects an
unexpected file during failed creation, and creates real Windows junctions for
the node root and a child node. Four cases fail for the two unavailable APIs;
the dangling `.mwf` and root legacy-link cases already pass through
`os.path.lexists`. The correction must use a Python-3.10-compatible reparse/
link check, preserve the original exception when annotation is unavailable,
leave the unexpected file untouched, and continue refusing node links without
following them.

The revised source uses `lstat`, the portable symlink mode, and the Windows
reparse-point file attribute. Cleanup rethrows the original initialization
error with the cleanup error as its cause, which is supported on Python 3.10.
All four former failures and the two already-correct dangling-entry cases pass;
`session-review-green-03` records 60 passes in 7.00 seconds. This finding and
the prior link-coverage gap are resolved.

### SF-COMP-005: parent identity permits a parent on a main session and a self-parent

The selected creation API accepts `parent_session_id` for a main session. It
also accepts an interrupt whose parent ID equals its own new ID because SQLite
allows a self-referential foreign key. Neither value represents an actual
parent relationship: a main is the top-level session, and a child must be
distinct from its parent.

`session-review-red-04` first creates a real retained parent record, then shows
that both invalid shapes are accepted. Creation must reject a non-null main
parent and an equal session/parent ID before entering the writer. This narrow
validation does not decide later parent admission, overlap, transfer, or
liveness behavior.

The corrected creation validation permits a parent ID only for an interrupt
and requires it to differ from the new session ID. The foreign key continues
to require a real persisted parent. `session-review-green-04` passed all 62
cases in 6.76 seconds. This finding is resolved in the selected source.

### SF-COMP-006: a competing ordinary store can win after the fresh claim

The fresh creator checks for old state and atomically creates an empty `.mwf`
directory, but ordinary `FileStorage` does not treat that claimed empty
directory as ownership. If an ordinary store initializes version 4 before the
fresh creator enters `_init_sqlite_state`, the same-process schema cache can
make the fresh object return without version 5. In a separate process, the
fresh initializer instead attempts a version-4 to version-5 conversion; its
failure cleanup then removes the competing process's database.

`session-review-red-05` places that exact ordinary initialization between the
directory claim and fresh initialization in both one-process and two-process
forms. The fresh path must recheck the claimed directory before its own
initialization, refuse the contradictory state without converting it, avoid
the ordinary cache shortcut, and remove SQLite artifacts only after this
attempt established their ownership. The competing run JSON and version-4
database must remain unchanged.

The revised fresh call always enters schema initialization instead of accepting
the ordinary same-process cache entry. Under the schema transaction it refuses
any already-initialized database before setting attempt ownership. Cleanup
discards and removes state only when this attempt established ownership and
only when rollback left the database without objects. Both same-process and
separate-process competing version-4 stores remain intact.

### SF-COMP-007: the schema decision precedes its write transaction

Schema initialization reads the table names and version before entering the
`BEGIN IMMEDIATE` embedded in the DDL script. A second process can therefore
retain a stale version-4 plan while the fresh creator commits complete version
5, then resume and publish schema version 4 over the already-created session
tables. A later open correctly diagnoses the inconsistent shape, but the
initializers have already reported success.

The first concurrent probe let ordinary version 4 commit before fresh version
5 and therefore passed; it did not exercise the stale-plan direction and is
excluded. `session-review-red-06b` pauses ordinary initialization immediately
before its write transaction, lets fresh version 5 commit, and then resumes the
stale ordinary plan. The permanent case must require the write transaction
before reading schema objects or version metadata and hold it through the
decision, validation or DDL, and final marker. Together with SF-COMP-006, the
process that acquires the database write transaction first must leave one
complete, correctly marked version without an automatic fresh conversion of a
committed version 4.

Schema initialization now enters `BEGIN IMMEDIATE` before reading table and
version metadata and executes each fixed DDL statement without
`executescript`, which would otherwise commit the existing transaction. The
decision, validation or DDL, and version marker share the acquired write
transaction. `session-review-green-05` records 65 passes for both SF-COMP-006
and SF-COMP-007. These findings are resolved in the selected source.

### SF-COMP-008: unordered outer scope collections, resolved

The initial API accepted sets and frozen sets for the outer selected-component
and selected-job collections even though their iteration order determines the
persisted position. Four focused RED cases showed that unordered collections
were accepted. The revised validation requires a non-text `Sequence` for both
outer collections while still canonicalizing the unordered members inside each
component. `session-review-green-06` passed all 69 cases in 6.79 seconds. This
finding is resolved.

No source, compatibility, liveness, or architecture finding remains open in
the reviewed section.

## Final verification and immutable selection

The final selected source passed the following recorded gates:

- `session-review-green-08.log`: all 71 focused session cases passed in 8.67
  seconds after the last review correction.
- `session-sol-compat-01.log` and XML: my independent compatibility selection
  passed 95 cases in 14.75 seconds across session storage, Windows process
  safety, live-legacy migration, and advisory-lock recovery.
- `session-safety-independent-01.log`: the other independent selection passed
  78 cases in 44.47 seconds across session storage, SQLite contention recovery,
  and connection-finalizer reentrancy.
- `session-ordinary-01.log` and XML: the ordinary suite passed 480 cases with
  one marked stress case deselected in 467.66 seconds.
- `session-cycle-1.log` through `session-cycle-4.log`: the four required cyclic
  cases each passed in a fresh process in 3.42, 11.30, 8.08, and 4.47 seconds.
- The bounded ordinary version-4 initialization comparison passed all 2,400
  durability checks. Its candidate-to-baseline median ratios were 0.9826365
  for creation and 0.9499752 for reopening, both below the predeclared 1.20
  limit. This local comparison is not a general throughput claim.

`session-initialization-manifest.json` records the performance-tested
isolated-source freeze, commands, dependencies, program, samples, and checks.
Its SHA-256 is
`316A0C28F13B89FCE506894D6A8425C1F680C2831BB4E1471452EF5B8F5F3175`.
After those checks, one trailing blank line was removed from
`session_liveness.py` in both the direct and selected copies. The before and
after files produce identical `ast.dump(ast.parse(...))` output; no executable
statement or syntax changed. The performance manifest therefore retains the
pre-format file hash, while the table below records the final reviewed bytes.
No test rerun is needed for this AST-identical formatting correction. The final
selected executable hashes reviewed here are:

| File | SHA-256 |
| --- | --- |
| `micro_workflow_manager/storage/filesystem.py` | `BA5A15F020F4B50A44E7D17043E6C6CBC1069FC54E77B034560B8606B576C6A9` |
| `micro_workflow_manager/storage/sqlite/connection.py` | `07E1E9E14DC6DCCD7A4C9BA38A4D064A9472042164A0A422469DF0308B6B03F8` |
| `micro_workflow_manager/storage/sqlite/schema.py` | `7D280A10D4CC6A14F5858DC23A3204A7C765B21A29B34C9E396680CA85D3C5CD` |
| `micro_workflow_manager/storage/execution_sessions.py` | `522C4A3A71C0352D6940CAC9F37C3EF7930FA9C8AAB9AFC2AC8C1FC198610E4B` |
| `micro_workflow_manager/session_liveness.py` | `B362224E3033A507E73ED9C705BED5CFEE7283DCE97A213A245F8881D91652A5` |
| `micro_workflow_manager/cli/active_run.py` | `B61218F94385AF417FFF62A77F2E3176514796EFC638799C2A739F68EF80FA21` |
| `tests/test_069_execution_sessions.py` | `ADF1B81F39A5019E2CE50289EA1F94992A9F0D18CAFA10EB76DC7D8FAF315920` |

The direct MWF copies of these seven files matched the isolated selection when
the review was frozen. After the corrected file was restaged,
`git diff --cached --check` reported no whitespace error.

## Disposition

This review passes only the private storage section. Requirements 44-SES-001,
44-SES-002, 44-SES-030 through 44-SES-032, and 44-SES-035 through 44-SES-041
receive the partial evidence described above. None is complete. The final
ledger wording correctly limits 44-SES-001 and 44-SES-002 to persisted row
cardinality; 44-SES-030 and 44-SES-032 to an internal SQLite foundation while
supported execution still uses run JSON; 44-SES-031 to the fields actually
stored while reservations, holds, and job ownership remain pending;
44-SES-035 through 44-SES-040 to named SQLite APIs while existing generic
callers remain on JSON; and 44-SES-041 to an exact listing API while monitoring
integration remains pending.

Requirements 44-SES-003 through 44-SES-029 receive no evidence from this
section. Complete legacy import and exclusion under 44-SES-033 and 44-SES-034
remain outside it, with AQ4 unchanged. The accepted migration preflight remains
the only existing partial credit for 44-SES-034. Public activation, session
admission, reservations, holds, job ownership, caller migration, recovery,
monitoring integration, the full S2 stage, and the release remain pending.
