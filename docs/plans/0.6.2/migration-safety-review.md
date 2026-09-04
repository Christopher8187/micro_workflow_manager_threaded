# Migration safety review

## Disposition

The corrected isolated tree passes this narrow, check-only section. The review
found two pre-write failures in the first candidate, and both are fixed in the
final files:

1. `init_project()` used to extract an auto-detected deployment archive before
   checking a pre-existing live run.
2. A syntactically valid but non-object run value such as `[]` used to pass the
   liveness helper. Applied migration could then convert layout, remove legacy
   locks, create SQLite files, and fail only after those changes. An ordinary
   run could continue after the same unsafe classification.

The final source checks existing run state before archive discovery or
extraction in `cli/project.py:27`, retains the later layout check at line 35,
and rejects non-object run JSON in `cli/active_run.py:128-129`. The direct MWF
files and isolated review files have matching SHA-256 values for
`active_run.py`, `project.py`, `layout.py`, `migration.py`, and
`test_067_live_legacy_migration.py`.

This disposition is limited to the preflight added by this section. It does not
accept complete 44-SES-034 behavior. Concurrent admission by an older process,
ordinary direct storage/API loading, and one-time import of two legacy records
remain unresolved or explicitly deferred.

## Governing behavior

The final resolution for [Settle the MWF workflow-management model for
0.6.2](https://github.com/Christopher8187/product/issues/44#issuecomment-5539997969)
at `requirements.md:257` requires safe one-time legacy run import and says
migration must never occur underneath a live legacy process. Christopher
explicitly agreed to the same rule in Q112 of the local **Review GitHub issue
#44** task. The approved migration analysis further requires the live owner
check before layout moves, SQLite initialization, WAL creation, metadata
deletion, graph synchronization, router effects, starter creation, and recovery
(`state-migration-review.md:74-90`).

The section record deliberately implements only the applied-migration and old
layout preflight. It excludes session import, future SQLite upgrades during
ordinary startup, component conversion, and AQ1 through AQ4. That boundary is
coherent only if the record continues to describe this work as partial.

## Entry-point review

The final selected source covers the intended CLI loading paths:

| Entry | First relevant action |
|---|---|
| `mwf init`, including direct `init_project()` | Unconditional raw run-file check at `cli/project.py:27`, before archive resolution or extraction. |
| Applied `mwf migrate` | `cli/main.py:60-61` dispatches directly to `migrate_command()`; `cli/migration.py:72-74` checks liveness before layout or storage. |
| Direct `migrate_command()` | Uses the same unconditional check before `ensure_runtime_layout()`. |
| Ordinary main CLI commands | `cli/main.py:62` reaches `ensure_runtime_layout()` before command-specific loading. |
| `read_config()`, `setup_graph()`, and `load_workflow()` | `cli/files.py:55` calls `ensure_runtime_layout()` before reading configuration or constructing a workflow. |
| Standalone restart and thread CLIs | Both call `ensure_runtime_layout()` before storage construction (`cli/restart.py:276`, `cli/threads.py:346`). |
| Engine and migration dry run | Their existing early dispatch remains intentional because those paths are observational and bypass mutating bootstrap. |

`ensure_runtime_layout()` checks both `.mwf_run.json` and `.mwf/run.json` when
old configuration, run/thread files, or lock areas are present. Applied
migration also checks unconditionally, which closes the current-layout case
where a live `.mwf/run.json` is the only old artifact. The helper reads both
locations in root-to-current order. A terminal record in one location cannot
hide a live record in the other.

The helper delegates to the unchanged host, PID, process-instance, and
heartbeat classifier. Final tests preserve the intended behavior for matching
same-host owners, fresh other-host owners, terminal runs, and recycled PIDs.

## Failure handling and retained data

Invalid JSON already failed before writes. The corrected helper now gives the
same fail-closed behavior to a valid JSON scalar or collection. My first probe
against the uncorrected candidate showed a return code of 1 only after the old
`.mwf` file became a directory, `.mwf_run.json` and `.mwf_locks` disappeared,
and SQLite, WAL, shared-memory, and subscriber artifacts appeared. Re-running
the same probe against the correction produced no filesystem delta.

A dictionary without `status` is still classified as non-running. Current MWF
writers publish the complete run dictionary atomically and always include that
field, so this is not a partial-write route by which a supported live writer can
be missed. It remains invalid or unclassifiable input for the deferred import.
The later import work should accept only a type-valid record that it can
classify as dead or terminal; it should not infer ownership from missing data.

When both locations contain terminal records, current layout behavior keeps
`.mwf/run.json` and deletes `.mwf_run.json`. The probe retained
`current-finished` and removed `older-finished`. This liveness-only section does
not implement import. Future one-time session import must inspect and resolve
both records before layout cleanup. Calling the current layout mover first
would irreversibly discard one source record.

## Unresolved concurrency and API boundary

The preflight has a check-to-write race. No lock spans raw run-file inspection
and the later layout, JSON, and SQLite changes, and an already installed older
writer does not know about any barrier introduced only in 0.6.2. In a controlled
schedule, I inserted a fresh live `.mwf/run.json` immediately after the final
layout check. Applied migration returned success, created SQLite state, and
stamped that live record with schema version 2. This demonstrates the reachable
ordering; its frequency remains unknown.

The full requirement therefore needs an explicit admission decision: either
migration requires externally established project quiescence, or the design
must define a barrier that every supported old writer can honor. Rechecking can
narrow the window but cannot make an uncooperative old binary mutually
exclusive. This is a genuine HITL architecture question under the final
resolution and requires Christopher's decision.

Direct storage/API loading is also outside the new guard. Constructing
`FileStorage`, including through `MicroWorkflow.__init__()` at `system.py:40`,
still initializes SQLite without calling the raw liveness helper. With a live
current-layout run, my direct-storage probe created `state.sqlite3` and
`state_subscribers`. The section record already excludes ordinary-startup
SQLite conversion, so this does not fail its narrow disposition. It prevents
acceptance of full migration safety until the eventual loading sequence places
the raw preflight ahead of direct storage initialization as well.

## Verification

The focused RED record has the three expected failures: archive extraction
left a file, non-object applied migration changed state before failing, and an
ordinary run accepted the non-object record. The corrected run passed all 14
guard cases plus seven selected deployment cases, 21 tests in 2.63 seconds. I
independently reran the final 14 guard cases with Python 3.12.14 against the
isolated source; all passed in 1.04 seconds.

The final unchanged source and tests passed 403 ordinary tests with one stress
case deselected in 439.62 seconds. Four cyclic checks also passed in separate
processes in 5.07, 15.15, 9.03, and 5.82 seconds.

An earlier ordinary run reached 402 passes, one failure, and one deselection.
The failure was the retained replacement-generation restart case whose own
0.4-second checkpoint deadline expired. Three alternating baseline/candidate
pairs of that exact case all passed, six runs total, and the later full quiet
rerun passed without source or test changes. No candidate-only failure was
reproduced; the precise cause of the earlier timeout remains unconfirmed.

The reproducible edge probes and exact deltas are in
`migration-safety-probe.py` and `migration-safety-probe.log` beside this review.
