# Live legacy migration guard compatibility review

Status: **PASS for the assigned compatibility, test-sensitivity, and
documentation review of the corrected check-only section.** This review does
not accept complete 44-SES-033/034 behavior, S2, the SQLite session registry,
general component migration, or MWF 0.6.2.

Reviewer: GPT-5.6 Sol with xhigh reasoning. I reviewed the isolated source at
`test_area/mwf-062-issue45-20260904/migration-guard`, not the direct unfinished
working files. The fixed implementation baseline is
`24f584413619d1bbe94da2264032600b9b401105`; the later
`696c61bfe383f66ed80be263953ae595475acc5c` only records architectural
questions. I rechecked
[Settle the MWF workflow-management model for 0.6.2](https://github.com/Christopher8187/product/issues/44),
including the original Q112 exchange, and the complete preparation for
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).
I also checked current callers, both migration preparation reviews, the stage
record, `docs/operations.md`, and `tests/README.md`.

Reviewed isolated-file hashes:

- `active_run.py`: `17B139BDA5E20C133C289B646F73EC219D0D32FF9D14173797AE31871D829C7D`
- `layout.py`: `19406552604881D37DCBF8CC43F9915C05C824DEF55A2CF445DCA0621D6CE9B7`
- `migration.py`: `614456F1ACCCE008784873A4C82161F63172F476FA62045C03204F38BAF73912`
- isolated `main.py`: `5042677BD45124AF609AFE27B4C5C38AC5C692A402C6C5BD1EDF31168CD6404C`
- `project.py`: `AD54F06F964FD1736FBF15AF233F1199E28C85F659A863D53A6E1A0C33FD49CF`
- `test_067_live_legacy_migration.py`: `8C9B856336E4677CFB1FF021C4B29F85B1310AC5BC8FEE5784E4FBB4EB45586C`

## Requirement disposition and loading order

This section implements only the applied-migration and old-layout guard parts
of 44-SES-034. Its disposition remains **partial**. It does not import an old
run into the future SQLite session registry, migrate component state, add
ordinary-startup SQLite upgrades, settle AQ1 through AQ4, or accept
`migrate --dry-run`.

The revised loading order is safe for the behavior this section claims:

1. Applied `migrate` dispatches before generic layout conversion. It reads both
   supported run-file locations without opening storage, then uses the same
   conversion path as ordinary commands.
2. Old-layout conversion checks before its first layout, lock, JSON, or SQLite
   write.
3. `init` now performs that read-only check before archive resolution and
   extraction. A pre-existing live run can no longer leave partially extracted
   deployment contents behind.
4. Syntactically invalid JSON and valid non-object JSON fail closed before
   conversion. Finished and recycled process records continue to permit the
   existing migration path.

The new `project` import does not create a cycle: `active_run` depends only on
paths and process helpers and does not import project or layout code. The added
preflight is a no-op for a new project without either run file. Error precedence
now favors the live-run safety refusal over archive validation when both are
invalid; this is consistent with refusing any initialization work while the
recorded owner is live.

Ordinary run ownership retains the existing classifier. The stricter object
validation is in the migration preflight and does not replace
`live_active_run()` or `refuse_competing_run()`. Dry-run migration still skips
layout conversion and storage initialization. No added restart, recovery,
thread-control, or successful migration regression was found.

## Compatibility and test sensitivity

The original RED records remain valid. `migration-red-01.log` shows applied
migration creating SQLite and rewriting live run metadata. The first automatic
layout attempt lacked required graph folders and is correctly excluded.
`migration-red-02c.log` is the valid automatic-layout RED: layout, run state,
locks, and SQLite changed before the later competing-run refusal.

My first review found that every applied-migration fixture had a legacy lock
directory. That let the layout check mask removal of the unconditional check in
`migration.py`. The added current-layout CLI and direct variants omit all
legacy artifacts. Removing only the unconditional check made both cases fail
and visibly changed JSON and SQLite in `migration-review-red-01.log`; restoring
it made all eleven then-current cases pass in
`migration-review-green-01.log`.

The safety review then found two distinct pre-mutation gaps. Its three new cases
are sensitive and their RED is behavioral:

- `init` extracted `archive-created.txt` before the old liveness refusal.
- Applied migration converted a non-object root run record and created SQLite
  before later metadata validation failed.
- An ordinary command converted the same non-object record and returned
  success.

`migration-safety-red-01.log` records those three failures with eleven cases
deselected. Moving the initialization check before archive extraction and
rejecting non-object run data fixed them. The combined guard and deployment run
in `migration-safety-green-01.log` reports **21 passed in 2.63 seconds**: all
fourteen migration cases and seven retained deployment cases.

I independently ran the fourteen migration cases plus retained dry-run,
PID-reuse, active-run metadata, and metadata-migration cases against the exact
isolated copy, with the source virtual environment, exact-source `PYTHONPATH`,
disabled pytest cache, and a fresh base directory: **20 passed in 5.67
seconds**. `git diff --check` reports no whitespace errors for the corresponding
working changes.

The earlier broad result of 399 passed with one deliberate deselection predates
the `active_run.py` and `project.py` corrections. It is historical evidence,
not the final broad gate for this revised source. The replacement ordinary run
reported 402 passes, one failure, and one deliberate deselection. The failed
active-restart case left generation 1 with its own 0.4-second checkpoint timeout
after writing `fresh.txt`; it did not report a migration preflight refusal or
the abandoned generation's old watch. Three sequential runs passed on the fixed
baseline in 2.58, 2.48, and 2.66 seconds, and three passed on the candidate in
2.68, 2.83, and 2.70 seconds. That comparison found no candidate-only failure,
although it does not establish the exact cause of the broad-run event. No test
or runtime source was changed to suppress it.

The clean sequential ordinary replacement then passed **403 tests with one
deliberate stress deselection in 439.62 seconds**. All four cyclic cases passed
again in fresh processes in **5.07, 15.15, 9.03, and 5.82 seconds**. No
scheduler loop, job-admission, or persistence hot path changed, so the stage
appropriately did not select a performance workload or stress run.

## Remaining boundary and documentation

The check is deliberately check-only. It cannot exclude an older process that
starts immediately after the read. The safety reviewer reproduced that race.
Race-free admission and validated session import therefore remain future work,
and the complete 44-SES-034 requirement remains pending. When both run-file
locations exist, current layout conversion can discard the root copy; future
import must inspect and resolve both records before conversion.

Direct storage construction is also outside this section. `FileStorage` and
`MicroWorkflow` can still initialize SQLite without calling this raw-file
preflight. That is consistent with the stated exclusion of general and
ordinary-startup migration, but it prevents treating the section as complete
migration safety. A dictionary that lacks the writer's required `status`
field is likewise treated as non-running by the old classifier. Supported
writers publish that field atomically; future import must still validate and
classify the entire record before accepting it.

AQ4 records the remaining concurrency decision accurately. Neither the final
resolution, Q112, nor the complete implementation preparation approves an
external-quiescence prerequisite, a barrier that existing older writers honor,
or a supported-version cutoff. Repeating the read cannot close the window.
The ledger therefore correctly blocks complete 44-SES-033 and 44-SES-034 on
AQ4 while leaving new-project session structures and readers, direct-storage
preflight, and strict two-record validation available independently.

`docs/operations.md` and `stage-migration-guard.md` now state the initialization
preflight, non-object refusal, check/start race, unfinished concurrent
admission, and dual-record import boundary. The stage history correctly keeps
the earlier eleven-case no-runtime-correction result separate from the later
three-case safety correction.

`tests/README.md` now describes applied migration, automatic conversion,
initialization before extraction, non-object records, and the finished or
recycled-owner allowance. No unresolved documentation finding remains.

No other unresolved compatibility finding remains in this assigned review.
The separate safety review passed the corrected check-only section, while
preserving the race limitation above. The stage record accurately includes the
corrected RED/GREEN history, the unconfirmed broad-run event, the successful
replacement gates, and the partial boundary. Its status is awaiting the links
to the two completed independent reviews; this PASS supplies the compatibility
review half and does not imply wider acceptance.
