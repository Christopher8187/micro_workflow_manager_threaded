# MWF 0.6.2 architectural decisions

Christopher approved the recommendations for AQ1 through AQ6 on 2026-09-05
with “Use all six recommendations.” The exact supplemental approval is Parent
Repo `testing_ground/issue-45/approved-architecture-decisions-20260905.md`.
The original investigations remain below because they explain the choices and
their limits. Christopher later approved Q7 through Q9 with “Yes I agree with
Q7-9.” The
[published supplemental approval](https://github.com/Christopher8187/product/issues/45#issuecomment-5552346343)
matches Parent Repo
`testing_ground/issue-45/approved-architecture-decisions-20260905.md`, SHA-256
`603EE27F7E2BCEF75BD25B4F6FB3FB660E8EFA8F77BB98DA3F3D97DB096748CC`.
None of these nine questions remains open within its recorded scope.

Christopher settled [Q10](#q10-dynamic-component-discovery-during-admitted-execution)
on 2026-09-07: merging components during an admitted run is unsupported and
assumed not to occur. The autostart redesign is deferred to approximately
0.6.4. Q10 is no longer an unanswered 0.6.2 architecture blocker.

These decisions supplement
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).
Decision approval does not implement behavior, satisfy verification, or accept
the unfinished release. Newly settled work proceeds through the existing
test-first and review procedure.

The implementer checked the final behavioral resolution, current code, and the
complete local preparation task **Find objective for issue #45**, including its
later corrections. The relevant approvals are Q1 and Q2 on dependent work and
stage ordering, Q6 on factual disagreements, Q8 and Q9 on authorization and
preservation, and the later requirement to read the full preparation before
deferral. Those turns establish the decision procedure. They do not answer the
initial three behavior questions at that point; the supplemental approval above
now answers them. Sol xhigh reviewers also read the complete preparation before
reporting these questions.

An [independent Sol xhigh review](architecture-questions-review.md) checked
the initial questions and their 27 narrowly blocked requirement dispositions at
that review point. It found
no already-settled answer or invented behavior. The implementer has also read
the complete original behavioral task **Review GitHub issue #44**, including
the missing Q99-Q111 exchange recovered from that task's local rollover.

## AQ1: SQLite coordination during read-only previews

May a preview change an already-existing `state.sqlite3-shm` file while taking
a transactionally consistent snapshot of a live database?

**Approved decision, 2026-09-05:** yes, within a narrow existing-file
allowance. A read-only preview or read-only migration check may change an
already-existing SQLite shared-memory coordination file while reading a live
database consistently. It must not execute mutating SQL or change workflow,
task, or job data or schema, and it must not create project files. Preview
execution still does not apply migration or recovery. Closed-database paths
must continue to avoid creating sidecars.

The [final workflow-management resolution](https://github.com/Christopher8187/product/issues/44#issuecomment-5539997969)
requires no durable or externally visible mutation. The unfinished preview
opens SQLite with `mode=ro`, enables `query_only`, and starts a read transaction.
On Windows with Python 3.12.14 and SQLite 3.53.1, this creates WAL sidecars after
a clean shutdown and changes existing shared-memory coordination bytes while
reading live WAL state. Preventing SQL writes does not prevent those effects.
[SQLite WAL documentation](https://www.sqlite.org/wal.html) explains the shared
coordination files.

The independent experiments found these consequences:

- Permitting existing shared-memory coordination changes allows a normal safe
  reader followed by an in-memory backup. Closed databases still need a path
  that avoids creating project sidecars.
- Requiring every original byte to remain unchanged needs a different storage
  or availability decision. A private stable copy succeeded in quiet cases but
  refused 17 of 20 attempts under continuous commits.
- `immutable=1` missed a committed WAL-only row in the experiment and disables
  locking and change detection. It is not a valid substitute on a mutable
  original. [SQLite URI documentation](https://www.sqlite.org/uri.html).

The investigation originally assumed no exception and therefore blocked live
SQLite preview acceptance for 44-CMD-039 and 44-CMD-046, complete plan/dry-run
acceptance, sample-plan persistence checks, stale-session previews, recovery
dry-run, related regression coverage, and documentation. The approved
existing-file allowance settles that choice. All implementation, distinction
between allowed coordination changes and other effects, and verification remain
pending. Pure selection and graph calculations remain independent.

The local investigation is `testing_ground/issue-45/preview-storage-review.md`
in the Parent Repo. The unfinished loader and its failing WAL regression remain
outside accepted commits.

The same coordination choice previously blocked a complete preflight for direct
initialization of an existing but incomplete SQLite store. A live project may
legitimately reopen a complete database. An incomplete database must refuse
migration under an observed live legacy owner before any mutation. Determining
which case exists through `mode=ro` can itself create sidecars or change SHM
before refusing. The private-copy and immutable alternatives retain the limits
above. No completion marker outside SQLite or availability rule is assumed.

The implementer checked the current constructor, schema/import ordering,
workflow-loading callers, and final resolution, then re-read both complete
preparation transcripts as the final check. Q1/Q2 and the later correction
authorize deferring dependent work; they supply no exception for SQLite
coordination writes. A Sol xhigh preparation review reached the same conclusion.
The same choice previously blocked complete direct-loading preflight and its
before-mutation checks under 44-SES-034 when SQLite already existed. That work
is now architecturally unblocked within the same existing-file allowance.
Filesystem-only validation of both run records, preserving both during layout
refusal, and a live-owner guard when the database is absent remain independently
accepted within their recorded boundaries.

## AQ2: Converting legacy component results

How should migration convert ambiguous legacy raw-node state into one component
lifecycle and successful-result lineage?

**Approved decision, 2026-09-05:** preserve the legacy data and refuse reuse
when component result, stability, or required history cannot be established.
Explain the affected work that needs explicitly requested fresh preparation.
Do not automatically reset or delete it and do not invent successful results,
stability, sample history, or interrupt origins. Approved Q7 requires explicit
fresh preparation of the reliably affected scope, or the whole project when
that scope cannot be established, while preserving data until that separate
request.

The resolution requires one component record with lifecycle, stability, exact
instability origin, misalignment, and alignment generation. Existing
`storage/sqlite/transfer.py` imports separate raw-node statuses. Existing
`workflow/component_state.py` permits those values to differ. Neither the final
resolution nor the preparation defines a migration result for `{done, queued}`,
`{done, failed}`, or stale `{running, waiting}`. Legacy selected-job and sample
execution also lacks the new durable sample lineage.

The investigation presented refusal until explicit fresh preparation,
conservative conversion, and reconstruction from validated job evidence. The
approved decision selects refusal and preserves the rule that missing
historical interrupt or sample identity must not be invented.

Legacy conversion for 44-CMP-002 through 44-CMP-010 and 44-CMP-025/026 is now
architecturally unblocked where the affected historical membership is reliable.
Implementation and verification of refusal and explicit fresh preparation
remain pending. Dependent raw-node displays, readiness, reuse, sampling,
stability propagation, misalignment, lineage diagnostics, recovery, and
membership repair remain unfinished. Approved Q7 settles the
missing-membership scope without authorizing fresh preparation in this record.
New-project calculations and the independently settled session registry
continue where no legacy result is assumed.

## AQ3: Authoritative component membership before migration

What source determines exact component membership before migration and before
router mounting or starter creation can mutate state?

**Approved decision, 2026-09-05:** persist the actual graph and exact component
membership associated with the work produced under them, and use that record as
the authority for historical membership. Later discoveries follow the existing
membership-change rules. Dynamic component-forming relationships need not all
be statically declared. For legacy work with no reliable stored historical
membership, approved Q7 requires explicit fresh preparation of the reliably
affected scope or the whole project when the scope is unknown; current scanned
membership must not silently replace historical membership.

Current `cli/autostart_scan.py` recognizes literal `add(..., autostart=True)`.
Supported `add_many`, `add_job`, `add_jobs`, and computed routing can register
autostart relationships later through `workflow/job_creation.py`. Legacy state
does not retain the historical autostart shape. Stored raw edges plus the
current scanner therefore cannot universally reconstruct the component that
produced reusable work.

The investigation compared static declarations, broader discovery, and a
persisted synchronized shape. The approved choice selects the persisted actual
graph and membership. Existing membership-change behavior governs later dynamic
discoveries; approved Q7 settles the missing historical-shape case through
explicit fresh preparation rather than reconstruction.

General migration and graph-update integration for 44-MEM-002 through
44-MEM-010 is now architecturally unblocked when historical membership is
available. Implementation still must preserve the producing shape before node
deletion, order conversion before router and starter writes, and apply overlap
repair. Approved Q7 settles unknown historical membership without adding an
automatic mutation. Component-keyed
ownership, lifecycle, preparation, scheduling, and the already specified exact
sorted-member calculation still require implementation and review.

The local S2 investigation is
`testing_ground/issue-45/state-migration-review.md` in the Parent Repo. It records
the safe loading order and separate settled session tests. Observed live-run
refusal before legacy migration, SQLite-only session writes, one main with several
interrupts, exact new job ownership, and non-guessing readers are settled and
remain eligible for independent implementation.

## AQ4: Excluding older processes during migration

What may migration require to prevent an older MWF process from starting after
the live-run check and before the migration finishes?

**Approved decision, 2026-09-05:** require an operator-controlled migration
window. The operator must stop old MWF processes and automatic launchers that
can access the project, explicitly confirm migration may proceed, and keep the
old software stopped throughout migration. MWF still checks live owners and
refuses any it finds. The implementation need not exclude arbitrary old
software that violates this prerequisite. This decision does not authorize
stopping processes or migrating any project in this work session.

The final resolution and Christopher's Q112 approval require that migration
never occur underneath a live legacy process. The new preflight checks both run
files before mutation, but it cannot exclude a subsequent start. The safety
reviewer's controlled ordering publishes a fresh live record immediately after
the final check. Applied migration then creates SQLite and stamps the live JSON
record. The [migration preflight record](stage-migration-guard.md) preserves this
limit. The experiment demonstrates the ordering, without estimating its frequency.

The investigation compared externally established project quiescence, an
admission barrier, and another liveness check. The approved decision selects
operator-established quiescence plus MWF's retained live-owner check. Repeating
the check alone remains insufficient.

Root checked the final resolution, the complete Q112 exchange, current loading
and run-claim code, and the safety review. Root then re-read the complete local
preparation task, including all later corrections, before deferring this choice.
That conversation specifies how to defer ambiguity but supplies no migration
quiescence or older-writer admission rule.

Complete legacy session import and exclusion under 44-SES-033/034 are now
architecturally unblocked but remain unimplemented and unreviewed. New-project
SQLite session records, reservations, ownership, and session readers remain
independent. Missing-database direct loading and validation of both legacy run
records are
[accepted within their boundary](stage-legacy-loading.md). The existing-database
completion check uses the separately approved AQ1 allowance.

## AQ5: Stability while sampled work resumes

When an aligned sampled component resumes and its lifecycle becomes `running`,
does its component record continue to expose the prior compatible stability
and exact instability origin, or do their current values become null until it
becomes `sampled` or `done` again? The required lineage JSON keys remain present.

**Approved decision, 2026-09-05:** keep the prior compatible stability and
exact instability origin visible while the aligned sampled component resumes
as `running`. They describe the established result so far. The component
remains incomplete and cannot satisfy downstream predecessor readiness. Keep
the required lineage JSON keys and the previously settled sampled and terminal
outcomes under the single component-level authority.

The final resolution's component model defines stable and unstable as
successful result forms, preserves established lineage for sampled work, and
explicitly removes successful stability from queued and failed components
(44-CMP-004, 44-CMP-007, and 44-CMP-008). Its sampling section requires a
successful resume to preserve compatible stability or the exact origin when
promoting to done (44-SMP-054/055). Neither section specifies the intervening
running record. The answer affects observable component and lineage output as
well as persistence validation.

The current `workflow/component_state.py`, `workflow/component_scheduler.py`,
and `cli/run_commands.py` derive or update raw-node lifecycle. They have no
separate component stability or origin fields, so retained implementation does
not supply the missing rule. After examining those paths and the final
resolution, the implementer re-read both complete preparation transcripts as
the final check. At that review point, Q1/Q2 permitted deferring dependent work
and reorganizing stages, Q10 approved tests for settled persistence behavior,
and none chose the running representation. The Sol xhigh preparation review
independently identified the same narrow ambiguity after reading the full
preparation.

Keeping prior result values visible while running would expose the established
lineage alongside an active lifecycle. Clearing them would require retaining
enough history separately to restore the compatible result on completion.
Neither choice could make a running component satisfy successful predecessor
readiness. The approved choice retains established lineage. No implementation
or verification credit follows from that decision.

The formerly dependent work is limited to the approved running-row result
values, retaining them when sampled work starts running, restoring that exact
in-flight representation during recovery, and displaying or testing those
values during the interval. This affects only that portion of 44-CMP-014/025,
44-SMP-054, 44-TRC-004/007, and their integrations. It is now architecturally
unblocked and still unimplemented. The transition to running itself, single
component authority, settled sampled and terminal outcomes including
44-SMP-055, and general recovery or diagnostic work remain independent.
Exact component identity, producing graph shape, reservations, holds, and
explicit job-claim ownership also remain independent. The next private storage
section starts with identity and shape; it does not permit the rejected
null-lineage running form as temporary behavior.

The [independent Sol xhigh source and specification review](running-lineage-review.md)
accepted this question and its narrowed dependent boundary after checking the
complete preparation history. Its two documentation findings are resolved.

## AQ6: Execution ownership after trace clearing

Which execution-owner records survive trace clearing, fresh preparation, and
job deletion, and which durable reference identifies the current or last
restartable owner after an execution finishes?

**Approved decision, 2026-09-05:** preserve restart and recovery ownership,
with an explicit reference to the relevant execution, independently of optional
detailed trace history. Clearing diagnostic trace must not erase operational
ownership. Superseded detailed ownership history may be removed when nothing
needs it; permanent retention of every owner row is not required. Approved Q8
selects the active claim when present and otherwise the most recent committed
claim, including an unstarted released claim. Approved Q9 clears the current or
last owner on explicit fresh preparation or deletion and assigns ownership to
newly prepared work only when it is newly claimed.

The final resolution requires exact ownership for every claimed execution and
requires job-scoped restart to use its recorded session without guessing
44-SES-027/042/044 and 44-REC-005. Existing detailed-event cleanup follows
`--keeptrace`. Neither requirement says that execution ownership is removable
detailed trace or defines its lifetime. The earlier suggestion in
`next-s2-preparation.md` to clear owners with events was a preparation inference,
not an approved decision.

The source makes that distinction consequential. Terminal publication clears
`jobs.active_execution_id`. Reset and unstarted release can produce multiple
executions for one node, job, and generation. The private owner reader takes an
exact execution ID; it cannot identify a terminal job's current owner from those
three fields alone. Default descendant resume also clears detailed events while
retaining jobs. Deleting owners whenever those events clear could remove state
needed by restart or recovery. Retaining every owner indefinitely would choose
a lifetime without resolving which attempt a job-scoped operation means.

After inspecting the final requirements, event deletion, claim, terminal,
restart, and cleanup paths, root read both complete preparation transcripts as
the final check. At that review point, Q1 and Q6 required deferring unresolved
state ownership, Q10 authorized tests for settled persistence rules, and none
settled owner retention or terminal-owner selection. The independent Sol xhigh
preparation reached the same conclusion and corrected its earlier assertion
that deletion was settled.

The investigation compared retaining ownership independently of detailed
events with a separate operational reference that permits unneeded superseded
history to be removed. The approved decision settles their shared operational
rule: trace clearing cannot erase restart or recovery ownership, and the
relevant execution must be referenced explicitly. Approved Q8 and Q9 settle
the selection and cleanup boundaries stated below. No implementation or
retention cleanup has been accepted.

Detailed trace clearing is now architecturally independent from operational
ownership. Q8's job-scoped current/last owner after an unstarted release and
Q9's fresh-preparation or deletion cleanup are also architecturally settled.
Their implementation remains pending, including the affected portions of
restart, recovery, lineage, and transfer.
Exact claim-time ownership, lookup by execution ID, reservations, holds, and
unrelated session operations remain independent. The private claim section
records exact executions but still supplies no job-scoped owner selection or
reset/deletion behavior.

## Q7: Scope when historical component membership is unknown

If reusable legacy work has no reliable historical membership, should explicit
fresh preparation cover the reliably established affected scope, expanding to
the whole project only when that scope cannot be established, or should the
operator manually reconstruct membership before migration?

**Approved decision, 2026-09-05:** require explicitly requested fresh
preparation of the reliably established affected scope, or the whole project
when that scope is unknown. Preserve existing data until that separate request.
Manual reconstruction is not a prerequisite. AQ2 still requires refusal rather
than invented results, and AQ3 still makes persisted actual graph and
membership authoritative when present.

## Q8: Last owner after an unstarted claim is released

When no active claim exists, should the explicit current/last owner reference
select the most recent committed claim, including one released before handler
execution, or the last attempt that actually started?

**Approved decision, 2026-09-05:** use the active claim when present and
otherwise the most recent committed claim, including one released before
handler execution, as the explicit current or last owner. Retain that exact
reference after completion or release until Q9's reset/deletion boundary.
Existing session-validity checks still determine whether restart is allowed.
If session B claims after session A and releases before execution starts, B is
the recorded last owner.

## Q9: Ownership after fresh preparation or job deletion

Should explicit fresh preparation or deletion clear the job's current/last
operational owner so new work receives ownership only on a new claim, or should
the prior owner remain attached to a retained reset job?

**Approved decision, 2026-09-05:** explicit fresh preparation or deletion
clears the job's current or last operational owner. Newly prepared work,
including a reused numeric job ID, receives ownership only on a new claim and
must not inherit the previous work's owner. Preserve old owner rows while live
execution, recovery, or retained lineage needs them. Diagnostic trace clearing
alone never clears operational ownership.

## Q10: Dynamic component discovery during admitted execution

**Status: settled by Christopher, 2026-09-07.** The intended cycle is to change
code, run it, then change code again. Component merging during an admitted run
is unsupported and assumed not to occur. If job-scoped programmatic autostart
discovers a relationship that would merge active components, treat that case
as unsupported project misuse or a limitation of the existing autostart API,
not as a supported behavior that 0.6.2 must implement.

Christopher intends a later redesign toward node-scoped programmatic autostart
initialized explicitly in code, approximately in 0.6.4. This is future scope,
not a requirement to redesign autostart or add a new declaration API in 0.6.2.

The earlier recommendation to retain a runtime discovery, refuse its triggering
operation, and reconcile that discovery before a later admission was not
approved. Neither that mechanism nor live merging is required by this answer.
Do not add special discovery retention, automatic retry/reconciliation, active
claim relabelling, reservation expansion, or a new recovery scheme for this
unsupported case merely to close Q10. Do not claim graceful handling that has
not been implemented. Existing guards may still refuse unsupported changes.

This decision supersedes the earlier pending-Q10 restrictions and any reading
of AQ3 that requires supporting component merging during an admitted run.
The actual producing graph and membership remain historical authority. The
already agreed membership-change rules for code changes between runs remain
applicable; this decision does not cancel their implementation or verification.
Known-shape autostarts and supported job creation within admitted membership
remain in scope. Q10 no longer blocks independent or dependent 0.6.2 work on
the assumption of a fixed admitted membership.

Source: Christopher's explicit answer in the monitoring conversation on
2026-09-07, followed by his explicit request to relay it to the implementation
task. Earlier reviews that call Q10 undecided are historical on this point.
