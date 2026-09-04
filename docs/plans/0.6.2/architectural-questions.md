# Architectural questions awaiting Christopher

These questions remain open under [Implement and verify the agreed MWF 0.6.2
workflow-management changes](https://github.com/Christopher8187/product/issues/45).
They do not change the approved behavior or accept the unfinished implementation.
Independent work continues before the next grilling.

The implementer checked the final behavioral resolution, current code, and the
complete local preparation task **Find objective for issue #45**, including its
later corrections. The relevant approvals are Q1 and Q2 on dependent work and
stage ordering, Q6 on factual disagreements, Q8 and Q9 on authorization and
preservation, and the later requirement to read the full preparation before
deferral. Those turns establish the decision procedure. They do not answer the
three behavior questions below. Sol xhigh reviewers also read the complete
preparation before reporting these questions.

An [independent Sol xhigh review](architecture-questions-review.md) checked
the questions and their 27 narrowly blocked requirement dispositions. It found
no already-settled answer or invented behavior. The implementer has also read
the complete original behavioral task **Review GitHub issue #44**, including
the missing Q99-Q111 exchange recovered from that task's local rollover.

## AQ1: SQLite coordination during read-only previews

May a preview change an already-existing `state.sqlite3-shm` file while taking
a transactionally consistent snapshot of a live database?

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

No exception has been assumed. Live SQLite preview acceptance is blocked for
44-CMD-039 and 44-CMD-046. This also blocks complete plan/dry-run acceptance,
sample-plan persistence checks, stale-session previews, recovery dry-run,
related regression coverage, and documentation that claims every such path is
read-only. Pure selection and graph calculations remain independent.

The local investigation is `testing_ground/issue-45/preview-storage-review.md`
in the Parent Repo. The unfinished loader and its failing WAL regression remain
outside accepted commits.

## AQ2: Converting legacy component results

How should migration convert ambiguous legacy raw-node state into one component
lifecycle and successful-result lineage?

The resolution requires one component record with lifecycle, stability, exact
instability origin, misalignment, and alignment generation. Existing
`storage/sqlite/transfer.py` imports separate raw-node statuses. Existing
`workflow/component_state.py` permits those values to differ. Neither the final
resolution nor the preparation defines a migration result for `{done, queued}`,
`{done, failed}`, or stale `{running, waiting}`. Legacy selected-job and sample
execution also lacks the new durable sample lineage.

Christopher must choose whether ambiguous reusable state refuses until explicit
fresh preparation, receives a specified conservative conversion, or is derived
from validated job evidence under an approved decision table. Automatically
calling legacy completion stable would choose a policy that has not been
approved. Missing historical interrupt or sample identity must not be invented.

Legacy conversion for 44-CMP-002 through 44-CMP-010 and 44-CMP-025/026 remains
blocked. Dependent activation includes raw-node displays, readiness, reuse,
sampling, stability propagation, misalignment, lineage diagnostics, recovery,
and membership repair on migrated projects. Their new-project calculations and
the independently settled session registry may proceed where no legacy result
is assumed.

## AQ3: Authoritative component membership before migration

What source determines exact component membership before migration and before
router mounting or starter creation can mutate state?

Current `cli/autostart_scan.py` recognizes literal `add(..., autostart=True)`.
Supported `add_many`, `add_job`, `add_jobs`, and computed routing can register
autostart relationships later through `workflow/job_creation.py`. Legacy state
does not retain the historical autostart shape. Stored raw edges plus the
current scanner therefore cannot universally reconstruct the component that
produced reusable work.

The alternatives have different responsibilities. Requiring a statically
discoverable declaration constrains project routing. Broader discovery needs
defined limits. Persisting a synchronized component shape requires deciding how
later dynamic discoveries and missing historical shapes are handled. No choice
has been made.

General migration and graph-update integration for 44-MEM-002 through
44-MEM-010 remain blocked. This includes overlap repair, preserving the prior
shape before node deletion, and the ordering of component conversion before
router and starter writes. It also blocks final acceptance of component-keyed
ownership, lifecycle, preparation, and scheduling on those affected projects.
The exact sorted-member key calculation itself is already specified.

The local S2 investigation is
`testing_ground/issue-45/state-migration-review.md` in the Parent Repo. It records
the safe loading order and separate settled session tests. Liveness refusal
before any legacy migration, SQLite-only session writes, one main with several
interrupts, exact new job ownership, and non-guessing readers are settled and
remain eligible for independent implementation.
