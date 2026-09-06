# One supported 0.6.2 project model

Status: approved scope change, effective 2026-09-05. Legacy compatibility is
cancelled, not paused or deferred.

The scope is now published in [Implement and verify the agreed MWF 0.6.2
workflow-management changes, approved scope change](https://github.com/Christopher8187/product/issues/45#approved-scope-change-one-new-project-model).
The coordination task verified the issue-body readback at
`2026-09-05T21:16:23Z`. Its prepared body was read in full here and has SHA-256
`2299FEFEEC11C807DE819DDBBF2FBFA129E428AB30098C10AE5458A667DFBCA3`.
The [publication verification](../../../../testing_ground/issue-45/github-scope-publication-verification-20260905.json)
records an exact match. This publication changes neither source-push nor
production permissions. The relay below remains the original local history.

Christopher's instruction was relayed from the local task
`Check issue 45 progress (2)`, task ID
`01a071cf-399b-7712-8409-461f1ad4746a`. His stated direction was:

> I don't want some version of the app to work for two different kinds of projects.

The relay explicitly cancelled old-state import, conversion, backfill,
reconstruction needed only for legacy state, dual schema execution modes, and
adapters or branches needed only to keep old project specifications working.
Tests and reviews serving only those obligations are also cancelled. Their
historical source, results, and findings remain records of earlier work.

## Active implementation boundary

The completed application supports one new 0.6.2 project model. Remove
compatibility-only machinery from its supported execution path. Do not retain
it indefinitely as dormant complexity. Existing old projects and their data
remain untouched. The application may refuse them clearly and direct the user
to the separate project migration guide.

Current-version job identity, valid reopening, recovery, ownership,
transactional rollback, lineage, reset, sampling, and the other agreed 0.6.2
functionality remain required. A historical use of the word compatibility does
not cancel native behavior, current-version state validation, or correctness.

The legacy import obligation in 44-SES-033 and the runtime migration-window
obligation in 44-SES-034 are cancelled. The old generic-reader adapter role in
44-SES-036 through 44-SES-040 is cancelled. Exact main-session and multi-session
APIs and ambiguity refusal for native ownership remain required. The legacy
data portion of 44-SES-044 is cancelled; damaged native ownership must still
refuse ambiguity.

AQ2's old-result conversion, AQ4's embedded migration window, and Q7's missing
legacy membership handling are historical decisions and no longer require
runtime implementation. AQ1 retains only its current-version read-only preview
allowance. AQ3's legacy migration application is cancelled, while its actual
producing graph and exact membership remain authoritative for native stored
state. AQ5, AQ6, Q8, and Q9 remain applicable to native lineage and ownership.
Current-version membership repair, preview, and recovery rules also remain.

## Requirement reconciliation

The exact cancelled rows are 44-SES-033, 44-SES-034, and 44-SES-036 through
44-SES-040. In 44-SES-044, only the legacy-data branch is cancelled. Damaged
native ownership must still refuse ambiguity.

The later instruction partially supersedes the migration portions of
45-SRC-008, 45-PLN-011, and 45-TDD-011. It does not cancel their other runtime,
planning, persistence, ownership, or recovery work. The native several-session
requirement 44-REC-004 no longer depends on the cancelled legacy rows.

The negative read-only rule in 44-CMD-040 remains without requiring a migration
engine. The project-wide API-limit behavior in 44-SES-054 through 44-SES-057 is
native 0.6.2 behavior rather than an old-project adapter. Starting from the
published 0.6.1 source tree also remains an implementation instruction; it does
not require the completed runtime to open 0.6.1 project data.

The audit keeps the original 690 requirement meanings and adds supplemental
45-SM-001 through 45-SM-004 for the one native model, removal of old-state
machinery while leaving original projects untouched, native reopening and
transactional rollback, and the final guide. None of those rows has
implementation or acceptance credit merely because Christopher settled the
scope.

## Guide at the end

Christopher corrected the first relay about who would write the guide:

> You don't need to write it. I want the other AI to write this specification on how to migrate at the end.

This implementation task writes `C:/Business/product/migration.md` at the end,
using the completed runtime's actual APIs and behavior. It must give a future
AI enough context to revamp an older project's source, graph and tasks, inputs,
documentation, and instructions in a separate project, then initialize fresh
runtime state. It does not describe an embedded old-state conversion engine.
Guide writing and speculative migration trials are not a detour during runtime
implementation. Original examples and projects remain unchanged.

## Immediate effect

The prepared `sample-calculations-job-instance-failure-preservation-01` freeze
was not executed. The last completed identity selection was 66 passes in
18.45 seconds on `sample-calculations-job-instance-paths-preservation-01`.
Its legacy cases and the earlier backfill results remain historical. Native
identity and failure-preservation work continues after retiring cancelled
portions. No stage acceptance follows from this scope record.

Local progress commits remain authorized. This change grants no push,
publication, release, production, or original-project mutation permission.
