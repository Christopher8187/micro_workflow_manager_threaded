# Native public component execution progress

This change preserves ordinary component activation, guarded parent readiness,
and nested repair continuation from baseline
`9b4c1c124c9b67f1bffe1685eceb74160908dee7` on
`codex/mwf-062-workflow-management`. Christopher requested one coherent local
commit after the included failing cases are corrected and relevant checks
pass without failures. This record covers that bounded local change.

The current executable candidate covers 329 Python files in the immutable
`sample-calculations-native-component-node-repair-epoch-green-01` freeze,
SHA-256 `ACABAB560856ECF9B77DA51966A585DDF14AC38374AB0A7BA9F25B3184F87FC6`.
Test 092 has SHA-256
`5C38F832A9376418F84AED01B3D70BDBC339473360EDC96425A708A3726827B1`.

## Included behavior

Ordinary full execution records proposed lineage when the component begins.
The writer rechecks native parent observations before starting it. Successful
parents publish while the session retains reservations, allowing selected
descendants to start. Empty full components can complete. An optional
readiness callback only narrows native eligibility.

Nested calls validate their active task parent and reuse its session.
Same-component failure cleanup retires only attempts retained by the nested
operation. Pending records allow session exit to discover accepted nested
repairs, including work started in a process worker. Every accepted sibling
repair is retained before ordinary queues reopen.

Job claims check unfinished work inside the writer transaction. Failed work,
accepted queued replacements, and claimed replacements keep ordinary work
queued. A batch containing only validated accepted successors can proceed.
Accepted repairs precede pending terminal metadata validation; damaged
completion records still prevent later settlement. An ordinary batch's
validation error cannot roll back a separately valid replacement batch.
Full singleton calls preserve their successful Python values in admission
order, close the old source, and arbitrate incomplete work before returning.
Node calls finish accepted replacements before reopening buffered ordinary
jobs and the original source. An explicit session-driver stop keeps ordinary
admission paused. Actual job errors take precedence over an admission pause.
Finite selected calls within a pending full component propagate admission
refusal. Independent finite calls retain their existing claim behavior. A caught nested
failure without an accepted restart leaves ordinary jobs untouched and ends
the component and session as failed.

Ownership observations, sampled transitions, node scheduling, and component
failure cleanup now live in separate focused modules. This restores the
existing source-module size limit without changing its exceptions.

## Verification and review

The final frozen source passed
[268 surrounding execution checks in 348.71 seconds](../../../../testing_ground/issue-45/sample-calculations-native-component-node-repair-epoch-execution-01-run.log)
and [341 storage and public lifecycle checks in 216.43 seconds](../../../../testing_ground/issue-45/sample-calculations-native-component-node-repair-epoch-storage-01-run.log).
All 609 checks across the 15 full modules passed without failures, errors, or
skips. The same source also
[passed 57 focused checks in 84.50 seconds](../../../../testing_ground/issue-45/sample-calculations-native-component-node-repair-epoch-focused-01-run.log),
including all four preceding threaded failures and retained-value controls.

These selections cover all changed test modules, exact ownership, active
repairs, claim batching, native missing-session refusal with preserved data,
accepted repair before damaged terminal metadata rejection, module limits,
and the affected direct, threaded, API, and process execution paths. All 23
changed Python files match the frozen bytes. The full 329-file map matches
Direct source content after accounting for Windows line endings.

Independent GPT-5.6 Sol reviews at xhigh reasoning passed both
[Standards](../../../../testing_ground/issue-45/native-component-checkpoint-standards-final-addendum.md)
and [Spec](../../../../testing_ground/issue-45/native-component-acab-final-spec-review.md)
for this bounded local commit. They accept the corrections and verification
without granting broader stage or release completion.

The
[verification history](../../../../testing_ground/issue-45/native-component-public-verification-progress.md)
retains the earlier failures, corrections, exact sources, and bounded reviews.
No failing case has been removed from the final selection.

## Remaining work

This change does not complete the whole component lifecycle stage or any
complete 0.6.2 requirement group. Process nested-singleton coverage, CLI native
readiness, raw-node authority removal, recovery, strict previews, clipboard
transactions, interruption, transfer, sampling, and dynamic membership remain
unfinished. No performance limit was relaxed. No push, release, or final Astra
review is included in this commit. The whole ordinary suite, separate cyclic
suite, and release checks were not rerun for this bounded local change and
receive no new completion credit.
