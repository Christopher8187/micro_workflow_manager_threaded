# Automatic programmatic fresh preparation

This continues [Implement and verify the agreed MWF 0.6.2 workflow-management
changes](https://github.com/Christopher8187/product/issues/45). After one completed
`run_node`, the next call tried to start a terminal component without fresh
preparation.

The temporary 30-pass ownership control explicitly prepared each API round.
Review rejected that as acceptance of automatic entry. The original regression
is restored. MWF now prepares automatically.

## Current implementation

The coordinator moved from `cli/cleanup.py` to `workflow/preparation.py`.
CLI imports the same function. Independent `run`, `run_concurrently`,
`run_node`, and `run_component` calls request full fresh preparation.

Before creating a session, the shared entry observes one graph shape, ordered
component selection, and external-parent state snapshot. Blocked external
parents refuse before files, jobs, events, alignment, or sessions change.
Parents selected for this same run may still be queued. Native readiness
remains mandatory even when `ignore_readiness` bypasses the scheduler's
convenience check.

After admission and reservation, MWF checks the exact admitted shape and
component set, then repeats the observation. Shared preparation checks the
shape again and reads all selected component states and job plans before
staging files. The writer-side component begin decision remains the final
parent-state race check. Arbitrary graph edits after the last locked snapshot
remain part of the outstanding active-relationship work.

Preparation advances alignment, retains and requeues root jobs, and uses
staged-file restoration and exact SQLite decisions. Preparation errors pass
through session failure handling, which releases reservations and preserves
the original error. Nested task calls reuse their owner without preparing
again. Selected-job and queue-only calls retain their existing behavior.

## Verification

All runs use immutable Test Area copies. Manifests, hashes, logs, and JUnit XML
remain under `C:/Business/product/testing_ground/issue-45`. Names below start
with `sample-calculations-programmatic-fresh-`. Durations come from JUnit XML.
Every completed run has zero skips and collection errors.

| Source suffix | Passed | Failed | Seconds | Meaning |
| --- | --- | --- | --- | --- |
| `red-01` | 5 | 9 | 29.56 | Eight new cases used a nonexistent reader; the original repeated API case reproduced its lifecycle failure. |
| `red-02` | 0 | 8 | 5.50 | Corrected reader; all full entries failed the required alignment advance. |
| `green-01` | 6 | 9 | 16.71 | Initial wiring used a missing workflow root attribute. |
| `green-02` | 15 | 0 | 59.93 | Full entries, repeated API rounds, checkpoints, and module boundaries passed. |
| `boundaries-01` | 397 | 19 | 515.37 | Surrounding failures required native trace, repeated-run, and damage-injection corrections. |
| `preflight-red-01` | 17 | 7 | 38.40 | Six blocked-parent cases demonstrated mutation before refusal; one shape-refusal control differed only in message text. |
| `preflight-green-01` | 31 | 0 | 83.93 | Readiness, admitted-selection refusal, repeated execution, restoration, checkpoints, and module boundaries passed. |
| `native-controls-02` | 285 | 0 | 459.28 | Six complete modules include corrected lifecycle, restart, and CLI-readiness controls. |

The 19 surrounding failures are retained. Unstarted-job checks now require
exact preparation-only trace, no owner, and no output. Blocked external parents
retain their pre-command rows and trace. Repeated full calls must rerun and
advance alignment, with new execution identities. Two damaged-receipt tests
now increment the stored generation instead of assigning the valid value 1.
CLI readiness fixtures register topology explicitly instead of invoking a
blocked run solely for setup. Queue-only trace assertions remain unchanged.

The final combined source is `sample-calculations-input-ownership-final-03`,
with 347 Python files and freeze SHA-256
`57AAE1DDD674EF7F50E921AE50F486621AF3E8F9367B1636E70A848D190015BD`.
Its 22-module regression run passed all 774 checks in 1045.53 seconds.
The preceding `final-02` run
passed 772 and failed two in 1048.39 seconds. Both failures were the
same creation-trace expectation in partial-restart controls. Those assertions
now require preparation-only trace while preserving exact job control, no
owner or output, accepted successor execution, and reservation release. Both
corrected cases passed in 4.33 seconds. Both independent reviews returned
bounded PASS. [Ownership progress](managed-input-ownership-progress.md#final-source-and-review)
links the final source, review, and commit records. This record grants no
whole-requirement or release acceptance.
