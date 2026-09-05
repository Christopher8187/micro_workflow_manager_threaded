# AQ5 running-lineage source and specification review

Review date: 2026-09-05  
Reviewer assignment: GPT-5.6 Sol, xhigh reasoning  
Review type: bounded preparation and specification review  
Disposition: **PASS. AQ5 accurately records an unresolved architectural question, and both documentation findings from the initial review are resolved.** This review accepts no executable behavior, stage, or release requirement.

## Reviewed question

AQ5 in `mwf/docs/plans/0.6.2/architectural-questions.md`, finally reviewed at SHA-256 `17B23A7219E27BECE31D862A0E3CC9E539AB6E3C56162EF86A0306BB4F64BE45`, asks what happens to the compatible stability value and exact instability origin while an aligned sampled component resumes with lifecycle `running`.

The core question is valid. Neither the approved component clauses nor the approved sampling clauses select one of these representations:

1. The running component continues to expose the compatible stability and exact origin established by its sampled work.
2. The running component clears its current result values while retaining enough component-owned history to restore the compatible stability or exact origin when the component returns to `sampled` or reaches `done`.

Both representations can satisfy the settled endpoint behavior. Choosing either one now would add semantics that the approved sources do not contain.

## Sources examined

The specification review used:

- [Settle the MWF workflow-management model for 0.6.2](https://github.com/Christopher8187/product/issues/44#issuecomment-5539997969), specifically `Component model`, `The nine graph commands`, `Execution sampling`, `Tracing`, and `Required reconciliation with current MWF`, as preserved in `testing_ground/issue-45/requirements.md` at SHA-256 `03F08568707AC4495A7C63823223CE41C56A7C76518718DC01811D934FF8F82D`.
- The requirement rows `44-CMP-003` through `44-CMP-009`, `44-CMP-014`, `44-CMP-025/026`, `44-CMD-005`, `44-SMP-041`, `44-SMP-044` through `44-SMP-055`, `44-TRC-004/007`, `44-REC-021`, `44-REC-026`, and `44-REC-039` in `mwf/docs/plans/0.6.2/requirements-audit.md`, finally reviewed at SHA-256 `FB0038D9B6ABB1AA6BBC679D77C14ED6064980B93AD253938FFE143F609836CF`.
- `mwf/micro_workflow_manager/workflow/component_state.py`, especially lines 160 through 268, at SHA-256 `DE917D45EA2BE69B46D7B305CFD9D4671383912940E57EC28FC130E37AC4AA3A`.
- `mwf/micro_workflow_manager/workflow/component_scheduler.py`, especially `run_component()`, at SHA-256 `9AE176B57A891396C32348F621F261E94895AF47C22C1FA82B38D8C27ED4538F`.
- `mwf/micro_workflow_manager/cli/run_commands.py`, especially `_prepare_node_for_resume()`, `resume_node()`, and `resume_from()`, at SHA-256 `5447950EA50762D57253DD3DD4550EAF0804610BE73E424432EC9DAA27050F39`.
- `mwf/micro_workflow_manager/cli/run_selected.py`, especially `_run_selected_jobs()`, at SHA-256 `3C3FE982BD0FCBD66F4FA7C7C7F28B128D38F04A3067092106FC5AF6856A7615`.
- The private component-ownership preparation and current stage boundary in `testing_ground/issue-45/next-s2-preparation.md` and `mwf/docs/plans/0.6.2/stage-component-ownership.md`.

After completing that source and specification review, I read both complete preparation transcripts as the required final check:

- `testing_ground/issue-45/preparation-transcript.txt`, 224 lines, SHA-256 `6BC1CBE2AE91ADCE816A2FDE3C85D1C18CDD8F8E849D39E688056B1AF899F94B`.
- `testing_ground/issue-45/preparation-later.txt`, 249 lines, SHA-256 `253582430EF832268DDB4A8EF12B15C67FBA1651678D36CA5BF1C794FC021ED2`.

The transcripts contain the approved deferral, staging, review, and test-first procedures. They contain no decision about stability or origin values during a sampled-to-running resume. The full-history condition for reporting AQ5 as unresolved is therefore satisfied.

## What the approved requirements settle

The final resolution establishes all of the following:

- `running` is one of the five component lifecycle values.
- `stable` and `unstable` are successful result forms rather than lifecycle values.
- A sampled component is not done and still exposes the compatible stability lineage and exact origin established by completed work so far. The resolution gives an explicit sampled and unstable record example.
- Queued and failed components have no successful stability result.
- An aligned sampled component may resume. If all remaining eligible work succeeds and no unprocessed work remains, it becomes done while preserving the compatible stable result or exact origin.
- A sampled component blocks quotient descendants. Ordinary readiness requires complete successful predecessors with compatible stability and origin.
- Component lifecycle and result data have one component-level authority. Raw-node rows must not hold separate authoritative result copies.
- Text and JSON lineage report component lifecycle, stability, and origin. The JSON minimum shape includes `component.stability` and `component.instability_origin`.

These clauses settle the sampled starting record, the successful done record, and readiness. They do not say what the two result values contain after the lifecycle changes to `running` and before it returns to a result-bearing state.

The phrase “successful forms of a done result” does not close the question. The same component section expressly allows a non-done sampled lifecycle to carry compatible stability lineage and origin. That exception demonstrates that lifecycle alone does not determine whether result lineage is visible. No equivalent rule is supplied for `running`.

A running component cannot satisfy successful predecessor readiness under either answer. Retained lineage values would describe established prior work; they would not turn the active lifecycle into a complete predecessor. Readiness must continue to test lifecycle together with compatible result information.

## Current-source assessment

The current runtime offers no retained precedent that can answer AQ5:

- `ComponentStateMixin.refresh_component_status()` derives raw-node status from raw job counts and writes `RUNNING`, `WAITING`, `QUEUED`, `DONE`, or `FAILED` to raw-node rows. It has no component stability or instability-origin fields.
- `_prepare_node_for_resume()` requeues unsuccessful raw jobs and then writes or derives raw-node status. It does not distinguish a resume that began from `sampled`, and it carries no prior component result value through the transition.
- `ComponentSchedulerMixin.run_component()` coordinates raw jobs and refreshes raw-node status. It has no component result record to retain or clear.
- `_run_selected_jobs()` writes the addressed raw node as `RUNNING`; current sampling code does not provide the final component lifecycle and lineage model.

Current raw-node behavior therefore cannot be elevated into an answer. The missing component record is part of the work being implemented under the final resolution.

## Finding RL-001: resolved

The initial AQ5 text asked whether the result fields were absent. That wording was safe for nullable persistence values only if “absent” meant that the current values were null or cleared. It was too broad for compact JSON lineage.

`44-TRC-007` requires `component.stability` and `component.instability_origin` in the versioned JSON minimum shape. AQ5 does not reopen whether those keys exist. If the architectural answer hides the current result during running, the JSON keys still need a settled representation, normally null values, while component-owned historical state preserves what `44-SMP-055` needs at successful completion.

The corrected question now asks whether “their current values become null” and immediately states, “The required lineage JSON keys remain present.” This fully resolves RL-001 without choosing an answer to AQ5.

## Finding RL-002: resolved

The initial dependency paragraph reached the right areas, and its “running portion” qualifier prevented a blanket block. Its opening list was broader than the unresolved choice. The required granularity was:

| Requirement area | AQ5-dependent portion | Work that remains independent |
| --- | --- | --- |
| `44-CMP-025` | Allowed stability and origin values, validation, and update behavior for a component running after a sampled resume | Component identity, columns, all settled queued, failed, sampled, and done combinations, misalignment, and alignment generation |
| `44-CMP-026` | The answer constrains where any latent prior lineage may live | The single component-level authority and prohibition on authoritative raw-node result copies are already settled and should not be described as blocked |
| `44-CMP-014` | Display of stability and origin for this running interval | Diagnostics for every settled state and all other component fields |
| `44-SMP-054` | Whether the sampled-to-running write retains or clears current result values | Alignment preflight, permission to resume, and the lifecycle transition to `running` |
| `44-SMP-055` | The storage handoff that carries prior lineage through the active interval | The successful terminal outcome is fully settled: `done` with compatible stability or the exact origin |
| `44-TRC-004/007` | Values reported while observing this specific running interval | Command shape, required JSON keys, all other lineage fields, and output for settled component states |
| `44-REC-021`, `44-REC-026`, `44-REC-039` | Assertions about visible values during sampled resume, including restart or recovery from that exact in-flight form | Other component-state, sample readiness, descendant blocking, resume endpoint, and lineage regressions |
| `44-DOC-019`, `44-DOC-021`, `44-DOC-023` | Documentation and CLI wording for this exact running representation | Documentation of all settled lifecycle, sampling, tracing, and diagnostic behavior |

“Recovery” should mean only recovery of a persisted sampled-resume execution whose component was recorded as running. General session recovery, job recovery, and all recovery behavior that does not interpret this result combination remain available.

“Monitoring, lineage, documentation, and regression behavior” should likewise mean only the current stability and origin values shown for this interval. Those subsystems can proceed for every settled state and field.

The core rule in `44-CMP-026` is a constraint on either answer rather than an unresolved requirement. If prior lineage is retained outside the current result values, it must still be component-owned and must not become another authoritative raw-node result. That storage detail needs the AQ5 answer, while the one-authority rule itself does not.

## Independent work boundary

The current private component-ownership section correctly remains independent by omitting lifecycle and result columns and any generic component-state writer. Exact component identity, producing graph shape, session reservations, reference-counted holds, and explicit job-claim ownership do not depend on whether running exposes prior sampled lineage.

The same reasoning permits later work on schema capacity and settled component-state combinations, provided it does not validate, write, display, or document either running-lineage alternative as approved behavior. Nullable physical capacity alone does not select an answer.

AQ5 does not unsettle terminal or sampled outcomes. It also creates no new question about predecessor readiness: lifecycle `running` remains incomplete even if prior lineage values stay visible.

## Final correction verification

The corrected AQ5 now:

- presents the open alternatives as retained current values or null current values;
- expressly preserves the required compact-lineage JSON keys;
- limits dependent work to allowed running-row result combinations, the result-value mutation when sampled work starts running, recovery of that exact in-flight representation, and display or tests of those values during the interval;
- states that the lifecycle transition to `running`, single component authority, settled sampled and terminal outcomes including `44-SMP-055`, and general recovery or diagnostic work remain independent; and
- keeps component identity, graph shape, reservations, holds, and explicit job-claim ownership independent.

The corrected requirements audit applies direct, narrow AQ5 links to `44-CMP-014`, `44-CMP-025`, `44-SMP-054`, `44-TRC-004`, `44-TRC-007`, and `44-REC-021`. Each disposition says that only the running-state lineage portion is blocked and that other settled work remains independent. It removes the AQ5 link from `44-CMP-026`, whose single-authority rule is settled, and from `44-SMP-055`, whose terminal behavior is settled. The broader resume, lineage-regression, and documentation rows remain pending without being presented as wholly blocked.

These edits resolve RL-001 and RL-002. AQ5 now records the missing decision without broadening it or authorizing either running-lineage representation.
