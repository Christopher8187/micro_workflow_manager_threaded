---
name: mwf-debug-workflow
description: Debug or diagnose MWF execution, inspect input, trace lineage, or investigate component and session state. Follow persisted ownership, exact file paths, and directly related jobs to a concrete cause.
---

# Debug MWF work

1. Read the framework [README](../../../README.md), relevant
   [glossary](../../../CONTEXT.md) terms, and
   [operations](../../../docs/operations.md). Resolve the installed version,
   project root, project README, affected node READMEs and RUN files, their
   central component RUN, and the observed failure.
   Finish when expected behavior and actual behavior have distinct evidence.
2. Inspect the named job and its component. Keep job status separate from
   component state, stability, exact instability origin, and misalignment.
   Inspect every receiving node's recorded first cause for the current
   alignment generation. Identify the exact execution owner and session;
   distinguish an abandoned session from a live handler that is stalled or
   waiting for an interrupt child or API capacity.
3. For input problems, follow the producing raw node to the receiving raw
   node's producer-qualified path. Compare the task's actual exact-path or
   fixed-depth read with the published tree. Read the task-facing file rules in
   [task architecture](../../../docs/architecture/task.md). Verify file presence,
   publication ownership, and the generation in which the input arrived.
4. Run `mwf trace <node> job <id> --lineage`, adding `--json` for structured
   comparison. Follow a creating job or directly created job with another
   explicit lineage command. Continue only along relationships relevant to the
   failure. The compact view does not recursively traverse ancestry.
5. Use the full trace, `--errors`, and the node's `filter` stages when the cause
   depends on task attempts, fallbacks, validation, or output bodies. Test
   deterministic explanations first. Inspection sampling helps review a large
   population but does not establish correctness of uninspected jobs.
6. State the demonstrated cause, supporting command output or source, affected
   components, and the smallest corrective action. Keep diagnosis read-only
   until the user authorizes mutation. If repair is already authorized, use
   [mwf-run-workflow](../mwf-run-workflow/SKILL.md) for the exact reset, fresh run,
   resume, restart, or recovery procedure. Use `mwf-test` for framework changes.
7. Finish when the original failure is explained and an authorized repair has
   been verified, or when the missing evidence or unresolved decision is named.
   Report consulted sources and any assumptions still affecting the diagnosis.
   When updating documentation, record current defects and repair pointers in
   the affected node README and useful job findings in RUN, following the
   [documentation guide](../../../docs/workflow-documentation.md).
