---
name: mwf-run-workflow
description: Run, resume, reset, interrupt, sample, or isolate work in an MWF project. Translate the requested result into an exact selection, inspect its plan, and follow execution through its owning session.
---

# Run MWF work

1. Read the framework [README](../../../README.md), relevant
   [glossary](../../../CONTEXT.md) terms, and
   [operations](../../../docs/operations.md). Resolve the project's root,
   installed MWF version, root README, affected node READMEs and RUN files,
   and their central component RUN. Finish when
   the available commands and the project's durable inputs and outputs are
   known for that version.
2. Translate the requested result into named raw nodes, quotient components,
   and a completion condition. Choose one component, its descendants, or the
   half-open quotient interval that excludes its end component. For selected jobs, record
   the exact IDs or sample selectors. Distinguish fresh execution from resuming
   retained work before choosing a command.
3. Preview a consequential selection with its `--plan` or `--dry-run` form.
   Account for every selected component, expanded membership repair, preserved
   start input, changed downstream file or job, and affected receiver outside
   the selection. Resolve any refusal before executing. Existing user
   authorization applies; ask only when a required choice or affected work lies
   outside it.
4. For sampling, inspect each member's eligible population, selected IDs, seed,
   and digest. Reuse an explicit seed for repeatable selection and pass the
   reviewed population digest when selection drift must refuse execution.
   Report sampled component state separately from completed selected jobs.
5. For interrupt components, resolve the command's stop/run policy and each
   component's choice. An explicit interrupt command targets its start
   component; descendants retain ordinary readiness. Follow pause and transfer
   progress through exact session IDs. An interrupted parent may remain at a
   checkpoint until its child settles.
6. Execute the authorized command from the resolved project root. Monitor its
   session, component states, and job failures until the completion condition
   holds or a specific refusal or failure prevents it. Use native recovery for
   abandoned ownership. Route unexplained behavior to
   [mwf-debug-workflow](../mwf-debug-workflow/SKILL.md).
7. Keep useful current costs, filter statistics, job problems, and operating
   findings in the affected RUN, following the
   [documentation guide](../../../docs/workflow-documentation.md#node-run).
   Report the exact command, selection, sample and interrupt identities when
   present, session outcome, durable result locations, and any remaining failed,
   sampled, stopped, or misaligned work. A stopped session or sampled component
   does not establish completion of the whole downstream workflow.
