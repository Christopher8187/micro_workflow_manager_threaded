---
name: mwf-analyze-architecture
description: Analyze an existing MWF project's graph, node, and task architecture and recommend improvements. Use for assessment, diagnosis, or a broad request to improve a workflow.
---

# Analyze MWF architecture

1. Read the framework `README.md`, relevant glossary and architecture pages,
   project and node READMEs, source, tests, and available run evidence. Finish
   when the intended design and current behavior can be compared at graph, node,
   and task scales.
2. Trace Job Scope through semantic paths, quotient-DAG vertices, Hoeflein
   components, task files and parameters, validation, failure stages, output
   provenance, and recovery. Inspect data and diagnostics without mutating the
   project.
3. Classify each finding as a demonstrated contradiction, framework risk,
   missing evidence, or undecided behavior. Link the source, test, event, or
   document that supports the classification.
4. Recommend bounded changes in dependency order. State the expected behavior,
   affected architecture, regression needed, migration or data risk, and any
   decision the user must make.
5. Remain read-only unless the user explicitly asks to apply a selected
   recommendation. Return consulted sources, findings, provisional assumptions,
   and the narrowest useful next action.
