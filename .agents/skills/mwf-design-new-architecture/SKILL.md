---
name: mwf-design-new-architecture
description: Design graph, node, and task architecture for an MWF project that has no established design. Use for a new workflow or a project whose architecture has not been settled.
---

# Design new MWF architecture

1. Read the framework `README.md`, relevant `CONTEXT.md` terms, and all three
   architecture documents. Then inspect the target project's root README,
   node READMEs, `src/README.md` when present, source, and tests. Finish when the
   current inputs, outputs, constraints, and existing decisions are accounted
   for.
2. Define the start state, durable end state, semantic oases, and Job Scope on
   each path. Design the quotient DAG and Hoeflein components before raw nodes.
   Finish when every path has a purpose, cardinality, circulation bound, and
   durable result.
3. Define every node's role, runner, concurrency, main task, fallbacks,
   validation hierarchy, routing, replay boundaries, and output-provenance tree. Then define
   each task's parameters, files, function, validation, idempotency, and
   carried-forward paths. Finish when connected task paths agree and no retry or
   fallback system is hidden inside task code.
4. Record documentation and source disagreements using `AGENTS.md`. Continue
   independent design with visible provisional assumptions.
5. Write design documentation only when requested. Implement nothing unless the
   user explicitly asks. Return consulted sources, settled decisions,
   assumptions, inconsistencies, and the next decision or implementation step.
