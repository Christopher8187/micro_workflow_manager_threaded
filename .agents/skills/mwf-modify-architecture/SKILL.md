---
name: mwf-modify-architecture
description: Apply a known architecture change to an existing MWF project. Use when the desired change is already identified, including requested code, test, or documentation work.
---

# Modify existing MWF architecture

1. Read the framework `README.md`, relevant glossary and architecture pages,
   then the project's root and affected node READMEs, source, and tests. State
   the known change and the current behavior. Finish when its graph, node, and
   task effects are bounded.
2. If the desired change is still broad or unknown, stop modification and use
   `mwf-analyze-architecture`. Otherwise trace affected Job Scope, routes,
   components, task parameters and files, fallbacks, validation, output provenance,
   replay, and recovery.
3. Handle every documentation and source disagreement through `AGENTS.md`.
   Await review before executable work that depends on an unresolved point.
4. Match the user's requested action. A design or documentation request does not
   authorize code. Before changing source, engine code, tests, benchmark
   programs or results, examples, skill scripts, or other executable material,
   apply the explicit one-change approval gate in `AGENTS.md`. An implementation
   request includes only the separately approved source, regression coverage,
   and documentation changes for the known change.
5. Use `mwf-test` for verification. Finish when the requested behavior, affected
   documents, regression result, preserved dirty state, and unresolved risks are
   reported.
