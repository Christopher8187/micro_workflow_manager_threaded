# MWF agent entry point

These instructions apply to every request that may need Micro Workflow Manager
knowledge. This includes framework questions and changes, architecture, testing,
documentation, and work on a project built with MWF.

## Product Workspace context

Read `C:\Business\product\CONTEXT-MAP.md` before planning Parent Repo directory
or multi-repository work, referring to another Product Workspace repository,
changing AFSR instructions, or preparing a reply about Product Workspace files
or repositories. Follow its permission boundaries before changing another
repository or a production system.

## Read in this order

1. Start at `README.md` for the documentation map and current user-facing
   behavior.
2. Read the relevant definitions and boundaries in `CONTEXT.md`.
3. Follow the README links to the relevant graph, node, task, or testing
   document.
4. For an MWF project, read its root `README.md`, the README for every affected
   node, and `src/README.md` when present.
5. Inspect the relevant source and tests before asserting current behavior or
   planning a change.
6. Use a repository skill when the request matches a repeatable procedure.

An informational request may stop after the documents and current source answer
the question. It does not need a skill.

## Route by intent

| Request | Route |
| --- | --- |
| Explain MWF or an MWF project | Read the relevant documentation and source. No skill is required. |
| Design a project without an established architecture | Use `mwf-design-new-architecture`. |
| Apply a known architecture change to an existing project | Use `mwf-modify-architecture`. |
| Assess or broadly improve an existing architecture | Start with `mwf-analyze-architecture`. Apply a selected recommendation only after the user asks. |
| Test MWF or a project built with MWF | Use `mwf-test`. |
| Create or update an MWF project's documentation | Use `mwf-document-workflow`. |

Architecture skills divide work by intent. Each may need graph, node, and task
documentation. A broad request to improve a workflow authorizes analysis, not a
broad rewrite.

## Handle disagreement without hiding it

When documentation and source disagree, continue work that does not depend on
the disputed point. Record all of the following:

- the intended documented behavior;
- the current implementation behavior and supporting source or test;
- the interpretation used for unaffected work;
- each provisional assumption;
- every decision that depends on the disagreement.

Wait for individual review before making an executable change that depends on
the unresolved point. Distinguish a demonstrated contradiction, a framework
risk, missing evidence, and undecided behavior. Do not turn one category into
another.

## Working boundaries

Preserve user data, dirty files, and linked-worktree state. Treat MWF project
state as durable unless the user authorizes a reset, paste, fresh
run, deployment, or other mutation.

Before editing MWF framework source, engine code, tests, benchmark programs or
results, skill scripts, examples, or any other executable material, obtain
Christopher's explicit approval for one narrow change. Independent executable
changes need separate approval and must not be bundled. Documentation and
instruction-only skill changes may proceed after their meaning is settled.

Do not generate a context snapshot or mandatory handoff file. Put lasting
decisions in the document that owns them. In the response for a specific run,
list the sources consulted, decisions made, provisional assumptions, and
unresolved inconsistencies.
