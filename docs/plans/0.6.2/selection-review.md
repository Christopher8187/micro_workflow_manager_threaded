# Stage 1 selection review preparation and S1a review

## Disposition

This note prepares the stage 1 review for selection calculations and read-only command paths. It also records the completed adversarial review of the first narrow section, S1a, which adds `MicroWorkflow.component_interval()` and its focused documentation and tests.

S1a passes review after one correction. The first diff leaked NetworkX errors for unknown endpoint names. The corrected code validates both names before graph traversal and raises `InvalidGraphError`. No other code or documentation finding remains. Acceptance of the larger stage still depends on the pending command wiring, preview isolation, and the implementing agent's broader checks.

## Sources and precedence

The review read these sources in full where they govern this section:

- `testing_ground/issue-45/issue-45.json`, including the approved execution procedure and all four comments.
- `testing_ground/issue-45/requirements.md`, which contains the issue 44 cross-session architecture clarification and final consolidated resolution.
- Cached issues 15, 17, 22, and 44. Issue 22 closes the 0.6.1 prerequisite at commit `837f931746d94a1b4f1ec5f8d5e92ae605aad3a7`. Issue 17's later note makes current MWF documentation authoritative where its old output wording differs. Issue 15 preserves the release boundary and points issue 45 to its bounded approval procedure.
- `CONTEXT-MAP.md`, `docs/agents/issue-tracker.md`, and the MWF `AGENTS.md`, `README.md`, `CONTEXT.md`, `docs/testing.md`, `tests/README.md`, `docs/architecture/graph.md`, and `docs/operations.md`.
- The complete local preparation task through `preparation-transcript.txt` and `preparation-later.txt`.
- The complete local task `Review GitHub issue #44`. The task connector was unavailable in this reviewer session, so I used the authorized rollout fallback with `FileShare.ReadWrite`. I read the archived parent history through Q1-Q21 and every later task window through Q123. This included the final audit and corrections, not only a summary.

The final consolidated issue 44 resolution supersedes the initial resolution and its audit comment. Earlier worked examples remain useful only after later corrections are applied.

## Exact selection requirements

The nine-command model combines `run`, `resume`, and `reset` with one component, `from`, and `between` selections. `from` contains the starting Hoeflein component and every quotient-DAG descendant. It reruns the starting component without resetting its incoming input.

For raw endpoint names A and B, let `C_A` and `C_B` be their Hoeflein components. The closed interval is:

```text
(descendants(C_A) union {C_A})
intersection
(ancestors(C_B) union {C_B})
```

Every `between A B` command selects the half-open interval `[C_A, C_B)`. It includes all of `C_A`, excludes all of `C_B`, and includes every quotient component on at least one directed route from A to B. `C_B` must be a strict directed descendant of `C_A`. Same-component endpoints, reversed endpoints, disconnected endpoints, and endpoints joined only in the underlying undirected graph are invalid.

The calculation must use ancestor and descendant reachability rather than enumerating routes. All nine commands must apply the same component selection in parsing, planning, execution, monitoring, help, descriptions, and tests. Selected components may publish managed jobs or input to an unselected receiver, but that publication does not admit the receiver for execution.

Every between command has a preview. `runbetween` and `resumebetween` use `--plan`; `resetbetween` uses `--dry-run`. The preview must show:

- selected quotient components and expanded raw nodes;
- the excluded B component;
- entering and leaving edges;
- prerequisite state;
- unselected receivers that may receive publications.

Read-only means no durable or externally visible mutation. A preview may read source and graph information. It may not migrate storage, mount or refresh runtime state, create starter jobs, reserve a session, prepare a component, or run task code.

## Exact execution-sampling calculations

Execution sampling is valid only on plain `run`, including `run ... --interrupt`. It is invalid on `resume`, every `from` or `between` command, and every reset command.

Supported forms include:

```text
mwf run A sample 30
mwf run A sample 10%
mwf run X sample X=30 Y=10
mwf run X sample X=10% Y=25%
```

The shorthand applies only to the named raw node, even when it belongs to a multi-node component. Named assignments must name raw members of the starting component. Omitted raw members select zero. With no positive selector, report no work and leave component state unchanged.

When `--status` is absent, all existing jobs in the addressed raw node form the population. When present, apply the status filter first. For percentage `p` and filtered population size `N`:

```text
0                  when p = 0
ceil(p * N / 100)  when p > 0
```

A positive percentage therefore selects at least one job when `N > 0`. Rank jobs with SHA-256 keyed by the recorded seed and stable job identity, then take the lowest ranks. This is uniform selection without replacement within each raw node. Persist the algorithm version. Independent samples may overlap.

Generate and print a fresh seed by default. `--seed` repeats a selection only while the population and relevant input remain unchanged. A plan prints but does not persist its seed. It shows each raw node's population, selector, selected count and IDs, seed, and digests. It prints a replay command with `--seed` and `--expect-population`. Drift refuses before preparation.

A partial sample obeys ordinary readiness. Parents must all be done and stable, or all done and unstable with the same exact origin. Incomplete parents, mixed stable and unstable parents, or different origins refuse unless an explicit interrupt is valid for the starting component. A partial success leaves the whole component sampled and blocks quotient descendants.

A sample becomes an ordinary full run only when at least one job is selected, every raw member with nonzero eligible work selects its entire population, all same-component causal work succeeds, and no newer work remains. Zero-job members are vacuously covered. Stability still comes from parent results.

Selected roots receive selected-job fresh preparation. Old same-component causal descendants of those roots are cleared, not executed. New same-component causal jobs created by this invocation run recursively. Unrelated existing jobs and quotient descendants do not run. Exact selected-job execution follows the same rule.

## Worked examples and corrections

### Interval examples

- Q6 introduced `A -> {X,Y} -> B` and required a preview. Its initial wording preserved A. Q12 corrected that point: A must run again because `runfrom A` preserves A's incoming input. B remains excluded. Publications may leave the interval, while execution may not.
- Q11 established the ancestor/descendant intersection and Christopher accepted the term and calculation.
- Q16 consolidated the final half-open behavior: include and freshly prepare A's component, exclude B, preserve A's input, exclude descendants of A that cannot reach B, allow publications outside, and never schedule outside components. The final resolution adds explicit same-component, reverse, and unreachable rejection.

Regression graph:

```text
A -> B -> D -> After
|         ^
-> C -----|
A -> Side
Outside -> C
```

`between A D` selects A, B, and C. It excludes D, After, Side, and Outside. If A and B collapse into one Hoeflein component, either endpoint name selects that complete component.

### Sampling examples

- Q9 chose causal circulation. Selecting `A/17` may run newly created `B/42` and `A/91` in the same component, but not unrelated existing jobs.
- Q15 settled per-raw-node percentages, unspecified members at zero, status filtering, SHA-256 ranking, ceiling rounding, plans, and a population drift guard. Q21 corrected only the default seed: each invocation gets a fresh seed, while an explicit seed repeats the selection.
- Q59 rejected the proposal that sampling itself is unstable. Partial work is sampled. Later Q64 separated sampled lifecycle from retained stability information.
- Q65 and Q66 settled that the sample invocation can finish while descendants stay blocked, and that ordinary `resume A` may preserve sampled work. Sampling syntax stays on `run` only.
- Q67 permits `sample` with plain `run --interrupt`. Q68 makes sampled state component-wide.
- Q69's proposal that every 100 percent sample remain sampled was rejected. Q70 corrected Christopher's first shorthand: full coverage uses ordinary readiness and may produce stable or compatible unstable output. It is not unconditionally stable.
- Q71 defines full coverage across every nonzero raw-member population. Q72 accepted counts and percentages, unnamed members at zero, fresh seeds, and same-component causal execution.
- Q73's special resume reasoning was superseded by Q77 and Q81. An aligned sampled component resumes normally while retaining its stability and origin. Misalignment is the separate resume blocker.
- Q76 corrected an interim statement that ordinary partial samples require stable parents. They have the same readiness as an ordinary run, so same-origin unstable parents are also allowed.
- The final transcript audit recovered ceiling rounding, status filtering, drift checks, detailed plans, and explicit zero-work behavior. Christopher then approved the audited additions before the final consolidated resolution was published.

Concrete calculation cases:

```text
p=0, N=50    -> 0
p=1, N=1     -> 1
p=10, N=11   -> 2
p=25, N=4    -> 1
p>0, N=0     -> 0
```

For component `{X,Y,Z}` with filtered populations `X=11`, `Y=4`, and `Z=100`, `sample X=10% Y=25%` selects 2 X jobs, 1 Y job, and 0 Z jobs.

## Preview bootstrap boundary

The Q1-Q123 discussion does not require previews to execute current `graph.py`, and it grants no exception for source-import side effects. The final resolution requires the observable result to be strictly read-only.

Current MWF provides a compatible route. Graph synchronization is explicit, `.mwf/project.json` stores synchronized edges, and only `mwf graph` applies graph metadata and node-folder changes. `mwf engine` already reads those edges and uses AST-only autostart scanning without importing graph or task modules or constructing `FileStorage`.

Stage 1 previews should therefore calculate against synchronized stored edges, use source text or AST only where needed, and bypass `ensure_runtime_layout`, `load_workflow`, router mounting, and normal `FileStorage` initialization. Applied execution retains separate graph synchronization validation.

This is an implementation reading of the current documentation plus the no-mutation requirement. It does not restrict authored graph syntax. Executing arbitrary or callable `EDGES` during a preview cannot guarantee no external mutation because current `import_file()` uses `exec` and `read_edges()` may call user Python. If previews are later required to establish semantic equality with unsynchronized dynamic graph source, that mechanism remains an architectural question for Christopher. The present specification does not require it.

Regression fixtures should include a graph module whose top level and callable `EDGES` each write a sentinel, a router that would mount schemas or starter jobs, and legacy SQLite state that would migrate on normal open. A between or sample preview must leave all sentinels, configuration bytes, database schema and bytes, node folders, and external state unchanged.

## S1a review record

I compared the S1a working tree with the exact issue 22 baseline,
`837f931746d94a1b4f1ec5f8d5e92ae605aad3a7`. The S1a review covers:

- `micro_workflow_manager/workflow/component_state.py:101-116`;
- `tests/test_063_quotient_selection.py:1-80`;
- `docs/architecture/graph.md:82-98`;
- `tests/README.md:41` and the removal of its obsolete fixed module count.

Other uncommitted stage work, including `tests/test_064_read_only_previews.py`,
is outside this narrow disposition.

The first review found missing unknown-endpoint validation. `s1-red-03.log` shows an unknown start leaking `NetworkXError` and an unknown end receiving the wrong strict-descendant error. The corrected code checks both raw-node names before building or traversing the quotient DAG. `s1-green-03.log` reports 16 passing checks across all 11 interval cases, `test_036_hoeflein_scheduling.py`, and the source-module size check.

The earlier TDD record is coherent: `s1-preserve.log` has 1 passing preservation check; `s1-red-01.log` fails because the interval method is absent; `s1-green-01.log` has 2 passes; `s1-red-02.log` has the four expected missing-rejection failures; `s1-green-02.log` has 6 passes; and `s1-adjacent-01.log` has 14 passes.

The corrected method validates endpoint existence, requires a strict directed descendant component, intersects descendant and ancestor sets, removes the end component, and returns a deterministic lexicographical topological order. It does not enumerate routes or mutate state. The docs state the same behavior and correctly identify this as the basis for pending between-command work.

Review result: S1a passes its focused code, documentation, and regression review.
The broader verification is not yet green. `s1-ordinary-01.log` reached 100 percent
and ended with `.....F................... [100%]`, without a final summary. The same
log records a nested pytest cache `PermissionError` and `PytestCacheWarning` for
the isolated source `.pytest_cache`. This does not identify an interval defect,
but it cannot serve as a passing ordinary-suite result. The implementing agent
must resolve or rerun that check before final S1a acceptance.
