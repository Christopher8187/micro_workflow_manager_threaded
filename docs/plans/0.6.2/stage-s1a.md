# S1a: quotient interval calculation

This section implements the interval calculation required by
[Settle the MWF workflow-management model for 0.6.2](https://github.com/Christopher8187/product/issues/44#issuecomment-5539997969).
It is one reviewed section of
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).
The remaining selection, preview, execution, storage, and management work is open.

## Scope and baseline

The fixed point is published MWF 0.6.1, commit
`837f931746d94a1b4f1ec5f8d5e92ae605aad3a7`, on the dedicated implementation
branch `codex/mwf-062-workflow-management`.

The change adds `component_interval(start_node, end_node)` to
[`ComponentStateMixin`](../../../micro_workflow_manager/workflow/component_state.py).
It validates both raw-node names, requires a strict directed descendant end
component, intersects descendants-or-self with ancestors-or-self, excludes the
end component, and returns a deterministic topological ordering. No route
enumeration or storage mutation occurs.

The calculation addresses requirement clauses `44-CMD-016` through `44-CMD-022`.
CLI registration and use of the calculation remain pending. Existing descendant
selection is covered by a preservation check for `44-CMD-014`.

The other changed paths are
[`test_063_quotient_selection.py`](../../../tests/test_063_quotient_selection.py),
[`graph.md`](../../architecture/graph.md), and
[`tests/README.md`](../../../tests/README.md). Preview work in separate CLI files
and `test_064_read_only_previews.py` is excluded from this section.

## Test-first record

Tests ran in copied source under the derived sibling Test Area, using native
Python 3.12 and an isolated environment with the declared `.[test]` dependencies.
Each run used its own pytest temporary directory. Logs are retained locally at
`C:/Business/product/testing_ground/issue-45/`.

| Evidence file | Result and interpretation |
| --- | --- |
| `s1-preserve.log` | 1 passed against the baseline. Existing descendant selection preserves whole components and directed branches. |
| `s1-red-01.log` | Expected failure because `component_interval` did not exist. |
| `s1-green-01.log` | 2 passed after the initial calculation. |
| `s1-red-02.log` | Four expected failures because invalid endpoint relationships were not rejected. |
| `s1-green-02.log` | 6 passed after strict directed-descendant validation. |
| `s1-adjacent-01.log` | 14 passed, including component scheduling and the source-module size check. |
| `s1-red-03.log` | Two expected failures for an unknown start or end, following the review finding. |
| `s1-green-03.log` | 16 passed after endpoint validation, including all 11 interval cases. |

The final focused invocation selected `tests/test_063_quotient_selection.py`,
`tests/test_036_hoeflein_scheduling.py`, and
`tests/test_046_module_boundaries.py`. Cases cover diamonds, side branches,
outside incoming edges, component aliases, reversed or unreachable endpoints,
declaration-order independence, unchanged persisted files, and 30 branching
layers representing more than a billion routes.

## Broader verification

An immutable S1a source copy was tested with:

```text
python -m pytest -q --ignore=tests/test_autostart_cycles.py --basetemp=<unique-directory> --junitxml=<result-file>
```

`PYTEST_ADDOPTS=-p no:cacheprovider` avoids a Windows cache permissions problem.
The result was **385 passed, 1 deselected** in 435.68 seconds, recorded in
`s1-ordinary-02.log` and `s1-ordinary-02.xml`.

The first ordinary attempt had a concurrency-test failure followed by a pytest
cache error that prevented a final summary. It is not counted as passing. The
identified concurrency test passed alone in `s1-reliability-01.log`, and the
unchanged full suite then passed together. No runtime code or test was changed
to obtain that rerun.

Each of the four cases in `tests/test_autostart_cycles.py` passed in a separate
`python -m pytest -q file.py::test_name` process. Results are in
`s1-cycle-1.log` through `s1-cycle-4.log`.

Documentation checks passed for heading uniqueness, fenced blocks, the new test
reference, and agreement with the implementation. `git diff --check` passed.
Stress, release-artifact, and example execution checks were not selected for
this additive calculation. Existing measured execution paths have no callers
of the new method, so no runtime benchmark was selected.

## Review and disposition

The independent `gpt-5.6-sol` reviewer used `xhigh` reasoning. It read the
consolidated behavior, the complete behavioral discussion through Q123, and the
complete preparation discussion, including later corrections and approvals.

Its first finding was missing unknown-endpoint validation. The two failing
regressions reproduced that finding, and the correction passed re-review.
The final focused code, test, and documentation disposition was PASS. The
reviewer's pending ordinary-suite condition is now satisfied by the results
above. Its full local report is `selection-review-preparation.md`.

S1a is accepted as the interval-calculation section only. The full S1 stage and
the implementation task remain open. Final Astra review has not started.
