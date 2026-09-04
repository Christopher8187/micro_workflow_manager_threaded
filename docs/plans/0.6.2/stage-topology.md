# Shared topology calculations

This section of [Implement and verify the agreed MWF 0.6.2 workflow-management
changes](https://github.com/Christopher8187/product/issues/45) extracts the pure
component calculations used by the runtime and graph engine. It preserves the
accepted half-open interval calculation for 44-CMD-016 through 44-CMD-022.
It does not accept preview storage, component migration, or new commands.

## Scope

The immediate fixed point is command-retirement commit
`55e02dbc4d365253ac407b27d0f4c4cacef445c9` on
`codex/mwf-062-workflow-management`. The original published MWF 0.6.1 baseline
remains `837f931746d94a1b4f1ec5f8d5e92ae605aad3a7`.

`ComponentTopology` owns graph augmentation, component membership, quotient
edges, predecessors, descendants, intervals, and execution ordering. Runtime
methods retain their names and signatures and delegate to this calculation
object. Each runtime access uses the current graph and autostart relationships,
including later registration or replacement. The engine retains its existing
display identifiers and layout. It now uses the same quotient calculation.

This follows the preview review's finding that a read-only representation
should compose graph calculations without inheriting runtime mutation methods.
The unfinished preview caller was adapted separately and is excluded from this
section. No lifecycle, waiting, job-claim, storage, or migration rule changes.

## Preservation and verification

The new preservation tests passed before the extraction. They independently
specify components and quotient edges for reordered graph declarations, check
runtime/engine agreement and unchanged engine-project files, and verify later
autostart registration and replacement. These are preservation checks for a
refactor, so no new behavior required an expected failing run.

| Check | Result |
| --- | --- |
| Existing calculation plus new preservation tests on unchanged source | 14 passed in 15.01 seconds |
| Adjacent modules | 26 passed in 32.46 seconds |
| Ordinary suite | 390 passed, 1 stress case deselected, in 430.98 seconds |
| Four cyclic tests, separate processes | Each passed, in 3.50, 9.22, 7.20, and 3.67 seconds |
| Marked deterministic Markov-chain stress | 1 passed in 7.18 seconds |

Adjacent modules were `test_066_shared_topology.py`,
`test_063_quotient_selection.py`, `test_036_hoeflein_scheduling.py`,
`test_062_engine_and_sampling.py`, and `test_046_module_boundaries.py`.
The ordinary invocation was `python -m pytest -q
--ignore=tests/test_autostart_cycles.py`. Cyclic selections were the four names
recorded in [the retirement stage](stage-retirement.md#test-first-and-preservation-checks).
Stress used `python -m pytest -q -m stress tests/test_markov_chain_stress.py`.

All runs used native Python 3.12.14, SQLite 3.53.1, and the declared `.[test]`
dependencies in the existing isolated Test Area environment. `PYTHONPATH`
selected the `topology-only` source copy. Every process had a unique temporary
directory and `PYTEST_ADDOPTS=-p no:cacheprovider`. Local logs named
`topology-preserve-01`, `topology-adjacent-01`, `topology-ordinary-01`,
`topology-cycle-1` through `topology-cycle-4`, and `topology-stress-01` are under
`C:/Business/product/testing_ground/issue-45/`.

## Performance and review

The default multi-seed waiting benchmark
left queued jobs in both the accepted baseline and the candidate despite exit
status zero and an empty error field. Those timings are invalid. The original
state and measurements remain available for diagnosis; the failure is not
attributed to topology extraction.

A revised workload uses `benchmark_hoeflein_wait.py --seeds 1 --rounds 200
--threads 1 --delay 0.001`. Before running, the gate was fixed at three
alternating baseline/candidate repetitions, exact A=200 and B=199 completed
jobs in every repetition, no error or queued/running/failed residue, and
candidate median elapsed time at most 1.20 times the baseline median.

| Repetition | Baseline seconds | Candidate seconds |
| --- | --- | --- |
| 1 | 90.8388028 | 91.5898530 |
| 2 | 89.3441165 | 87.9124478 |
| 3 | 87.2029430 | 88.5576565 |
| Median | 89.3441165 | 88.5576565 |

All six runs completed exactly A=200 and B=199 jobs, with no queued, running,
or failed residue, no reported error, and exit status zero. The median ratio
is 0.9911974, within the predeclared 1.20 limit. This establishes the bounded
workload comparison, not a general speed improvement or repair of the separate
multi-seed early-stop finding.

The machine ran Windows 11 build 26200 on AMD64 with 16 logical processors,
Intel Family 6 Model 186 Stepping 2. Relevant dependencies were networkx 3.6.1,
greenlet 3.5.5, pytest 9.1.1, and installed MWF metadata 0.6.1. Both processes
imported their explicitly selected source copies. The complete source hashes,
dependency versions, commands, private temporary paths, and raw measurements
are in local `topology-benchmark-single-manifest.json`,
`topology-benchmark-single-results.jsonl`, and
`topology-benchmark-single-summary.json` beside the test logs.

The independent GPT-5.6 Sol xhigh reviewer has completed source and functional
review without an actionable topology finding. It checked unchanged calculation
bodies, retained method signatures, and 300 randomized graphs against
independent membership, quotient, ordering, interval, and nonmutation checks.
The selected source/test/documentation tree is
`3c00f0a46ccd44b5b9f026eb343591fe27ec3325`. The tested copy differs only in blank
whitespace in the new topology file. Git's staged whitespace check passes.
The [independent Sol xhigh review](topology-review.md) accepts this narrow
section. No actionable source or documentation finding remains. The separate
scheduler early-stop investigation remains unresolved. Packaging, example
execution, and final Astra review are outside this section.
