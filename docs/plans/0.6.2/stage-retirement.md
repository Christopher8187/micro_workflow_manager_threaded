# Retirement of four commands

This section of [Implement and verify the agreed MWF 0.6.2 workflow-management
changes](https://github.com/Christopher8187/product/issues/45) removes `clean`,
`cleanfrom`, `wipe`, and `wipefrom`. It advances requirements 44-CMD-025 through
44-CMD-028 and 44-REC-041. It preserves shared preparation for 44-CMD-030 and the
example boundary for 44-CMD-029. It does not complete the new nine-command family.

## Scope and order

The branch is `codex/mwf-062-workflow-management`. Published MWF 0.6.1 remains
the original baseline, `837f931746d94a1b4f1ec5f8d5e92ae605aad3a7`.
This section starts from implementation commit
`7a2bb8e2c3db788a5cdf1d9410e1a5ebf1e00a49`.

Command retirement moved ahead of the remaining S4 and S6 work because it does
not depend on preview storage, component migration, or session ownership.
Parser registration, dispatch, command-name collections, descriptions, help,
generated examples, and obsolete command tests now agree. No redirect or
compatibility stub remains. The unused private cleanup function was removed;
reset and run retain their shared fresh-preparation function.

Current README, operations, architecture, testing, and agent guidance stop
teaching those commands. Historical release notes remain explicitly historical.
Only the root examples README and nine example READMEs received narrow command
documentation edits. Example graphs, tasks, and behavior were not changed.

The authoritative MWF `AGENTS.md` and `CONTEXT.md` were synchronized to both
existing linked worktrees under the Parent Repo instructions. Their prior copies
were clean tracked files. No other linked-worktree source was changed.

## Test-first and preservation checks

The regression expected parser rejection before project bootstrap and no
filesystem change. Its expected result came directly from the final resolution,
not an inferred implementation detail or a cited local worked example.

The accepted RED run uses a valid legacy project with a graph source. All eight
cases fail on old command behavior. Four commands execute instead of raising
parser error 2; four descriptions remain available. After removal, all eight
pass. The earlier incomplete fixture and a mistyped preservation-test selector
are recorded as setup failures, not behavioral evidence.

| Local evidence | Result |
| --- | --- |
| `retire-red-01b.log` | 8 expected behavioral failures |
| `retire-green-01.log` | 8 passed |
| `retire-preserve-02.log` | 6 passed against the unchanged stage baseline |
| `retire-adjacent-01.log` and `.xml` | 81 passed in 65.48 seconds |
| `retire-ordinary-01.log` and `.xml` | 387 passed, 1 stress case deselected, in 450.96 seconds |
| `retire-cycle-1.log` through `retire-cycle-4.log` | Each passed in its own process, in 4.25, 12.36, 9.47, and 3.42 seconds |

Logs are retained at `C:/Business/product/testing_ground/issue-45/`.
Native Python 3.12.14 ran from the isolated environment under
`C:/Business/product/test_area/mwf-062-issue45-20260904/source/.venv/` with the
declared `.[test]` dependencies. `PYTHONPATH` selected the `retire-only` source
copy. `PYTEST_ADDOPTS=-p no:cacheprovider` avoided the known Windows cache issue.
Every pytest process had a unique temporary directory inside that Test Area.

The adjacent invocation selected `test_065_removed_commands.py`,
`test_cli_help_and_reset.py`, `test_054_destructive_preparation_commands.py`,
`test_051_refuseafter_trace_retention.py`, `test_framework_improvements.py`, and
`test_031_inspect_failed_and_examples.py`. It checked retained job definitions,
input, reset scope, confirmation, dry-run, orphan trace, and clipboard behavior.

The ordinary command was `python -m pytest -q
--ignore=tests/test_autostart_cycles.py`, with the recorded temporary and XML
paths. Each cyclic command was `python -m pytest -q
tests/test_autostart_cycles.py::test_name`, selecting respectively:

- `test_runfrom_supports_self_and_mutual_autostart_cycles_before_downstream`
- `test_threaded_diamond_cycle_spawns_100_seed_jobs_without_deadlock`
- `test_threaded_ring_cycle_spawns_100_seed_jobs_without_deadlock`
- `test_threaded_stochastic_game_engine_spawn_cycle_finishes`

No performance path changed, so benchmarks were not selected. Stress, example
execution, package builds, and publication were not selected for this removal.

## Review and acceptance

The independent [Sol xhigh review](retirement-review.md) inspected the
specification, actual source, changed tests, documentation, external callers,
and retained behavior. It also independently ran 14 relevant checks. No
actionable finding remained.

The tested tree was `fa1ee43d5716f1777410fc078961317d752dcee6`. Removing two
trailing blank lines from one test produced
`d1fa4660018ed90839742224217bcc801bd6567f`; the reviewer checked that sole
nonbehavioral difference. Stage records and requirement dispositions accompany
the accepted source. Documentation links, fenced blocks, test-file references,
and `git diff --cached --check` are checked before commit.

Only command retirement is accepted. Preview work, shared-topology follow-up,
component/session changes, and the rest of MWF 0.6.2 remain unfinished. Final
Astra review has not started.
