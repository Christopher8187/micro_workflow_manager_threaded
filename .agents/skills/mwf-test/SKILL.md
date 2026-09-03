---
name: mwf-test
description: Test MWF framework source or a project built with MWF in an isolated sibling test_area. Use for regressions, release checks, example acceptance, benchmarks, or failure diagnosis.
---

# Test MWF in isolation

Read the framework `README.md`, relevant `CONTEXT.md` terms,
`docs/testing.md`, `tests/README.md`, and `benchmarks/README.md`. Then follow
this sequence. Running already approved checks does not authorize editing them.
Before changing source, engine code, tests, benchmark programs or results,
examples, skill scripts, or other executable material, apply the explicit
one-change approval gate in `AGENTS.md`.

First choose the fixed point for the task and classify every task-owned change
against it. Use `git status`, the staged and unstaged diffs, and direct
inspection of relevant untracked paths. Record pre-existing unrelated dirty
paths before excluding them. When every task-owned change is documentation or
an instruction-only `SKILL.md`, skip pytest and other executable suites. Check
local links and heading fragments, fenced blocks, referenced paths, terminology
ownership, and documented inventories. Compare current-behavior claims with
the relevant source and tests. When delegation is available and authorized,
give an independent subagent the changed documents and relevant source or tests,
and ask whether the documents match current functionality. Report that pytest
was not run because the task-owned diff was documentation-only. If executable
or runtime configuration changed, continue with the isolated sequence below.

1. Resolve the exact source root, record its branch, commit, dirty files,
   interpreter, declared dependencies, and available wheel. Derive
   `<source-parent>/test_area`; create a unique run directory there. Finish when
   every later path is inside that directory.
2. For framework testing on Windows, copy the selected tree with `robocopy`:

   ```powershell
   $mwfSource = (Resolve-Path .).Path
   $testArea = Join-Path (Split-Path $mwfSource -Parent) "test_area"
   $run = Join-Path $testArea ("mwf-" + (Get-Date -Format yyyyMMddHHmmss) + "-" + [guid]::NewGuid().ToString("N").Substring(0,8))
   $copy = Join-Path $run "source"
   New-Item -ItemType Directory -Force -Path $copy
   robocopy $mwfSource $copy /E /XD .git .venv venv __pycache__ build dist test_area /XF .git *.pyc
   ```

   Treat robocopy exit codes 0 through 7 as successful. On another platform,
   use its native copy command with the same exclusions. Preserve tracked and
   relevant untracked source from the selected tree.
3. Create a virtual environment inside the copied source and install
   `.[test]`. Run the focused regression, adjacent modules, then the ordinary
   suite:

   ```text
   python -m pytest -q --ignore=tests/test_autostart_cycles.py
   ```

   Run each test in `tests/test_autostart_cycles.py` as its own
   `python -m pytest -q file.py::test_name` process. Run
   `python -m pytest -q -m stress tests/test_markov_chain_stress.py` when the
   selected scope requires stress or release verification.
4. For another project built with MWF, copy the exact project tree into its
   derived Test Area. Read its root and affected node READMEs, install its own
   declared test dependencies, and install the MWF wheel or version the project
   records. Do not assume that project supports `.[test]`. Run its focused
   tests and workflow acceptance from the copy, never its durable working
   directory.
5. For release verification, build the candidate wheel and source archive from
   the tested copy. Inspect their version, metadata, and file lists. Extract the
   source archive into a fresh run directory and repeat the ordinary, cyclic,
   and release-selected stress checks there. Install the copied wheel into a
   fresh environment, verify its version, import location, and CLI, and compare
   its package files with the tested source. A compilation pass may find syntax
   errors, but it does not establish import-time behavior; run the import and
   execution checks as well.
6. For example acceptance, build or select the intended wheel. Copy that wheel
   and the example into a new run directory, create a virtual environment,
   install the wheel by its copied path, run `mwf init`, register
   `src/graph.py`, and execute the start node named by the example README. Verify
   durable outputs, failure and recovery behavior, and import location. A failed
   copy may remain unchanged only while its failure is active. Do not repair the
   example in this run.
7. Run a benchmark only when `docs/testing.md` selects it or the user asks.
   Record its full manifest and correctness fields before comparing speed.
8. Retain failed directories with a short failure record only while the failure
   remains active, and never add the record to the repository. After recovery
   succeeds and the result is recorded, remove the temporary record and copy.
   Resolve its absolute path inside the intended Test Area before cleanup.
   Report exact commands, counts, skipped checks, retained paths, and source
   changed-file state.
