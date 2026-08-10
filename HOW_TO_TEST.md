# How to test micro-workflow-manager

This is the **authoritative execution order for testing MWF 0.5.3**.

AI coding agents and human contributors should follow this file before declaring a
framework change verified. The important distinction is:

- **ordinary tests are intentionally run together as a batch**; and
- **autostart/cyclic tests are intentionally run one test per fresh Python process**.

Do not combine those two categories into one large pytest invocation. In
particular, do **not** run all of `tests/test_autostart_cycles.py` in one pytest
process and do not append its individual tests to another pytest command. The
cycle tests exercise process-global scheduler/thread lifecycle and timing. A
fresh interpreter per case is part of the test protocol, not an optional
workaround.

## 0. Start from a clean source tree

Run tests from the repository root (the directory containing `pyproject.toml`).
Use the interpreter/environment that will be used to build the package.

Install test dependencies when needed:

```bash
python -m pip install -e ".[test]"
```

For release verification, prefer extracting the candidate archive into a fresh
directory and running the commands below there. Do not rely only on a dirty
working tree or an ambient installed copy of MWF.

## 1. Compile every Python source file

Before pytest, compile all repository Python files except generated/build/VCS
areas. This catches syntax/import-time source mistakes in package code, tests,
benchmarks, and examples even when pytest would not import a file in the current
selection.

Linux/macOS/PowerShell-compatible Python command:

```bash
python - <<'PY'
from pathlib import Path
import py_compile

root = Path.cwd()
skip_parts = {".git", ".venv", "venv", "build", "dist", "__pycache__"}
files = [
    p for p in root.rglob("*.py")
    if not any(part in skip_parts for part in p.parts)
]
for path in files:
    py_compile.compile(str(path), doraise=True)
print(f"compiled {len(files)} Python files")
PY
```

Any compilation failure is a release failure.

## 2. Run the ordinary pytest suite in batches

### Preferred: one complete ordinary batch

When the test harness allows a sufficiently long single process, run the whole
normal suite **together**, excluding the special autostart-cycle file:

```bash
python -m pytest -q --ignore=tests/test_autostart_cycles.py
```

This is the strongest batched check because it exercises cross-test cleanup and
state leakage across the entire ordinary suite.

### Hard outer-timeout fallback: multi-file batches

Some agent/container harnesses terminate one command before the full ordinary
suite can finish. In that case, do **not** fall back to one test file at a time.
Run these multi-file batches, each in one pytest process:

```bash
python -m pytest -q \
  examples/agent_reference_architecture/tests/test_smoke.py \
  tests/test_0*.py
```

```bash
python -m pytest -q \
  tests/test_active_job_restart.py \
  tests/test_checkpoint_keyword_api.py \
  tests/test_cli_help_and_clean_wipe.py \
  tests/test_cli_monitor.py \
  tests/test_deploy.py \
  tests/test_file_entry_node_input_import.py \
  tests/test_filesystem_objects.py \
  tests/test_framework_improvements.py \
  tests/test_graph_sync_and_fans.py \
  tests/test_init_clipboard_debug_028.py
```

```bash
python -m pytest -q \
  tests/test_output_and_runner_edges.py \
  tests/test_reliability.py \
  tests/test_runtime_thread_overrides.py
```

`tests/test_markov_chain_stress.py` is deliberately omitted from these ordinary
batches because its marked long stress case is run explicitly in step 4.
`tests/test_autostart_cycles.py` is deliberately omitted because each required
cycle case must run alone in step 3.

If any ordinary batch fails or hangs, diagnose the actual failing/stuck test. Do
not silently replace a failing batch with only individually passing files and
call the suite verified.

## 3. Run every autostart/cyclic test separately

Each command below must be a **separate shell command and therefore a fresh
pytest/Python process**. Do not combine them.

```bash
python -m pytest -q tests/test_autostart_cycles.py::test_runfrom_supports_self_and_mutual_autostart_cycles_before_downstream
```

```bash
python -m pytest -q tests/test_autostart_cycles.py::test_threaded_diamond_cycle_spawns_100_seed_jobs_without_deadlock
```

```bash
python -m pytest -q tests/test_autostart_cycles.py::test_threaded_ring_cycle_spawns_100_seed_jobs_without_deadlock
```

```bash
python -m pytest -q tests/test_autostart_cycles.py::test_threaded_stochastic_game_engine_spawn_cycle_finishes
```

Why separately: these cases intentionally stress live Hoeflein/autostart
components, worker teardown, process-global thread state, and tight timing. A
single combined interpreter can turn test-harness residue/timing into a false
framework diagnosis. Conversely, a cycle test that only passes when combined is
also suspicious; the required acceptance condition is that every command above
passes from a fresh process.

When invoking them from an external harness, give each process an extended outer
wall-clock timeout. A harness timeout is evidence to inspect; it is not by itself
proof of a scheduler deadlock.

## 4. Run the marked long stress test explicitly

The default pytest configuration does not substitute for the explicit long
stress run:

```bash
python -m pytest -q -m stress tests/test_markov_chain_stress.py
```

Run it after the ordinary batch and cycle tests so a long stress failure is easy
to attribute.

## 5. Extra mandatory tests for scheduler/lifecycle changes

For changes touching Hoeflein scheduling, queue admission, resume/restart,
cleanup, component failure, threaded/API runners, or lifecycle publication, also
run these focused files together as a batch (they may already have run in step
2; rerunning them after a focused fix is intentional):

```bash
python -m pytest -q \
  tests/test_036_hoeflein_scheduling.py \
  tests/test_038_fresh_resume_restart_semantics.py \
  tests/test_cli_help_and_clean_wipe.py
```

If the change has its own new regression file, run that focused file first,
then still complete steps 1-4.

## 6. Release/archive verification

Before returning a source archive or wheel:

1. Build/package from the tested source tree.
2. Extract the source archive into a fresh directory.
3. Repeat **step 1** from the clean extraction.
4. Repeat the **ordinary batched suite** from step 2 (one full batch when the harness permits it; otherwise all documented multi-file batches).
5. Repeat **each cycle command separately** from step 3.
6. Repeat the explicit stress test from step 4.
7. If a wheel is included, inspect its metadata/version and preferably install
   it in an isolated environment or compare its package files with the tested
   source tree.

Do not report “full tests passed” if only focused tests passed, if the ordinary
batch timed out partway through, or if autostart-cycle tests were run in the
wrong combined process. State the exact commands/counts and any intentionally
separate timing-sensitive test.

## Failure diagnosis rules

A wall-clock timeout is evidence, not a diagnosis. For scheduler failures inspect
at least:

- active run and Hoeflein component state;
- node queued/running/done/failed counts;
- job event chronology and checkpoint deadlines;
- whether provider/external waits still advance;
- live Python threads/fibers/processes;
- SQLite mutation-writer/backlog state; and
- file-descriptor usage/limits under high concurrency.

For a stubborn issue, reproduce it at low and high concurrency and preserve a
focused regression test. Do not weaken scheduler semantics merely to make a
timing test pass.
