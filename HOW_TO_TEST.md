# How to test micro-workflow-manager

This is the **authoritative execution order for testing MWF 0.5.4**.

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

The numbered suite is intentionally split into **two multi-file batches** in
short-horizon agent harnesses. Do not replace these with one-file-at-a-time runs.

```bash
python -m pytest -q \
  examples/agent_reference_architecture/tests/test_smoke.py \
  tests/test_030_runtime_updates.py \
  tests/test_031_inspect_failed_and_examples.py \
  tests/test_033_filter_icons_design.py \
  tests/test_034_sqlite_api_runner.py \
  tests/test_036_hoeflein_scheduling.py \
  tests/test_037_advisory_lock_recovery.py \
  tests/test_038_fresh_resume_restart_semantics.py \
  tests/test_039_sqlite_contention_recovery.py \
  tests/test_040_high_fanout_batching.py \
  tests/test_041_live_component_pumping.py \
  tests/test_042_cooperative_api_scaling.py \
  tests/test_043_waiting_nodes.py \
  tests/test_043_watchdog_networking.py \
  tests/test_044_queue_transport_scaling.py \
  tests/test_045_terminal_recovery.py \
  tests/test_046_module_boundaries.py \
  tests/test_046_resume_restart_wait.py \
  tests/test_047_event_state_top.py \
  tests/test_048_ghost_free_admission.py \
  tests/test_049_job_trace.py \
  tests/test_050_windows_process_signal_safety.py \
  tests/test_051_refuseafter_trace_retention.py \
  tests/test_052_sqlite_finalizer_reentrancy.py \
  tests/test_053_windows_extended_paths.py
```

```bash
python -m pytest -q \
  tests/test_054_destructive_preparation_commands.py \
  tests/test_055_threaded_prefetch_and_nofile.py \
  tests/test_056_resumefrom_refuseafter_052.py \
  tests/test_057_hoeflein_live_sync_053.py \
  tests/test_058_http_fanout_scaling_054.py
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
  tests/test_cli_help_and_clean_wipe.py \
  tests/test_055_threaded_prefetch_and_nofile.py \
  tests/test_056_resumefrom_refuseafter_052.py \
  tests/test_057_hoeflein_live_sync_053.py \
  tests/test_058_http_fanout_scaling_054.py
```

If the change has its own new regression file, run that focused file first,
then still complete steps 1-4.


## Local HTTP delay/throttle benchmark (required for networking or high-concurrency scheduler changes)

MWF ships a local service so performance work never depends on a paid provider or
a public test endpoint. Run it in a **separate terminal/process** from the
benchmark. The service uses real TCP sockets; HTTP/2 mode generates a temporary
one-day localhost certificate with `openssl`.

HTTP/2 (closest to Kaicenat/OpenRouter):

```bash
python benchmarks/local_http_delay_server.py --port 8766 --http2
```

HTTP/1.1 control:

```bash
python benchmarks/local_http_delay_server.py --port 8765
```

The endpoint is `/transfer?bytes=65536&bps=262144&delay_ms=5&chunk=4096`.
`bps=0` means unlimited. Throttling is per response/HTTP2 stream.

Run the benchmark from the repository root with the repository on `PYTHONPATH`:

```bash
PYTHONPATH="$PWD" python benchmarks/benchmark_http_fanout_matrix.py \
  --endpoint https://127.0.0.1:8766 --http2 \
  --mode workflow --concurrency 32 --fanout-nodes 4 --jobs 64 \
  --response-bytes 65536 --bytes-per-second 262144 --delay-ms 5 --repeats 3
```

`--repeats 3` prints each sample and a median summary. Use medians for comparisons.
The three modes must be understood before diagnosing a bottleneck:

- `transport`: direct httpx, no MWF runner/storage;
- `runner`: MWF ApiRunner + shared transport, no SQLite/filesystem workflow;
- `workflow`: full durable MWF DAG/lifecycle/restart path.

### Benchmark exploration order

**Start low first** to verify the harness and locate the transfer-bound edge:

```bash
for mode in transport runner workflow; do
  for rate in 0 262144 65536; do
    PYTHONPATH="$PWD" python benchmarks/benchmark_http_fanout_matrix.py \
      --endpoint https://127.0.0.1:8766 --http2 --mode "$mode" \
      --concurrency 32 --fanout-nodes 4 --jobs 64 --response-bytes 65536 \
      --bytes-per-second "$rate" --delay-ms 5 --repeats 3
  done
done
```

Then hold transfer fast and sweep **fan-out width**:

```bash
PYTHONPATH="$PWD" python benchmarks/benchmark_http_fanout_matrix.py \
  --endpoint https://127.0.0.1:8766 --http2 --mode workflow --matrix \
  --concurrencies 512 --fanout-node-counts 1,4,10,20 \
  --transfer-rates 0 --jobs 1024 --response-bytes 1024 --delay-ms 5 --repeats 3
```

Then hold width at 20 and push **aggregate concurrency** into the thousands:

```bash
PYTHONPATH="$PWD" python benchmarks/benchmark_http_fanout_matrix.py \
  --endpoint https://127.0.0.1:8766 --http2 --mode workflow --matrix \
  --concurrencies 128,512,1024,2048 --fanout-node-counts 20 \
  --transfer-rates 0 --response-bytes 1024 --delay-ms 5 --repeats 3
```

For the three-dimensional transfer/width/concurrency map, use staged grids rather
than one enormous command so a harness outer timeout does not destroy every
sample. A representative grid is:

```bash
PYTHONPATH="$PWD" python benchmarks/benchmark_http_fanout_matrix.py \
  --endpoint https://127.0.0.1:8766 --http2 --mode workflow --matrix \
  --concurrencies 128,512 --fanout-node-counts 1,10,20 \
  --transfer-rates 0,262144,65536 --response-bytes 65536 --delay-ms 5 \
  --jsonl benchmark-results.jsonl
```

Run the 2048-concurrency cells separately with an extended outer timeout and a
fresh local service. If the **transport/runner controls also collapse**, the
local HTTP stack/service/data movement is part of the limit; do not label that
as framework overhead. If runner remains fast while workflow collapses, inspect
SQLite mutation backlog, job lifecycle events, node-status writes and FDs.

A deliberate near-edge cell used for 0.5.4 is 2048 concurrency, 20 nodes and
4096 small jobs (about 205 jobs/node). Do not start there. Dial upward only after
32/128/512 cells are healthy.

HTTP/1.1 shard control:

```bash
PYTHONPATH="$PWD" python benchmarks/benchmark_http_fanout_matrix.py \
  --endpoint http://127.0.0.1:8765 --mode runner --concurrency 512 \
  --fanout-nodes 1 --jobs 1024 --response-bytes 1024 --delay-ms 5 \
  --streams-per-connection 100 --http1-connections-per-shard 16 --repeats 3
```

See `HTTP_FANOUT_BENCHMARKS_054.md` for the measured 0.5.3/0.5.4 region map and
why the final fixes were selected. Benchmark passes **do not replace** the
correctness/release tests below.

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
