# Benchmarks and diagnostic programs

This directory contains 12 executable programs and three saved result files.
Run a program only when a change can affect its measured path or Christopher
requests it. Read [the testing model](../docs/testing.md) and use the `mwf-test`
skill for isolation and reporting.

These programs do not share one pass condition. Some are comparisons, some are
load generators, and some preserve a diagnosis. Inspect the process exit code,
error and failed-job fields, residue, and stated threshold. Printed numbers alone
do not make a release gate.

## Programs

### `benchmark_dag_fanout.py`

Measures a four-way threaded DAG fan-out shaped like a large unequal producer.
It needs MWF and a platform with Python's `resource` module. Important values are
elapsed time, jobs per second, per-sink completion, open-file use, and the effect
of artificial payload-read delay. Compare the same job count, payload size,
thread count, and file-descriptor limit. A result is invalid when any sink is
incomplete or the run reports an error.

### `benchmark_explode_pump_function.py`

Compares API controller-pump allocations on the ten-node explode shape. It needs
MWF, the localhost delay service or its configured endpoint, and the same node
limits and initial populations for every candidate. Important values include
pump vector, admission timing, total elapsed time, failed jobs, missing monitor
rows, and output-to-terminal delay. It relates to startup-strategy and job-loading
comparisons. Reject a faster candidate that leaves residue or loses visibility.
The program returns a nonzero process status when any sample reports failed
jobs.

### `benchmark_hoeflein_pump.py`

Loads a two-node `A <-> B` threaded Hoeflein component for a chosen number of
seeds and hops. It imports Python's Unix-only `resource` module and also probes
`/proc` and `/dev/null`, so it does not run natively on Windows. It can hold
extra file descriptors per job. Important
values are completed hops, elapsed time, throughput, peak descriptors, and any
terminal error. Compare identical seeds, hops, threads, payload size, and host
limits. Every expected hop must finish.

### `benchmark_hoeflein_sync.py`

Stresses live-member synchronization between an explode node and several
handlers under optional payload and handler delays. Important values include
`post_start_max_q_gt0_r0_seconds`, completion counts, elapsed time, and residue.
The queued-with-zero-running interval should stay bounded by payload loading
rather than member teardown and restart. Compare on the same CPU allocation and
delay settings. A caught workflow exception is included in the JSON result and
also produces a nonzero process status.

### `benchmark_hoeflein_wait.py`

Measures an alternating two-node component that uses explicit `wait_for` gates.
It needs only MWF and local temporary storage. Important values are completed A
and B jobs, total elapsed time, throughput, and error. Use it for changes to
waiting admission or component cleanup. All expected rounds must finish without
queued or running residue.

### `benchmark_http_fanout_matrix.py`

Measures a three-axis HTTP fan-out matrix over aggregate concurrency, transfer
rate, and downstream node count. It supports `transport`, `runner`, and
`workflow` modes so transport cost can be separated from runner and durable MWF
cost. It needs `httpx`, MWF, and `local_http_delay_server.py`; HTTP/2 also needs
TLS support. Record jobs per second, MiB per second, elapsed time, failures,
client and descriptor peaks, writer backlog, and transfer-floor efficiency.
Compare identical protocol, response size, delay, job count, connection settings,
and repetitions. Related historical samples live in
`results/http_fanout_054_current.jsonl` and
`results/http_big_fan_054_baseline_final.json`.

### `benchmark_network_manager_skew.py`

Compares direct dispatch with the central network manager on a 22-node, 20:1
job skew. It runs both runner-only and durable-workflow modes. It needs the local
HTTP service and the same aggregate slot allocation for both architectures.
Important values are total throughput, big-to-small throughput ratio, network
send and receive delay, persisted node count, and ingress wakeups per request.
Every one of the 6,000 configured jobs and all 22 node observations must be
present. The related historical observation is
`results/network_manager_skew_056_observed.json`.

### `compare_job_loading_models.py`

Runs admission models in fresh child processes against observed, skewed, and
capacity-bound explode profiles. It needs MWF, `httpx`, and the reproduction
module in this directory. Important values are admission timing, provider
completion visibility, final residue, and exact output-to-terminal p95 and max.
A candidate is eligible only when all jobs are durably terminal, no provider
completion lacks a monitor row, and its configured ghost-delay gate passes.

### `compare_startup_strategies.py`

Compares API startup strategies in fresh child processes using the same state
measurements as `mwf top`. It needs MWF and
`reproduce_explode_ghost.py`. Important values are time to admit all jobs, first
provider response, total elapsed time, missing monitor rows, residue, and
output-to-terminal p95. A strategy is eligible only when every repetition has
no missing rows or final queued, running, or failed jobs and p95 stays within
the configured threshold.

### `local_http_delay_server.py`

Provides the local HTTP/1.1 or HTTP/2 `/transfer` endpoint used by networking
programs. It controls response bytes, per-response throughput, initial delay,
and chunk size. HTTP/2 mode needs the `h2` package and either supplied TLS files
or OpenSSL for a temporary localhost certificate. This service is a fixture,
not a performance result. Verify `/health`, protocol, and transfer duration
before interpreting a client benchmark.

### `reproduce_explode_ghost.py`

Reproduces monitor lag on the ten-handler explode component with deterministic
mock-provider delays. It records provider return, `output.json` publication, and
SQLite terminal visibility. It needs MWF and `httpx`. Important values are
missing monitor rows, queued or running residue, and output-to-terminal delay.
Use it to reproduce or reject a visibility regression, not to estimate general
provider throughput.

### `reproduce_windows_ctrl_c_probe.py`

Preserves the diagnosis that `os.kill(pid, 0)` is unsafe as a Windows liveness
check. It is Windows-only and refuses to send `CTRL_C_EVENT` unless the explicit
flag is present. Run it only in an expendable console. Its meaningful result is
the observed signal behavior. It is not a benchmark and has no release-speed
threshold.

## Saved results

### `results/http_big_fan_054_baseline_final.json`

Records eight historical HTTP/2 durable-workflow cells for MWF 0.5.4 at 2,048
or 4,096 concurrency and one to eight fan-out nodes. It stores baseline and
final jobs per second and final failed counts. It omits commit, host, interpreter,
repetitions, raw samples, and an acceptance threshold. Use it to reconstruct a
historical comparison, not as current release evidence.

### `results/http_fanout_054_current.jsonl`

Stores per-cell HTTP fan-out samples with workload, throughput, failure count,
file-descriptor and client peaks, mutation statistics, and node-status writes.
The file omits a complete run manifest tying every row to an MWF commit, host,
interpreter, repetition plan, and threshold. Use rows only when their parameters
match the comparison and report the missing metadata.

### `results/network_manager_skew_056_observed.json`

Stores four observed 2026-08-11 cells for direct and manager architectures in
runner and workflow modes. It includes the workload note, throughput, skew
ratio, network delay, node persistence, and ingress wakeups. It omits MWF
version and commit, machine and interpreter details, repetitions, raw samples,
and a pass threshold. Treat it as a historical observation.

## Recording a new result

Record the program and command, MWF version and commit, dirty source state,
wheel or source tree, OS and machine, Python and dependency versions, workload,
random seed, repetitions, raw samples, summary statistic, failure and residue
fields, and the comparison threshold chosen before the run.

Keep a new result only when it answers a stated question. A speed improvement
does not compensate for missing terminal state, failed jobs, incorrect output,
lost event order, or higher tail latency beyond the selected boundary.
