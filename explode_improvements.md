# Explode performance improvements

## Result

The best bounded live run completed 704 explode-handler jobs with no failures in
89.167 seconds. The original valid baseline completed 705 in 171.578 seconds.
That is 1.92x handler throughput and a 48.0% reduction in time to the workflow's
handler-only stop boundary.

The live result does not reach 5x end to end. Controlled local measurements show
that MWF's router publication path is no longer the limiting layer: the same
3,732-job, 10-target router benchmark improved from 211.01 jobs/s at commit
`7a82e62` to 375.11 jobs/s on this branch (1.78x). It is also 6.37x the original
live fan-out rate. In the complete workflow, OpenRouter response time and output
size dominate after local mutation pressure is removed.

All live runs followed `explode_testing_workflow.md`. The stop count is the
combined terminal count for the ten explode handler nodes only; central
`explode` and `redistribute` jobs are excluded. Each run was killed at the first
of more than 700 handler jobs or five minutes, with `mwf monitor` and `mwf top`
captured separately.

## Stage comparison

| Metric | Baseline | Best run | Change |
|---|---:|---:|---:|
| Time to bounded stop | 171.578 s | 89.167 s | 1.92x faster; -48.0% |
| Handler completions/s | 4.109 | 7.895 | 1.92x; +92.2% |
| Fan-out: `explode` to handlers | 58.890 jobs/s | 124.138 jobs/s | 2.11x; +110.8% |
| Mixed: `explode` to handlers | 10.574 jobs/s | 34.666 jobs/s | 3.28x; +227.9% |
| Mixed: handlers back to `explode` | 3.614 jobs/s | 4.590 jobs/s | 1.27x; +27.0% |
| Mixed: both directions combined | 14.188 jobs/s | 39.255 jobs/s | 2.77x; +176.7% |
| Peak queued SQLite requests | 4,990 | 228 | -95.4% |
| Worst sampled terminal-lag p95 | 0.246 s | 0.143 s | -41.9% |
| Handler failures | 0 | 0 | unchanged |

The best run used a run-scoped aggregate API budget of 128 for initial fan-out,
then changed it to 512 after the first handler completion. The switch occurred
at 9.656 seconds. This is intentionally admission control, not a late network
semaphore: work above the budget remains `queued` and is not falsely reported as
`running`.

## Root causes

### Same-priority FIFO admission waves

Execution admission, successful terminal publication, and failed terminal
publication already share the exact runtime-critical priority:

```text
RUNTIME_CRITICAL_PRIORITY = 5
ADMISSION_PRIORITY = 5
TERMINAL_PRIORITY = 5
```

Both success and failure use the terminal path. This invariant is preserved and
covered by regression tests.

Equal priority is necessary but not sufficient. The SQLite mutation queue is
FIFO within a priority. The old default API startup strategy could enqueue a
large wave of same-priority execution claims before servicing provider-complete
fibers. Later terminal updates then sat behind claims that had already entered
the FIFO. This produced the apparently impossible state where provider output
existed but thousands of jobs still appeared `running`.

The default is now the event-prioritized single-lane strategy. It services
provider callbacks between bounded admission slices and yields on the mutation
writer's urgent event. Claims retain priority 5, terminal success retains
priority 5, and terminal failure retains priority 5; scheduling now gives all
three a chance to enter the shared FIFO in time order.

### Excess provider concurrency

The ten handlers request 5,300 API fibers in total. Cooperative fibers avoid OS
thread explosion, but 5,300 simultaneous long OpenRouter requests still overload
the provider, sockets, and response-processing path. A standalone small-output
probe found a throughput knee around 256 concurrent requests and no benefit at
1,024. The real, heterogeneous explode workload performed best around 512 after
the initial fan-out phase.

MWF now exposes a run-scoped aggregate budget:

```powershell
mwf threads --api-total 128
# after the first explode-handler completion:
mwf threads --api-total 512
```

The budget is divided deterministically in proportion to the active API nodes'
effective `max_threads` requests. Per-node requests remain weights and upper
bounds. It can be changed live and clears with the run, like existing thread
overrides.

### Redundant durable and filesystem work

The mixed path generated thousands of synchronous trace/event inserts and paid
two durable barriers for a common single-child route: child publication followed
by the parent's `jobs_created` event. It also created and removed a temporary
directory for every routed child before creating the final job directory.

The retained implementation:

- groups concurrent event appends into one SQLite `executemany` transaction
  while each caller still waits for its own durable result;
- groups auto-ID child publication across all target nodes, not just one target;
- allocates per-node IDs, moves payloads, inserts child rows and creation events,
  advances sequences, and marks nodes queued in one mutation;
- writes the parent's `jobs_created` journal row in that same child-publication
  transaction;
- stages each payload as one flat temporary file, eliminating one temporary
  directory create/remove cycle per routed child;
- keeps idempotency resolution and cleanup correct under concurrent same-key
  publishers.

At 1,000 local routes, a 1 ms group window produced 397.15 jobs/s, versus
240.20 jobs/s with grouping disabled. Longer 3 ms and 10 ms windows were slower,
so 1 ms is the measured plateau rather than an arbitrary batch delay.

## Experiments retained and rejected

| Experiment | Outcome |
|---|---|
| Default single admission plus grouped events | 177.953 s; rejected |
| Event-prioritized admission | 159.269 s; retained as default |
| Static aggregate budget near 512 | 93.497 s best static sample |
| Static aggregate budget 256 | 106.548 s; fan-out improved, mixed phase slower |
| Static aggregate budget 128 | 181.884 s; too little provider parallelism |
| 128 to 768 phase switch | 95.514 s; 768 overloaded mixed central routing |
| 128 to 512 phase switch | 89.167 s; best coherent live run |
| Late network cap of 256 | 229.928 s; rejected and removed |

The late network cap was structurally wrong for explode: thousands of jobs had
already been claimed and marked `running` before waiting for network capacity.
Its code was removed. The retained aggregate control acts at admission, so
monitor state remains truthful and the impossible backlog does not return.

## Additional diagnostics and commands

The following commands were added or used beyond the original workflow:

```powershell
mwf threads --api-total 128
mwf threads --api-total 512
mwf threads --api-total reset
mwf monitor --interval 0.5 --json --no-clear
mwf top --interval 0.5 --json --no-clear
mwf doctor
mwf filter explode
mwf inspect explode
mwf inspect explode failed
mwf inspect <node> job <job-id>
mwf trace <node> job <job-id>
```

`mwf monitor` supplied the handler-only stop count. `mwf top` exposed terminal
lag, event rate, and mutation-writer queue pressure. The inspect/filter/trace
commands are read-only drill-down tools and did not replace or delay either hard
stop condition.

The separate OpenRouter concurrency probe requested by the testing instructions
is at `C:\Users\Chris\Videos\openrouter_concurrency_probe.py`. Its result files
are `openrouter_concurrency_results.json` and
`openrouter_concurrency_results_high.json` in the same folder. The probe imports
the already configured key without printing or copying it and uses minimal
four-token responses.

## Correctness and design notes

- No bounded explode run reported a handler failure.
- The new aggregate budget affects only API admission; direct, threaded, and
  process runner semantics are unchanged.
- A direct/thread/process checkpoint now excludes synchronous framework
  persistence time from the user's next checkpoint interval.
- The supervisor releases its thread-local SQLite connection while waiting only
  for restart-listener idle grace, preventing a completed workflow from keeping
  an otherwise idle handle open.
- A killed run still requires the exact reset/paste preparation sequence before
  another test.

## Final validation

The retained branch passed the repository's complete Windows test gate:

- `284 passed, 1 deselected` in the ordinary suite (the deselection is the
  intentionally separate marked stress case);
- all four autostart-cycle cases passed in four fresh Python processes;
- the explicit Markov-chain stress case passed;
- `92 passed` in the mandatory scheduler/lifecycle/fan-out focused batch; and
- `34 passed` in the network-manager/watchdog/transport focused batch.

The required real-socket localhost HTTP/2 controls also completed with zero
failed jobs. At concurrency 32, median rates were 460.55 jobs/s for direct
transport, 438.31 jobs/s for the bare API runner, and 48.95 jobs/s for the full
durable workflow. With 1,024 small jobs and 512 aggregate concurrency, the
fan-out-width medians were 39.55, 139.32, 205.23, and 170.76 jobs/s for 1, 4,
10, and 20 handler nodes respectively. At 20 nodes the aggregate-concurrency
sweep plateaued at 166.23, 172.60, 176.79, and 181.29 jobs/s for limits 128,
512, 1,024, and 2,048, confirming that simply admitting more work yields little
after the durable lifecycle path saturates.

In the documented 22-node skew A/B, the central NetworkManager improved the
bare-runner rate from 742.27 to 779.58 jobs/s and the full-workflow rate from
116.88 to 121.10 jobs/s. Its full-workflow ingress wakeups fell from 1.0 to
0.094 per request while preserving the configured 20:1 concurrency weighting.
The benchmark harness now explicitly closes Windows SQLite handles, waits
through the intentional restart-listener grace, and treats residual temporary
cleanup as best effort; none of that teardown time is included in the measured
run duration.

The remaining variance is external: repeated runs with the same code and budget
show materially different OpenRouter request durations. Further live tuning at
this point changes sample noise more than framework behavior. The admission
curve has been bracketed on both sides, the rejected late-cap design is removed,
and the local routing path has reached its measured 1 ms group-commit plateau.
