# MWF 0.5.4 Big-Fan HTTP Benchmark Pass

This pass stress-tested the full durable workflow path against the local HTTP/2 delay service with:

- 1 KiB HTTP responses
- 5 ms server delay
- jobs = `2 * aggregate concurrency`
- 2,048 and 4,096 aggregate API fibers
- 1, 2, 4, and 8 independent downstream fan nodes
- production execution leases/fences, output files, runtime metadata, terminal events, and SQLite durability

The runner-only and transport controls were used only to identify where remaining headroom lived.

## Baseline

The uploaded source defaulted dense API nodes to the `balanced` startup strategy, which creates two coordinated admission pumps for large refreshable queues.

| Fibers | Fan nodes | Baseline jobs/s |
|---:|---:|---:|
| 2048 | 1 | 246.4 |
| 2048 | 2 | 296.6 |
| 2048 | 4 | 319.9 |
| 2048 | 8 | 291.5 |
| 4096 | 1 | 282.3 |
| 4096 | 2 | 284.9 |
| 4096 | 4 | 292.8 |
| 4096 | 8 | 274.4 |

## Accepted optimizations

### 1. One cooperative startup/admission pump per API node

The default `MWF_API_STARTUP_STRATEGY` is now `single` rather than `balanced`.
One fiber pump already multiplexes thousands of synchronous controllers. On a durable refreshable source, adding a second pump duplicated claim/controller work and increased SQLite/state contention more than it improved admission.

The previous `balanced`, `elastic`, `adaptive`, and explicit `lanes:N` strategies remain available for experiments and unusual source-bound workloads.

### 2. Retire the SQLite mutation writer immediately when a batch drains the queue

The mutation writer previously remained alive for an additional 250 ms empty-queue timeout. At very high fan-out this could leave a late diagnostic write racing temporary-project teardown.

The writer now performs a guarded queue recheck immediately after a drained batch and retires at once. The guard is race-safe with enqueue: a request that arrives first keeps the writer alive; a request arriving after `_thread=None` starts a new writer.

### 3. Benchmark teardown durability fence

The benchmark now calls `flush_db_mutations()` after the timed `workflow.run()` interval and before deleting the temporary project. This does not change the measured production run time; it only prevents advisory low-priority runtime metadata from racing benchmark teardown.

## Final retest

All eight cells completed with zero failed jobs.

| Fibers | Fan nodes | Baseline jobs/s | Final jobs/s | Change |
|---:|---:|---:|---:|---:|
| 2048 | 1 | 246.4 | 434.8 | +76.4% |
| 2048 | 2 | 296.6 | 421.4 | +42.0% |
| 2048 | 4 | 319.9 | 373.2 | +16.7% |
| 2048 | 8 | 291.5 | 361.2 | +23.9% |
| 4096 | 1 | 282.3 | 419.3 | +48.5% |
| 4096 | 2 | 284.9 | 380.3 | +33.4% |
| 4096 | 4 | 292.8 | 356.8 | +21.8% |
| 4096 | 8 | 274.4 | 322.8 | +17.6% |

The final 4096/8 cell processed 8,192 jobs with zero failures while publishing 8,192 runtime updates and 8,193 terminal updates.

## Controls and rejected changes

The 2048-fiber controls were approximately:

- raw HTTP transport: 1,018 jobs/s
- `ApiRunner` without workflow durability: 594 jobs/s
- final full durable workflow: 435 jobs/s at one fan node

The remaining gap is therefore mostly durable lifecycle/filesystem work rather than an obvious HTTP transport limit.

The following were tested and deliberately **not** made defaults:

- `event`/writer-pressure admission: substantially slower (about 240 jobs/s at 2048/1 and 320 jobs/s at 2048/8).
- 1,024-job maximum admission burst: no consistent gain and a regression at multi-node width.
- mutation-writer batch slices of 64 or 256 instead of 128: both regressed the tested 4096/4 cell.
- completion service interval 32: helped some cells but regressed others; interval 64 produced a severe long-tail stall. The stable default remains 16.
- removing initial runtime persistence: only a small diagnostic gain and would reduce active-run observability.
- removing output files: measurably faster, but output durability is required framework behavior and was not sacrificed for the benchmark.

The next meaningful gains would require a larger architectural change (for example reducing or coalescing required per-job durable metadata/file writes) rather than another obvious admission/scheduler constant change.
