# MWF 0.5.4 localhost HTTP fan-out benchmark findings

This document records the benchmark-driven performance work for MWF 0.5.4.
The executable benchmark is `benchmarks/benchmark_http_fanout_matrix.py`; the
local delay/throttle service is `benchmarks/local_http_delay_server.py`.
`HOW_TO_TEST.md` is the authoritative reproduction runbook.

## What the benchmark isolates

The benchmark sweeps three independent axes:

1. **aggregate concurrency** (`--concurrency`),
2. **per-response transfer rate** (`--bytes-per-second`, `0` = unlimited), and
3. **fan-out width** (`--fanout-nodes`).

It has three modes so a slow cell can be attributed rather than guessed:

- `transport`: direct `httpx` control, no MWF runner/storage;
- `runner`: MWF cooperative `ApiRunner` + shared HTTP transport, no durable workflow;
- `workflow`: full durable DAG fan-out, restart fencing, traces, SQLite lifecycle,
  output, scheduler and the same shared HTTP transport.

HTTP/2 is the closest shape to Kaicenat/OpenRouter because many logical requests
share a small number of physical connections. HTTP/1.1 is retained as a separate
stress/control because large connection pools have very different behavior.

The checked-in `benchmarks/results/http_fanout_054_current.jsonl` is a sampled
broad grid from this container. Absolute rates are machine-dependent; use the
relative layer comparisons and matrix shape.

## Region map

### Low concurrency: transfer rate can dominate

32 concurrency, 4 nodes, 64 jobs, 64 KiB response, HTTP/2, 5 ms fixed delay;
three-sample medians:

| Per-response rate | transport | runner | full workflow | interpretation |
|---|---:|---:|---:|---|
| unlimited | 551.4/s | 477.8/s | 215.5/s | framework-bound |
| 256 KiB/s | 123.0/s | 121.4/s | 96.1/s | mixed transfer + framework |
| 64 KiB/s | 33.2/s | 33.0/s | 30.2/s | transfer-bound |

At the slowest setting, full MWF is about 91% of transport-only throughput, so
there is little framework performance to recover there. Speeding SQLite would
not materially improve a provider that is itself delivering one 64 KiB response
per second per active stream.

### Hundreds of concurrent streams: durable framework work dominates

A broad 64 KiB-response grid shows the transfer-rate effect mostly disappearing
once enough streams overlap:

| concurrency | nodes | unlimited | 256 KiB/s | 64 KiB/s |
|---:|---:|---:|---:|---:|
| 128 | 1 | 221/s | 205/s | 101/s |
| 128 | 10 | 215/s | 183/s | 78/s |
| 128 | 20 | 165/s | 141/s | 73/s |
| 512 | 1 | 228/s | 223/s | 212/s |
| 512 | 10 | 186/s | 203/s | 197/s |
| 512 | 20 | 192/s | 184/s | 190/s |
| 2048 | 20 | 179/s | 173/s | container/service stress before a reliable 64 KiB/s sample |

At 512 streams, changing each stream from unlimited to 64 KiB/s barely changes
full-workflow throughput. Parallel waits hide per-stream transfer latency and the
durable MWF lifecycle becomes the limiting resource. At 2048 streams with 64
KiB bodies, the local benchmark service/container itself begins to be part of
the ceiling; compare `runner`/`transport` controls before attributing those cells
to MWF.

For example, 2048 concurrent H2 streams with 64 KiB unlimited responses deliver
about 364 requests/s through `runner`, while the full durable 20-node workflow
is about 179 jobs/s. Both local HTTP processing/memory movement and framework
work matter in that corner.

### Thousands of concurrent streams with small replies: framework-bound

With 1 KiB responses and a 5 ms delay:

- 1024 concurrent / 20 nodes: `runner` median ~1081/s, full workflow median
  ~271/s.
- 2048 concurrent / 20 nodes: `runner` median ~754/s, full workflow median
  ~309/s after the 0.5.4 fixes.
- 2048 concurrent / 20 nodes / **4096 jobs (two waves)** completes at ~265/s,
  zero failed jobs, with ~232 peak FDs. This is intentionally near the practical
  stress edge of this container.

The transport/fiber layer therefore supports far more concurrent waits than the
durable workflow can commit. In this region the SQLite mutation lane and durable
job lifecycle are the dominant framework cost, not payload transfer.

### Fan-out width has a scheduler cost, but 0.5.4 removes the accidental O(width²) part

At 512 aggregate concurrency, 1024 jobs, 1 KiB H2 responses, 5 ms delay, current
three-sample medians were approximately:

| nodes | jobs/node | median jobs/s |
|---:|---:|---:|
| 1 | 1024 | 328 |
| 4 | 256 | 341 |
| 10 | ~102 | 328 |
| 20 | ~51 | 276 |

Some width cost remains because there are genuinely more node queues/components,
but 0.5.3 also repeatedly republished already-terminal siblings as each peer
finished. In an exact 2048-concurrency / 20-node baseline sample, child nodes
were written `done` roughly 6-10 times each and some were repeatedly written
`running`/`queued` as well. 0.5.4 publishes each child `RUNNING` once and `DONE`
once during execution; this is protected by a regression test.

## 0.5.4 fixes justified by the matrix

### 1. Fold the first `task_started` event into the execution-claim transaction

0.5.3 performed a grouped durable claim, then every admitted job immediately
submitted a separate synchronous `task_started` mutation. At 2048 jobs this was
~2070 `plain:append` submissions in addition to runtime and terminal mutations.

0.5.4 records the initial valid main-task `task_started` event in the grouped
claim transaction. Missing-required-param jobs are masked so they do **not** get
a fabricated task-start trace before invocation validation.

On the 2048-concurrency / 20-node / 2048-job cell:

- 0.5.3: ~6641 mutation submissions, ~230 jobs/s in the exact control sample.
- 0.5.4: ~4265 mutation submissions, two-sample median ~309 jobs/s.

That is about **36% fewer mutation submissions** and roughly **34% higher
throughput** in that stressed sample.

At 512 concurrency / 20 nodes / 1024 jobs, exact 0.5.3 median was ~235 jobs/s;
0.5.4's comparable small-response medians are in the high-200s, while mutation
submission falls from ~3445 to ~2250.

### 2. Stop repeatedly rewriting sibling node status

`finalize_ready_nodes()` used to rescan/rewrite terminal siblings after every
completion in a wide DAG. 0.5.4 bulk-reads statuses, skips terminal and currently
in-flight components, and gives component execution one owner for initial
`RUNNING` and final `DONE` publication.

This removes measurable node-width bookkeeping without weakening durability.

### 3. HTTP/1.1 uses elastic 16-connection client shards

A single `httpx` HTTP/1.1 pool with hundreds of live connections becomes a
transport bottleneck before MWF's fiber capacity is reached. 0.5.4 defaults to
16 H1 connections per client shard and adds shards elastically. HTTP/2 keeps the
existing stream-per-connection behavior (Kaicenat uses 80 streams/connection).

512 concurrent H1 waits, 1024 small jobs, 5 ms local response delay:

- 0.5.3 100-connection shards: median ~164 req/s;
- 0.5.4 16-connection shards: median ~583 req/s.

That is about **3.55x** faster with essentially the same ~519 peak descriptors;
the gain is pool scheduling, not cheating by reducing concurrency.

### 4. Retain included programmatic routers by object identity

The repeated wide-fanout benchmark exposed a correctness bug: MWF remembered
only `id(router)`. A short-lived generated router could be garbage-collected and
CPython could reuse that numeric id, causing a later distinct router to be
silently skipped as "already included". 0.5.4 retains included router objects
and deduplicates by actual object identity. A GC-forced 100-router regression
protects this.

## Why optimization stops here

After the fixes, a 2048-concurrency/20-node small-response diagnostic cell was
~286.5 jobs/s normally. Artificially suppressing the remaining per-job
runtime/watchdog publication increased it only to ~295.6/s (~3%); suppressing
user-facing output artifacts was ~301.1/s (~5%).

The remaining dominant mutations are now the intentional durable state:

- one runtime/watchdog observation for the active attempt; and
- one terminal publication/result state per job.

Eliminating those would trade restart/watchdog correctness or inspectability for
single-digit-percent synthetic gains. That is not an obvious safe improvement,
so 0.5.4 keeps them.

## Practical interpretation for Kaicenat

- **Slow provider / low concurrency:** network/model transfer dominates; MWF
  changes cannot create much throughput.
- **Hundreds-thousands of overlapping API waits:** transfer latency is hidden;
  durable lifecycle throughput matters. 0.5.4 specifically reduces avoidable
  mutation/status work here.
- **Tens of fan-out nodes:** node width has some real cost, but repeated sibling
  status rewrites are removed.
- **HTTP/1.1 with hundreds of sockets:** use the new small elastic shards.
- **HTTP/2/OpenRouter shape:** keep ~80 streams/connection; the benchmark shows
  the bottleneck at high concurrency is predominantly durable workflow work,
  not the number of H2 physical connections.
