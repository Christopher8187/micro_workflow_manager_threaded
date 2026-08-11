# MWF 0.5.6 central NetworkManager experiment

## Decision

The central network-manager architecture is worth keeping. It improves the
network data plane and measurably improves the durable 22-node skew benchmark,
while preserving the existing MWF task API and execution-lease semantics.

MWF applications still write the same code:

```python
response = shared_http_transport.request("GET", url)
```

There is no required `NetworkManager(...)` object, queue, event loop, or database
configuration in application code. `configure_shared_http_transport(...)`
continues to be optional transport tuning. The new manager backend is the
default; `architecture="direct"` exists for A/B diagnostics.

## Filesystem architecture

Networking is no longer implemented as one large `networking.py` module:

```text
micro_workflow_manager/
  network/
    __init__.py
    manager.py      # event loop, request ingress, pooled clients, telemetry
    transport.py    # thin synchronous/async facade and watchdog integration
  networking.py     # compatibility re-export for existing imports
  storage/
    network_state.py
```

The SQLite schema is version 4 and adds `network_state`, a current per-node
snapshot table. The manager aggregates counters in memory and asks each
workflow storage object for one low-priority bulk upsert at most once every two
seconds. Network telemetry is advisory: it never sits on the request critical
path and a telemetry persistence error cannot fail a job.

## Inspiration from established high-concurrency designs

The prototype follows three common patterns from mature networking stacks:

1. **Persistent connection-pool ownership.** HTTPX documents that a `Client`
   reuses TCP connections and can reduce latency, CPU use, round trips, and
   congestion versus top-level per-call APIs. Its async documentation likewise
   recommends one scoped/global client instead of creating clients in a hot
   loop. https://www.python-httpx.org/advanced/clients/
   https://www.python-httpx.org/async/
2. **One session/pool rather than per-request sessions.** aiohttp documents
   `ClientSession` as the recommended interface and the owner of the connection
   pool/keep-alives. https://docs.aiohttp.org/en/stable/client_reference.html
3. **Event-driven connection ownership.** NGINX documents event modules such as
   epoll/kqueue and a worker connection model where a small number of event
   workers own many sockets instead of assigning one blocking thread per
   connection. https://nginx.org/en/docs/events.html
   https://nginx.org/en/docs/dev/development_guide.html

MWF keeps HTTPX because it is already a dependency and already provides the H1
and H2 behavior MWF needs. The optimization is architectural: one process-wide
manager owns long-lived client shards and coalesces cross-thread request
submission before creating asyncio tasks.

## What changed in the data path

Previous A/B path (`architecture="direct"`):

```text
node fiber -> run_coroutine_threadsafe(request coroutine)
           -> network event loop -> client shard -> socket
           <- Future completion <- event loop
node pump resumes fiber
```

New default manager path:

```text
node fiber -> NetworkRequest -> process-wide SimpleQueue
           -> one coalesced event-loop wake drains up to 4096 submissions
           -> asyncio tasks -> persistent client shards -> sockets
           <- NetworkFuture completion
node pump resumes fiber

                         every <= 2 s
in-memory per-node counters -----------------> one low-priority SQLite upsert
```

The manager does not impose a second global job-concurrency limit. API-node
`max_threads` remains the source of admission pressure. HTTP/2 keeps the same
80-stream-per-client-shard benchmark configuration used by the existing fanout
work.

## 22-node skew A/B

Benchmark shape:

- 22 API nodes;
- two nodes with 2,000 jobs each;
- twenty nodes with 100 jobs each;
- 6,000 total requests;
- 512 aggregate API slots allocated proportionally over the full budget;
- big nodes get 171 slots each; small nodes get 8 or 9;
- HTTP/2, 80 streams per connection/client shard;
- 4 KiB response, 5 ms local server delay, unlimited bandwidth.

Observed local prototype samples are checked in at
`benchmarks/results/network_manager_skew_056_observed.json`.

| Mode | Direct path | Central manager | Change |
|---|---:|---:|---:|
| runner jobs/s | 628.7 | 669.4 | +6.5% |
| runner big:small | 19.743:1 | 19.882:1 | closer to 20:1 |
| workflow jobs/s | 318.0 | 338.5 | +6.4% |
| workflow big:small | 11.685:1 | 13.158:1 | +12.6% ratio |
| workflow response-ready -> resume p99 | 1.175 s | 0.960 s | -18.3% |

In the manager-mode durable sample, enqueue-to-dispatch max was 0.247 s and
response-ready-to-handler-resume max was 1.074 s, both below the requested two
seconds. The durable run persisted state for all 22 network nodes.

The manager reduces a major source of cross-thread scheduling overhead. In the
direct workflow path there is approximately one event-loop ingress wake per
request. In the manager workflow sample there were about 0.050 ingress wakes per
request because dense request waves were drained together.

## Why this does not reach 20:1 by itself

The network manager improves the runner control to essentially 20:1, which is
strong evidence that the central network data plane is not preferentially
serving small nodes. The full durable workflow remains lower because a response
still returns to the owning node's one cooperative controller. That controller
must finish handler code, execution fencing, output publication, terminal SQLite
publication, and fiber retirement before replacement jobs fully recycle.

A big node has roughly 171 live fibers sharing one controller; a small node has
8-9. Centralizing sockets removes network submission overhead but does not make
those durable post-response operations disappear. The manager sample reduces
the response delivery backlog, but the remaining 13.16:1 ratio is still a
control-plane/durability issue.

I deliberately did **not** make terminal lease publication fire-and-forget.
That would benchmark faster by removing a Future wait from the node pump, but a
restart can race between output fencing and terminal commit. Ignoring the
terminal Future would lose the synchronous `JobRestartedError` path and weaken
MWF's execution-generation guarantees. A future architecture can move terminal
coordination to a central scheduler only if restart/retry ownership is moved
with it rather than dropped.

## Delay server

The prior local-server fixes are retained:

- the final/only H1 and H2 data chunk is paced before publication, so 4 KiB at
  4 KiB/s takes about one second;
- H2 socket `drain()` happens after releasing the H2 protocol-state lock, so
  backpressure cannot prevent the reader from processing `WINDOW_UPDATE`;
- flow-control waits periodically re-check the window to avoid a lost shared
  event wake.

## Reproduce

Start a fresh H2 delay service:

```bash
python benchmarks/local_http_delay_server.py --port 8766 --http2
```

A/B runner and durable workflow:

```bash
for architecture in direct manager; do
  for mode in runner workflow; do
    PYTHONPATH="$PWD" python benchmarks/benchmark_network_manager_skew.py \
      --endpoint https://127.0.0.1:8766 --http2 \
      --architecture "$architecture" --mode "$mode" \
      --concurrency 512 --response-bytes 4096 --bytes-per-second 0
  done
done
```
