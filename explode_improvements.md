# Explode performance improvements

## Outcome

MWF 0.5.7 now completes more than 2,400 jobs across the ten Explode handler
nodes within the requested six-minute window while preserving every declared
node concurrency. The accepted live run reached job 2,401 at **329.817 seconds
(5:29.8)**, 30.2 seconds inside the target. No handler job failed and there was
no post-start freeze of five seconds or longer.

The closest unmodified-transport 0.5.7 comparison reached job 2,401 at
529.848 seconds. The final design is therefore **1.607x faster** to the target,
or **200.031 seconds / 37.75% less time**. A separate ten-minute baseline did
not reach the threshold: it completed 2,011 handler jobs in 602.055 seconds and
included a 50-second all-node freeze.

All runs followed `explode_testing_workflow.md`: reset `explode`, paste
`explode`, paste `redistribute`, verify declared/effective concurrency, then
run only `mwf run explode`. `explodeexercise` remained 1,400, the aggregate API
budget remained unset, and each run stopped at more than 2,400 combined handler
completions or ten minutes. Central `explode` and `redistribute` jobs are not in
the completion count.

## What caused the long-run freezes

The evidence does not support a Wi-Fi fault or an account-wide provider outage.
It shows a provider bulk-queue collapse that the framework was making much more
likely:

- During a 105-second all-node bulk freeze, independent same-model canaries
  continued to complete in roughly 1.5–6.3 seconds through both DigitalOcean
  and StreamLake.
- TCP probes to OpenRouter and to an independent public address had zero
  failures during the live tests.
- The SQLite writer was effectively idle during the freeze, so persistence was
  not withholding completion registration.
- Direct synthetic requests using the same account, model, and 22 KB prompt
  size completed 1,024/1,024 at 32 streams per connection. The controlled
  1,024-request sweep produced 35.26 requests/s at width 32.
- The same controlled total concurrency at width 80 fell to 4.53 requests/s in
  the earlier probe: a **7.79x throughput disparity** caused only by connection
  stream width.
- Allowing 1,536 long-prompt requests in flight completed one live run in
  342.804 seconds, but a repeat froze for 104.749 seconds and accumulated 1,489
  network failures by the ten-minute stop. At the directly proven 1,024-active
  envelope, the live run completed in 329.817 seconds.

The immediate freeze is upstream bulk scheduling: tiny calls and TCP remain
healthy while long generations across every Explode node stop completing. The
framework fault was unbounded amplification. It created another HTTP client
whenever existing connections filled, so thousands of admitted jobs could
become simultaneous provider work. This is also why lower-concurrency
`explodejas` looked disproportionately fast: it contributed less to the shared
bulk queue. Increasing a node's declared concurrency increased queue pressure,
not useful provider service.

## Retained framework design

The existing simultaneous-node pump function remains:

```text
B = max(k, min(sum(independent_plateau_i), logical_processors + 5))
pump_i starts at 1
next pump goes to argmax_i n_i / (pump_i * (pump_i + 1))
independent_plateau_i = min(12, ceil(n_i / 64))
```

For the ten simultaneously running Explode handlers, `B=21` and the vector is:

| Node | Declared concurrency | Pumps |
|---|---:|---:|
| `explodeclaim` | 200 | 1 |
| `explodecontext` | 400 | 2 |
| `explodedefinition` | 800 | 3 |
| `explodeexample` | 500 | 2 |
| `explodeexercise` | 1,400 | 4 |
| `explodeexplanation` | 400 | 2 |
| `explodejas` | 400 | 2 |
| `explodenotation` | 200 | 1 |
| `exploderemark` | 400 | 2 |
| `explodetheorem` | 600 | 2 |
| **Total** | **5,300** | **21** |

Every active node is guaranteed one pump. A measured five-pump exercise
candidate admitted exercise faster but worsened time to 2,401 from 369.440 to
449.479 seconds and reintroduced a network-wide stall. Four pumps is therefore
the better backpressure allocation for this simultaneous node vector.

The new transport pressure plane is independent from that job admission plane:

- Requested HTTP/2 stream width is capped to 32 per connection by default.
- The central manager actively dispatches at most 1,024 network requests; later
  requests wait inside the manager.
- No node `max_threads`, pump lane sum, job status, or aggregate API admission
  setting is changed.
- `MWF_HTTP2_STREAM_SAFETY_CAP` and
  `MWF_NETWORK_ACTIVE_REQUEST_LIMIT` explicitly override the defaults for a
  provider with a different measured envelope.
- Snapshots expose requested/effective width, safety cap, active capacity,
  client count, and in-flight count.

Execution admission, successful terminal publication, and failed terminal
publication all retain runtime priority 5. This matters in Explode: giving
completion/failure lower priority than admission can fill every worker with
work whose terminal state cannot be registered, producing an impossible
feedback backlog inside the Hoeflein component.

## Fan-out and mixed-stage measurements

The stage rates below compare the ten-minute unmodified-transport 0.5.7
baseline with the accepted final run. They count durable job-creation events,
not network requests.

| Measurement | 0.5.7 baseline | Final | Change |
|---|---:|---:|---:|
| Time to 2,401 handler completions | not reached in 602.055 s | 329.817 s | goal reached |
| Time to 2,000 | 595.061 s | 293.610 s | **2.027x; -50.66%** |
| Handler throughput over observed run | 3.340/s | 7.269/s | **2.176x; +117.6%** |
| Fan-out-only duration before first feedback | 13.753 s | 10.039 s | **-27.0%** |
| Fan-out-only, `explode` to handlers | 63.114/s | 56.380/s | **-10.7%** |
| Mixed, `explode` to handlers | 6.262/s | 15.602/s | **2.491x; +149.2%** |
| Mixed, handlers back to `explode` | 1.394/s | 5.760/s | **4.132x; +313.2%** |
| Mixed, both directions | 7.656/s | 21.362/s | **2.790x; +179.0%** |
| Final network failures | 1,595 | 10 | **-99.37%** |
| Post-start all-node freeze | 50.3 s observed | none >=5 s | eliminated |

The pure initial fan-out rate did not improve; it declined 10.7%, although
feedback began 27.0% sooner. That is acceptable and useful: initial fan-out was
not the limiting stage, and pushing it harder only overloaded the provider.
The breakthrough is the mixed phase, where transport completions, handler
feedback, and new handler work coexist. Bidirectional durable creation improved
2.79x while avoiding the long provider queue collapse.

## Rejected candidates and plateau decision

- Unbounded shards with only the 32-stream fix: 2,401 at 369.440 seconds in one
  run but 444.267 seconds in an identical repeat.
- Five exercise pumps within the unchanged 21-pump budget: 449.479 seconds and
  a six-second all-node stall.
- 48 shards / 1,536 active requests: 342.804 seconds once, but an identical
  repeat failed to reach 2,401 in ten minutes because the provider bulk queue
  froze for 104.749 seconds.
- Widths 16, 20, 24, and 28 all underperformed width 32 in the controlled
  1,024-request sweep. Width 80 suffered the severe tail collapse above.

The accepted width 32 / active 1,024 design is the tested plateau: it is the
fastest controlled width, the directly proven stable bulk envelope, and it
reached the end-to-end target. Raising admission or active bulk pressure beyond
it traded a possible fast sample for catastrophic variance, which is not a
valid performance improvement.

## Added diagnostic commands

Run these in separate terminals when investigating timing or provider issues:

```powershell
mwf monitor --interval 0.5 --json --no-clear
mwf top --interval 0.5 --json --no-clear
mwf threads
mwf inspect explode
mwf filter explode
```

The authorized provider-isolation tools are stored in `C:\Users\Chris\Videos`:

```powershell
python openrouter_provider_canary.py
python openrouter_mwf_shard_probe.py --concurrency 1024 --requests 1024 --stream-widths 16,20,24,28,32 --output openrouter_mwf_shard_probe_width_sweep_1024.json
```

The live harness also ran a two-second TCP connectivity probe and recorded
monitor/top JSON, exact milestone times, persistence backlog, network counters,
provider generation statistics, and freeze intervals. It never prints or
copies the configured API key.

## Observability correction

Persisted network high-water fields previously used SQL `MAX(...)` across
separate manager processes. Immediately after a reset, monitor could therefore
show the prior run's peaks until fresh rows arrived, creating a false startup
freeze and misleading ingress-delay values. Fresh snapshots now replace those
per-run peaks. Within one manager process the counters themselves remain
monotonic, so live high-water semantics are unchanged.

## Verification

The exact final source tree passed the authoritative `HOW_TO_TEST.md` protocol:

- all 264 packaged/source Python files compiled;
- 294 ordinary tests passed as one batch (`test_autostart_cycles.py` excluded
  only for its required isolated execution);
- all four autostart-cycle tests passed, each in its own fresh Python process;
- the explicit marked stress test passed;
- the mandatory scheduler/lifecycle/network repeat batch passed 68 tests; and
- the focused network/storage suite passed 41 tests.

Real-socket localhost HTTP/2 validation also had zero failures. The required
durable workflow cell (32 concurrency, four nodes, 64 responses of 64 KiB at
256 KiB/s) had median throughput 42.82 jobs/s across three samples. A
1,024-concurrency, 20-node runner cell completed 2,048 jobs with median
throughput 608.65 jobs/s across three samples.

The 0.5.7 wheel metadata was inspected, and the source distribution was
extracted into a fresh directory. That fresh archive independently compiled all
264 Python files and passed the same 294-test ordinary batch, four isolated
cycle tests, and marked stress test. `MANIFEST.in` now ensures the archive
contains the benchmark harness, examples, contributor/test instructions, and
this report rather than only package code and tests.
