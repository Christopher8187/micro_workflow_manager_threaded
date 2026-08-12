# Explode performance improvements

## Outcome

The retained MWF design makes large-concurrency nodes useful without changing
their configured concurrency. Simultaneously running API nodes receive one
globally allocated controller-pump vector. Every node gets a pump; large nodes
receive more pumps only while there is measured marginal benefit; each node's
lane concurrency shares always sum to its exact declared `max_threads`.

In the final fixed-declared-concurrency five-minute live comparison, the ten
explode handlers completed 1,609 jobs versus 1,367 at baseline, with no terminal
handler failures in either run. Handler throughput rose from 4.5422/s to
5.3393/s: **1.175x, or +17.5%**. Time to 701 handler completions fell from
214.025 s to 156.558 s: **1.367x faster, or -26.8%**.

The original 5x goal was reached for the isolated large-node MWF controller
bottleneck, not for the provider-bound live workflow. A local real-socket H2
benchmark of one node at declared concurrency 1,400 improved from 40.53 jobs/s
with one pump to 268.81 jobs/s at the measured pump plateau: **6.63x**. With all
ten explode-shaped nodes running simultaneously, the shared 21-pump allocator
measured 241.43 jobs/s versus 126.6 jobs/s for the single-pump control:
**1.91x**. Live OpenRouter throughput and heterogeneous response work then
limit the end-to-end gain.

All final live measurements obeyed `explode_testing_workflow.md`: no aggregate
API budget, no per-node override, every effective limit equal to its declaration
(including `explodeexercise=1400`), separate `mwf monitor` and `mwf top`, and a
hard stop at the first of more than 2,400 combined handler completions or five
minutes. Both compared runs reached the five-minute boundary. Central `explode`
and `redistribute` jobs are excluded from every handler count below.

## Live stage measurements

| Fixed-concurrency metric | Baseline | Retained design | Change |
|---|---:|---:|---:|
| Handler completions in five minutes | 1,367 | 1,609 | +17.7% |
| Handler completions/s | 4.5422 | 5.3393 | 1.175x; +17.5% |
| Time to first handler completion | 19.721 s | 13.410 s | 1.471x; -32.0% |
| Time to 100 completions | 77.729 s | 53.322 s | 1.458x; -31.4% |
| Time to 250 completions | 106.479 s | 87.238 s | 1.221x; -18.1% |
| Time to 500 completions | 163.120 s | 130.586 s | 1.249x; -19.9% |
| Time to 701 completions | 214.025 s | 156.558 s | 1.367x; -26.8% |
| Fan-out-only, `explode` to handlers | 89.8686 jobs/s | 76.4077 jobs/s | 0.850x; -15.0% |
| Mixed, `explode` to handlers | 7.4522 jobs/s | 10.6713 jobs/s | 1.432x; +43.2% |
| Mixed, handlers back to `explode` | 1.6404 jobs/s | 2.0121 jobs/s | 1.227x; +22.7% |
| Mixed, both directions | 9.0927 jobs/s | 12.6834 jobs/s | 1.395x; +39.5% |
| Peak mutation durability backlog | 8,147 | 5,915 | -27.4% |
| Mutation backlog p95 | 2,007 | 1,027 | -48.8% |
| Peak queued SQLite requests | 378 | 221 | -41.5% |
| Queued SQLite requests p95 | 112 | 78 | -30.4% |
| Terminal handler failures | 0 | 0 | unchanged |

The pure initial fan-out phase did **not** improve in the live A/B; it regressed
15.0%. This phase is short and its boundary moved from the first feedback at
23.590 s to 16.569 s, so its rate is particularly sensitive to which provider
responses arrive first. The controlled local router benchmark separately
improved the 3,732-job, ten-target publication path from 211.01 to 375.11
jobs/s (**1.78x**). The live result therefore does not justify claiming a
provider-facing fan-out speedup. The repeatable live breakthrough is the mixed
phase, where feedback, new fan-out, API completion, journaling, and terminal
publication contend at the same time.

The disparity that motivated this work is substantially reduced. At baseline,
`explodeexercise` (declared 1,400) did not show its first completion until
226.944 s, while `explodejas` completed at 20.075 s. With pump allocation,
exercise's first completion arrived at 22.464 s (**10.10x sooner**) and JAS's at
13.239 s (**1.52x sooner**). Exercise completed 506 jobs in the window versus
459 at baseline, while JAS completed 68 versus 61. The large node no longer
waits behind one overloaded controller pump.

## Global pump function

For the set of actually simultaneous API nodes, let declared node concurrency
be `(n_1, ..., n_k)` and allocated pump count be `(p_1, ..., p_k)`. MWF computes:

```text
isolated_ceiling_i = min(12, ceil(n_i / 64))
B = max(k, min(sum(isolated_ceiling_i), max(12, logical_processors + 5)))
p_i = 1 for every i
while sum(p_i) < B:
    choose eligible i maximizing n_i / (p_i * (p_i + 1))
    p_i += 1
```

`n_i / (p_i(p_i+1))` is the marginal reduction in controller load under an
even `n_i/p_i` partition. It is a separable diminishing-return objective, so
greedily taking the largest remaining marginal benefit gives the optimal
integer allocation for this model. Ties are deterministic by node name.

On this 16-logical-processor host and the ten explode handlers, `B=21` and the
allocation is:

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

The 14–21 range is a host/workload operating region, not a rule that can
override the no-starvation constraint. If more than 21 API nodes run at once,
the mathematical requirements conflict; MWF correctly makes `B >= k`, so the
one-pump-per-node guarantee wins. A 30-node regression verifies that all 30 get
one pump.

The budget is global across overlapping DAG waves, not merely global within one
scheduler iteration. Pumps owned by an earlier still-running component remain
charged when a later branch becomes ready. A newly ready component waits if the
remaining capacity cannot give each of its API members one pump. Pumps are
non-preemptive within a running component, avoiding controller teardown while
jobs are live.

Pump count is not request concurrency. `_LaneCoordinator` partitions each
node's current effective concurrency exactly across its pumps. For exercise,
four live lane limits sum to exactly 1,400; if one pump exits, the remaining
shares are recomputed and still sum to exactly 1,400. The allocator never
changes a declared/effective concurrency value.

## Pump plateau

The simultaneous ten-node real-socket H2 benchmark held all declared limits and
job populations fixed and changed only the shared pump budget:

| Total pumps | Median jobs/s | Relative to 21 |
|---:|---:|---:|
| 14 | 177.90 | 0.737x |
| 18 | 189.43 | 0.785x |
| 20 | 216.43 | 0.896x |
| **21** | **241.43** | **1.000x** |
| 24 | 211.02 | 0.874x |

Twenty-one is 35.7% faster than fourteen and 14.4% faster than twenty-four.
The regression at 24 is why the default uses a bounded global host budget
instead of blindly increasing pumps with concurrency. `min(12, ceil(n/64))`
also caps the isolated curve after the 1,400-node benchmark plateaued.

## Other retained bottleneck fixes

### Non-blocking, lease-fenced event journals

An API handler formerly blocked its pump on individual SQLite commits for
`trace`, `output_written`, and `input_forwarded`. In live exercise traces these
ordinary post-provider events could be separated by 12–95 seconds under writer
pressure. API fibers now enqueue those rows into the ordered grouped mutation
writer and continue cooperatively. Each append carries the execution generation
and ID; stale attempts are rejected in the transaction. The handler flushes its
pending event futures in submission order before fallback or terminal
publication, preserving durable-before-terminal provenance.

### Coalesced checkpoint observations

Priority-20 runtime JSON is advisory observability, not execution ownership.
Not-yet-executing asynchronous snapshots for the same node/job/generation/
execution/priority now share one replaceable slot. The writer freezes the newest
snapshot only when its transaction begins. Synchronous timeout records retain
their barrier and the terminal job row remains authoritative. This reduced the
live priority-20 request peak from 370 to 195 and prevented observability work
from overwhelming the pump gain.

### Equal critical priority

The required invariant is explicit and regression-tested:

```text
RUNTIME_CRITICAL_PRIORITY = 5
ADMISSION_PRIORITY = 5
TERMINAL_PRIORITY = 5
```

Successful and failed completion use the same lease-fenced terminal path.
Admission, success, and failure therefore have equal priority. FIFO order within
that class, bounded claim transaction weight, cooperative callback servicing,
and asynchronous lower-priority observations prevent an impossible completion
backlog without letting one kind of terminal outcome jump another.

## Provider control and rejected experiments

The direct minimal OpenRouter probe succeeded at every tested level, but scaled
sublinearly: 5.00 requests/s at concurrency 32, 7.60 at 128, 25.28 at 512, and
32.36 at 1,024. Median latency rose from 3.79 s to 18.43 s. More admitted work
therefore still raises throughput, but nowhere near proportionally; that is an
external limit on a 5x live result.

Rejected or non-default experiments:

- 24 global pumps: 211.02 jobs/s, slower than 21 pumps' 241.43 jobs/s.
- Asynchronous events without checkpoint coalescing: only 1,478 live handler
  completions and a 18,808 durability-backlog peak.
- Manual SQLite `VACUUM`: shrank a 1.024 GB database with about 75% free pages
  to 97 MB, but the compacted live sample fell to 1,242 handler completions. It
  is not an automatic framework action.
- An HTTP/2 client-shard width of 32 improved a small direct OpenRouter probe,
  but did not improve the heterogeneous live workflow. The transport change was
  removed.
- Earlier aggregate-concurrency experiments predate the fixed-concurrency rule
  and are intentionally excluded from the final A/B. The retained allocator
  changes controller pumps only; final live runs used no `--api-total` setting
  and no per-node override.

## Added diagnostics and benchmark commands

In addition to the required preparation/run commands, these read-only MWF
commands were used. They never delayed either hard stop:

```powershell
mwf threads
mwf monitor --interval 0.5 --json --no-clear
mwf top --interval 0.5 --json --no-clear
mwf doctor
mwf filter explode
mwf inspect explode
mwf inspect explode failed
mwf inspect <node> job <job-id>
mwf trace <node> job <job-id>
```

Framework-local and provider controls used these additional commands/scripts:

```powershell
python benchmarks/local_http_delay_server.py --port 8766 --http2
python benchmarks/benchmark_explode_pump_function.py --endpoint https://127.0.0.1:8766 --global-budget 21 --repeats 2
python C:\Users\Chris\Videos\openrouter_concurrency_probe.py
python C:\Users\Chris\Videos\openrouter_mwf_shard_probe.py
```

The provider probes and JSON results are in `C:\Users\Chris\Videos` and use the
existing configured key without printing or copying it. The reusable pump
benchmark is committed under `benchmarks/benchmark_explode_pump_function.py`.

## Verification

The final branch passed the authoritative `HOW_TO_TEST.md` process:

- compiled all 264 Python files in the final tree (the first full-suite compile
  contained 263; the cohesive runtime-observation module was then extracted);
- `291 passed, 1 deselected` in one complete ordinary batch (the deselection is
  the explicitly separate stress test);
- all four autostart/cycle cases passed in four fresh Python processes;
- the explicit marked Markov-chain stress case passed;
- `59 passed` in the mandatory scheduler/lifecycle/fan-out focused batch;
- `38 passed` in the focused networking/storage/pacing batch; and
- focused pump/event/checkpoint tests passed, including exact 1,400 lane-share
  conservation, the ten-node vector, 30-node no-starvation, stale-event fencing,
  runtime coalescing, and the cross-DAG-wave global-budget invariant.

The performance plateau is now bracketed on both sides, the 5x-plus isolated
large-node controller result is reproduced with real sockets, the final live
comparison keeps all user concurrency settings fixed, and further MWF pump
growth demonstrably regresses. Remaining live scaling is primarily provider and
application-response work rather than an unallocated large-node controller.
