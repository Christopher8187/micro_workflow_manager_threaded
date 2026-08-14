# Explode improvements in MWF 0.5.9

## Outcome

The long-run failure was a framework-level HTTP/2 stream-lifecycle defect, not
an account-wide OpenRouter slowdown, ordinary Wi-Fi loss, Astrill being globally
disconnected, or SQLite scanning an ever-growing set of done jobs. One stream
could remain nonterminal on an otherwise healthy multiplexed connection while
hundreds of newer streams on that connection completed. The old transport then
waited until the unchanged 930-second caller lease expired.

MWF 0.5.8 now detects and recovers both observed forms of this defect:

1. a checksum-valid complete JSON response whose HTTP/2 stream never supplies
   its terminal signal; and
2. a nonterminal stream for which enough newer same-shard streams have already
   terminated to prove that the shard is making progress around it.

Recovery retires the affected shard and replays the request on a healthy
previous shard with capacity, or on a capacity-required shared replacement.
It does not change the caller's 930-second transport lease, lower any declared
node concurrency, add an aggregate provider-request gate, or disable Astrill.

The clean 0.5.8 acceptance run exposed an additional framework defect in that
recovery path. At 1,069 seconds it had completed 9,033 router jobs and 7,911
handler jobs with no job failure or qualifying debug error, but 728 cohort
replays had created shard IDs through 824. Only 78 shards had retired; 746
clients were still live, including 269 idle clients, while the controller had
grown to about 3.5 GiB and was still increasing roughly 4 MiB/s. The run was
stopped to prevent paging. MWF 0.5.9 fixes this client multiplication and is the
release described below.

The final acceptance state reached normal completion under MWF 0.5.9 with no
failed jobs and exact router/handler parity:

| Node | Done |
|---|---:|
| `explode` | 13,522 |
| `explodeclaim` | 2,192 |
| `explodecontext` | 1,012 |
| `explodedefinition` | 1,346 |
| `explodeexample` | 913 |
| `explodeexercise` | 3,228 |
| `explodeexplanation` | 975 |
| `explodejas` | 1,612 |
| `explodenotation` | 343 |
| `exploderemark` | 607 |
| `explodetheorem` | 1,294 |
| **All handlers** | **13,522** |

The persisted run record says `status: done`, `mwf_version: 0.5.9`, and records
normal completion at 07:27:21 on 2026-08-13. A one-shot final monitor found no
queued, running, or failed jobs. The clean fixture was originally started with
0.5.8. That run exposed the client-retention defect and was stopped before the
machine paged; the exact durable state was then resumed, without reset or
paste, by 0.5.9. This is therefore a clean-fixture full-component acceptance
completed by 0.5.9, not a claim that every request began in a from-zero 0.5.9
controller.

One semantic terminal failure occurred in the first 0.5.9 resume:
`explodedefinition` job 361 exhausted its configured source-repair fallbacks.
It was restarted exactly as the testing workflow requires, requeued at
generation 2, and completed on the next sole-controller resume. Its final
validation passed. This was not a network failure.

## Proof of the poisoned-stream root cause

The decisive reproduction was `explodeexample` job 116 on shard 37:

- its repair request entered the response-body phase at about 19:43:12;
- the same socket and shard stayed alive;
- more than 100 newer sibling requests terminated on that shard (the final
  shard snapshot recorded 136 started and 131 completed); and
- at 19:58:51 that one request produced the exact 930-second transport-lease
  error from the user's report.

Connection-wide liveness was therefore not enough. HTTP/2 activity from sibling
streams kept the socket readable while one stream remained stranded. This also
explains the apparent `1/t` slowdown: healthy work drains first, leaving a
growing fraction of jobs held by individually poisoned streams until their long
leases expire.

Independent evidence ruled out the simpler alternatives:

- OpenRouter's logged routing overhead, time to first token, and generation
  speed stayed reasonable for requests that reached a terminal response.
- Raw and MWF-shaped same-model probes completed normally. The largest shaped
  probe completed 2,400/2,400 requests in about 31 seconds using 72 elastic
  shards, with no aggregate request gate.
- Astrill traffic used the expected Wintun route. Keeping Astrill enabled did not
  stop sibling streams on an affected connection from completing.
- A representative live process used about 1.97 CPU cores, 1.03 GiB working set,
  1.33 GiB private memory, 154 threads, 865 handles, and 96 established TCP
  connections. A later resume used about 1.58 cores, 435 MiB working set,
  1.53 GiB private memory, and 821 handles. That is material controller and
  protocol work, but it does not explain one stream surviving 100+ sibling
  terminals.
- The mutation writer normally returned to a durability backlog of 0 or 1.
  It was not withholding terminal publication during the poisoned-stream proof.

Some separate provider-originated HTTP/2 `StreamReset` code 1 responses were
also observed. Those are explicit remote terminal errors and configured
fallbacks handled them. They are different from a complete or partially
complete stream that never terminates, and they were not misclassified as the
framework poison.

## Framework changes

### HTTP/2 recovery and observability

- Requests are assigned round-robin across persistent HTTP/2 shards. A shard is
  widened only to the connection-local safety width of 32 streams; additional
  demand opens another shard immediately.
- There is no global active-request gate. Provider pressure still follows the
  application's declared concurrency and MWF's node-pump admission.
- JSON callers opt into `expect_json`. After the content-coding trailer and a
  valid complete JSON entity have arrived, a missing stream terminal is given a
  five-second grace period, then the response is returned and the shard drains.
- A stream nonterminal for 300 seconds after at least 16 newer same-shard
  terminals is replayed away from its retiring source shard. Healthy previous
  shards are reused first; simultaneous replays share replacement shards up to
  the normal 32-stream connection width. The evidence is monotonic through a
  quiet tail.
- A request can receive at most two cohort replays. MWF 0.5.11 applies the same
  caller-configured lease value to each physical transport attempt instead of
  silently making later replays consume only the first attempt's remainder.
  Time in MWF ingress does not consume a physical attempt's lease, and the
  task's total timeout remains active across all attempts.
- Connection-level read/write/protocol failures retire the affected shard and
  receive the same capacity-aware shared-pool treatment rather than leaving a
  known-bad client eligible for new jobs.
- Cancelling an MWF network future now cancels its live asyncio/socket task.
- A shared SSL context and TCP keepalive defaults (30-second idle, 10-second
  interval, three probes) cover connection-wide half-open VPN/TUN failures.
- `.mwf/network_manager.json` reports active request phase, shard, stream, node,
  job ID, byte progress, physical-attempt count, replay reason, OpenRouter
  `X-Generation-Id` when supplied, newer sibling terminal evidence, shard
  retirement, complete-JSON recovery, cohort replay counters, healthy-shard
  reuse, and new recovery-shard creation.

### Redistribute multi-node acceptance diagnosis (0.5.11)

The clean `runfrom redistribute refuse embed` run exposed a lease-accounting
defect that Explode's shorter common-case requests did not exercise. At stop,
the manager had 1,557 requests in flight, 69 clients, 43 retiring clients, 409
cohort replays, and 2,277 connection-error replays. The SQLite writer had only
six pending mutations, ruling out done-job scanning or mutation publication as
the synchronized failure source. Some requests had also spent up to about 69
seconds in manager ingress before dispatch.

MWF previously began one external scheduler lease before manager submission.
When a five-minute cohort proof or connection error caused a hidden physical
replay, the replay inherited only the original lease's remainder. Hundreds of
valid bounded recoveries therefore entered the hook fallback layer together at
the unchanged 450/550-second outer deadline. MWF 0.5.11 separates the states:
submission immediately suspends checkpoint expiry, physical dispatch arms the
configured attempt lease, and each bounded physical replay re-arms that same
duration. No model, node concurrency, stream width, retry limit, or caller
timeout value is changed.

A deterministic 64-request mass-stall regression proves that all 64 replays can
be active simultaneously while occupying 8 shared eight-stream shards. The old
0.5.8 recovery would have created 64 clients. A separate regression proves that
a healthy previous shard is reused without creating any new client.

In the real recovery run, four requests crossed the 300-second cohort boundary
and were transparently replayed with zero job failures. Examples had 261, 221,
235, and 126 newer sibling terminals. The initial run also recovered 40 complete
JSON bodies that lacked a proper stream terminal and drained 25 affected shards.

Hoeflein component startup was also tightened after the full one-process suite
reproduced delayed Redistribute-style consumer admission. Submitting all member
controllers to an executor did not prove that every empty consumer had already
constructed and subscribed its live queue source; under accumulated CPU load a
fast producer could publish first. MWF 0.5.11 now gates the first handler until
all ordinary live members report their complete source stack ready. This is a
one-time component-start barrier, not a concurrency gate, and it leaves the
steady-state pump vector unchanged.

### 0.5.11 local transport and release evidence

The required localhost HTTP/2/TLS benchmark separated provider behavior from
framework overhead. With 64 jobs, 32 concurrency, four nodes, 64 KiB responses,
262,144 bytes/s per stream, and three repeats, the medians were 96.27 jobs/s for
direct `httpx`, 94.09 jobs/s for `ApiRunner` plus MWF's shared transport, and
35.19 jobs/s for the full durable workflow. The runner retained **97.7%** of
direct transport throughput with zero failures. The workflow control includes
payload files, generation fencing, SQLite claims/runtime/terminal events, and
node lifecycle writes.

A second real HTTP/2 cell used 20 simultaneous fan-out nodes, concurrency 512,
1,024 jobs, 1 KiB responses, and three repeats. It completed with zero failures
at a 120.23 jobs/s median. The mutation queue briefly reached 512 objects during
the wave and then drained normally; transient writer backlog did not become a
freeze or a terminal-publication failure.

The exact 0.5.11 source then passed 340 ordinary tests in one process (the cycle
file intentionally excluded), all four required cycle tests in four fresh
Python processes, the explicit marked stress test, 34 networking/watchdog tests,
and the final 73-test scheduler/lifecycle repeat batch. All 270 Python files
compiled before the final startup-gate helper was added; the final candidate is
compiled again during packaging verification.

### Fan-out persistence and observer overhead

Default multi-job declarations previously paid the complete durable publication
path once per job. They now prepare and publish one SQLite batch while writing
independent payload files concurrently. Admission, successful terminal
publication, and failed terminal publication all keep runtime priority 5. That
priority equality is important: lowering terminal publication behind admission
can fill every worker with work whose completions cannot be registered, making
the Explode feedback backlog impossible to drain.

`mwf monitor` now bulk-reads the selected node summaries. `mwf top` uses a
bounded reverse journal walk (`NOT INDEXED`) and honors its redraw interval
during dense terminal waves. These changes remove observer load from the same
CPU and SQLite budget used by the workflow.

The 0.5.9 diagnostic path also groups active requests by shard once, changing a
snapshot from `O(clients * requests)` to `O(clients + requests)`. SQLite writer
bookkeeping retains only genuinely pending serials. In the stopped acceptance
run, the displayed durability gap was 41,404 even though only 531 mutation
objects were queued; completed serial IDs can no longer accumulate behind one
older low-priority observation.

## Fan-out improvement

The clean local comparison below used the same machine, same 2,400 default-job
declaration, and the same restored Kaicenat database:

| Measurement | MWF 0.5.7 | MWF 0.5.8 | Change |
|---|---:|---:|---:|
| Publish 2,400 durable default jobs | 78.789 s | 1.035 s | **76.14x faster** |
| Durable jobs per second | 30.46/s | 2,319.23/s | **+7,513%** |

An earlier 0.5.8 sample was 0.971 seconds (2,472.49 jobs/s), consistent with the
final comparison. On the completed roughly 975 MiB database snapshot, the old
top query had a 1,117.2 ms median and 4,325 ms maximum; the bounded reverse walk
had a 17.1 ms median and 20.0 ms maximum, a **65.49x median improvement**. After
the fixture was restored to a short 3,732-event journal, the same comparison was
11.89 ms versus 3.44 ms, still **3.46x** faster.

In the live full run, initial handler admission no longer remained hundreds of
jobs behind indefinitely. The clean 0.5.8 controller reached 2,407 handler
completions at 452.887 seconds (7 minutes 32.887 seconds). At that checkpoint it
had completed 4,811 router jobs, with 61 handler jobs queued and 2,343 running.
`explodeexercise`'s initial queue peaked at 996 jobs at 102.478 seconds and
returned to zero at 241.386 seconds; at that point 126 exercises were done and
1,102 were running. Relative to the previously measured 8 minutes 37 seconds,
the 2,400-handler time improved by 64.113 seconds, **12.40%**, or **1.14x**.

That does not meet the requested six-minute or fivefold end-to-end target, so
this report does not claim that it does. The evidence instead separates the
stages: local publication of the original 2,400 durable jobs improved 76.14x,
while end-to-end handler completion remains dominated by provider generation,
semantic validation/fallbacks, and the workload's continuing feedback. Local
fan-out is no longer the primary bottleneck.

## Mixed-stage improvement

The useful mixed-stage result is sustained correctness under bidirectional
feedback, rather than a throttled short-run headline:

| Checkpoint | Router done | Handler done | Failed jobs | Qualifying network errors |
|---|---:|---:|---:|---:|
| 0.5.8 stopped for memory safety, 1,068.379 s | 9,033 | 7,911 | 0 | 0 job errors; framework memory defect |
| 0.5.9 first resume, 600.388 s | 12,617 | 12,458 | 0 | 0 |
| 0.5.9 first resume exit, 1,493.100 s | 12,777 | 12,776 | 0 active; job 361 requeued | 0 |
| 0.5.9 final resume exit, 1,818.197 s | 13,522 | 13,522 | 0 | 0 |

The first 0.5.9 resume completed 4,807 additional handler jobs from its first
sample to exit. It transparently handled 392 transport-error retries and 57
cohort-stall retries. Those recovery allocations reused healthy capacity 440
times and created only seven new shared shards. The run ended with one idle
client, no in-flight request, and writer durability/pending/heap counts all
zero. The one semantic failure was requeued before exit as described above.

The final resume completed the feedback tail. It exercised another five cohort
retries: four reused the existing healthy pool and all simultaneous remaining
retries shared one replacement shard. It had no transport error, no qualifying
debug error, and no failed job. At normal exit it had one idle client, no
in-flight requests, no queued writer mutation, and a final observation/write
watermark difference of one. Thus a real poisoned-shard tail was recovered
without either a 930-second job error or per-replay client multiplication.

The earlier 0.5.7 report's 2.79x mixed-stage rate came from a design that capped
active provider requests at 1,024. That gate has been removed at the user's
request, so those numbers are intentionally not presented as 0.5.8 performance.
The current design preserves the requested provider pressure. Remaining mixed
stage duration is dominated by real model generation, semantic fallbacks, and
explicit remote resets rather than local fan-out or undetected poisoned streams.

### MWF 0.5.11 Redistribute/hook acceptance

The Redistribute acceptance used the framework in its real DAG shape rather
than a synthetic request loop. `redistribute` was already durable, and the
controller resumed 2,128 unfinished hook jobs while preserving 412 completed
hook results. `refhook`, `loosehook`, and `summaryhook` retained their declared
concurrency of 1,000, shared the same transport used by Explode, and ran the
April DeepSeek V4 Flash primary, the same Flash repair, and DeepSeek V4 Pro
final repair. `refuse embed` remained an exclusive boundary throughout.

The initial ramp demonstrated that neither DAG admission nor SQLite terminal
publication was the bottleneck:

| Time | Durable command `done` count | Increment from 413 | Mean increment rate |
|---|---:|---:|---:|
| 20:26:06 | 413 | 0 | - |
| 20:29:25 | 1,778 | 1,365 | 6.86 jobs/s |
| 20:31:41 | 2,435 | 2,022 | 6.04 jobs/s |

At peak observation, 1,217 provider requests were in flight while the controller
used 453.69 MiB. As demand fell, memory fell to approximately 388-400 MiB rather
than continuing the former 4 MiB/s climb. Forty-five five-minute cohort stalls
were transparently replayed; all 45 reused existing healthy capacity, no
recovery-only shard was created, and no client-per-replay growth returned.

The run then exposed a distinct quiet-tail defect. Six remaining requests were
spread across six of 25 mostly idle clients by the old round-robin selector.
Each request therefore lacked the fixed 16 same-shard terminals needed for the
early cohort proof. HTTPX's poisoned HTTP/2 body iterator also failed to raise
its configured 500-second read timeout, so the scheduler emitted five
550-second transport-lease errors. The controller was killed immediately under
the testing rule at 2,534 completed hook jobs, preserving the final six.

MWF 0.5.11 closes that gap without changing the 500-second HTTP timeout, the
550-second supervisor lease, declared concurrency, or provider pressure:

- draining requests are packed onto the busiest healthy shard rather than
  scattered across idle connections;
- when a shard has fewer than 16 available peers, every available peer must
  terminate and at least two terminals are still required for an early
  cohort-outlier replay; and
- the JSON reader independently applies the existing read-idle timeout between
  body chunks, converting a silent iterator into the same bounded transparent
  transport replay before the supervisor lease expires.

The preserved six-job resume ran from 03:33:52 to 03:36:43. All six requests
shared one shard, completed on physical attempt 1, and produced zero new cohort,
transport, JSON-terminal, network-debug, or credit errors. The controller exited
normally with `refhook` 482/482, `loosehook` 1,691/1,691, and `summaryhook`
367/367 done; failed and running counts were zero; `embed` was the sole queued
boundary job and `storedatabase` remained queued downstream.

An exhaustive post-run audit parsed all 2,540 result JSON files and checked
their hook identity, nonempty unique integer orders, model-validation
provenance, durable job counts, refusal boundary, and post-fix debug interval.
It found zero issues across 12,987 enriched updates. Model outcomes were 2,269
primary Flash results, 261 Flash-repair results, and 10 DeepSeek Pro final
repairs. Thus model enrichment remained mandatory, while 10.28% of results
needed the first repair and 0.39% needed the final repair.

Stopping the earlier controller also exposed Windows PID reuse: its dead PID
33248 was later assigned to an Edge renderer, and a PID-only run guard treated
that unrelated process as the controller. New run records now include the OS
process-start identity. A legacy record with a stale heartbeat cannot be revived
by bare PID existence, so safe resume no longer requires manually altering
`.mwf/run.json`.

## Pump allocation retained

The simultaneous-node pump allocator remains:

```text
B = max(k, min(sum(independent_plateau_i), logical_processors + 5))
pump_i starts at 1
next pump goes to argmax_i n_i / (pump_i * (pump_i + 1))
independent_plateau_i = min(12, ceil(n_i / 64))
```

For the ten handlers, the total is 21 pumps with vector
`[1,2,3,2,4,2,2,1,2,2]` in claim/context/definition/example/exercise/
explanation/jas/notation/remark/theorem order. Every runnable node is guaranteed
one pump, while marginal allocation gives larger nodes additional pumps. The
declared node concurrency remains unchanged, including 1,400 for
`explodeexercise`.

## Diagnostic commands added

Use separate terminals for the run, monitor, top view, and recovery/debug work:

```powershell
mwf monitor --interval 0.5 --json --no-clear
mwf top --interval 0.5 --json --no-clear
mwf threads
mwf doctor
mwf filter explode
mwf inspect explode
mwf inspect explode failed
mwf inspect <node> job <job-id>
mwf trace <node> job <job-id>
Get-Content .mwf/network_manager.json -Raw
```

For a suspected poison, record the request's shard, stream ID, phase, response
bytes, age, newer sibling terminal count, and shard retirement reason. A live
TCP socket or recent sibling activity is not proof that the individual stream
is healthy.

## Pre-0.5.9 verification history

Before the memory acceptance exposed the per-replay client defect, the 0.5.8
release source passed:

- 309 ordinary tests in one batch, with only the explicitly marked stress test
  deselected;
- all four autostart-cycle tests in separate fresh Python processes;
- the explicit marked stress test;
- the mandatory scheduler/lifecycle/network repeat batch (60 tests);
- the version/metadata and focused regression batch (45 tests); and
- real OpenRouter raw/MWF-shaped probes, including the 2,400-request shaped
  probe described above.

The network manager was split into focused configuration, diagnostics, recovery,
and type modules so every architecture-guarded module remains below 500 lines.
The 0.5.8 wheel passed the original clean packaging checks. MWF 0.5.9 adds the
bounded recovery/client and mutation-watermark corrections found by that paid
acceptance run. Kaicenat is pinned to the 0.5.9 wheel.

### MWF 0.5.9 memory acceptance

The user's observed approximately 4 MiB/s memory growth was real and came from
the framework's recovery architecture. MWF 0.5.8 requested a `fresh` client for
each stalled replay. Because those one-request clients were still eligible to
remain live after their request, cumulative recovery count—not current demand—
drove client count and memory. Its active-diagnostic snapshot also scanned all
requests separately for every client, amplifying CPU cost as both collections
grew. Finally, writer serial IDs that completed out of order could remain
retained behind one older mutation even though their mutation objects had
already been released.

MWF 0.5.9 fixes all three mechanisms:

- recovery excludes only the poisoned source shard, then uses the same
  capacity-aware shared pool as ordinary requests;
- diagnostics group requests by shard in one pass, `O(clients + requests)`;
- the writer keeps a bounded pending-serial set/heap rather than a historical
  completed-serial set; and
- replaceable low-priority network snapshots coalesce before persistence.

The live comparison was decisive:

| Measurement | Defective 0.5.8 controller | 0.5.9 acceptance |
|---|---:|---:|
| Peak live clients | 779 | 21 |
| Late idle clients | 269 | at most 1 |
| Recovery at representative exit | 728 cohort retries | 57 cohort + 392 transport retries |
| Recovery allocation behavior | effectively one fresh client per cohort replay | 440 healthy/shared reuses, 7 new shared shards |
| Peak measured working set | about 3.5 GiB before safety stop | 1,102.07 MiB |
| 600–1,100 s working-set slope | still rising rapidly in the observed failure | 0.0126 MiB/s |
| Writer at representative exit | 531 objects queued; misleading serial gap 41,404 | queued 0; pending 0; heap 0; durability gap 0 |

The 0.5.9 working set rose while hundreds of responses and validation objects
were concurrently live, then plateaued: 1,089.54 MiB at 600.388 seconds,
1,097.25 MiB at 900.315 seconds, 1,098.32 MiB at 1,050.366 seconds, and
1,100.98 MiB at 1,200.280 seconds. It peaked at 1,102.07 MiB, roughly **69.2%
below** the 3.5 GiB safety-stop level, despite processing 449 recovery events.
The final resume, which began with only the feedback tail, peaked at 284.68 MiB
working set and 287.62 MiB private memory. It ended with a 44 MiB sampled
working set immediately before process exit.

These measurements also separate network conditions from framework behavior.
The first 0.5.9 resume's independent watcher recorded a brief machine-wide
reachability disturbance: 9 of 775 two-second internet TCP probes and 8 of 775
OpenRouter probes failed, each timing out at about two seconds. MWF recovered
the resulting transport attempts without a terminal job network error or
client explosion. The final resume recorded 909/909 successful internet probes
and 909/909 successful OpenRouter probes. In both cases, the framework's client
count followed current in-flight demand back down instead of cumulative errors.

### Final release verification

The exact 0.5.9 repository source passed after the live acceptance:

- 315 ordinary tests in one batch, with only the marked stress case deselected;
- all four autostart-cycle tests, each in its own fresh Python process; and
- the explicit marked filesystem stress test.

The built `micro_workflow_manager-0.5.9.tar.gz` was extracted to a clean
directory and independently passed the same 315 ordinary tests, four isolated
cycle tests, and marked stress test. The installed Kaicenat environment reports
MWF 0.5.9, and the MWF distribution wheel and Kaicenat vendored wheel have the
same SHA-256:
`13960dc8376e3d9dc66216265b3d8ccdf4654e19574f52fc034bae83539be91c`.

Kaicenat's completed Explode SQLite state and generated outputs were deliberately
left untouched after verification. No reset or paste was performed, so the
finished state remains available for the user's subsequent `redistribute` run.

### MWF 0.5.11 final verification

The exact 0.5.11 source, including quiet-tail recovery and process-identity run
ownership, passed 350 ordinary tests with only the marked stress case
deselected. The marked stress test passed separately, all four autostart-cycle
tests passed in independent Python processes, the final networking/watchdog set
passed 38/38, and the post-PID-change active-run/restart/resume set passed 17/17.
The Kaicenat vendored and installed 0.5.11 wheel used for the successful final
resume has SHA-256
`343fb57256c21d37bf4b77b9135c7881d88e7e0228c7e92208ed56ea4de6812c`.
No Explode or Redistribute reset/paste was performed after acceptance.
