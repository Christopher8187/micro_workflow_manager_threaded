# Explode improvements in MWF 0.5.8

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

Recovery retires the affected shard and replays the request on a fresh shard.
It does not change the caller's 930-second transport lease, lower any declared
node concurrency, add an aggregate provider-request gate, or disable Astrill.

The saved Explode state reached normal completion with no failed jobs and exact
router/handler parity:

| Node | Done |
|---|---:|
| `explode` | 12,808 |
| `explodeclaim` | 1,951 |
| `explodecontext` | 1,173 |
| `explodedefinition` | 1,252 |
| `explodeexample` | 836 |
| `explodeexercise` | 3,113 |
| `explodeexplanation` | 915 |
| `explodejas` | 1,365 |
| `explodenotation` | 261 |
| `exploderemark` | 646 |
| `explodetheorem` | 1,296 |
| **All handlers** | **12,808** |

The run was deliberately stopped and resumed when a qualifying network error
or a newly discovered recovery edge case appeared. The last resume ended at
20:37:29 on 2026-08-12 with every component job done, no queued/running/failed
jobs, and no qualifying network error. The behavioral code used for the final
resume is the code released as 0.5.8. The run metadata still said 0.5.7 because
the user requested the version bump after the live investigation; the 0.5.8
wheel and metadata were built and verified afterward.

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
  terminals is replayed on a fresh shard. The evidence is monotonic: it remains
  valid during the quiet tail when no new sibling has completed recently.
- A request can receive at most two cohort replays, all within the original
  caller lease. The 930-second transport lease itself is untouched.
- Cancelling an MWF network future now cancels its live asyncio/socket task.
- A shared SSL context and TCP keepalive defaults (30-second idle, 10-second
  interval, three probes) cover connection-wide half-open VPN/TUN failures.
- `.mwf/network_manager.json` reports active request phase, shard, stream, node,
  job ID, byte progress, newer sibling terminal evidence, shard retirement,
  complete-JSON recovery, and cohort replay counters.

In the real recovery run, four requests crossed the 300-second cohort boundary
and were transparently replayed with zero job failures. Examples had 261, 221,
235, and 126 newer sibling terminals. The initial run also recovered 40 complete
JSON bodies that lacked a proper stream terminal and drained 25 affected shards.

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
jobs behind indefinitely. By about 4.5 minutes all initial handler work was
admitted; `explodeexercise` alone had 1,078 running and none queued. More than
2,400 handler jobs completed at about 8 minutes 37 seconds. That did not meet
the old six-minute performance wish, so this report does not claim a fivefold
end-to-end speedup. It does show that local durable fan-out is no longer the
limiting stage.

## Mixed-stage improvement

The useful mixed-stage result is sustained correctness under bidirectional
feedback, rather than a throttled short-run headline:

| Checkpoint | Router done | Handler done | Failed jobs | Qualifying network errors |
|---|---:|---:|---:|---:|
| First full-run stop | 8,606 | 6,791 | 0 | 1 poisoned stream at 930 s |
| First resume | 11,489 | 11,009 | 0 | 0 |
| Cohort-recovery resume | 12,773 | 12,767 | 0 | 0 |
| Final resume | 12,808 | 12,808 | 0 | 0 |

The cohort-recovery resume is the important stress point: four streams that
would otherwise have joined the long tail were replayed at roughly 300 seconds,
with no terminal job failure and a final mutation-writer backlog of 1. This
prevents the healthy jobs from draining away around a set of 930-second zombies
and removes the framework-caused freeze/lease-failure cycle from the mixed
stage.

The earlier 0.5.7 report's 2.79x mixed-stage rate came from a design that capped
active provider requests at 1,024. That gate has been removed at the user's
request, so those numbers are intentionally not presented as 0.5.8 performance.
The current design preserves the requested provider pressure. Remaining mixed
stage duration is dominated by real model generation, semantic fallbacks, and
explicit remote resets rather than local fan-out or undetected poisoned streams.

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

## Verification

The release source passed:

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
The built wheel is `micro_workflow_manager-0.5.8-py3-none-any.whl`; Kaicenat is
pinned to that exact wheel and both runtime and installed distribution metadata
report 0.5.8.

A completely fresh paid Explode rerun using the newly versioned wheel was not
started because the execution safeguard requires explicit approval to transmit
repository-derived Explode prompts to OpenRouter. The saved fixture is prepared
and idle for that acceptance run. This limitation does not affect the completed
candidate-code run or the local/replay test results above, but it is stated here
so the version history is exact.
