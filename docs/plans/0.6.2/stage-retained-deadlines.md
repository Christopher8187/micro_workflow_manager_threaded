# Retained checkpoint deadlines

Status: **accepted within the retained-deadline and runtime-publication
boundary**. Focused and adjacent checks cover initial arming, rearm maintenance,
replay heartbeat interleaving, timeout observations, timeout-event ownership,
and delayed runtime publication. The final 298-file combined selection passed
ordinary, cyclic, selected stress, repeated-use, and state-integrity checks and
received an independent Sol xhigh PASS. Sampling and component-lifecycle
integration, the whole issue, release, and final Astra review remain pending.

This correction preserves task-facing timeout behavior while implementing
[Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45).
It receives no sampling requirement credit. Its base is
`4ea961b8a894df2adfaba5fbf747a2962d4662de`; the initial selected source excludes
unfinished preview, ownership, and readiness work. The final combined selection
described below includes the accepted private ownership and pure readiness work.

## Observed failure and correction

The second sampling ordinary run failed job A/52 at the second execution check
inside `ctx.checkpoint("model request started", timeout=0.02)`. It had not entered
the network call. The saved error named the newly installed checkpoint.

Two controlled real-API cases isolate framework time. Both freeze the supervisor
clock and observe durable task results. The condition case holds the real
supervisor lock while advancing the clock. The submission case blocks the
checkpoint's runtime update. An observed return to condition waiting establishes
that the supervisor evaluated the deadline before either gate is released.

The condition case failed with a checkpoint timeout on unchanged source in
2.13 seconds. Sampling the deadline under the owned condition made it pass in
1.75 seconds. The submission case still failed after that correction; the two
cases finished with one pass and one failure in 54.97 seconds.

Checkpoint reporting now clears checkpoint expiry while submitting its runtime
observation, then rearms a full interval from a fresh clock sample if the
attempt remains active. Total timeout remains scheduled. API submission still
does not wait for commit; other runners retain synchronous visibility. Both
framework-delay cases passed in 21.56 seconds.

## Preservation and sensitivity

The four-case test also advances time after checkpoint return and past the
total limit during blocked submission. It requires the appropriate durable
timeout before releasing blocked code. The exact strengthened test passed all
four cases in 15.53 seconds. It asserts job status, checkpoint runtime, errors,
and timeout events without asserting private watch fields.

Two deliberately wrong source copies check sensitivity. Disabling checkpoint
rearming fails the caller case in 1.66 seconds because no checkpoint timeout is
recorded. Suppressing total scheduling while checkpoint expiry is paused fails
the total case in 2.02 seconds because no total timeout is recorded. Neither
mutation is part of the candidate.

Parent Repo records use the `sample-calculations-checkpoint-` prefix. They retain
commands, source hashes, logs, and JUnit results for each RED, GREEN, preservation,
and negative-control run. The source-only diagnosis is
`sample-checkpoint-report-diagnosis.md`. Independent source review found no
blocker in the correction or four-case instrumentation; formal stage review
and exact final-source verification remain required.

The measured comparison must exercise real checkpoint-heavy API work, include
terminal publication, and verify correctness before interpreting speed. The
same-source stability control must pass before comparing changed source.
The validated control and candidate comparison are recorded below.

## Combined revision 03

`sample-calculations-selected-files-03.json` freezes seven selected Python files,
with 184 other Python files unchanged from the reviewed base. Its SHA-256 is
`CE3D5852F0AA9A5707734A7168B3349BFF05DA32E93B317AFDB206C27BE6B88A`.
All 191 run-source hashes matched this complete selection before results were
interpreted.

The combined adjacent run finished with 124 passes and one failure in 302.72
seconds. The checkpoint-reporting regression, restart checks, keyword API,
ready-node overlap, pure sampling calculations, and repeated API rounds passed.
The hundred-request case failed A/57 after `model request completed`, at the
outer cancellation check after handler invocation returned.

That trace identifies when timeout state won, but cannot establish whether its
deadline became due before the user handler returned. The controlled regression
below isolates expiry during framework completion. Parent Repo
`network-completion-deadline-diagnosis.md` records the source investigation.

The formal checkpoint-report source review passed. Its Parent Repo report is
`checkpoint-report-correction-review.md`, SHA-256
`0E3991E92F0F56AEA960E5E56F4D82288892681184516E23B59B07A5D745C128`.
The new adjacent failure keeps broad acceptance outstanding.

## Handler completion

A real API handler finishes within its checkpoint interval, then waits inside
framework event flushing while the controlled clock advances. The corrected
success fixture failed on unchanged source in 3.48 seconds and passed with an
exit marker in 2.25 seconds. Adding an exceptional exit exposed a second failure,
one pass and one failure in 4.54 seconds. Recording exceptional exit made both
cases pass in 3.60 seconds. An earlier fixture wrongly expected an implicit
network checkpoint to be persisted; its result receives no behavioral credit.

The handler now publishes one revision and monotonic-time tuple before success
or exception flushing. The supervisor exempts only that revision's checkpoint
when exit is strictly before expiry. Total deadlines, equality, later
checkpoints, restart, and already-published timeout state remain authoritative.

The first preservation run crashed because fixture cleanup closed the mutation
writer's active SQLite connection during its final network snapshot. It has no
usable test result. Corrected cleanup closes transport, drains submitted writes,
waits for writer retirement, then closes only the test thread's connection.
No framework storage behavior changed.

The corrected selection passed ten controlled cases in 22.84 seconds. These
cover checkpoint reporting, successful and exceptional handler exits, caller
work, total expiry, exact expiry, and a later checkpoint. Three deliberately
wrong source variants each failed at the expected missing-timeout assertion,
with no unrelated errors. Suppressing total expiry failed in 7.74 seconds,
accepting equality in 6.96 seconds, and ignoring the revision in 12.10 seconds.

Parent Repo records use the `sample-calculations-completion-` prefix. Independent
Sol reviews in `handler-exit-deadline-review.md` and
`handler-exit-compatibility-review.md` accept the source and focused evidence.
Broader acceptance remains outstanding.

## Combined revision 04 and measured control

`sample-calculations-selected-files-04.json` freezes nine selected Python files
and verifies 182 other Python files against the earlier reviewed selection.
Its SHA-256 is
`440C8E9C8C508A56EC632A21F35039AE64DEFCD20ACE67FCF35E10BB1A49C700`.
The broader adjacent run passed 143 cases and failed the hundred-request
networking case in 369.49 seconds. A/84 expired before external-wait admission.
The trace cannot distinguish caller time from framework lock delay.

The fixed checkpoint workload uses 128 API jobs and eight reports per job,
with one excluded warmup and four measured fresh-project waves per process.
Six same-source processes passed all correctness checks. Their B/A median ratio
was 0.8938454082428701, within the declared reciprocal 1.20 bounds. Independent
validation binds every result to the source recorded before measurement.

Parent Repo `checkpoint-reporting-control-validation-02.json` has SHA-256
`CA739FFC16C32615C603F282B2EA709F5067CB1D0D88390E25653E6AA9572B93`.
At that review point, the candidate comparison remained pending. The completed
comparison below retains this control, workload, and limit and uses revision 06.

## Network admission

A further controlled real-API regression gates external-wait condition entry
and advances the supervisor clock before admission. On revision 04 it recorded
a checkpoint timeout and failed in 2.07 seconds. A distinct revision and entry
time, published before condition acquisition, made it pass in 2.12 seconds.
The supervisor consults this entry only for the matching checkpoint deadline
and only when entry was strictly before expiry. Total deadlines remain active.

Five admission cases cover framework delay, caller delay, total expiry, exact
expiry, and a later checkpoint. Together with the ten earlier controlled cases,
they passed 15 cases in 13.13 seconds. These records use the Parent Repo
`sample-calculations-entry-` prefix. Three negative controls failed at the
expected missing-timeout assertion. Total suppression failed in 8.34 seconds,
equality in 4.33 seconds, and omitted revision matching in 10.64 seconds.
Independent Sol source and compatibility reviews passed in
`network-entry-correction-review.md` and `network-entry-compatibility-review.md`.
The controlled failure establishes the lock-boundary defect;
it does not establish which delay caused A/84.

The final selection below includes this further source change. The measured
comparison uses that selection.

The hundred-request fixture now holds all 100 physical responses open before
advancing only supervisor time by 0.08 seconds. It requires a completed deadline
scan and zero timeout events while responses remain blocked, then checks 100
completed jobs and exact job-specific outputs. The checkpoint, total, and
transport limits remain 0.02, 3.0, and 0.5 seconds. An initial fixture passed
in 17.87 seconds, but its deliberately broken variant stopped at the scan guard.
The refined fixture observes durable timeout events while awaiting the scan,
so a supervisor that cancels every watch cannot exit without a useful failure.
The corrected negative control failed on A/1's checkpoint-timeout event in
15.30 seconds. The final sixteen-case preservation run passed in 23.44 seconds.
This tests cascade prevention without requiring every caller
to reach transport within 20 milliseconds on the host. Caller expiry remains
covered by the separate controlled cases.

## Revision 06 verification and subsequent test corrections

The measured runtime candidate was `sample-calculations-revision-06`. Its
`sample-calculations-selected-files-06.json` manifest has SHA-256
`43F102DA842C390A6E645A02B9A4DAF7EC7B4D7584BDBBD04C60FA6AE95E3A33`.
It contains nine selected Python files and 182 unchanged Python files.
Revision 05 was an intermediate freeze before the stronger timeout observation;
it has no broad acceptance result.

The combined adjacent check passed all 149 cases in 253.88 seconds, with no
failures, errors, or skips. All 191 run-source Python hashes match this selection.
The reviewed measured-comparison controller binds revision 06 and revalidates the retained same-source
control against the current environment before launching any measurement.
The candidate comparison passed all thirty warmup and measured correctness
checks across six fresh processes. Baseline totals were 2.2612601, 2.0904362,
and 2.0327058 seconds. Candidate totals were 2.4295728, 2.2237176, and 2.3904050
seconds. Their medians were 2.0904362 and 2.3904050 seconds, respectively.
The ratio was 1.1434957928862448, within the fixed 1.20 limit. The candidate is
about 14.35 percent slower in this measured workload.

Parent Repo `checkpoint-reporting-continuation-plan-01.json` has SHA-256
`5ED7F84EDFE605152E25280059AE1A45AE35325F0C0725CE7CF50584E8787FE2`.
`checkpoint-reporting-continuation-01.json` has SHA-256
`9ACEA9D402EDA03FF1CED71B57248A91C65D423A8CDC813B05E351BA0A2CF38A`.
The original control and all source, environment, correctness, and metric
checks passed again. Independent result review passed in
`sample-retained-concurrency-performance-review.md`, SHA-256
`43E8A995C25D9BE495C84805D6F5B47AA70D0CD44A1095EFC82D7BFE598A4BF2`.
Revision 06's ordinary run passed 643 cases and failed two in 402.83 seconds,
with one stress case deselected. The central-watchdog test exceeded its
half-second startup guard and leaked a worker exception into the following
router-schema test. The second failure was that exception, with no failed
schema assertion.

A controlled 0.6-second startup delay reproduced the first failure while
reliable cleanup allowed the schema companion to pass. The final test freezes
only supervisor time, retains the one-second checkpoint interval, and uses real
ten-second coordination guards. It captures worker exceptions and checks exact
results, durable outputs, and four completed jobs before closing the caller's
connection. Both startup variants and their adjacent checks passed 13 cases
in 8.71 seconds. Independent review passed in Parent Repo
`central-watchdog-test-review.md`, SHA-256
`0CC3AC1719DEA980D3A66D7F3F183E0FB3522F307C376370427BD1288AF72FF0`.
This changes no runtime code. At that point, a new combined selection and
ordinary run remained required, along with cyclic and selected stress checks.

The first repeated-round timing program stopped in its initial warmup because
the validator omitted the expected `output_written` event. Cleanup passed, but
no timing result was accepted. Its programs, plan, and result remain preserved.
The corrected program then passed all fifteen warmup and measured correctness
checks in three fresh processes. The early median was 1.7537655 seconds; the
late median was 1.9061631 seconds, within the unchanged 6.2612965-second
allowance. Each per-project comparison also passed. This supports the declared
timing rule and does not promise that every later round is faster.

Parent Repo `repeated-api-rounds-plan-02.json` has SHA-256
`1597137ED9ABBA6F55784987150C99FFFB3BC2E6FB8EA594279FE14A5C3086F2`;
`repeated-api-rounds-result-02.json` has SHA-256
`C7EB6ECEB48C0217DB638ACA707648C14CFE18AE161982AFDBB7A9F74033F174`.
Independent result review passed in
`sample-retained-concurrency-performance-review.md`, SHA-256
`BA0AB947C4CB4C976998DA79689AE90AFC2603FA47FCCF9E90A552CC93330189`.
The reusable benchmark and the ordinary test's deterministic cleanup and
durable-state refinement are tracked in
[repeated API verification](stage-repeated-api-verification.md).

## Revision 08 verification

The final source is `sample-calculations-revision-08`, with manifest SHA-256
`FE280BB644A4DCB725BEC8C395EF1EDF3C5EFD676FA74CC6B79770A68A8D10DB`.
All 132 runtime Python files are identical to the measured revision 06. The
checkpoint comparison therefore applies to the final runtime bytes. It passed
the fixed 1.20 limit while measuring a 14.35 percent slowdown, not a speed gain.

The final ordinary suite passed 660 tests in 440.41 seconds, with one stress
case deselected. The selected stress check passed in 7.04 seconds. Both run
manifests match all 204 mapped package, test, and benchmark Python files.
Four fresh source-07 cyclic checks
passed in 3.81, 6.98, 6.75, and 3.10 seconds; their runtime and cyclic-test bytes
are unchanged in source 08. Exact commands, run-manifest hashes, and the
unchanged-source boundary are recorded in
[sampling calculations](stage-sample-calculations.md#final-verification-and-review-boundary).

The reusable source-07 repeated-API measurement also passed, and its measured
functions are unchanged in source 08. The independent result and compatibility
review is Parent Repo `reusable-api-benchmark-review.md`, SHA-256
`2AB98ACD4C5E6650C17494DC156E6B89BCA969EF8DFFAD6D3F5F5ADA789CC58E`.
The subsequent final review found the initial-arming defects below. This section covers retained
timeout behavior and its tests; it assigns no sampling integration or release
acceptance.

## Final review finding RDL-001

The independent final review found two mechanisms. RDL-001a is that
`begin_handler_execution()` samples
the initial arming time before acquiring the supervisor condition. Both total
and checkpoint deadlines use that stale sample after acquisition. Framework
lock delay can therefore install an already-expired deadline before user work.
The method's existing boundary excludes framework pre-invocation work, so this
is an implementation defect rather than an architectural question.

Two framework-delay cases and two caller-time preservation cases are prepared
at the real API task boundary for initial checkpoint and total deadlines.
They freeze supervisor clocks, gate initial condition acquisition, and observe
durable timeout events and outputs. The results and correction are recorded below.
Correction, broader verification, measurement applicability, and independent
re-review remain required. The passing revision-08 records remain preserved
within their exact source and coverage limits.

RDL-001b concerns provisional initial deadlines. The registered active watch
receives provisional total and checkpoint fields before initial runtime
persistence completes. A concurrent task can cross the ordinary deadline-heap
compaction threshold while that write is blocked. Rebuilding the heap reads
every active watch and can schedule those provisional fields at the current
revision, exposing an initial timeout before handler invocation. This is a
source-supported reachable race; its controlled failure and correction are recorded below.
It required a separate persistence-gated regression exercising real heap
compaction. Shared watch deadlines must remain unset until explicit arming;
the initial runtime observation can receive its configured remaining limits
as explicit values instead.

The final retained review is Parent Repo
`retained-deadlines-final-stage-review.md`, SHA-256
`5F6008170015BA598D7B3598D14A55A52D8B414BFB5DD602612E6F1EFFF2B10C`.
It blocks acceptance on both initial-arming mechanisms and accepts the other
reviewed source and evidence only within their recorded boundaries.

## Initial-arming corrections

The condition-delay RED produced the expected checkpoint and total timeout
failures while both caller-time preservation cases passed, two failures and
two passes in 11.40 seconds. Moving the in-memory scheduler start and arming
time inside the owned condition made all four pass in 4.04 seconds.

The separate persistence RED uses two real API tasks. Task A blocks in initial
runtime submission. Task B makes 300 ordinary checkpoint reports while the
watchdog is paused, crossing the unchanged heap-compaction threshold. The
fixture observes a real heap rebuild, releases the scan, and requires no
durable A timeout before its handler enters. Both checkpoint and total variants
failed on their premature timeout events in 6.97 seconds. The strengthened
RED also preserved the initial runtime payload's configured limits and
nonempty deadline fields; both cases still failed in 13.81 seconds.

Registered deadline fields now stay unset throughout initial submission.
The runtime payload receives explicit configured remaining values for total
and checkpoint limits. The shared watch is armed only afterward, under the
owned condition, using a fresh clock sample. Caller expiry and asynchronous
API versus synchronous ordinary persistence remain unchanged.

The combined six cases passed in 6.96 seconds on
`sample-calculations-initial-persistence-green-01`. Parent records use the
`sample-calculations-initial-` prefix. The revision-04 test runner additionally
records the 86 example Python files before launch, alongside the 204 package,
test, and benchmark files. Broader checks, correction review, a final freeze,
and the revised measurement assessment remained pending at that freeze.

The full watchdog-networking, checkpoint-keyword, framework-improvement, and
active-restart selection then passed 85 cases in 134.87 seconds. Its manifest
is `sample-calculations-initial-preservation-01-run-manifest.json`, SHA-256
`A61BB5BF279503E17753ED2DAE533D440E7D4F51E19523C4F3DD1A2EEF33B776`.
Both independent Sol reviewers accepted RDL-001a and RDL-001b within their
controlled boundaries but found another pre-handler ordering defect.

## Initial heap maintenance

RDL-001c occurs after the fresh initial clock sample. Full heap rebuilding and
possible supervisor-thread startup still ran before user code. The new real-API
case keeps forty tasks active, submits seventy checkpoint reports, then
completes those tasks while the watchdog is paused. Their stale entries cross
the unchanged rebuild threshold when the next task begins. The fixture gates
the actual heap rebuild, advances only the supervisor clock, and observes
durable results after a real deadline scan. Caller-time controls advance the
same clock after handler entry.

Both framework cases failed on the expected checkpoint or total timeout;
both caller controls passed, two failures and two passes in 29.73 seconds.
Heap maintenance and possible thread startup now precede the final initial
clock sample. Required deadline publication remains after it. The ten initial
condition, persistence, and maintenance cases passed in 25.04 seconds on
`sample-calculations-initial-maintenance-green-01`.

The rearm review also found post-sample heap maintenance in checkpoint return,
outermost network completion, and physical-attempt lease renewal. Renewal also
samples before condition acquisition. These require separate sensitive
regressions and corrections. No existing total-time rule is relaxed.

## Checkpoint and network-return maintenance

RDL-002a uses the same real heap preparation with A already inside its handler.
A starts a checkpoint while forty other attempts keep the rebuild threshold
high. Its runtime submission is gated while those attempts complete. Only the
post-submission rearm then rebuilds the heap. The framework-checkpoint case
failed on the expected durable timeout, while six initial and timing controls
passed, one failure and six passes in 36.10 seconds.

Checkpoint rearm now finishes heap maintenance and possible thread recovery
before sampling its new interval. The earlier initial and checkpoint cases
passed together, 17 passes in 41.35 seconds. The caller-time control still
expires, and the total timer still expires during framework maintenance.
The independent Sol report `initial-checkpoint-maintenance-review.md`, SHA-256
`76D4CEA1B14FE8B5FE46B8C299778F192813FEC0374343BB38A5B3A7C14BC78B`,
accepts RDL-001c and RDL-002a within these focused boundaries.

RDL-002b holds a physical response from the real shared transport before
preparing stale heap entries. Releasing the response triggers the actual
outermost network-return rebuild before caller code resumes. Its framework
case failed on a premature checkpoint timeout; caller-checkpoint and
framework-total controls passed, one failure and two passes in 18.27 seconds.
The correction moves maintenance before the renewed checkpoint sample while
preserving nested waits and active total time. Seventeen selected maintenance,
external-wait, and framework-network cases passed in 136.24 seconds. Independent
Sol review accepts that focused boundary in
`network-end-and-replay-renewal-review.md`, SHA-256
`20B9388AF6B34029CE5C1DCF9383ACB87D47938EB177A4C421CD4D36D1D615B2`.

The renewal preparation RED covers initial and replayed physical requests.
Four framework cases produced the expected premature external expiry.
Four preservation controls also failed because network cleanup changed timeout
metadata before persistence. The run recorded eight failures in 199.98 seconds.
Those control failures are an additional implementation defect, not passing
preservation evidence. The separate observation regression and correction are
recorded below.

## Timeout observations during network cleanup

The observation regression gates timeout persistence after the real supervisor
decision, then lets physical-response cleanup finish before publication. Four
external/total and initial/replayed-request cases failed on the prior source in
25.21 seconds. Timeout limits and checkpoint names had changed after the
decision. These are observation failures, separate from incorrect expiry.

The supervisor now retains a shallow watch copy under its condition after
deciding the timeout and before signalling cancellation. Both timeout publication
and the later attempt-finish runtime write use that observation. Formatting,
SQLite writes, and error handling remain outside the condition. All four cases
passed in 5.82 seconds. Publication timestamps retain their existing meaning.

Independent Sol source and compatibility reviews pass this focused boundary
in Parent Repo `timeout-publication-snapshot-review.md`, SHA-256
`F2FA81665A236091A999BCACC9970541DB51FA96263E85E0C4B4858DC44F8F45`,
and `timeout-observation-compatibility-review.md`, SHA-256
`6273693F5076F074315312D8644CA0FC4A2B743C90D76DE7F14EE78E42CE68F6`.
Both identify the remaining timeout-event race between its current-execution
precheck and the later event insert. That race still requires a sensitive
regression, a correction, and preservation of legitimate timeout events.

## Physical-attempt renewal preparation

With observations corrected but renewal ordering unchanged, the eight renewal
cases produced four expected framework-delay failures and four preservation
passes in 61.46 seconds. Renewal now acquires the condition, validates state,
updates physical-attempt observations, and completes heap maintenance and
possible supervisor-thread setup before sampling the new lease start.
The configured lease length and active total deadline are unchanged.

All eighteen initial, checkpoint, network-return, and renewal maintenance cases,
plus five related checks, passed in 131.09 seconds on
`sample-calculations-renewal-preparation-green-01`. Its run manifest has SHA-256
`CB7C8EF60390874BACAF8AFE565323E9BC8652A66ADA5D969BDAB41D78FBFDCE`.

A separate real replay exposed the old lease expiring while a physical-attempt
callback that entered before expiry waited for the condition. The prior source
produced one expected failure and four preservation passes in 5.33 seconds.
Publishing a revision/time entry before condition acquisition made all five
pass in 5.46 seconds. Three single-change negative controls each lost its
required timeout: total-kind broadening in 4.14 seconds, equality acceptance
in 2.02 seconds, and missing revision matching in 2.15 seconds.

Independent review accepts the fresh sample and maintenance correction but
finds that the general revision is too broad for the replay marker. A supported
heartbeat checkpoint can advance that revision while retaining the same old
external deadline. The marker then stops protecting already-entered retry
preparation. The real heartbeat regression reproduced that premature expiry,
one failure and five preservation passes in 37.29 seconds.

The Parent Repo review is `replay-renewal-correction-review.md`, SHA-256
`56B7392C0939ED4B37BBF28B80186712578A75D3F55043BDEC912B37D007584D`.
Its focused passes do not establish final combined acceptance.

The corrected marker records the prior external deadline and entry time.
Heartbeat checkpoints retain that lease identity. Successful renewal clears
the marker before its new clock sample, including when old and new numeric
deadlines coincide. The final eight phases passed in 7.93 seconds on
`sample-calculations-replay-heartbeat-green-02`. Its run manifest has SHA-256
`9E3696BDC1428414C02CC7AEE674664C2468D23988041FD21E393FA4FE2CE22A`.
Three new single-change controls failed their required public timeout
assertions: missing clearing in 6.24 seconds, accepted equality in 2.84 seconds,
and total-kind broadening with coincident deadlines in 3.33 seconds. Independent
focused review passes in Parent Repo `replay-renewal-final-correction-review.md`,
SHA-256 `04D6B0A98133A438F44CC853489CF506B08D2F35AC7A86FFBF835BA2932D0A32`.

## Timeout-event execution ownership

A real API caller can observe cancellation and terminalize while the supervisor
is still publishing its timeout. Pausing before the active-execution precheck
lost the legitimate event, while pausing just before insertion preserved it,
one failure and one pass in 4.20 seconds. Removing the non-atomic precheck made
both orderings and the four observation cases pass, six passes in 6.79 seconds.

A separate direct-runner case pauses the old timeout insert, requests a real
active-job restart, and lets the original controller complete generation one.
The replacement's exact output/runtime and the stale handler's rejected write
passed, but the old timeout event was inserted. That RED produced one failure
and two same-execution preservation passes in 4.39 seconds.

Timeout appends now carry generation and execution ID into the SQLite mutation.
An explicit internal timeout-only option also permits the same generation and
execution ID in valid terminal status data, matching the existing runtime rule.
Ordinary events retain active-execution checks. Stale timeout rejection returns
quietly; other errors retain existing handling and the controller is still
woken. The common active-event query is unchanged.

Four storage cases rejected valid terminal-owner timeouts on the old event
store in 2.65 seconds. The corrected source passed thirteen focused cases in
19.27 seconds, covering the three real races, four observations, four terminal
statuses, and two existing asynchronous event checks. The frozen source is
`sample-calculations-timeout-fence-green-02`, with run-manifest SHA-256
`52368E71FA8573CD690014166E32C30CDCC5AD9EF9090B197E8AEA9CC2FB1232`.

Single-change controls each failed on the intended durable event assertion:
omitted execution identifiers in 5.67 seconds, disabled terminal ownership in
5.05 seconds, and restored active-only precheck in 4.68 seconds. All coordination
and replacement-state checks passed. The surrounding eight-file check passed
153 cases in 303.42 seconds. Its run manifest has SHA-256
`5A871AB6257CEDE6559B50DB272761F7CA9E40DA15846F769C1AE1B473BEF3C5`.
Source review identified another correctness race: a delayed timeout runtime
can replace an observation already published by a later retry or fallback
under the same execution. Its earlier timeout event remains legitimate history.
The subsequent regression and correction are recorded below.

## Delayed timeout runtime ordering

The real API regression paused the first attempt's timeout publisher while a
retry or fallback completed and stored its own named checkpoint. Both cases
failed only when the released old publisher replaced that later runtime. The
earlier timeout event was correctly retained. The initial RED recorded two
failures and 64 deselections in 8.41 seconds, under
`sample-calculations-timeout-later-attempt-red-01-run`.

The correction assigns a private observation order across one invocation's
main task, retries, repetitions, and fallbacks. Both queued-slot replacement
and writer-batch reduction retain the later watch. The writer then reads the
stored watch and refuses an older observation inside the same SQLite
transaction. Registering or attempting a later write does not retire the
earlier stored observation. Failed writes therefore leave that earlier watch
eligible to record its timeout. Runtime JSON and the durable execution fence
retain their existing formats and roles.

The first corrected selection passed five cases in 4.50 seconds. Its selector
also named the passive-fallback case, but that case lives in another module
and was not selected. The same limitation applies to the later eight- and
17-case focused selections. Passive-fallback preservation must come from the
surrounding `test_framework_improvements.py` run.

Focused storage checks then exercised queued replacement, same-writer batch
reduction, and a higher-priority latest timeout while the middle observation
had never reached SQLite. The corrected selection passed eight cases in 4.77
seconds. The first batch-reduction control was insensitive because two
`FileStorage` objects have separate writers. The corrected test queues a
separate slot through the synchronous `write_job_runtime` branch in the same
writer. A test-only wrapper makes its grouped submission asynchronous so that
the slot can join the batch.
The original insensitive result remains recorded and supplies no sensitivity
credit.

| Disabled correction | Result | Run manifest SHA-256 |
| --- | --- | --- |
| Later-watch choice in the asynchronous slot | Expected runtime mismatch, 1 failure in 0.83 s | `02AEDA45DE8352BE1ACF6812BF0AB3CB8AC4812029A119AE09E2E86B76B6D2B7` |
| Later-watch choice in the same-writer batch | Expected runtime mismatch, 1 failure in 0.97 s | `B6FE4529423F9AA8F4C2EB93F0CA86C61F67A944ED953BC68EB27E665C769B62` |
| Transactional stored-watch comparison | Retry and fallback runtime mismatches, 2 failures in 1.87 s | `3F722450754FBFE8F2BD38536000A722D292D91B56308B99117589DB785B34A0` |

Additional preservation cases exercise serialization failure, a real SQLite
trigger rejection, and missing, non-object, or malformed terminal-owner JSON.
They require unchanged earlier runtime after failure, successful later runtime
publication, refusal of obsolete updates, unchanged damaged job rows, and no
new timeout event on damaged ownership data. The final focused selection
passed 17 cases with 72 deselections in 7.71 seconds. Its manifest SHA-256 is
`9FBB39A34431E2E334D1F477F26A19FA43740AA9E867A812233B9483DE27439F`.

The final source is
`sample-calculations-timeout-runtime-order-preservation-01`; its 290-file
freeze has SHA-256
`796182D47FA5E2F20ACA0241D6DBA7ECE6C1CD9D880C53608DF0B3C719D995B9`.
The five corrected runtime files match the earlier design-review freeze:

| File | SHA-256 |
| --- | --- |
| `storage/runtime_observations.py` | `5F02C072831FCC61E833D89C64186644CF41535C328684A4968C991A1E1CE829` |
| `workflow/supervisor_watch.py` | `C997F450B50EC3596F2ED373DF91AA8D1DD241697EEC94DC8D16E3683E924D0A` |
| `workflow/supervisor_attempts.py` | `87EB9D32C14E96092E00692C38E53E1F73BE50A445BF045DD8C878D1E59C907E` |
| `workflow/supervisor_persistence.py` | `699B03A0C08573963FFF0E3EABDF170132235FA994BDF9C8DFD2BD392875B2F1` |
| `workflow/task_execution.py` | `3E306B0CE2FDC5FA3F0D66C74137936EA4AAFBB88E3601E775FD98E1F99C15BF` |
| `tests/test_043_watchdog_networking.py` | `E3361996AF26C04C9E7999BCCF67CE66CE3E2779D21CF374E898D661F603A6FF` |
| `tests/test_047_event_state_top.py` | `A9D8C56F132941705FBAA94357BAFF5A4CE0CE702B0389A672F1C96059433435` |

The independent [design review](../../../../testing_ground/issue-45/timeout-runtime-order-design-review.md)
found the correction provisionally sound. Its SHA-256 is
`19097B5701D50D3706157DB4F66FE2B6F7860685A6E194F09DF8215FCC277BBE`.
Its passive-fallback coverage claim is corrected above. The first surrounding
command used a nonexistent checkpoint-test filename and ran no tests; the
corrected command is `sample-calculations-timeout-runtime-order-adjacent-02-run`.
That command passed 169 tests in 247.98 seconds. It includes the passive
fallback test and all six storage-contention cases, including repeated API
rounds. Its manifest SHA-256 is
`1EFA2A9ABEC17FA490E338227F8E1506DF1A17412B81B2BAE73D996091540FDE`.

The [final correction review](../../../../testing_ground/issue-45/timeout-runtime-order-final-correction-review.md)
passed this runtime-ordering boundary and resolved the timeout-event review's
same-execution blocker. Its SHA-256 is
`67C041513898E2B57B042B88EA91D97E02D1DC72C88CC5A2CF8DBEA51D500E4D`.
At that point, final combined-source ordinary, cyclic, and state-preservation
checks remained pending. They are completed below. This does not accept the
whole workflow-management implementation.

## Persisted observation boundary

The initial SQLite runtime row remains a pre-arm observation. Its displayed
start and projected deadline can precede live watchdog arming after framework
delay. Later checkpoint submission has the same observation boundary. API
fibers submit asynchronously; ordinary runners retain synchronous submission.
The scheduler enforces its live monotonic deadlines. Inspection displays the
persisted fields, and doctor can warn about an overdue persisted checkpoint
while directing the operator to check the independent scheduler heartbeat.
The correction does not promise atomic equality between these observations.

The new runtime hashes invalidate automatic reuse of the earlier checkpoint
comparison and repeated-API timing. Christopher's subsequent
[performance instruction](requirements-audit.md#performance-priority) prioritizes
correctness and completion; fresh timing comparisons are no longer required
solely to gate modest costs. The earlier 14.35-percent measured slowdown remains
a historical result, not a measurement of the changed runtime. Functional
repeated-run, ordinary, cyclic, and state-integrity checks were still required
at that point. They are completed below. Historical results retain their
original source boundaries.

## Combined functional selection

`sample-calculations-combined-functionality-01` contains 298 Python files. Its
freeze record has SHA-256
`F688AC7E0B395AAC607D17B411145C543E63852D7E5ADD6DE6198E12E30412FF`.
The preparation verified all 290 prior runtime-export hashes, then copied the
eleven reviewed ownership files, the readiness calculation and its test, and
the four reviewed deprecation files after checking their exact expected hashes.
The source includes the pure sampling module and retained deadline corrections.
Unfinished preview changes and the newly prepared component-state regression
are absent from this selection.

The ordinary suite passed 884 cases with one marked stress case deselected in
581.88 seconds. Four fresh-process cyclic cases each passed in 3.24, 7.33,
7.60, and 3.22 seconds. The separately selected marked stress case passed in
6.84 seconds. Every run manifest contains the exact same 298 source hashes;
all JUnit files report zero failures, errors, and skips, and all native child
processes exited zero and drained.

The independent
[combined functionality acceptance review](../../../../testing_ground/issue-45/combined-functionality-acceptance-review.md)
has SHA-256
`1BD24E4345935161A89D9BCAB55974CB08E317F6B815E55D397632FF56367C2F`.
It independently rehashed all 298 files and matched every result manifest to
that source. It found no remaining functional blocker in this retained-runtime
boundary. The ordinary JUnit includes the 71 watchdog/network cases, 18 event
state cases, six SQLite contention and repeated-use cases, passive fallback,
restart isolation, delayed timeout publication across retry and fallback, and
the formerly failing ghost-visibility case.

This selection provides the shared functional boundary for the reviewed
sections. It does not activate pending session, sampling, readiness, or
component-lifecycle integration. The newer 300-file component-state work and
`tests/test_075_component_state_storage.py` are excluded. Historical timing
failures retain their original results under the performance-priority
disposition above. This acceptance does not cover the whole issue, release, or
final Astra review.
