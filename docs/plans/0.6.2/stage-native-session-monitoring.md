# Native session monitoring

Status: bounded source review passed; wider stage acceptance remains pending.
This portion implements `44-SES-041`; it completes no broader
session, component, preview, or control requirement.

`workflow_snapshot` and `top_snapshot` read all exact native session rows.
The returned `sessions` list replaces the old singleton fields. Both text views
show each ID, kind, command, parent, selected components and jobs, status,
terminal outcome, and recorded failures. Running elapsed time accepts native
timestamps with or without a timezone.

`top` also returns diagnostics by session ID. Process metrics require the same
host and a verified process instance. Foreign, terminal, reused-PID, and
missing-identity sessions receive unavailable metrics. This does not change
native liveness rules for a nullable identity. The top-level writer field is
explicitly the observer process. Another local process's saved writer sample
requires matching host, PID, and process identity and a finite timestamp.
Writer samples now record those identity fields. Recorded API settings come
from each session's native details.

## Verification

Root alone ran executable work from immutable Test Area copies. Records use
the `sample-calculations-native-monitor-` prefix under Parent Repo
`testing_ground/issue-45/`. Runner revision 04 was used throughout. Freezers
11 and 12 extend the existing verified copy procedure; revision 12 adds the
writer source to the permitted overlays and has SHA-256
`D61E54D76FE3F28E45C88870493ADC145885EB7E8C5EEF8BEF725DE851EB2F02`.

| Run | Result | Observation |
| --- | --- | --- |
| `sessions-red-01` | 3 failed | Both views still used old run state; native per-session diagnostics were absent. |
| `sessions-green-01` | 3 passed in 2.12 seconds | Exact native lists, rendered identities, and foreign-host refusal. |
| `diagnostics-red-01` | 3 failed, 6 passed | Three fixture failures, missing required arguments and Windows launcher PID assumptions, carry no behavioral credit. |
| `diagnostics-red-02` | 3 failed | Corrected cases expose timezone rendering errors and unavailable child-process writer attribution. |
| `diagnostics-green-01` | 9 passed in 4.41 seconds | Timezones and verified child writer attribution corrected. |
| `observations-red-01` | 2 failed | Invalid saved timestamp accepted; recorded API settings absent from text. |
| `observations-green-01` | 10 passed in 4.27 seconds | Both corrections pass. |
| `adjacent-01` | 81 passed, 1 failed in 78.79 seconds | New test wrongly required a saved writer sample to reflect later drain completion. |

The writer publishes timestamped samples at a bounded frequency. An idle child
can therefore retain an earlier sample with nonzero backlog. The corrected test
compares the observed saved values and timestamp, checks their age, and checks
the committed queued job. Runtime code was unchanged for this refinement.
The corrected `observations-green-02` freeze has 313 Python files and SHA-256
`68C12DD19B0E8EFA121EC99BDFC1D265CA1A29D9E673C3580157D4534B142943`.
Its adjacent run selects 23 ordinary-main, 47 programmatic, 10 monitoring, and
two existing CLI top cases. All 82 passed in 79.30 seconds in `adjacent-02`.

## Remaining work

The independent GPT-5.6 Sol xhigh
[source review](../../../../testing_ground/issue-45/native-session-monitoring-source-review.md)
passes this bounded monitoring portion. Root read it in full, then read its
corrected description of the first foreign-session RED. The final report's
SHA-256 is `83E2A6385C312D2A70A4D0295BC967F05A2D8D230F3534A99CF1D0CF7C476B54`.
That test intercepted a local probe of `None` through empty old state,
establishing absent native per-session diagnostics.
The reviewer verified all 313 source hashes against the completed adjacent run.
The shared saved writer file holds
one process's latest observation; another session receives unavailable metrics
when that sample belongs elsewhere. No session ownership is inferred from it.
Component lifecycle display, readiness, projectwide capacity across sessions,
restart, recovery, threads, native read-only preview guarantees, and retirement
of the remaining old readers still require implementation and verification.
Q10's dynamic membership decision remains unanswered.
