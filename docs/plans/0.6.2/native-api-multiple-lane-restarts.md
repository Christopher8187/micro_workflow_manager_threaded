# Multiple API lanes and accepted restarts

Status: focused check, 310-case surrounding selection, and [bounded review](../../../../testing_ground/issue-45/native-api-multilane-restart-source-review.md) passed.
This extends [mixed runner continuation](native-mixed-runner-continuation.md).

The [preparation](../../../../testing_ground/issue-45/native-api-multilane-restart-preparation.md)
distinguishes configured concurrency from physical API controller threads.
The test uses two supported startup lanes with six execution slots and seven
queued jobs. Two barriers establish a completed peer and two started failures
before either failure stops ordinary admission.

The two pumps release three unstarted claims in total. Public cancellation
and one CLI command then restart all three before session exit. Each successor
runs once with its original incarnation and owning session. The completed peer
retains its exact output and events. The seventh job remains queued and unowned;
the two original failures remain failed. The session terminates failed and
releases its reservation after the replacements finish.

The immutable `sample-calculations-native-api-two-lane-restarts-01` source passed
the case in 2.71 seconds without another runtime change. Its freeze SHA-256 is
`9FB43CA78087DB11E9F66E56F38C1AE6B9AEACF3D102FCD0BBF05A8B589D6260`.
This adds one observed multi-lane ordering. Aggregate API capacity, arbitrary
interleavings, interrupts, recovery, and Q10 remain unfinished.
