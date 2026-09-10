# Workflow and clipboard decisions approved on 2026-09-08

These decisions supplement [Implement and verify the agreed MWF 0.6.2 workflow-management changes](https://github.com/Christopher8187/product/issues/45). Christopher approved monitoring questions Q1, Q2, and Q4 with "I agree with Q1,2,4", then Q5 through Q7 with "I agree with 5,6,7", and explicitly authorized their relay to the implementation task. His Q3 clarification confirms the original behavioral discussion's Q32.

The identifiers below distinguish this round from AQ1 through AQ6 and Q7 through Q10 in [the architectural decisions](architectural-questions.md). Approval settles behavior. Implementation, verification, and review remain required. The single native model and Q10's fixed membership during admitted execution remain in force.

## MQ20260908-1: Resume retains the established sampled origin

If aligned component I has a sampled result originating from interrupt session I1, `resume I --interrupt` creates a fresh execution session I2 while preserving completed work and origin I1. Incomplete predecessors alone do not require a fresh run. Do not relabel retained work to I2. Existing misalignment and compatibility refusals still apply. This resolves the interaction between sampled-resume preservation and the fresh-interrupt origin rule.

## MQ20260908-2: A later ordinary command can cross an earlier fence

A later user-invoked command such as `run D` may authorize its selected progression through an earlier interrupt result without another `--interrupt` flag. Normal readiness, stability, ownership, and selection rules still apply. An older session cannot continue automatically, use an incomplete sampled parent, or bypass incompatible results.

## MQ20260908-3: Ordinary stopping follows reachability and readiness

For A → I → M and A → B → M, choosing I=stop prevents I from executing in that command. B may continue, and M may remain reachable through B. M still requires every normal prerequisite. An unfinished or unusable I blocks M. A retained I result is usable only while valid under preparation, input-change, and stability rules. Do not block every descendant solely because I was stopped. Explicit partial interruption retains its separately specified fence.

Christopher described the outer scheduling behavior as already handling this case. The earlier recommendation to block every descendant was withdrawn and was not approved.

## MQ20260908-4: Paste preserves completed work and supporting history

Pasting a saved node with 80 done jobs and 20 queued jobs retains those completed jobs. The native saved format must carry and validate the history supporting their results. Copying done labels alone is insufficient. Legacy compatibility is not authorized.

## MQ20260908-5: Clipboard restoration stays node-scoped

If A and B share a component, `paste A` restores only A's saved jobs, results, history, and node content. Leave B's files and jobs untouched. Validate the combined component state using existing rules. If restored A cannot coexist consistently with current B, refuse before mutation and explain why.

## MQ20260908-6: Copy requires an idle component

`copy A` refuses while A's component has active work and explains that it must finish or stop first. Do not add automatic pausing for copy. Native recovery governs stale running state.

## MQ20260908-7: Paste stays within the originating project

A saved copy from project P can restore within P. Pasting that copy into project Q refuses before mutation. Cross-project transfer of saved result or ownership history is outside this clipboard scope.
