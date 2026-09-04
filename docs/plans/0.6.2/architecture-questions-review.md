# Architectural-question record review

Status: **PASS for this documentation and preparation section only.** This
review accepts neither runtime behavior nor an MWF stage or release. All three
questions remain for Christopher, and every affected requirement remains
pending.

Reviewer: GPT-5.6 Sol with xhigh reasoning. I read the final consolidated Issue
#44 resolution, the complete local **Find objective for issue #45** preparation
in `preparation-transcript.txt` and `preparation-later.txt`, the recovered
original Q99-Q111 messages, `preview-storage-review.md`, and
`state-migration-review.md`. The recovered selected-job, misalignment, and
interrupt answers do not answer any question in this record.

Reviewed document hashes:

- `architectural-questions.md`: `F00E356351E984AE58DE267CBC9239D9871F4D2B163F32DAC04FC593E99CF199`
- `requirements-audit.md`: `056ADB0F6658B49EE0564C18BCC178D015666A75197AC248DFE30087FD3871BD`

## Question review

| Question | Disposition | Reason |
| --- | --- | --- |
| AQ1 | Unanswered and correctly deferred | The final resolution forbids durable or externally visible mutation, but it does not say whether required changes to an already-existing SQLite shared-memory coordination file fall inside that rule. The experiments establish a real choice between that narrow allowance and reduced availability or a different storage design. `immutable=1` and unlocked live reads are correctly rejected. |
| AQ2 | Unanswered and correctly deferred | The resolution defines the new component lifecycle and successful-result fields. It gives no conversion table for mixed legacy raw-node values, and no rule creates missing historical sample or interrupt lineage. Refusal pending fresh preparation, a conservative conversion, and validated job-derived conversion are materially different behaviors. The document does not choose among them. |
| AQ3 | Unanswered and correctly deferred | The resolution specifies the exact sorted-member key and stored/current overlap behavior. It does not identify an authoritative historical autostart declaration. The current scanner recognizes literal `add(..., autostart=True)`, while supported routing forms can register relationships later. Static-only declarations, broader discovery, and a persisted synchronized shape assign different responsibilities and need Christopher's decision. |

## Ledger review

The 27 changed dispositions match the three blocked areas: seven AQ1 rows,
eleven AQ2 rows, and nine AQ3 rows. Each disposition names the blocked portion
and leaves independent work pending. None claims that the whole requirement is
unimplementable or accepted.

AQ2 and AQ3 are cumulative for general legacy component conversion. AQ2
controls how old results convert; AQ3 controls which exact component receives
them. The record expresses this across the two question sections while keeping
new-project state calculations and synthetic stored/current membership logic
available. Implementers must satisfy both answers before accepting the general
migration path.

The dependency boundaries are accurate:

- AQ1 blocks acceptance of live SQLite previews and the listed preview,
  recovery, sampling, regression, and documentation integrations. Pure graph
  and selection calculations remain independent.
- AQ2 blocks conversion of legacy result authority and lineage. The new
  component-state model can still be built and tested where no legacy result is
  assumed.
- AQ3 blocks general migration and graph-update integration that needs exact
  historical and current memberships. The sorted-member calculation itself is
  settled.

The record correctly leaves the SQLite session registry, liveness refusal
before legacy mutation, SQLite-only session writes, one-main and
several-interrupt representation, exact ownership of new job claims, and
non-guessing readers available for independent implementation. The settled
safe loading order also remains usable; AQ3 blocks supplying its general
membership input, not the requirement to perform conversion before router or
starter mutation.

No blocking omission, answered question presented as open, invented behavior,
or whole-release acceptance was found.
