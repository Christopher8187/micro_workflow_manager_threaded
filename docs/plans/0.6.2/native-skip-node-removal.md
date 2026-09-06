# Remove the public component skip operation

Requirement `44-CMP-023` explicitly removes `MicroWorkflow.skip_node()`.
The method is deleted with its unused import. There is no alias or redirect.
Skipped jobs remain supported and may count as successful under the existing
job rules. Component lifecycle integration remains separate work.

The preservation control passed before the runtime edit. Two public calls,
covering a singleton and a communicating component, then failed because the
removed operation was still available. After deletion, all 58 focused and
surrounding cases passed in 38.19 seconds. They include the new controls,
programmatic execution, and command retirement.

The immutable source is `sample-calculations-native-skip-node-green-01`,
SHA-256 `5857340D4735E8EBD0206D318ACAA89BA128F76826840A15E784C5FE5CC8334A`.
Parent Repo records use the `sample-calculations-native-skip-node` prefix.
Earlier fixture errors receive no behavioral failure credit.

The [independent review](../../../../testing_ground/issue-45/native-skip-node-removal-review.md)
passed with GPT-5.6 Sol at xhigh reasoning. This removal is accepted.
Broader component state, waiting, cancellation, and raw-node
terminology changes are outside this removal.
