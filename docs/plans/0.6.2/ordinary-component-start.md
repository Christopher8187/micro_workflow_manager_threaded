# Guarded queued component start

This private storage increment adds `begin_queued_component_execution()`.
It changes one component from queued to running after checking the selected
running session, exact reservation, producing shape, alignment, and generation
inside the same writer transaction. It preserves every other stored value.
Repeated starts refuse because the component is no longer queued.

The missing-method check failed before implementation, then passed with reopen
and unchanged-row assertions. The surrounding run passed 194 tests in 63.79
seconds and failed four historical version-4 checks. Those checks depend on the
cancelled old-model default described in [one-model scope](single-model-scope.md).
They receive no pass credit. Earlier fixture errors also receive no behavioral
failure credit.

The final 25 focused checks passed in 5.90 seconds. They cover stored damage,
ownership and generation changes before submission, concurrent starts,
rollback after aborted or suppressed writes, and persistence. Production bytes were unchanged from
the surrounding run.

The final freeze is `sample-calculations-ordinary-component-start-rollback-01`,
SHA-256 `D9A607B44B75F0CEB1A3DF5F85EE94935646A733999DC030C934347CF9999BA8`.
The [independent review](../../../../testing_ground/issue-45/ordinary-component-start-review.md)
passed with GPT-5.6 Sol at xhigh reasoning. This private transition is accepted.
Terminal arbitration, consistent parent observations, and shared
public activation remain required.
