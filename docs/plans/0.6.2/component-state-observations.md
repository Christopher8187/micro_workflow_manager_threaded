# Consistent component-state observations

`read_component_states()` reads requested records from one SQLite snapshot.
It retains order, validates each
record, and requires the expected producing shape. Missing or damaged records
refuse the whole read. Its savepoint closes on success or failure without
committing or rolling back a caller transaction.

The missing-method check failed before implementation.
A concurrent-change regression then observed one old component and one new
component from one atomic writer update. The savepoint made both
observations use the earlier snapshot. Cleanup checks cover successful
reads, invalid state, missing components, wrong shapes, and existing transactions.

The selection passed 189 cases in 39.27 seconds with one cancelled
version-4 case deselected. It combines ten focused checks with readiness,
native component storage, queued activation, and terminal settlement checks.
Freeze `sample-calculations-component-observations-cleanup-01` has SHA-256
`55A7FF1D119E1A0447AB9C4444CBC89AD064941E26E43F438A6BEBF437C9692A`.

[Independent review](../../../../testing_ground/issue-45/component-state-observations-review.md)
accepts this private reader. Parent discovery, readiness admission,
result calculation, and shared public lifecycle activation remain unfinished.
This method does not implement strict read-only previews.
