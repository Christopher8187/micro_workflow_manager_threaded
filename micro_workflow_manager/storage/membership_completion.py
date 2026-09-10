"""Bind the final active-membership writer to its complete preparation attempt."""

from .preparation_attempts import _require_exact_attempt, checked_attempt_receipts


class MembershipCompletion:
    def __init__(self, connection, operation_id, revision, session_id):
        row = connection.execute('SELECT * FROM preparation_attempts WHERE operation_id=?',
                                 (operation_id,)).fetchone()
        if row is None:
            raise RuntimeError('Membership preparation attempt is missing')
        self.expected = dict(row)
        self.guards = tuple(tuple(row) for row in connection.execute(
            'SELECT * FROM receiver_mutation_guards WHERE operation_id=? ORDER BY receiver_node', (operation_id,)))
        if (row['state'] != 'preparing' or row['session_id'] != session_id
                or row['membership_revision'] != revision or row['completed_membership_revision'] is not None):
            raise RuntimeError('Membership preparation has another completion requirement')
        self.require_prepared(connection)

    def require_prepared(self, connection):
        _require_exact_attempt(connection, self.expected, self.guards)
        checked_attempt_receipts(connection, self.expected)

    def commit(self, connection, revision):
        self.require_prepared(connection)
        receipts, complete = checked_attempt_receipts(connection, self.expected)
        if (revision != self.expected['membership_revision'] or not complete
                or any(row['state'] != 'committed' for row in receipts)):
            raise RuntimeError('Membership completion differs from its prepared result')
        changed = connection.execute(
            "UPDATE preparation_attempts SET completed_membership_revision=? WHERE operation_id=? "
            "AND state='preparing' AND membership_revision=? AND completed_membership_revision IS NULL",
            (revision, self.expected['operation_id'], revision),
        ).rowcount
        if changed != 1:
            raise RuntimeError('Membership completion did not persist')
        expected = dict(self.expected, completed_membership_revision=revision)
        _require_exact_attempt(connection, expected, self.guards)
        observed, complete = checked_attempt_receipts(connection, expected)
        if not complete or observed != receipts:
            raise RuntimeError('Membership preparation receipts changed during completion')
