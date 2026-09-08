from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from threading import Barrier

import pytest

from micro_workflow_manager import MicroWorkflow, NodeInputFileSystem
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.errors import JobFailedError
from micro_workflow_manager.models import now
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.component_states import ComponentTerminalOutcome
from micro_workflow_manager.storage.input_publication_files import InputFileChange
from tests.test_090_component_session_settlement import _close


def _replace_done_result_with_sampled(storage, component):
    key = encode_component_key(component)

    def replace(connection):
        state = connection.execute(
            'SELECT lifecycle, stability, instability_origin, shape_id, alignment_generation, '
            'retained_result_shape_id, retained_result_alignment_generation '
            'FROM component_states WHERE component_key=?', (key,),
        ).fetchone()
        assert state is not None
        assert (state['lifecycle'], state['stability'], state['instability_origin']) == (
            'done', 'stable', None,
        )
        assert (
            state['retained_result_shape_id'], state['retained_result_alignment_generation'],
        ) == (state['shape_id'], state['alignment_generation'])
        result = connection.execute(
            "UPDATE component_successful_results SET lifecycle='sampled' "
            "WHERE component_key=? AND shape_id=? AND alignment_generation=? "
            "AND lifecycle='done' AND stability='stable' AND instability_origin IS NULL",
            (key, state['shape_id'], state['alignment_generation']),
        ).rowcount
        current = connection.execute(
            "UPDATE component_states SET lifecycle='sampled' "
            "WHERE component_key=? AND shape_id=? AND alignment_generation=? "
            "AND lifecycle='done' AND stability='stable' AND instability_origin IS NULL",
            (key, state['shape_id'], state['alignment_generation']),
        ).rowcount
        assert (result, current) == (1, 1)

    storage.submit_db_mutation(replace)


def test_late_managed_input_marks_completed_receiver_without_rerunning_it(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    calls, published = [], []

    @workflow.task('A')
    def produce(ctx):
        calls.append('A')
        published.append(ctx.node('B').write_input('value.txt', str(len(calls))))

    @workflow.task('B')
    def consume(ctx):
        calls.append('B')
        return ctx.input_path('A', 'value.txt').read_text(encoding='utf-8')

    workflow.start('A')
    workflow.start('B')
    try:
        assert workflow.run() == ['A', 'B']
        before = storage.get_component_state(('B',))
        assert before['lifecycle'] == 'done' and before['misaligned'] is False
        control = storage.read_job_control('B', 1)
        events = storage.read_job_events('B', 1)
        owner = storage.read_job_current_owner('B', 1)
        output = storage.output_file('B', 1).read_bytes()

        workflow.start('A', job_id=2)
        workflow.run_job('A', 2, ignore_readiness=True)

        assert calls == ['A', 'B', 'A']
        assert [path.name for path in published] == ['value.txt', 'value_2.txt']
        assert published[-1].read_text(encoding='utf-8') == '3'
        assert storage.get_component_state(('B',)) == dict(before, misaligned=True)
        assert storage.read_job_control('B', 1) == control
        assert storage.read_job_events('B', 1) == events
        assert storage.read_job_current_owner('B', 1) == owner
        assert storage.output_file('B', 1).read_bytes() == output
        assert storage.get_component_reservation(('B',)) is None
    finally:
        _close(storage)


def test_receiver_keeps_first_actual_batch_path_after_later_arrivals_and_reopen(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'C')])
    storage = workflow.storage
    rounds = []

    @workflow.task('A')
    def produce(ctx):
        rounds.append(ctx.node('B').write_inputs([('first.txt', 'one'), ('second.txt', 'two')]))

    @workflow.task('B')
    def consume(ctx):
        return 'consumed'

    @workflow.task('C')
    def downstream(ctx):
        return 'retained'

    for node in ('A', 'B', 'C'):
        workflow.start(node)
    try:
        workflow.run()
        before = storage.get_component_state(('B',))
        downstream_before = storage.get_component_state(('C',))
        workflow.start('A', job_id=2)
        workflow.run_job('A', 2, ignore_readiness=True)
        expected = [{
            'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
            'producer_node': 'A', 'producer_job_id': 2,
            'arrival_kind': 'managed-input', 'path': 'A/first_2.txt',
        }]
        assert storage.read_component_misalignment_causes(('B',)) == expected
        assert storage.get_component_state(('C',)) == downstream_before
        workflow.start('A', job_id=3)
        workflow.run_job('A', 3, ignore_readiness=True)
        assert [path.name for path in rounds[-1]] == ['first_3.txt', 'second_3.txt']
        assert storage.read_component_misalignment_causes(('B',)) == expected
        storage.clear_job_events('A', [2])
        assert storage.read_component_misalignment_causes(('B',)) == expected
    finally:
        _close(storage)
    reopened = FileStorage(tmp_path)
    try:
        assert reopened.read_component_misalignment_causes(('B',)) == expected
        assert reopened.read_component_misalignment_causes(('C',)) == []
    finally:
        _close(reopened)


@pytest.mark.parametrize('lifecycle', ['done', 'sampled', 'failed'])
def test_terminal_receiver_keeps_lifecycle_and_gets_a_new_cause_after_full_repair(tmp_path, lifecycle):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    fail_receiver = lifecycle == 'failed'

    @workflow.task('A')
    def produce(ctx):
        ctx.node('B').write_input('data.txt', 'arrival')

    @workflow.task('B')
    def consume(ctx):
        if fail_receiver:
            raise ValueError('receiver failed')
        return 'received'

    workflow.start('A')
    workflow.start('B')
    try:
        if fail_receiver:
            with pytest.raises(JobFailedError, match='Job B/1 failed') as failed:
                workflow.run()
            assert isinstance(failed.value.__cause__, ValueError)
            assert str(failed.value.__cause__) == 'receiver failed'
        else:
            workflow.run()
        if lifecycle == 'sampled':
            _replace_done_result_with_sampled(storage, ('B',))
        before = storage.get_component_state(('B',))
        assert before['lifecycle'] == lifecycle
        assert storage.read_component_misalignment_causes(('B',)) == []
        workflow.start('A', job_id=2)
        workflow.run_job('A', 2, ignore_readiness=True)
        assert storage.get_component_state(('B',)) == dict(before, misaligned=True)
        first = storage.read_component_misalignment_causes(('B',))
        assert first == [{
            'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
            'producer_node': 'A', 'producer_job_id': 2,
            'arrival_kind': 'managed-input', 'path': 'A/data_2.txt',
        }]
        fail_receiver = False
        workflow.run_node('B')
        repaired = storage.get_component_state(('B',))
        assert repaired['lifecycle'] == 'done' and repaired['misaligned'] is False
        assert repaired['alignment_generation'] == before['alignment_generation'] + 1
        assert storage.read_component_misalignment_causes(('B',)) == []
        workflow.start('A', job_id=3)
        workflow.run_job('A', 3, ignore_readiness=True)
        assert storage.get_component_state(('B',)) == dict(repaired, misaligned=True)
        assert storage.read_component_misalignment_causes(('B',)) == [dict(
            first[0], alignment_generation=repaired['alignment_generation'],
            producer_job_id=3, path='A/data_3.txt',
        )]
    finally:
        _close(storage)


@pytest.mark.parametrize('failure', ['state', 'cause', 'event', 'receipt'])
def test_failed_arrival_decision_restores_files_owners_events_and_receiver(tmp_path, failure):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    receiver = tmp_path / 'node' / 'B' / 'input' / 'A'
    reject = False
    before = {}

    @workflow.task('A')
    def produce(ctx):
        if not reject:
            ctx.node('B').write_input('keep.txt', 'original')
            return
        events = storage.read_job_events('A', ctx.job_id)
        with pytest.raises(sqlite3.IntegrityError, match='injected arrival failure'):
            ctx.node('B').write_inputs([('keep.txt', 'replacement'), ('new.txt', 'new')], overwrite=True)
        assert storage.read_job_events('A', ctx.job_id) == events
        assert (receiver / 'keep.txt').read_bytes() == b'original'
        assert not (receiver / 'new.txt').exists()
        assert storage.read_node_input_owner('B', 'A/keep.txt') == before['owner']
        assert storage.read_node_input_owner('B', 'A/new.txt') is None
        assert storage.get_component_state(('B',)) == before['state']
        assert storage.read_component_misalignment_causes(('B',)) == []
        assert storage.db_connection().execute(
            'SELECT state FROM input_publications ORDER BY rowid DESC LIMIT 1'
        ).fetchone()[0] == 'aborted'

    @workflow.task('B')
    def consume(ctx):
        return 'original result'

    workflow.start('A')
    workflow.start('B')
    try:
        workflow.run()
        before = {'state': storage.get_component_state(('B',)),
                  'owner': storage.read_node_input_owner('B', 'A/keep.txt')}
        workflow.start('A', job_id=2)
        target = {
            'state': "UPDATE OF misaligned ON component_states WHEN NEW.misaligned=1",
            'cause': 'INSERT ON component_misalignment_causes',
            'event': "INSERT ON job_events WHEN NEW.event='input_forwarded'",
            'receipt': "UPDATE OF state ON input_publications WHEN NEW.state='committed'",
        }[failure]
        storage.submit_db_mutation(lambda connection: connection.execute(
            'CREATE TRIGGER refuse_arrival BEFORE ' + target +
            " BEGIN SELECT RAISE(ABORT, 'injected arrival failure'); END"
        ))
        reject = True
        workflow.run_job('A', 2, ignore_readiness=True)
    finally:
        storage.submit_db_mutation(lambda connection: connection.execute('DROP TRIGGER IF EXISTS refuse_arrival'))
        _close(storage)


def test_two_receivers_in_one_component_keep_their_causes_after_concurrent_publication(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='threaded', persist_graph=False)
    workflow.graph([('A', 'X'), ('X', 'A'), ('A', 'B'), ('X', 'C'), ('B', 'C'), ('C', 'B')])
    storage = workflow.storage
    publish_together = Barrier(2)
    publish_later = False

    @workflow.task('A')
    def first(ctx):
        if not publish_later:
            return
        publish_together.wait(timeout=10)
        ctx.node('B').write_input('first.txt', 'A')

    @workflow.task('X')
    def second(ctx):
        if not publish_later:
            return
        publish_together.wait(timeout=10)
        ctx.node('C').write_input('second.txt', 'X')

    @workflow.task('B')
    def consume_first(ctx):
        return 'B'

    @workflow.task('C')
    def consume_second(ctx):
        return 'C'

    for node in ('A', 'X', 'B', 'C'):
        workflow.start(node)
    try:
        workflow.run()
        before = storage.get_component_state(('B', 'C'))
        assert before['misaligned'] is False
        publish_later = True
        workflow.run_component(('A', 'X'))
        assert storage.get_component_state(('B', 'C')) == dict(before, misaligned=True)
        assert storage.read_component_misalignment_causes(('C', 'B')) == [
            {'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
             'producer_node': 'A', 'producer_job_id': 1, 'arrival_kind': 'managed-input', 'path': 'A/first.txt'},
            {'receiver_node': 'C', 'alignment_generation': before['alignment_generation'],
             'producer_node': 'X', 'producer_job_id': 1, 'arrival_kind': 'managed-input', 'path': 'X/second.txt'},
        ]
    finally:
        _close(storage)



def test_running_receiver_noop_does_not_hide_an_arrival_at_the_same_completed_generation(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A'), ('X', 'B')])
    storage = workflow.storage
    states = []

    @workflow.task('A')
    def produce(ctx):
        state = storage.get_component_state(('A', 'B'))
        states.append(state)
        assert storage.read_component_misalignment_causes(('A', 'B')) == []
        ctx.node('B').write_input('data.txt', 'arrived')
        if state['lifecycle'] == 'running':
            assert storage.get_component_state(('A', 'B')) == state
            assert storage.read_component_misalignment_causes(('A', 'B')) == []

    @workflow.task('X')
    def later_producer(ctx):
        states.append(storage.get_component_state(('A', 'B')))
        assert storage.read_component_misalignment_causes(('A', 'B')) == []
        ctx.node('B').write_input('data.txt', 'later arrival')

    @workflow.task('B')
    def consume(ctx):
        return 'received'

    workflow.start('A')
    workflow.start('B')
    try:
        workflow.run()
        workflow.start('X')
        workflow.run_job('X', 1)
        assert [state['lifecycle'] for state in states] == ['running', 'done']
        assert states[0]['alignment_generation'] == states[1]['alignment_generation']
        assert storage.get_component_state(('A', 'B')) == dict(states[1], misaligned=True)
        assert storage.read_component_misalignment_causes(('A', 'B')) == [{
            'receiver_node': 'B', 'alignment_generation': states[1]['alignment_generation'],
            'producer_node': 'X', 'producer_job_id': 1,
            'arrival_kind': 'managed-input', 'path': 'X/data.txt',
        }]
    finally:
        _close(storage)


def test_empty_delete_and_manual_edit_leave_receiver_aligned_until_real_batch_change(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    later = False

    @workflow.task('A')
    def produce(ctx):
        if not later:
            return
        owner = storage.read_job_current_owner('A', 1)
        before = storage.get_component_state(('B',))
        missing = InputFileChange('missing.txt', 'delete')
        def publish(changes):
            return storage.publish_managed_inputs(
                'A', 1, owner['generation'], ctx.execution_id, 'B', changes,
            )
        publish([missing])
        assert storage.get_component_state(('B',)) == before
        assert storage.read_component_misalignment_causes(('B',)) == []
        publish([missing, InputFileChange('real.txt', 'text', 'new')])
        assert storage.get_component_state(('B',)) == dict(before, misaligned=True)
        assert storage.read_component_misalignment_causes(('B',)) == [{
            'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
            'producer_node': 'A', 'producer_job_id': 1,
            'arrival_kind': 'managed-input', 'path': 'A/real.txt',
        }]

    @workflow.task('B')
    def consume(ctx):
        return 'received'

    workflow.start('A')
    workflow.start('B')
    try:
        workflow.run()
        before = storage.get_component_state(('B',))
        (tmp_path / 'node' / 'B' / 'input' / 'manual.txt').write_bytes(b'manual edit')
        assert storage.get_component_state(('B',)) == before
        assert storage.read_component_misalignment_causes(('B',)) == []
        later = True
        workflow.run_node('A')
    finally:
        _close(storage)


@pytest.mark.parametrize('damage', ['missing', 'flag', 'receiver', 'component', 'generation', 'producer', 'path'])
def test_reopened_cause_reader_refuses_damaged_current_history(tmp_path, damage):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage

    @workflow.task('A')
    def produce(ctx):
        ctx.node('B').write_input('data.txt', 'arrived')

    @workflow.task('B')
    def consume(ctx):
        return 'received'

    workflow.start('A')
    workflow.start('B')
    try:
        workflow.run()
        receiver_owner = storage.read_job_current_owner('B', 1)
        workflow.start('A', job_id=2)
        workflow.run_job('A', 2, ignore_readiness=True)
        assert len(storage.read_component_misalignment_causes(('B',))) == 1
        def corrupt(connection):
            if damage == 'missing':
                connection.execute(
                    "DELETE FROM component_misalignment_causes WHERE receiver_node='B'"
                )
            elif damage == 'flag':
                connection.execute("UPDATE component_states SET misaligned=0 WHERE component_key='[\"B\"]'")
            elif damage == 'producer':
                connection.execute("UPDATE component_misalignment_causes "
                                   "SET producer_execution_id=? WHERE receiver_node='B'",
                                   (receiver_owner['execution_id'],))
            else:
                column, value = {
                    'receiver': ('receiver_node', 'X'),
                    'component': ('component_key', '["A"]'),
                    'generation': ('alignment_generation', 0),
                    'path': ('relative_path', '../outside.txt'),
                }[damage]
                connection.execute(
                    'UPDATE component_misalignment_causes SET ' + column +
                    "=? WHERE receiver_node='B'", (value,)
                )
        storage.submit_db_mutation(corrupt)
    finally:
        _close(storage)
    reopened = FileStorage(tmp_path)
    try:
        with pytest.raises(RuntimeError, match='[Mm]isalignment'):
            reopened.read_component_misalignment_causes(('B',))
    finally:
        _close(reopened)



def test_repeated_arrivals_do_not_reinsert_an_established_first_cause(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    later = False

    @workflow.task('A')
    def produce(ctx):
        if later:
            for number in range(3):
                ctx.node('B').write_inputs([(f'{number}-first.txt', 'first'), (f'{number}-second.txt', 'second')])

    @workflow.task('B')
    def consume(ctx):
        return 'received'

    workflow.start('A')
    workflow.start('B')
    try:
        workflow.run()
        storage.submit_db_mutation(lambda connection: connection.execute('''
            CREATE TRIGGER reject_repeated_cause BEFORE INSERT ON component_misalignment_causes
            WHEN EXISTS(SELECT 1 FROM component_misalignment_causes
                        WHERE receiver_node=NEW.receiver_node AND alignment_generation=NEW.alignment_generation)
            BEGIN SELECT RAISE(ABORT, 'Repeated first-cause insertion'); END
        '''))
        later = True
        workflow.run_node('A')
        causes = storage.read_component_misalignment_causes(('B',))
        assert len(causes) == 1 and causes[0]['path'] == 'A/0-first.txt'
        assert len(list((tmp_path / 'node' / 'B' / 'input' / 'A').iterdir())) == 6
    finally:
        storage.submit_db_mutation(lambda connection: connection.execute('DROP TRIGGER IF EXISTS reject_repeated_cause'))
        _close(storage)


@pytest.mark.parametrize('receivers', [('B', 'B'), ('B', 'C')])
def test_process_publishers_keep_one_first_cause_per_receiver(tmp_path, receivers):
    workflow = MicroWorkflow(tmp_path, runner='threaded', persist_graph=False)
    workflow.graph([('A', 'B'), ('A', 'C'), ('B', 'C'), ('C', 'B')])
    storage = workflow.storage
    phase = 'idle'
    release = tmp_path / 'release-workers'
    ready = Barrier(2, action=release.touch)
    script = '''import json, sys, time
from pathlib import Path
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.input_publication_files import InputFileChange
from tests.test_090_component_session_settlement import _close
root, owner, receiver = Path(sys.argv[1]), json.loads(sys.argv[2]), sys.argv[3]
storage = FileStorage(root)
try:
    (root / (str(owner['job_id']) + '.ready')).touch()
    deadline = time.monotonic() + 20
    while not (root / 'release-workers').exists():
        assert time.monotonic() < deadline, 'Workers were not released'
        time.sleep(0.01)
    storage.publish_managed_inputs('A', owner['job_id'], owner['generation'], owner['execution_id'], receiver,
                                  [InputFileChange('job-' + str(owner['job_id']) + '.txt', 'text', 'child process')])
finally:
    _close(storage)
'''

    @workflow.task('A')
    def produce(ctx):
        if phase == 'idle':
            return
        receiver = receivers[ctx.job_id - 1]
        if phase == 'repeat':
            ctx.node(receiver).write_input('later.txt', 'later arrival')
            return
        owner = storage.read_job_current_owner('A', ctx.job_id)
        process = subprocess.Popen(
            [sys.executable, '-c', script, str(tmp_path), json.dumps(owner), receiver],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
        )
        try:
            deadline = time.monotonic() + 20
            while not (tmp_path / f'{ctx.job_id}.ready').exists():
                assert process.poll() is None, process.communicate()
                assert time.monotonic() < deadline, 'Worker did not become ready'
                time.sleep(0.01)
            ready.wait(timeout=20)
            stdout, stderr = process.communicate(timeout=30)
            assert process.returncode == 0, stdout + stderr
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=10)

    @workflow.task('B')
    def first_receiver(ctx):
        return 'B'

    @workflow.task('C')
    def second_receiver(ctx):
        return 'C'

    workflow.start('A', job_id=1)
    workflow.start('A', job_id=2)
    workflow.start('B')
    workflow.start('C')
    try:
        workflow.run()
        before = storage.get_component_state(('B', 'C'))
        phase = 'workers'
        workflow.run_node('A')
        assert storage.get_component_state(('B', 'C')) == dict(before, misaligned=True)
        causes = storage.read_component_misalignment_causes(('B', 'C'))
        assert [cause['receiver_node'] for cause in causes] == sorted(set(receivers))
        for cause in causes:
            assert cause['alignment_generation'] == before['alignment_generation']
            assert cause['producer_node'] == 'A' and cause['arrival_kind'] == 'managed-input'
            assert cause['producer_job_id'] in (1, 2)
            assert receivers[cause['producer_job_id'] - 1] == cause['receiver_node']
            assert cause['path'] == f"A/job-{cause['producer_job_id']}.txt"
        for job_id, receiver in enumerate(receivers, start=1):
            assert (tmp_path / 'node' / receiver / 'input' / 'A' / f'job-{job_id}.txt').read_text() == 'child process'
        phase = 'repeat'
        workflow.run_job('A', 1, ignore_readiness=True)
        assert storage.read_component_misalignment_causes(('B', 'C')) == causes
    finally:
        _close(storage)



@pytest.mark.parametrize('initial_lifecycle', ['queued', 'running'])
def test_native_receiver_noop_allows_later_terminal_arrival_in_the_same_session(tmp_path, initial_lifecycle):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    snapshot = workflow.topology.snapshot()
    storage.register_component_topology(snapshot)
    receiver_session = 'receiver-result'
    receiver_started = False

    def begin_receiver():
        nonlocal receiver_started
        assert not receiver_started
        receiver_started = True
        storage.create_execution_session(
            receiver_session, session_kind='interrupt', command='run',
            start_component=('B',), selected_components=[('B',)], started_at=now(),
            hostname=socket.gethostname(), pid=os.getpid(),
            process_identity=process_identity(os.getpid()), expected_shape=snapshot.shape_json,
        )
        assert storage.reserve_execution_components(
            receiver_session, expected_shape=snapshot.shape_json,
        ) is True
        assert storage.begin_queued_component_execution(
            receiver_session, ('B',), expected_shape=snapshot.shape_json,
            expected_alignment_generation=0, successful_lineage=('stable', None),
        ) is True

    def finish_receiver():
        decision = storage.decide_execution_session_exit(
            receiver_session, outcome='done', finished_at=now(),
            component_outcomes=[ComponentTerminalOutcome(
                ('B',), snapshot.shape_json, 0, 'done', 'stable', None,
            )],
        )
        assert decision == {'restarts': {}, 'released': 1}

    @workflow.task('A')
    def produce(ctx):
        owner = storage.read_job_current_owner('A', 1)
        before = storage.get_component_state(('B',))
        assert before['lifecycle'] == initial_lifecycle
        ctx.node('B').write_input('before.txt', 'while receiver has no established result')
        assert storage.get_component_state(('B',)) == before
        assert storage.read_component_misalignment_causes(('B',)) == []
        if not receiver_started:
            begin_receiver()
        finish_receiver()
        established = storage.get_component_state(('B',))
        assert established['alignment_generation'] == before['alignment_generation']
        ctx.node('B').write_input('after.txt', 'after receiver result')
        assert storage.read_job_current_owner('A', 1) == owner
        assert storage.get_component_state(('B',)) == dict(established, misaligned=True)
        assert storage.read_component_misalignment_causes(('B',)) == [{
            'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
            'producer_node': 'A', 'producer_job_id': 1,
            'arrival_kind': 'managed-input', 'path': 'A/after.txt',
        }]

    try:
        if initial_lifecycle == 'running':
            begin_receiver()
        workflow.start('A')
        workflow.run_node('A')
        sessions = {session['session_id']: session for session in storage.list_execution_sessions()}
        assert len(sessions) == 2
        assert sessions[receiver_session]['status'] == 'terminal'
        assert sessions[receiver_session]['outcome'] == 'done'
        producer, = [session for session in sessions.values() if session['session_id'] != receiver_session]
        assert producer['status'] == 'terminal' and producer['outcome'] == 'done'
    finally:
        _close(storage)


def test_failed_second_receiver_publication_does_not_cache_an_uncommitted_cause(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('A', 'C'), ('B', 'C'), ('C', 'B')])
    storage = workflow.storage
    later = False

    @workflow.task('A')
    def produce(ctx):
        if not later:
            return
        ctx.node('B').write_input('first.txt', 'first receiver')
        before = storage.get_component_state(('B', 'C'))
        first = storage.read_component_misalignment_causes(('B', 'C'))
        assert before['misaligned'] and len(first) == 1
        storage.submit_db_mutation(lambda connection: connection.execute('''
            CREATE TRIGGER refuse_second_receiver BEFORE UPDATE OF state ON input_publications
            WHEN NEW.receiver_node='C' AND NEW.state='committed'
            BEGIN SELECT RAISE(ABORT, 'injected second receiver failure'); END
        '''))
        try:
            with pytest.raises(sqlite3.IntegrityError, match='injected second receiver failure'):
                ctx.node('C').write_input('failed.txt', 'must be restored')
            assert storage.get_component_state(('B', 'C')) == before
            assert storage.read_component_misalignment_causes(('B', 'C')) == first
            assert not (tmp_path / 'node' / 'C' / 'input' / 'A' / 'failed.txt').exists()
        finally:
            storage.submit_db_mutation(lambda connection: connection.execute('DROP TRIGGER refuse_second_receiver'))
        ctx.node('C').write_input('accepted.txt', 'second receiver retry')
        assert storage.get_component_state(('B', 'C')) == before
        assert storage.read_component_misalignment_causes(('B', 'C')) == first + [{
            'receiver_node': 'C', 'alignment_generation': before['alignment_generation'],
            'producer_node': 'A', 'producer_job_id': 1,
            'arrival_kind': 'managed-input', 'path': 'A/accepted.txt',
        }]

    @workflow.task('B')
    def first_receiver(ctx):
        return 'B'

    @workflow.task('C')
    def second_receiver(ctx):
        return 'C'

    for node in ('A', 'B', 'C'):
        workflow.start(node)
    try:
        workflow.run()
        later = True
        workflow.run_node('A')
    finally:
        _close(storage)


@pytest.mark.parametrize('bytes_present', [True, False])
def test_managed_delete_marks_receiver_when_it_removes_bytes_or_remaining_ownership(tmp_path, bytes_present):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    later = False

    @workflow.task('A')
    def produce(ctx):
        entry = NodeInputFileSystem('B').file(ctx, 'keep.txt')
        if not later:
            entry.write_text('managed input')
            return
        assert storage.read_node_input_owner('B', 'A/keep.txt') is not None
        entry.delete()
        assert storage.read_node_input_owner('B', 'A/keep.txt') is None

    @workflow.task('B')
    def consume(ctx):
        return 'B'

    workflow.start('A')
    workflow.start('B')
    try:
        workflow.run()
        before = storage.get_component_state(('B',))
        path = tmp_path / 'node' / 'B' / 'input' / 'A' / 'keep.txt'
        if not bytes_present:
            path.unlink()
        assert storage.get_component_state(('B',)) == before
        later = True
        workflow.start('A', job_id=2)
        workflow.run_job('A', 2, ignore_readiness=True)
        assert not path.exists()
        assert storage.get_component_state(('B',)) == dict(before, misaligned=True)
        assert storage.read_component_misalignment_causes(('B',)) == [{
            'receiver_node': 'B', 'alignment_generation': before['alignment_generation'],
            'producer_node': 'A', 'producer_job_id': 2,
            'arrival_kind': 'managed-input', 'path': 'A/keep.txt',
        }]
    finally:
        _close(storage)
