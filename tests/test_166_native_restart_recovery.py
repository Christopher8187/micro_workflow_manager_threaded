"""Crash recovery for native manual-restart output staging."""

from __future__ import annotations

import json
import os
import socket
import time
from pathlib import Path
from threading import Event, Thread

import pytest

from micro_workflow_manager import MicroWorkflow, cli
from micro_workflow_manager.models import Job, now
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.storage import execution_restart
from micro_workflow_manager.storage.file_operation_receipts import validate_file_receipt


class SimulatedRestartProcessLoss(BaseException):
    pass


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.monotonic() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    storage.close_database_connections()


@pytest.fixture
def restart_scope(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    storage = workflow.storage
    snapshot = workflow.topology.snapshot()
    storage.register_component_topology(snapshot)
    for node in ('A', 'B'):
        storage.create_job(Job(node_name=node, job_id=1, params={'value': node}))
    storage.create_execution_session(
        'job-interrupt', session_kind='interrupt', command='run', start_component=('A', 'B'),
        selected_components=[('A', 'B')], started_at=now(), hostname=socket.gethostname(),
        pid=os.getpid(), process_identity=process_identity(os.getpid()), expected_shape=snapshot.shape_json,
    )
    storage.reserve_execution_components('job-interrupt', expected_shape=snapshot.shape_json)
    leases = {}
    for node in ('A', 'B'):
        lease = storage.claim_job_execution(
            node, 1, started_at=now(), session_id='job-interrupt', component=('A', 'B'),
        )
        storage.finalize_job_execution(node, 1, *lease, 'failed')
        storage.output_file(node, 1).write_bytes(f'original {node}\n'.encode())
        leases[node] = lease
    try:
        yield workflow, leases
    finally:
        _close(storage)


def _rows(storage):
    storage.db_mutation_barrier()
    connection = storage.db_connection()
    tables = [row['name'] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )]
    return {
        table: [dict(row) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')]
        for table in tables
    }


def _business_rows(storage):
    return {name: rows for name, rows in _rows(storage).items() if name != 'restart_receipts'}


def _job_files(storage):
    files = {}
    for node in ('A', 'B'):
        root = storage.job_base_dir(node, 1)
        files[node] = {
            path.relative_to(root).as_posix(): None if path.is_dir() else path.read_bytes()
            for path in root.rglob('*')
        }
    return files


def _restart_receipts(storage):
    rows = _rows(storage)
    assert 'restart_receipts' in rows, 'Native restart has no durable recovery receipt table'
    return rows['restart_receipts']


def _one_receipt(storage, state):
    [row] = _restart_receipts(storage)
    assert row['state'] == state
    validate_file_receipt('restart_receipts', row)
    return row


def _private_paths(tmp_path, receipt):
    manifest = json.loads(receipt['manifest_json'])
    return [tmp_path.joinpath(*item['output']['saved'].split('/'))
            for item in manifest['targets'] if item['output']['original'] is not None]


def _install_move_fault(patch, callback):
    real_move = execution_restart._move_restart_output

    def injected(source, destination):
        source, destination = Path(source), Path(destination)
        if destination.name.startswith('.restart-') and destination.name.endswith('-output.json'):
            return callback(source, destination, real_move)
        return real_move(source, destination)

    patch.setattr(execution_restart, '_move_restart_output', injected)


def _install_discard_fault(patch, callback):
    real_discard = execution_restart._discard_restart_output

    def injected(path):
        path = Path(path)
        if path.name.startswith('.restart-') and path.name.endswith('-output.json'):
            return callback(path, real_discard)
        return real_discard(path)

    patch.setattr(execution_restart, '_discard_restart_output', injected)


def _hide_restart_receipt_after_loss(patch, storage, loss_state):
    read_state = storage._restart_receipt_state

    def read_or_lose(operation_id):
        if loss_state['reached']:
            raise OSError('restart outcome unavailable')
        return read_state(operation_id)

    patch.setattr(storage, '_restart_receipt_state', read_or_lose, raising=False)


def _targets(storage):
    return storage.plan_owned_job_restarts([('A', 1), ('B', 1)])


@pytest.mark.parametrize('loss_point', ['before-first-move', 'after-first-move'])
def test_process_loss_around_first_batch_move_restores_every_output_without_restarting(
    restart_scope, monkeypatch, capsys, loss_point,
):
    workflow, _ = restart_scope
    storage = workflow.storage
    targets = _targets(storage)
    before_rows = _business_rows(storage)
    before_files = _job_files(storage)
    owners = {node: storage.read_job_current_owner(node, 1) for node in ('A', 'B')}
    lost = SimulatedRestartProcessLoss(loss_point)
    loss_state = {'reached': False}

    def lose(source, destination, rename):
        if loss_point == 'after-first-move':
            rename(source, destination)
        loss_state['reached'] = True
        raise lost

    with monkeypatch.context() as patch:
        _install_move_fault(patch, lose)
        _hide_restart_receipt_after_loss(patch, storage, loss_state)
        with pytest.raises(SimulatedRestartProcessLoss) as caught:
            storage.request_owned_job_restarts(targets)
        assert caught.value is lost

    receipt = _one_receipt(storage, 'prepared')
    private = _private_paths(storage.project_dir, receipt)
    if loss_point == 'before-first-move':
        assert _job_files(storage) == before_files
        assert all(not path.exists() for path in private)
    else:
        assert sum(path.exists() for path in private) == 1
        assert sum(storage.output_file(node, 1).exists() for node in ('A', 'B')) == 1

    capsys.readouterr()
    assert cli.main(['recover']) == 0
    assert _business_rows(storage) == before_rows
    assert _job_files(storage) == before_files
    assert {node: storage.read_job_current_owner(node, 1) for node in ('A', 'B')} == owners
    _one_receipt(storage, 'aborted')
    assert all(not path.exists() for path in private)

    after = _rows(storage), _job_files(storage)
    capsys.readouterr()
    assert cli.main(['recover']) == 0
    assert (_rows(storage), _job_files(storage)) == after


def test_live_restart_operation_is_reported_without_waiting_for_its_job_fence(
    restart_scope, monkeypatch, capsys,
):
    workflow, _ = restart_scope
    storage = workflow.storage
    entered, release = Event(), Event()
    result = {}

    def pause(source, destination, rename):
        entered.set()
        assert release.wait(20)
        return rename(source, destination)

    def restart():
        try:
            result['value'] = storage.request_owned_job_restarts(_targets(storage))
        except BaseException as error:
            result['error'] = error

    with monkeypatch.context() as patch:
        _install_move_fault(patch, pause)
        thread = Thread(target=restart, name='live-native-restart-operation', daemon=True)
        thread.start()
        try:
            assert entered.wait(15), result
            receipt = _one_receipt(storage, 'prepared')
            before = _business_rows(storage), _job_files(storage)
            capsys.readouterr()
            started = time.monotonic()
            assert cli.main(['recover']) == 0
            assert time.monotonic() - started < 5
            text = capsys.readouterr().out
            assert receipt['operation_id'] in text and 'Live file operation retained' in text
            assert (_business_rows(storage), _job_files(storage)) == before
        finally:
            release.set()
            thread.join(timeout=20)
            assert not thread.is_alive(), result
    assert 'error' not in result
    assert len(result['value']) == 2
    receipt = _one_receipt(storage, 'committed')
    assert all(not path.exists() for path in _private_paths(storage.project_dir, receipt))


def test_prepared_restart_is_restored_even_after_its_owning_session_finishes(
    restart_scope, monkeypatch, capsys,
):
    workflow, _ = restart_scope
    storage = workflow.storage
    lost = SimulatedRestartProcessLoss('restart process stopped after moving output')
    loss_state = {'reached': False}

    def lose(source, destination, rename):
        rename(source, destination)
        loss_state['reached'] = True
        raise lost

    with monkeypatch.context() as patch:
        _install_move_fault(patch, lose)
        _hide_restart_receipt_after_loss(patch, storage, loss_state)
        with pytest.raises(SimulatedRestartProcessLoss) as caught:
            storage.request_owned_job_restarts([_targets(storage)[0]])
        assert caught.value is lost

    receipt = _one_receipt(storage, 'prepared')
    assert storage.finish_execution_session('job-interrupt', outcome='failed', finished_at=now())
    storage.release_execution_components('job-interrupt')
    before = _business_rows(storage)
    capsys.readouterr()
    assert cli.main(['recover']) == 0
    assert _business_rows(storage) == before
    assert storage.output_file('A', 1).read_bytes() == b'original A\n'
    assert storage.get_execution_session('job-interrupt')['outcome'] == 'failed'
    assert storage.get_component_reservation(('A', 'B')) is None
    _one_receipt(storage, 'aborted')
    assert all(not path.exists() for path in _private_paths(storage.project_dir, receipt))


def test_abandoned_running_owner_restart_uses_the_same_durable_output_operation(
    restart_scope,
):
    workflow, _ = restart_scope
    storage = workflow.storage
    storage.create_job(Job(node_name='A', job_id=2, params={'value': 'abandoned'}))
    generation, execution_id = storage.claim_job_execution(
        'A', 2, started_at=now(), session_id='job-interrupt', component=('A', 'B'),
    )
    storage.output_file('A', 2).write_bytes(b'abandoned output\n')
    owner = storage.read_job_current_owner('A', 2)
    assert owner['execution_id'] == execution_id
    session = storage.get_execution_session('job-interrupt')
    assert storage.finish_execution_session('job-interrupt', outcome='failed', finished_at=now())
    storage.release_execution_components('job-interrupt')

    restarted = storage.request_job_restart('A', 2, reason='manual abandoned restart')

    assert restarted['previous_generation'] == generation
    assert restarted['generation'] == generation + 1
    assert storage.get_job_status('A', 2) == 'queued'
    assert storage.read_job_current_owner('A', 2) == owner
    assert storage.get_job_execution_owner(owner['execution_id']) == owner
    terminal = storage.get_execution_session('job-interrupt')
    assert terminal == dict(session, status='terminal', outcome='failed', finished_at=terminal['finished_at'])
    assert storage.get_component_reservation(('A', 'B')) is None
    assert not storage.output_file('A', 2).exists()
    receipt = _one_receipt(storage, 'committed')
    assert receipt['session_id'] == 'job-interrupt'
    assert all(not path.exists() for path in _private_paths(storage.project_dir, receipt))
    assert [event['event'] for event in storage.read_job_events('A', 2)[-2:]] == [
        'queued', 'restart_requested',
    ]


def test_synchronous_batch_writer_failure_restores_outputs_and_records_exact_abort(
    restart_scope,
):
    workflow, _ = restart_scope
    storage = workflow.storage
    targets = _targets(storage)
    storage.submit_db_mutation(lambda connection: connection.execute(
        "CREATE TRIGGER fail_second_restart_event BEFORE INSERT ON job_events "
        "WHEN NEW.node_name='B' AND NEW.event='restart_requested' "
        "BEGIN SELECT RAISE(ABORT, 'injected restart event failure'); END",
    ))
    before_rows = _business_rows(storage)
    before_files = _job_files(storage)
    owners = {node: storage.read_job_current_owner(node, 1) for node in ('A', 'B')}

    with pytest.raises(Exception, match='injected restart event failure'):
        storage.request_owned_job_restarts(targets)

    assert _business_rows(storage) == before_rows
    assert _job_files(storage) == before_files
    assert {node: storage.read_job_current_owner(node, 1) for node in ('A', 'B')} == owners
    receipt = _one_receipt(storage, 'aborted')
    assert all(not path.exists() for path in _private_paths(storage.project_dir, receipt))


def test_committed_restart_cleanup_preserves_a_new_execution_and_output(
    restart_scope, monkeypatch, capsys,
):
    workflow, leases = restart_scope
    storage = workflow.storage
    old_owner = storage.read_job_current_owner('A', 1)
    lost = SimulatedRestartProcessLoss('restart process stopped before private cleanup')

    with monkeypatch.context() as patch:
        _install_discard_fault(patch, lambda path, unlink: (_ for _ in ()).throw(lost))
        with pytest.raises(SimulatedRestartProcessLoss) as caught:
            storage.request_owned_job_restarts([_targets(storage)[0]])
        assert caught.value is lost

    receipt = _one_receipt(storage, 'committed')
    [private] = _private_paths(storage.project_dir, receipt)
    assert private.read_bytes() == b'original A\n'
    replacement = storage.claim_job_execution(
        'A', 1, started_at=now(), session_id='job-interrupt', component=('A', 'B'),
    )
    storage.output_file('A', 1).write_bytes(b'new generation output\n')
    storage.finalize_job_execution('A', 1, *replacement, 'done')
    new_owner = storage.read_job_current_owner('A', 1)
    assert replacement[0] == leases['A'][0] + 1
    assert new_owner['execution_id'] != old_owner['execution_id']
    before = _business_rows(storage)

    capsys.readouterr()
    assert cli.main(['recover']) == 0
    assert _business_rows(storage) == before
    assert storage.output_file('A', 1).read_bytes() == b'new generation output\n'
    assert storage.read_job_current_owner('A', 1) == new_owner
    assert not private.exists()
    _one_receipt(storage, 'committed')


def test_prepared_restart_refuses_generation_and_visible_output_drift_without_clobbering(
    restart_scope, monkeypatch, capsys,
):
    workflow, _ = restart_scope
    storage = workflow.storage
    lost = SimulatedRestartProcessLoss('staged old output')
    loss_state = {'reached': False}

    def lose(source, destination, rename):
        rename(source, destination)
        loss_state['reached'] = True
        raise lost

    with monkeypatch.context() as patch:
        _install_move_fault(patch, lose)
        _hide_restart_receipt_after_loss(patch, storage, loss_state)
        with pytest.raises(SimulatedRestartProcessLoss) as caught:
            storage.request_owned_job_restarts([_targets(storage)[0]])
        assert caught.value is lost

    receipt = _one_receipt(storage, 'prepared')
    [private] = _private_paths(storage.project_dir, receipt)
    storage.submit_db_mutation(lambda connection: connection.execute(
        "UPDATE jobs SET generation=generation+1, status='queued', active_execution_id=NULL, "
        "active_pid=NULL, active_thread_id=NULL, active_started_at=NULL WHERE node_name='A' AND job_id=1",
    ))
    storage.output_file('A', 1).write_bytes(b'independent new output\n')
    before = _rows(storage), _job_files(storage)

    capsys.readouterr()
    assert cli.main(['recover']) == 1
    captured = capsys.readouterr()
    assert receipt['operation_id'] in captured.err and 'changed' in captured.err.lower()
    assert (_rows(storage), _job_files(storage)) == before
    assert private.read_bytes() == b'original A\n'
    assert storage.output_file('A', 1).read_bytes() == b'independent new output\n'


@pytest.mark.parametrize('damage', ['receipt-intent', 'private-output'])
def test_restart_recovery_refuses_damaged_receipt_or_private_output_exactly(
    restart_scope, monkeypatch, capsys, damage,
):
    workflow, _ = restart_scope
    storage = workflow.storage
    lost = SimulatedRestartProcessLoss('staged restart output')
    loss_state = {'reached': False}

    def lose(source, destination, rename):
        rename(source, destination)
        loss_state['reached'] = True
        raise lost

    with monkeypatch.context() as patch:
        _install_move_fault(patch, lose)
        _hide_restart_receipt_after_loss(patch, storage, loss_state)
        with pytest.raises(SimulatedRestartProcessLoss) as caught:
            storage.request_owned_job_restarts([_targets(storage)[0]])
        assert caught.value is lost

    receipt = _one_receipt(storage, 'prepared')
    [private] = _private_paths(storage.project_dir, receipt)
    if damage == 'receipt-intent':
        storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE restart_receipts SET manifest_json=manifest_json || ' ' WHERE operation_id=?",
            (receipt['operation_id'],),
        ))
    else:
        private.write_bytes(b'replaced private output\n')
    before = _rows(storage), _job_files(storage)

    capsys.readouterr()
    assert cli.main(['recover']) == 1
    assert receipt['operation_id'] in capsys.readouterr().err
    assert (_rows(storage), _job_files(storage)) == before
