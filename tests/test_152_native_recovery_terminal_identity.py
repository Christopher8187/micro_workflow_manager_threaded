from __future__ import annotations

import json

import pytest

from micro_workflow_manager import cli
from tests.test_064_read_only_previews import (
    _close,
    _initialize_native_project,
    _install_import_sentinels,
    _snapshot,
    _wait_writer,
)
from tests.test_121_native_preview_recovery import _rows
from tests.test_146_native_applied_recovery import (
    _create_active_scope,
    _job_row,
    _session_row,
)


def _output_bytes(case, owner):
    payload = {
        'generation': owner['generation'],
        'execution_id': owner['execution_id'],
        'status': case if case in ('done', 'skipped', 'failed', 'cancelled') else 'done',
        'result_type': 'str',
        'result_repr': "'original output'",
    }
    if case == 'missing':
        return None
    if case == 'malformed':
        return b'{unfinished output'
    if case == 'non-object':
        return b'[0, "done"]'
    if case == 'boolean-generation':
        assert owner['generation'] == 0
        payload['generation'] = False
    elif case == 'float-generation':
        payload['generation'] = float(owner['generation'])
    elif case == 'string-generation':
        payload['generation'] = str(owner['generation'])
    elif case == 'different-generation':
        payload['generation'] += 1
    elif case == 'different-execution':
        payload['execution_id'] = '0' * 32
        assert payload['execution_id'] != owner['execution_id']
    elif case == 'missing-execution':
        del payload['execution_id']
    elif case == 'nonterminal':
        payload['status'] = 'running'
    return json.dumps(payload, indent=2).encode('utf-8') + b'\n'


@pytest.mark.parametrize('case', [
    'done', 'skipped', 'failed', 'cancelled',
    'missing', 'malformed', 'non-object', 'boolean-generation',
    'float-generation', 'string-generation', 'different-generation',
    'different-execution', 'missing-execution', 'nonterminal',
])
def test_public_recovery_retains_only_exact_native_terminal_output(
    tmp_path, monkeypatch, capsys, case,
):
    edges = [('A', 'A')]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    storage = workflow.storage
    owner = _create_active_scope(
        storage, workflow.topology.snapshot().shape_json, 'A', 'abandoned-terminal-A',
    )
    output = storage.output_file('A', 1)
    content = _output_bytes(case, owner)
    if content is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(content)
    else:
        assert not output.exists()
    marker = lambda path: (
        path.stat().st_dev, path.stat().st_ino, path.stat().st_mode,
        path.stat().st_size, path.stat().st_mtime_ns,
    )
    original_marker = None if content is None else marker(output)
    retained_file = storage.job_base_dir('A', 1) / 'keep.txt'
    retained_file.write_bytes(b'input-side data must survive recovery')
    retained_marker = marker(retained_file)
    storage.db_mutation_barrier()
    _wait_writer(storage)
    external = _install_import_sentinels(tmp_path, edges)
    capsys.readouterr()
    try:
        assert cli.main(['recover']) == 0
        captured = capsys.readouterr()
        assert owner['session_id'] in captured.out + captured.err
        assert not external.exists()
        storage.db_mutation_barrier()
        terminal = case in ('done', 'skipped', 'failed', 'cancelled')
        row = _job_row(storage, 'A')
        assert row[0] == (case if terminal else 'queued')
        assert row[1] == owner['generation'] + (0 if terminal else 1)
        assert tuple(row[2:]) == (None,) * 7
        if terminal:
            assert output.read_bytes() == content
            assert marker(output) == original_marker
        else:
            assert not output.exists()
        assert retained_file.read_bytes() == b'input-side data must survive recovery'
        assert marker(retained_file) == retained_marker
        assert storage.read_job_instance_id('A', 1) == owner['instance_id']
        assert storage.get_job_execution_owner(owner['execution_id']) == owner['owner']
        assert storage.read_job_current_owner('A', 1) == owner['owner']
        assert tuple(_session_row(storage, owner['session_id']))[:2] == ('terminal', 'failed')
        assert storage.get_component_state(('A',))['lifecycle'] == 'failed'
        assert storage.db_connection().execute(
            'SELECT 1 FROM pending_component_executions WHERE session_id=?',
            (owner['session_id'],),
        ).fetchone() is None
        assert storage.db_connection().execute(
            'SELECT 1 FROM component_reservations WHERE session_id=?',
            (owner['session_id'],),
        ).fetchone() is None
        after_rows = _rows(storage)
        after_files = _snapshot(tmp_path, mutable_existing_shm=True)
        assert cli.main(['recover']) == 0
        storage.db_mutation_barrier()
        assert _rows(storage) == after_rows
        assert _snapshot(tmp_path, mutable_existing_shm=True) == after_files
        assert not external.exists()
    finally:
        _close(storage)
