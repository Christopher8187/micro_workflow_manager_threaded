from __future__ import annotations

import os
import subprocess

import pytest

from micro_workflow_manager import InputFileSystem, MicroWorkflow, NodeInputFileSystem, OutputFileSystem, cli
from micro_workflow_manager.errors import JobRestartedError
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close


@pytest.mark.parametrize('operation', [
    'text', 'bytes', 'batch', 'copy', 'filesystem-text', 'filesystem-bytes', 'filesystem-copy', 'append',
])
def test_forwarded_files_use_the_producer_raw_node_prefix(tmp_path, operation):
    workflow = MicroWorkflow(project_dir=tmp_path, runner='direct')
    workflow.graph([('A', 'B')])
    source = tmp_path / 'source.txt'
    source.write_bytes(b'prepared input')
    unqualified = tmp_path / 'node' / 'B' / 'input' / 'evidence' / 'source.txt'
    unqualified.parent.mkdir(parents=True)
    unqualified.write_bytes(b'project-owned input')
    expected = tmp_path / 'node' / 'B' / 'input' / 'A' / 'evidence' / 'source.txt'

    @workflow.task('A')
    def publish(ctx):
        receiver = ctx.node('B')
        entry = NodeInputFileSystem('B', base='evidence').file(ctx, 'source.txt')
        if operation == 'text':
            written = receiver.write_input('evidence/source.txt', 'prepared input', overwrite=True)
        elif operation == 'bytes':
            written = receiver.write_input_bytes('evidence/source.txt', b'prepared input', overwrite=True)
        elif operation == 'batch':
            written, = receiver.write_inputs([('evidence/source.txt', 'prepared input')], overwrite=True)
        elif operation == 'copy':
            written = receiver.add_input_file(source, filename='evidence/source.txt', overwrite=True)
        elif operation == 'filesystem-text':
            written = entry.write_text('prepared input')
        elif operation == 'filesystem-bytes':
            written = entry.write_bytes(b'prepared input')
        elif operation == 'filesystem-copy':
            written = entry.copy_from(source, overwrite=True)
        else:
            written = entry.append_text('prepared input')
        return str(written), str(receiver.input_path('evidence/source.txt')), str(entry.path)

    try:
        workflow.start('A')
        assert workflow.run_job('A', 1, ignore_readiness=True) == (str(expected),) * 3
        assert expected.read_bytes() == b'prepared input'
        assert unqualified.read_bytes() == b'project-owned input'
        assert source.read_bytes() == b'prepared input'
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('missing_ok', [False, True])
def test_forwarded_file_deletion_preserves_other_owners(tmp_path, missing_ok):
    workflow = MicroWorkflow(project_dir=tmp_path, runner='direct')
    workflow.graph([('A', 'B'), ('C', 'B')])
    root = tmp_path / 'node' / 'B' / 'input'
    owned = root / 'A' / 'evidence' / 'source.txt'
    preserved = [root / 'evidence' / 'source.txt', root / 'C' / 'evidence' / 'source.txt']
    for path in [owned, *preserved]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('keep unless owned by A', encoding='utf-8')

    @workflow.task('A')
    def remove(ctx):
        NodeInputFileSystem('B', base='evidence').file(ctx, 'source.txt').delete(missing_ok=missing_ok)
        return str(ctx.node('B').input_dir)

    try:
        workflow.start('A')
        directory = workflow.run_job('A', 1, ignore_readiness=True)
        assert not owned.exists()
        assert all(path.read_text(encoding='utf-8') == 'keep unless owned by A' for path in preserved)
        assert directory == str(root / 'A')
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('operation', [
    'context-recursive', 'context-pattern', 'filesystem-recursive', 'filesystem-pattern',
    'entry-recursive', 'entry-pattern', 'bulk-recursive', 'bulk-pattern',
    'storage-recursive', 'storage-pattern', 'forwarded-recursive', 'forwarded-pattern',
])
def test_receiving_input_refuses_recursive_discovery(tmp_path, operation):
    workflow = MicroWorkflow(project_dir=tmp_path, runner='direct')
    workflow.graph([('A', 'B'), ('B', 'C')])
    path = tmp_path / 'node' / 'B' / 'input' / 'A' / 'nested' / 'value.json'
    path.parent.mkdir(parents=True)
    path.write_text('{"value": 7}', encoding='utf-8')

    @workflow.task('B')
    def read(ctx):
        incoming = InputFileSystem()
        entry = incoming.bind(ctx)
        forwarded = NodeInputFileSystem('C').bind(ctx)
        with pytest.raises(ValueError, match='fixed-depth'):
            if operation == 'context-recursive':
                ctx.input_files('*.json', recursive=True)
            elif operation == 'context-pattern':
                ctx.input_files('**/*.json')
            elif operation == 'filesystem-recursive':
                incoming.files(ctx, '*.json', recursive=True)
            elif operation == 'filesystem-pattern':
                incoming.files(ctx, '**/*.json')
            elif operation == 'entry-recursive':
                entry.rglob('*.json')
            elif operation == 'entry-pattern':
                entry.glob('**/*.json')
            elif operation == 'bulk-recursive':
                entry.read_jsons(recursive=True)
            elif operation == 'bulk-pattern':
                entry.read_jsons('**/*.json')
            elif operation == 'storage-recursive':
                workflow.storage.input_files('B', '*.json', recursive=True)
            elif operation == 'storage-pattern':
                workflow.storage.input_files('B', '**/*.json')
            elif operation == 'forwarded-recursive':
                forwarded.rglob('*.json')
            else:
                forwarded.glob('**/*.json')
        return 'refused'

    try:
        workflow.start('B')
        assert workflow.run_job('B', 1, ignore_readiness=True) == 'refused'
        assert path.read_text(encoding='utf-8') == '{"value": 7}'
    finally:
        _close(workflow.storage)


def test_input_reads_have_fixed_depth_and_output_reads_keep_recursion(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner='direct')
    workflow.graph([('A', 'B')])
    for scope in ('input', 'output'):
        root = tmp_path / 'node' / 'B' / scope / 'A'
        (root / 'nested').mkdir(parents=True)
        (root / 'one.json').write_text('{"value": 1}', encoding='utf-8')
        (root / 'nested' / 'two.json').write_text('{"value": 2}', encoding='utf-8')

    @workflow.task('B')
    def read(ctx):
        assert ctx.input_path('A', 'one.json').read_text(encoding='utf-8') == '{"value": 1}'
        assert [path.name for path in ctx.input_files('A/*.json')] == ['one.json']
        assert [entry.name for entry in InputFileSystem().files(ctx, 'A/nested/*.json')] == ['two.json']
        assert InputFileSystem(base='A').bind(ctx).read_jsons() == [('one.json', {'value': 1})]
        assert InputFileSystem(base='A').bind(ctx).read_jsons('nested/*.json') == [
            ('nested/two.json', {'value': 2}),
        ]
        assert OutputFileSystem(base='A').bind(ctx).read_jsons() == [
            ('nested/two.json', {'value': 2}), ('one.json', {'value': 1}),
        ]
        assert [path.name for path in ctx.output_files('*.json', recursive=True)] == ['two.json', 'one.json']
        return 'read'

    try:
        workflow.start('B')
        assert workflow.run_job('B', 1, ignore_readiness=True) == 'read'
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('operation', ['text', 'bytes', 'batch', 'copy', 'append', 'delete', 'path'])
def test_forwarding_refuses_links_into_another_producers_input(tmp_path, operation):
    workflow = MicroWorkflow(project_dir=tmp_path, runner='direct')
    workflow.graph([('A', 'B'), ('C', 'B')])
    receiver = tmp_path / 'node' / 'B' / 'input'
    other = receiver / 'C'
    other.mkdir()
    original = other / 'source.txt'
    original.write_bytes(b'owned by C')
    source = tmp_path / 'source.txt'
    source.write_bytes(b'owned by A')
    link = receiver / 'A'
    if os.name == 'nt':
        result = subprocess.run(['cmd', '/c', 'mklink', '/J', str(link), str(other)],
                                capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
        assert result.returncode == 0, result.stdout + result.stderr
    else:
        link.symlink_to(other, target_is_directory=True)

    @workflow.task('A')
    def publish(ctx):
        handle = ctx.node('B')
        entry = NodeInputFileSystem('B').file(ctx, 'source.txt')
        with pytest.raises(ValueError, match='Unsafe.*input'):
            if operation == 'text':
                handle.write_input('source.txt', 'owned by A', overwrite=True)
            elif operation == 'bytes':
                handle.write_input_bytes('source.txt', b'owned by A', overwrite=True)
            elif operation == 'batch':
                handle.write_inputs([('source.txt', 'owned by A')], overwrite=True)
            elif operation == 'copy':
                handle.add_input_file(source, overwrite=True)
            elif operation == 'append':
                entry.append_text('owned by A')
            elif operation == 'delete':
                entry.delete()
            else:
                handle.input_path('source.txt')
        return 'refused'

    try:
        workflow.start('A')
        assert workflow.run_job('A', 1, ignore_readiness=True) == 'refused'
        assert original.read_bytes() == b'owned by C'
        assert source.read_bytes() == b'owned by A'
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('formatted', [False, True])
def test_batch_json_forwarding_uses_the_bound_filesystem_base(tmp_path, formatted):
    workflow = MicroWorkflow(project_dir=tmp_path, runner='direct')
    workflow.graph([('A', 'B')])
    target = NodeInputFileSystem('B', base='evidence/{chapter}' if formatted else 'evidence/three')

    @workflow.task('A')
    def publish(ctx):
        values = {'chapter': 'three'} if formatted else {}
        paths = target.write_jsons(ctx, [('source.json', {'value': 7})], overwrite=True, **values)
        assert paths == [target.file(ctx, 'source.json', **values).path]
        return str(paths[0])

    @workflow.task('B')
    def read(ctx):
        return InputFileSystem(base='A/evidence/three').file(ctx, 'source.json').read_json()

    try:
        workflow.start('A', job_id=7)
        assert workflow.run_job('A', 7, ignore_readiness=True) == str(
            tmp_path / 'node' / 'B' / 'input' / 'A' / 'evidence' / 'three' / 'source.json',
        )
        workflow.start('B')
        assert workflow.run_job('B', 1, ignore_readiness=True) == {'value': 7}
        assert not (tmp_path / 'node' / 'B' / 'input' / 'A' / 'source.json').exists()
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('operation', ['text', 'bytes', 'batch', 'json-batch', 'copy'])
@pytest.mark.parametrize('filename', ['', '.', './', '../C/keep.txt', 'nested/../../C/keep.txt', 'absolute'])
def test_invalid_forwarded_filenames_refuse_before_changing_input(tmp_path, operation, filename):
    workflow = MicroWorkflow(project_dir=tmp_path, runner='direct')
    workflow.graph([('A', 'B'), ('C', 'B')])
    receiver = tmp_path / 'node' / 'B' / 'input'
    preserved = receiver / 'C' / 'keep.txt'
    preserved.parent.mkdir()
    preserved.write_bytes(b'owned by C')
    source = tmp_path / 'copy-source.txt'
    source.write_bytes(b'copy source')
    selected = str(tmp_path / 'outside.txt') if filename == 'absolute' else filename

    @workflow.task('A')
    def publish(ctx):
        handle = ctx.node('B')
        with pytest.raises(ValueError):
            if operation == 'text':
                handle.write_input(selected, 'invalid', overwrite=True)
            elif operation == 'bytes':
                handle.write_input_bytes(selected, b'invalid', overwrite=True)
            elif operation == 'batch':
                handle.write_inputs([('first.txt', 'must not appear'), (selected, 'invalid')], overwrite=True)
            elif operation == 'json-batch':
                NodeInputFileSystem('B', base='evidence').write_jsons(
                    ctx, [('first.json', {'invalid': False}), (selected, {'invalid': True})], overwrite=True,
                )
            else:
                handle.add_input_file(source, filename=selected, overwrite=True)
        return 'refused'

    try:
        workflow.start('A')
        assert workflow.run_job('A', 1, ignore_readiness=True) == 'refused'
        assert preserved.read_bytes() == b'owned by C'
        assert not (receiver / 'A').exists()
        assert not (tmp_path / 'outside.txt').exists()
        assert source.read_bytes() == b'copy source'
    finally:
        _close(workflow.storage)


def test_cyclic_producers_keep_separate_raw_node_paths_for_plural_copies(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner='direct')
    workflow.graph([('A', 'X'), ('X', 'A'), ('A', 'B'), ('X', 'B')])
    sources = {}
    for producer in ('A', 'X'):
        folder = tmp_path / 'sources' / producer
        folder.mkdir(parents=True)
        sources[producer] = [folder / 'one.txt', folder / 'two.txt']
        for path in sources[producer]:
            path.write_text(f'{producer}/{path.name}', encoding='utf-8')
    unqualified = tmp_path / 'node' / 'B' / 'input' / 'one.txt'
    unqualified.write_bytes(b'project-owned input')

    @workflow.task('A')
    def publish_a(ctx):
        return ctx.node('B').add_files(sources['A'])

    @workflow.task('X')
    def publish_x(ctx):
        return ctx.node('B').add_input_files(sources['X'])

    try:
        assert workflow.component_id('A') == ('A', 'X')
        for producer, job_id in [('A', 7), ('X', 9)]:
            workflow.start(producer, job_id=job_id)
            paths = workflow.run_job(producer, job_id, ignore_readiness=True)
            expected = [tmp_path / 'node' / 'B' / 'input' / producer / filename
                        for filename in ('one.txt', 'two.txt')]
            assert paths == expected
            assert [path.read_text(encoding='utf-8') for path in paths] == [
                f'{producer}/one.txt', f'{producer}/two.txt',
            ]
        assert unqualified.read_bytes() == b'project-owned input'
        assert sorted(path.name for path in unqualified.parent.iterdir()) == ['A', 'X', 'one.txt']
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('operation', ['text', 'bytes', 'batch', 'copy', 'append', 'delete', 'json-batch'])
def test_stale_forwarding_handles_cannot_change_producer_input(tmp_path, operation):
    workflow = MicroWorkflow(project_dir=tmp_path, runner='direct')
    workflow.graph([('A', 'B')])
    captured = []
    source = tmp_path / 'source.txt'
    source.write_bytes(b'stale replacement')

    @workflow.task('A')
    def publish(ctx):
        handle = ctx.node('B')
        handle.write_input('source.txt', 'original input', overwrite=True)
        captured.append((ctx, handle, NodeInputFileSystem('B').file(ctx, 'source.txt')))

    try:
        workflow.start('A')
        workflow.run_job('A', 1, ignore_readiness=True)
        (ctx, handle, entry), = captured
        target = NodeInputFileSystem('B')
        with pytest.raises(JobRestartedError):
            if operation == 'text':
                handle.write_input('source.txt', 'stale replacement', overwrite=True)
            elif operation == 'bytes':
                handle.write_input_bytes('source.txt', b'stale replacement', overwrite=True)
            elif operation == 'batch':
                handle.write_inputs([('source.txt', 'stale replacement')], overwrite=True)
            elif operation == 'copy':
                handle.add_input_file(source, overwrite=True)
            elif operation == 'append':
                entry.append_text('stale replacement')
            elif operation == 'delete':
                entry.delete()
            else:
                target.write_jsons(ctx, [('source.txt', {'stale': True})], overwrite=True)
        assert (tmp_path / 'node' / 'B' / 'input' / 'A' / 'source.txt').read_bytes() == b'original input'
        assert not (tmp_path / 'node' / 'B' / 'input' / 'source.txt').exists()
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('runner', ['direct', 'threaded', 'api', 'process'])
def test_full_run_forwards_input_to_the_receivers_declared_producer_path(tmp_path, monkeypatch, runner):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('A', 'B')]", runner=runner,
        files={
            'A': '''
                from micro_workflow_manager import NodeInputFileSystem, NodeRouter
                router = NodeRouter('A')
                router.create_job(number=1)
                target = NodeInputFileSystem('B', base='evidence')
                @router.task
                def publish(ctx):
                    target.file(ctx, 'source.json').write_json({'value': 17})
            ''',
            'B': '''
                from micro_workflow_manager import InputFileSystem, NodeRouter
                router = NodeRouter('B')
                router.create_job(number=1)
                incoming = InputFileSystem(base='A/evidence')
                @router.task
                def read(ctx):
                    value = incoming.file(ctx, 'source.json').read_json()
                    ctx.write_output('received.txt', str(value['value']))
                    return value
            ''',
        },
    )
    assert cli.main(['runfrom', 'A', '--runner', runner]) == 0
    storage = FileStorage(tmp_path)
    try:
        assert (tmp_path / 'node' / 'B' / 'output' / 'received.txt').read_text(encoding='utf-8') == '17'
        assert (tmp_path / 'node' / 'B' / 'input' / 'A' / 'evidence' / 'source.json').is_file()
        assert not (tmp_path / 'node' / 'B' / 'input' / 'evidence' / 'source.json').exists()
        assert storage.get_job_status('A', 1) == storage.get_job_status('B', 1) == 'done'
        assert storage.get_component_state(('A',))['lifecycle'] == 'done'
        assert storage.get_component_state(('B',))['lifecycle'] == 'done'
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'done')
    finally:
        _close(storage)
