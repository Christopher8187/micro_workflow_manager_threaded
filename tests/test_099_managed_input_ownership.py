from __future__ import annotations

import os
import sqlite3
import stat
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from threading import Event, Thread
from uuid import uuid4

import pytest

from micro_workflow_manager import MicroWorkflow, NodeInputFileSystem
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.input_publication_files import InputFileChange
from tests.test_090_component_session_settlement import _close


@pytest.mark.parametrize('selected', [False, True])
@pytest.mark.parametrize('operation', ['text', 'bytes', 'batch', 'copy', 'filesystem', 'json-batch', 'append'])
def test_forwarded_input_retains_exact_producing_execution_without_trace(tmp_path, selected, operation):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'X'), ('X', 'A'), ('A', 'B')])
    storage = workflow.storage
    source = tmp_path / 'source.txt'
    source.write_bytes(b'produced input')
    relative = 'A/evidence/source.txt'
    expected_path = tmp_path / 'node' / 'B' / 'input' / relative
    observed = []

    @workflow.task('A')
    def produce(ctx):
        handle = ctx.node('B')
        entry = NodeInputFileSystem('B', base='evidence').file(ctx, 'source.txt')
        if operation == 'text':
            result = handle.write_input('evidence/source.txt', 'produced input')
        elif operation == 'bytes':
            result = handle.write_input_bytes('evidence/source.txt', b'produced input')
        elif operation == 'batch':
            result, = handle.write_inputs([('evidence/source.txt', 'produced input')])
        elif operation == 'copy':
            result = handle.add_input_file(source, filename='evidence/source.txt')
        elif operation == 'filesystem':
            result = entry.write_bytes(b'produced input')
        elif operation == 'json-batch':
            result, = NodeInputFileSystem('B', base='evidence').write_jsons(ctx, [('source.txt', {'value': 7})])
        else:
            result = entry.append_text('produced input')
        assert result == expected_path
        owner = storage.read_job_current_owner('A', 7)
        assert storage.read_node_input_owner('B', relative) == owner
        observed.append(owner)
        return 'published'

    @workflow.task('X')
    def peer(ctx):
        return None

    try:
        workflow.start('A', job_id=7)
        if selected:
            assert workflow.run_job('A', 7, ignore_readiness=True) == 'published'
        else:
            workflow.run_component(('A', 'X'), ignore_readiness=True)
        owner, = observed
        storage.clear_job_events('A', [7])
        assert storage.read_job_events('A', 7) == []
        assert storage.read_node_input_owner('B', relative) == owner
        assert owner['component'] == ('A', 'X') and owner['node_name'] == 'A'
        assert owner['job_id'] == 7 and owner['alignment_generation'] == (0 if selected else 1)
        assert expected_path.is_file()
        assert storage.read_node_input_owner('B', 'project-owned.txt') is None
    finally:
        _close(storage)
    reopened = FileStorage(tmp_path)
    try:
        assert reopened.read_node_input_owner('B', relative) == owner
    finally:
        _close(reopened)


def test_deleting_forwarded_input_removes_its_owner_and_preserves_other_input(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('C', 'B')])
    storage = workflow.storage
    receiver = tmp_path / 'node' / 'B' / 'input'
    project_input = receiver / 'keep.txt'
    project_input.write_bytes(b'project input')

    @workflow.task('A')
    def produce(ctx):
        entry = NodeInputFileSystem('B').file(ctx, 'remove.txt')
        entry.write_text('temporary')
        assert storage.read_node_input_owner('B', 'A/remove.txt') == storage.read_job_current_owner('A', 1)
        entry.delete(missing_ok=False)
        assert storage.read_node_input_owner('B', 'A/remove.txt') is None
        assert not entry.path.exists()
        entry.delete()
        assert project_input.read_bytes() == b'project input'

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
        assert project_input.read_bytes() == b'project input'
        assert storage.read_node_input_owner('B', 'keep.txt') is None
    finally:
        _close(storage)


def test_failed_forwarded_batch_restores_prior_bytes_and_owner(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    receiver = tmp_path / 'node' / 'B' / 'input' / 'A'
    original_replace = os.replace
    injected = []

    @workflow.task('A')
    def produce(ctx):
        handle = ctx.node('B')
        handle.write_input('keep.txt', 'original')
        owner = storage.read_node_input_owner('B', 'A/keep.txt')
        assert owner == storage.read_job_current_owner('A', 1)

        def fail_second(source, destination, *args, **kwargs):
            if Path(destination) == receiver / 'fail.txt' and not injected:
                injected.append(True)
                raise OSError('injected second publication failure')
            return original_replace(source, destination, *args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(os, 'replace', fail_second)
            with pytest.raises(OSError, match='injected second publication failure'):
                handle.write_inputs([('keep.txt', 'replacement'), ('fail.txt', 'new')], overwrite=True)
        assert injected == [True]
        assert (receiver / 'keep.txt').read_bytes() == b'original'
        assert not (receiver / 'fail.txt').exists()
        assert storage.read_node_input_owner('B', 'A/keep.txt') == owner
        assert storage.read_node_input_owner('B', 'A/fail.txt') is None
        assert sorted(path.name for path in receiver.iterdir()) == ['keep.txt']
        handle.write_input('after.txt', 'still usable')
        assert storage.read_node_input_owner('B', 'A/after.txt') == owner

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
    finally:
        _close(storage)


@pytest.mark.parametrize('operation', ['overwrite', 'append'])
def test_managed_write_preserves_an_unowned_predecessor_as_ambiguous_ownership(tmp_path, operation):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    path = tmp_path / 'node' / 'B' / 'input' / 'A' / 'existing.txt'
    path.parent.mkdir()
    path.write_bytes(b'project input')

    @workflow.task('A')
    def produce(ctx):
        entry = NodeInputFileSystem('B').file(ctx, 'existing.txt')
        if operation == 'overwrite':
            entry.write_text('managed replacement')
        else:
            entry.append_text(' plus managed input')
        with pytest.raises(RuntimeError, match='[Aa]mbiguous'):
            storage.read_node_input_owner('B', 'A/existing.txt')

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
        assert path.read_bytes() == (b'managed replacement' if operation == 'overwrite'
                                     else b'project input plus managed input')
        storage.clear_job_events('A', [1])
        with pytest.raises(RuntimeError, match='[Aa]mbiguous'):
            storage.read_node_input_owner('B', 'A/existing.txt')
    finally:
        _close(storage)


@pytest.mark.parametrize('operation', ['overwrite', 'append'])
def test_distinct_executions_sharing_a_visible_input_keep_ambiguous_ownership(tmp_path, operation):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage

    @workflow.task('A')
    def produce(ctx):
        entry = NodeInputFileSystem('B').file(ctx, 'shared.txt')
        if ctx.job_id == 1:
            entry.write_text('first')
            assert storage.read_node_input_owner('B', 'A/shared.txt') == storage.read_job_current_owner('A', 1)
        elif operation == 'overwrite':
            entry.write_text('second')
        else:
            entry.append_text(' second')

    try:
        for job_id in (1, 2):
            workflow.start('A', job_id=job_id)
            workflow.run_job('A', job_id, ignore_readiness=True)
        path = tmp_path / 'node' / 'B' / 'input' / 'A' / 'shared.txt'
        assert path.read_bytes() == (b'second' if operation == 'overwrite' else b'first second')
        storage.clear_job_events('A', [1, 2])
        with pytest.raises(RuntimeError, match='[Aa]mbiguous'):
            storage.read_node_input_owner('B', 'A/shared.txt')
    finally:
        _close(storage)


@pytest.mark.parametrize('failure', ['transaction', 'event'])
def test_failed_publication_metadata_restores_all_bytes_and_ownership(tmp_path, monkeypatch, failure):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    receiver = tmp_path / 'node' / 'B' / 'input' / 'A'
    injected = []

    @workflow.task('A')
    def produce(ctx):
        handle = ctx.node('B')
        handle.write_input('keep.txt', 'original')
        owner = storage.read_node_input_owner('B', 'A/keep.txt')
        real_transaction = storage.db_transaction
        before = storage.db_connection().execute('SELECT max(rowid) FROM input_publications').fetchone()[0]

        @contextmanager
        def failing_transaction(**kwargs):
            with real_transaction(**kwargs) as connection:
                yield connection
                committed = connection.execute(
                    "SELECT 1 FROM input_publications WHERE rowid>? AND state='committed'", (before,),
                ).fetchone()
                if committed and not injected:
                    assert (receiver / 'keep.txt').read_bytes() == b'replacement'
                    injected.append(True)
                    raise OSError('injected publication commit failure')

        if failure == 'event':
            storage.submit_db_mutation(lambda connection: connection.execute('''
                CREATE TRIGGER refuse_input_event BEFORE INSERT ON job_events
                WHEN NEW.event='input_forwarded'
                BEGIN SELECT RAISE(ABORT, 'injected publication event failure'); END
            '''))
        try:
            with monkeypatch.context() as patch:
                if failure == 'transaction':
                    patch.setattr(storage, 'db_transaction', failing_transaction)
                error = OSError if failure == 'transaction' else sqlite3.IntegrityError
                with pytest.raises(error, match='injected publication'):
                    handle.write_inputs([('keep.txt', 'replacement'), ('new.txt', 'new')], overwrite=True)
        finally:
            if failure == 'event':
                storage.submit_db_mutation(lambda connection: connection.execute('DROP TRIGGER IF EXISTS refuse_input_event'))
        assert (receiver / 'keep.txt').read_bytes() == b'original'
        assert not (receiver / 'new.txt').exists()
        assert storage.read_node_input_owner('B', 'A/keep.txt') == owner
        assert storage.read_node_input_owner('B', 'A/new.txt') is None
        assert len([event for event in storage.read_job_events('A', 1) if event['event'] == 'input_forwarded']) == 1
        handle.write_input('after.txt', 'still usable')

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
        if failure == 'transaction':
            assert injected == [True]
    finally:
        _close(storage)


def test_notification_failure_after_commit_preserves_published_bytes_and_owner(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    injected = []

    @workflow.task('A')
    def produce(ctx):
        original = storage.notify_state_change

        def fail_notification():
            row = storage.db_connection().execute("SELECT 1 FROM input_publications WHERE state='committed'").fetchone()
            if row and not injected:
                injected.append(True)
                raise OSError('injected notification failure')
            original()

        with monkeypatch.context() as patch:
            patch.setattr(storage, 'notify_state_change', fail_notification)
            with pytest.warns(RuntimeWarning, match='committed.*notification failed'):
                result = ctx.node('B').write_input('value.txt', 'durable')
        assert result.read_bytes() == b'durable'
        assert storage.read_node_input_owner('B', 'A/value.txt') == storage.read_job_current_owner('A', 1)

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
        assert injected == [True]
    finally:
        _close(storage)


def test_unique_name_publication_records_the_actual_returned_paths(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    receiver = tmp_path / 'node' / 'B' / 'input' / 'A'
    receiver.mkdir()
    (receiver / 'value.txt').write_bytes(b'project input')

    @workflow.task('A')
    def produce(ctx):
        paths = ctx.node('B').write_inputs([('value.txt', 'first'), ('value_2.txt', 'second')])
        assert paths == [receiver / 'value_3.txt', receiver / 'value_2.txt']
        owner = storage.read_job_current_owner('A', 1)
        assert all(storage.read_node_input_owner('B', 'A/' + path.name) == owner for path in paths)
        assert storage.read_node_input_owner('B', 'A/value.txt') is None

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
        assert (receiver / 'value.txt').read_bytes() == b'project input'
    finally:
        _close(storage)


@pytest.mark.parametrize('read_only_first', [False, True])
def test_plural_copy_failure_restores_the_entire_publication(tmp_path, monkeypatch, read_only_first):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    receiver = tmp_path / 'node' / 'B' / 'input' / 'A'
    sources = [tmp_path / name for name in ('first.txt', 'second.txt')]
    for source in sources:
        source.write_bytes(b'copied payload')
    if read_only_first:
        sources[0].chmod(stat.S_IREAD)
    original = os.replace
    injected = []

    @workflow.task('A')
    def produce(ctx):
        def fail_second(source, target):
            if Path(target) == receiver / 'second.txt' and not injected:
                injected.append(True)
                raise OSError('injected plural copy failure')
            return original(source, target)

        with monkeypatch.context() as patch:
            patch.setattr(os, 'replace', fail_second)
            with pytest.raises(OSError, match='injected plural copy failure'):
                ctx.node('B').add_input_files(sources)
        assert not receiver.exists()
        assert storage.read_node_input_owner('B', 'A/first.txt') is None
        assert storage.read_node_input_owner('B', 'A/second.txt') is None

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
        assert injected == [True]
        assert all(source.read_bytes() == b'copied payload' for source in sources)
    finally:
        sources[0].chmod(stat.S_IREAD | stat.S_IWRITE)
        _close(storage)


def test_reservation_change_before_publication_decision_restores_prior_input(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    receiver = tmp_path / 'node' / 'B' / 'input' / 'A'
    original = os.replace
    injected = []

    @workflow.task('A')
    def produce(ctx):
        handle = ctx.node('B')
        handle.write_input('value.txt', 'original')
        owner = storage.read_job_current_owner('A', 1)

        def revoke_after_replace(source, target):
            result = original(source, target)
            if Path(target) == receiver / 'value.txt' and not injected:
                injected.append(True)
                storage.submit_db_mutation(lambda connection: connection.execute(
                    'DELETE FROM component_reservations WHERE session_id=?', (owner['session_id'],),
                ))
            return result

        try:
            with monkeypatch.context() as patch:
                patch.setattr(os, 'replace', revoke_after_replace)
                with pytest.raises(RuntimeError, match='no longer owns'):
                    handle.write_input('value.txt', 'replacement', overwrite=True)
        finally:
            storage.submit_db_mutation(lambda connection: connection.execute(
                'INSERT INTO component_reservations VALUES(?,?)', ('["A"]', owner['session_id']),
            ))
        assert (receiver / 'value.txt').read_bytes() == b'original'
        assert storage.read_node_input_owner('B', 'A/value.txt') == owner

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
        assert injected == [True]
    finally:
        _close(storage)


def test_owner_read_waits_for_the_file_publication_decision(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    reader = FileStorage(tmp_path)
    target = tmp_path / 'node' / 'B' / 'input' / 'A' / 'value.txt'
    original = os.replace
    started, finished = Event(), Event()
    observed, errors = [], []

    def read_owner():
        started.set()
        try:
            observed.append(reader.read_node_input_owner('B', 'A/value.txt'))
        except BaseException as error:
            errors.append(error)
        finally:
            reader.close_thread_connection()
            finished.set()

    thread = Thread(target=read_owner)

    @workflow.task('A')
    def produce(ctx):
        def pause_after_replace(source, destination):
            result = original(source, destination)
            if Path(destination) == target:
                thread.start()
                assert started.wait(5)
                assert not finished.wait(0.2), 'owner read returned before the publication decision'
            return result

        with monkeypatch.context() as patch:
            patch.setattr(os, 'replace', pause_after_replace)
            ctx.node('B').write_input('value.txt', 'published')
        assert finished.wait(5)
        assert errors == []
        assert observed == [storage.read_job_current_owner('A', 1)]

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
    finally:
        if thread.ident is not None:
            thread.join(5)
        _close(reader)
        _close(storage)


def test_owner_read_refuses_an_unfinished_publication(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage

    @workflow.task('A')
    def produce(ctx):
        path = ctx.node('B').write_input('value.txt', 'original')
        owner = storage.read_job_current_owner('A', 1)
        storage.submit_db_mutation(lambda connection: connection.execute(
            "INSERT INTO input_publications VALUES(?,?,?,'prepared',?)",
            (uuid4().hex, owner['execution_id'], 'B', '[]'),
        ))
        with pytest.raises(RuntimeError, match='unfinished publication'):
            storage.read_node_input_owner('B', 'A/value.txt')
        assert path.read_bytes() == b'original'

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
    finally:
        _close(storage)


@pytest.mark.parametrize('secondary', ['receipt-read', 'restore', 'abort', 'read-only-restore'])
def test_publication_cleanup_failure_keeps_the_original_error_and_recovery_material(tmp_path, monkeypatch, secondary):
    class PublicationFailure(OSError):
        add_note = None

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    receiver = tmp_path / 'node' / 'B' / 'input' / 'A'
    injected = []
    cleanup_failed = []

    @workflow.task('A')
    def produce(ctx):
        handle = ctx.node('B')
        handle.write_input('keep.txt', 'original')
        original_replace = os.replace
        original_connect = storage._new_db_connection
        original_transaction = storage.db_transaction
        copy_sources = [tmp_path / 'copies' / name for name in ('keep.txt', 'fail.txt')]
        if secondary == 'read-only-restore':
            copy_sources[0].parent.mkdir()
            for source in copy_sources:
                source.write_bytes(b'replacement')
            copy_sources[0].chmod(stat.S_IREAD)

        def fail_replace(source, target):
            if Path(target) == receiver / 'fail.txt' and not injected:
                injected.append(True)
                raise PublicationFailure('original publication failure')
            if secondary in {'restore', 'read-only-restore'} and injected and Path(target) == receiver / 'keep.txt':
                cleanup_failed.append(True)
                raise OSError(f'secondary {secondary} failure')
            return original_replace(source, target)

        def fail_read():
            if secondary == 'receipt-read' and injected and not cleanup_failed:
                cleanup_failed.append(True)
                raise OSError('secondary receipt-read failure')
            return original_connect()

        @contextmanager
        def fail_abort(**kwargs):
            with original_transaction(**kwargs) as connection:
                yield connection
                if secondary == 'abort' and connection.execute(
                    "SELECT 1 FROM input_publications WHERE state='aborted'",
                ).fetchone():
                    cleanup_failed.append(True)
                    raise OSError('secondary abort failure')

        with monkeypatch.context() as patch:
            patch.setattr(os, 'replace', fail_replace)
            patch.setattr(storage, '_new_db_connection', fail_read)
            patch.setattr(storage, 'db_transaction', fail_abort)
            with pytest.raises(OSError, match='original publication failure') as caught:
                if secondary == 'read-only-restore':
                    handle.add_input_files(copy_sources, overwrite=True)
                else:
                    handle.write_inputs([('keep.txt', 'replacement'), ('fail.txt', 'new')], overwrite=True)
        assert any(f'secondary {secondary} failure' in note for note in caught.value.__notes__)
        with pytest.raises(RuntimeError, match='unfinished publication'):
            storage.read_node_input_owner('B', 'A/keep.txt')
        backups = list((tmp_path / '.mwf' / 'input-publications').glob('*/*.old'))
        assert len(backups) == 1 and backups[0].read_bytes() == b'original'
        if secondary == 'read-only-restore':
            assert not (receiver / 'keep.txt').stat().st_mode & stat.S_IWRITE
            copy_sources[0].chmod(stat.S_IREAD | stat.S_IWRITE)

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
        assert injected == cleanup_failed == [True]
    finally:
        _close(storage)


def test_read_only_copy_source_can_be_published_with_its_owner(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    source = tmp_path / 'read-only.txt'
    source.write_bytes(b'preserved read-only source')
    source.chmod(stat.S_IREAD)
    published = []

    @workflow.task('A')
    def produce(ctx):
        path = ctx.node('B').add_input_file(source)
        published.append(path)
        assert path.read_bytes() == source.read_bytes() == b'preserved read-only source'
        assert storage.read_node_input_owner('B', 'A/read-only.txt') == storage.read_job_current_owner('A', 1)

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
    finally:
        source.chmod(stat.S_IREAD | stat.S_IWRITE)
        for path in published:
            path.chmod(stat.S_IREAD | stat.S_IWRITE)
        _close(storage)


@pytest.mark.parametrize('overwrite', [False, True])
def test_plural_copies_keep_existing_same_basename_behavior(tmp_path, overwrite):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    sources = [tmp_path / name / 'shared.txt' for name in ('first', 'second')]
    for source, payload in zip(sources, (b'first', b'second')):
        source.parent.mkdir()
        source.write_bytes(payload)

    @workflow.task('A')
    def produce(ctx):
        paths = ctx.node('B').add_input_files(sources, overwrite=overwrite)
        root = tmp_path / 'node' / 'B' / 'input' / 'A'
        assert paths == [root / 'shared.txt', root / ('shared.txt' if overwrite else 'shared_2.txt')]
        assert [path.read_bytes() for path in paths] == ([b'second', b'second'] if overwrite else [b'first', b'second'])
        owner = storage.read_job_current_owner('A', 1)
        assert all(storage.read_node_input_owner('B', 'A/' + path.name) == owner for path in paths)

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
    finally:
        _close(storage)


def test_live_task_handle_can_publish_from_its_helper_thread(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage

    @workflow.task('A')
    def produce(ctx):
        handle = ctx.node('B')
        with ThreadPoolExecutor(max_workers=1) as executor:
            path = executor.submit(handle.write_input, 'value.txt', 'helper result').result(timeout=10)
        assert path.read_bytes() == b'helper result'
        assert storage.read_node_input_owner('B', 'A/value.txt') == storage.read_job_current_owner('A', 1)

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
    finally:
        _close(storage)


def test_restarted_generation_preserves_prior_file_owner_and_records_shared_path_ambiguity(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    owners = []

    @workflow.task('A')
    def produce(ctx):
        handle = ctx.node('B')
        handle.write_input('shared.txt', 'current generation', overwrite=True)
        owners.append(storage.read_job_current_owner('A', 1))
        if len(owners) == 1:
            handle.write_input('first-only.txt', 'old generation')

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
        storage.request_job_restart('A', 1)
        workflow.run_job('A', 1, ignore_readiness=True)
        assert owners[1]['generation'] > owners[0]['generation']
        storage.clear_job_events('A', [1])
        assert storage.read_node_input_owner('B', 'A/first-only.txt') == owners[0]
        with pytest.raises(RuntimeError, match='[Aa]mbiguous'):
            storage.read_node_input_owner('B', 'A/shared.txt')
    finally:
        _close(storage)


@pytest.mark.parametrize('generation', [False, 0.0, '0', -1, None])
def test_managed_input_storage_refuses_invalid_generation_before_mutation(tmp_path, generation):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage

    @workflow.task('A')
    def produce(ctx):
        owner = storage.read_job_current_owner('A', 1)
        with pytest.raises(ValueError, match='generation'):
            storage.publish_managed_inputs('A', 1, generation, owner['execution_id'], 'B',
                                           [InputFileChange('invalid.txt', 'text', 'invalid')])
        assert storage.read_node_input_owner('B', 'A/invalid.txt') is None
        assert not (tmp_path / 'node' / 'B' / 'input' / 'A').exists()

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
    finally:
        _close(storage)


def test_failed_replacement_preserves_a_read_only_predecessor(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    target = tmp_path / 'node' / 'B' / 'input' / 'A' / 'value.txt'
    target.parent.mkdir()
    target.write_bytes(b'read-only project input')
    target.chmod(stat.S_IREAD)
    original = os.replace
    injected = []

    @workflow.task('A')
    def produce(ctx):
        def fail_target(source, destination):
            if Path(destination) == target and not injected:
                injected.append(True)
                raise OSError('injected read-only target replacement failure')
            return original(source, destination)

        with monkeypatch.context() as patch:
            patch.setattr(os, 'replace', fail_target)
            with pytest.raises(OSError, match='injected read-only target replacement failure'):
                ctx.node('B').write_input('value.txt', 'replacement', overwrite=True)
        assert target.read_bytes() == b'read-only project input'
        assert not target.stat().st_mode & stat.S_IWRITE
        assert storage.read_node_input_owner('B', 'A/value.txt') is None
        assert not list((tmp_path / '.mwf' / 'input-publications').glob('*/*'))

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
        assert injected == [True]
    finally:
        target.chmod(stat.S_IREAD | stat.S_IWRITE)
        _close(storage)


@pytest.mark.parametrize('failure', ['second-file', 'transaction'])
def test_failed_repeated_target_batch_restores_one_original_file_and_owner(tmp_path, monkeypatch, failure):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    sources = [tmp_path / name / 'shared.txt' for name in ('first', 'second')]
    for source, payload in zip(sources, (b'first', b'second')):
        source.parent.mkdir()
        source.write_bytes(payload)
    target = tmp_path / 'node' / 'B' / 'input' / 'A' / 'shared.txt'
    injected = []

    @workflow.task('A')
    def produce(ctx):
        handle = ctx.node('B')
        handle.write_input('shared.txt', 'original')
        owner = storage.read_node_input_owner('B', 'A/shared.txt')
        before_events = storage.read_job_events('A', 1)
        before_receipt = storage.db_connection().execute('SELECT max(rowid) FROM input_publications').fetchone()[0]
        original_replace = os.replace
        original_transaction = storage.db_transaction
        replacements = []

        def fail_second(source, destination):
            if Path(destination) == target:
                replacements.append(True)
                if len(replacements) == 2:
                    injected.append(True)
                    raise OSError('injected repeated-target publication failure')
            return original_replace(source, destination)

        @contextmanager
        def fail_commit(**kwargs):
            with original_transaction(**kwargs) as connection:
                yield connection
                if not injected and connection.execute(
                    "SELECT 1 FROM input_publications WHERE rowid>? AND state='committed'", (before_receipt,),
                ).fetchone():
                    injected.append(True)
                    raise OSError('injected repeated-target publication failure')

        with monkeypatch.context() as patch:
            if failure == 'second-file':
                patch.setattr(os, 'replace', fail_second)
            else:
                patch.setattr(storage, 'db_transaction', fail_commit)
            with pytest.raises(OSError, match='injected repeated-target publication failure'):
                handle.add_input_files(sources, overwrite=True)
        assert target.read_bytes() == b'original'
        assert storage.read_node_input_owner('B', 'A/shared.txt') == owner
        assert storage.read_job_events('A', 1) == before_events
        assert not list((tmp_path / '.mwf' / 'input-publications').glob('*/*'))

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
        assert injected == [True]
    finally:
        _close(storage)


def test_file_name_case_aliases_resolve_to_the_existing_managed_path(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage

    @workflow.task('A')
    def produce(ctx):
        handle = ctx.node('B')
        handle.write_input('Value.txt', 'first')
        handle.write_input('value.txt', 'second', overwrite=True)
        owner = storage.read_job_current_owner('A', 1)
        assert storage.read_node_input_owner('B', 'A/Value.txt') == owner
        assert storage.read_node_input_owner('B', 'A/value.txt') == owner

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
    finally:
        _close(storage)


def test_existing_case_variant_directory_keeps_the_exact_producer_raw_node(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    (tmp_path / 'node' / 'B' / 'input' / 'a').mkdir()

    @workflow.task('A')
    def produce(ctx):
        path = ctx.node('B').write_input('value.txt', 'produced')
        owner = storage.read_node_input_owner('B', 'A/value.txt')
        assert owner == storage.read_job_current_owner('A', 1)
        assert owner['node_name'] == 'A'
        assert path.read_bytes() == b'produced'

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
    finally:
        _close(storage)


@pytest.mark.parametrize('edges', [[('A', 'B'), ('a', 'B')], [('A', 'B'), ('A', 'b')]])
def test_distinct_raw_nodes_sharing_storage_refuse_managed_publication(tmp_path, edges):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph(edges)
    storage = workflow.storage

    @workflow.task('A')
    def produce(ctx):
        if os.path.normcase('A') == os.path.normcase('a'):
            with pytest.raises(RuntimeError, match='raw node names share filesystem storage'):
                ctx.node('B').write_input('value.txt', 'produced')
            assert not (tmp_path / '.mwf' / 'input-publications').exists()
            assert not (tmp_path / 'node' / 'B' / 'input' / 'A' / 'value.txt').exists()
        else:
            ctx.node('B').write_input('value.txt', 'produced')
            assert storage.read_node_input_owner('B', 'A/value.txt') == storage.read_job_current_owner('A', 1)

    workflow.start('A')
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
    finally:
        _close(storage)
