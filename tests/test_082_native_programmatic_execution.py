from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event, current_thread

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.errors import JobRestartedError, JobTimeoutError


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.perf_counter() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.perf_counter() < deadline, 'Mutation writer did not retire'
        time.sleep(0.01)
    storage.close_database_connections()


@pytest.mark.parametrize('entry', ['run_job', 'run_one', 'start'])
def test_single_programmatic_execution_has_one_native_owner(tmp_path, entry):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner='direct')
    observed = []

    @router.task
    def work(ctx):
        assert ctx.execution_id is not None, 'Programmatic execution has no native claim'
        main = workflow.storage.get_live_main_session()
        assert main is not None
        owner = workflow.storage.get_job_execution_owner(ctx.execution_id)
        assert owner['session_id'] == main['session_id']
        assert owner['component'] == ('A',)
        assert workflow.storage.get_component_reservation(('A',))['session_id'] == main['session_id']
        observed.append((ctx.execution_id, main['session_id']))
        return 42

    workflow.include_routers(router)
    try:
        if entry == 'run_job':
            job = workflow.add_job(None, 'A')
            result = workflow.run_job('A', job.job_id)
        elif entry == 'run_one':
            result = workflow.run_one('A')
        else:
            result = workflow.start('A', autostart=True)
        assert result == 42
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        session = sessions[0]
        assert session['session_kind'] == 'main'
        assert session['status'] == 'terminal'
        assert session['outcome'] == 'done'
        assert session['selected_components'] == [('A',)]
        assert session['selected_jobs'] == [('A', 1)]
        started = [event for event in workflow.storage.read_job_events('A', 1) if event['event'] == 'started']
        assert len(started) == 1
        assert observed == [(started[0]['execution_id'], session['session_id'])]
        assert workflow.storage.get_job_status('A', 1) == 'done'
        assert workflow.storage.get_component_reservation(('A',)) is None
        assert workflow.storage.get_live_main_session() is None
        assert workflow.execution_session_context is None
        assert not (tmp_path / '.mwf' / 'run.json').exists()
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('runner', ['direct', 'threaded', 'api'])
@pytest.mark.parametrize('entry', ['run', 'run_concurrently', 'run_node', 'run_component'])
def test_outer_programmatic_schedulers_share_one_native_session(tmp_path, runner, entry):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    observed = []

    def work(ctx):
        main = workflow.storage.get_live_main_session()
        assert main is not None
        owner = workflow.storage.get_job_execution_owner(ctx.execution_id)
        assert owner['session_id'] == main['session_id']
        assert owner['component'] == ('A', 'B')
        observed.append((ctx.current_node, owner['session_id']))
        return ctx.current_node

    for node in ('A', 'B'):
        router = NodeRouter(node, runner=runner, max_threads=2)
        router.task(work)
        workflow.include_routers(router)
        workflow.add_job(None, node)
    try:
        if entry == 'run':
            workflow.run()
        elif entry == 'run_concurrently':
            workflow.run_concurrently(['A', 'B'])
        elif entry == 'run_node':
            workflow.run_node('A')
        else:
            workflow.run_component({'A', 'B'})
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        main = sessions[0]
        assert main['status'] == 'terminal'
        assert main['outcome'] == 'done'
        assert main['selected_components'] == [('A', 'B')]
        assert main['selected_jobs'] == []
        assert set(observed) == {('A', main['session_id']), ('B', main['session_id'])}
        assert all(workflow.storage.get_job_status(node, 1) == 'done' for node in ('A', 'B'))
        assert workflow.storage.get_component_reservation(('A', 'B')) is None
        assert workflow.storage.get_live_main_session() is None
        assert workflow.execution_session_context is None
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('entry', ['run_jobs', 'run_node_jobs', 'run_queued_node_jobs'])
def test_programmatic_node_job_selection_has_one_native_owner(tmp_path, entry):
    workflow = MicroWorkflow(tmp_path, runner='threaded', persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner='threaded', max_threads=2)
    observed = []

    @router.task
    def work(ctx):
        main = workflow.storage.get_live_main_session()
        owner = workflow.storage.get_job_execution_owner(ctx.execution_id)
        assert owner['session_id'] == main['session_id']
        observed.append((ctx.job_id, owner['session_id']))
        return ctx.job_id

    workflow.include_routers(router)
    jobs = [workflow.add_job(None, 'A') for _ in range(2)]
    try:
        if entry == 'run_jobs':
            workflow.run_jobs('A', [jobs[0].job_id])
        elif entry == 'run_node_jobs':
            workflow.run_node_jobs('A', [jobs[0]])
        else:
            workflow.run_queued_node_jobs('A')
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        main = sessions[0]
        assert main['outcome'] == 'done'
        expected_ids = [1, 2] if entry == 'run_queued_node_jobs' else [1]
        assert set(observed) == {(job_id, main['session_id']) for job_id in expected_ids}
        assert main['selected_jobs'] == ([] if entry == 'run_queued_node_jobs' else [('A', 1)])
        assert workflow.storage.get_job_status('A', 2) == ('done' if 2 in expected_ids else 'queued')
        assert workflow.storage.get_component_reservation(('A',)) is None
        assert workflow.storage.get_live_main_session() is None
    finally:
        _close(workflow.storage)


def test_competing_same_workflow_call_cannot_restore_a_finished_owner(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner='direct')
    entered = Event()
    finish_handler = Event()
    second_admission = Event()
    release_second = Event()
    observed = []

    @router.task
    def work(ctx):
        observed.append((ctx.job_id, workflow.execution_session_context[0]))
        if ctx.job_id == 1:
            entered.set()
            assert finish_handler.wait(20), 'First handler was not released'
        return ctx.job_id

    workflow.include_routers(router)
    workflow.add_jobs(None, 'A', [{}, {}])
    original_lock = workflow.storage.interprocess_lock

    @contextmanager
    def gate_competing_admission(name, **kwargs):
        if name == 'active-run-state' and current_thread().name.startswith('competing-entry'):
            second_admission.set()
            assert release_second.wait(20), 'Competing admission was not released'
            raise RuntimeError('injected competing admission refusal')
        with original_lock(name, **kwargs):
            yield

    monkeypatch.setattr(workflow.storage, 'interprocess_lock', gate_competing_admission)
    first_pool = ThreadPoolExecutor(max_workers=1)
    second_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='competing-entry')
    first = first_pool.submit(workflow.run_job, 'A', 1)
    try:
        assert entered.wait(10)
        original_context = workflow.execution_session_context
        second = second_pool.submit(workflow.run_job, 'A', 2)
        deadline = time.perf_counter() + 10
        while not second.done() and not second_admission.is_set():
            assert time.perf_counter() < deadline, 'Competing call neither refused nor reached admission'
            time.sleep(0.01)
        assert workflow.execution_session_context is original_context
        finish_handler.set()
        assert first.result(timeout=10) == 1
        release_second.set()
        with pytest.raises(RuntimeError):
            second.result(timeout=10)
        assert workflow.execution_session_context is None, 'Refused call restored the finished main token'
        assert not second_admission.is_set(), 'Competing call reached storage admission'
        assert workflow.storage.get_job_status('A', 2) == 'queued'
        assert workflow.storage.get_component_reservation(('A',)) is None
        assert len(workflow.storage.list_execution_sessions()) == 1
        assert workflow.run_job('A', 2) == 2
        assert len(workflow.storage.list_execution_sessions()) == 2
        assert observed[0][1] != observed[1][1]
        assert workflow.execution_session_context is None
    finally:
        finish_handler.set()
        release_second.set()
        first_pool.shutdown(wait=True)
        second_pool.shutdown(wait=True)
        _close(workflow.storage)


@pytest.mark.parametrize('invalid', ['wrong-node', 'missing-job', 'duplicate-job'])
def test_invalid_selected_jobs_refuse_before_session_or_status_mutation(tmp_path, invalid):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    ran = []
    for name in ('A', 'B'):
        router = NodeRouter(name, runner='direct')
        router.task(lambda ctx: ran.append(ctx.current_node))
        workflow.include_routers(router)
        workflow.add_job(None, name, job_id=1 if name == 'A' else 2)
    before_statuses = {name: workflow.storage.get_node_status(name) for name in ('A', 'B')}
    pairs = [('A', 1), ('B', 2)]
    before_events = {name: workflow.storage.read_job_events(name, job_id) for name, job_id in pairs}
    try:
        with pytest.raises(FileNotFoundError if invalid == 'missing-job' else ValueError):
            if invalid == 'wrong-node':
                workflow.run_node_jobs('A', [workflow.storage.load_job('A', 1), workflow.storage.load_job('B', 2)])
            else:
                workflow.run_jobs('A', [1, 999] if invalid == 'missing-job' else [1, 1])
        assert ran == []
        assert workflow.storage.list_execution_sessions() == []
        assert workflow.execution_session_context is None
        assert workflow.storage.get_component_reservation(('A', 'B')) is None
        for name, job_id in pairs:
            assert workflow.storage.get_job_status(name, job_id) == 'queued'
            assert workflow.storage.get_node_status(name) == before_statuses[name]
            assert workflow.storage.read_job_events(name, job_id) == before_events[name]
    finally:
        _close(workflow.storage)


def test_top_level_batch_autostart_uses_one_session_for_all_results(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner='direct')
    owners = []

    @router.task
    def work(ctx, value):
        owners.append(workflow.storage.get_job_execution_owner(ctx.execution_id))
        return value * 2

    workflow.include_routers(router)
    try:
        assert workflow.add_jobs(None, 'A', [{'value': 2}, {'value': 3}], autostart=True) == [4, 6]
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]['selected_jobs'] == [('A', 1), ('A', 2)]
        assert sessions[0]['outcome'] == 'done'
        assert {owner['session_id'] for owner in owners} == {sessions[0]['session_id']}
        assert workflow.storage.get_component_reservation(('A',)) is None
        assert workflow.execution_session_context is None
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('runner', ['direct', 'threaded'])
def test_programmatic_restart_replaces_blocked_handler_and_fences_its_late_effect(tmp_path, runner):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner=runner, max_threads=1)
    first_entered = Event()
    release_stale = Event()
    stale_finished = Event()
    replacement_entered = Event()
    owners = []
    stale_errors = []

    @router.task
    def work(ctx):
        owners.append(workflow.storage.get_job_execution_owner(ctx.execution_id))
        if len(owners) == 1:
            first_entered.set()
            try:
                assert release_stale.wait(20), 'Stale handler was not released'
                try:
                    ctx.node('B').add()
                except (JobRestartedError, JobTimeoutError) as error:
                    stale_errors.append(error)
                return 'stale'
            finally:
                stale_finished.set()
        replacement_entered.set()
        return 'fresh'

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    pool = ThreadPoolExecutor(max_workers=1)
    future = pool.submit(workflow.run_job, 'A', 1)
    try:
        assert first_entered.wait(10)
        workflow.storage.request_active_job_restart('A', 1, reason='native programmatic restart check')
        assert replacement_entered.wait(10), 'Replacement stayed blocked behind the stale handler'
        assert future.result(timeout=10) == 'fresh'
        release_stale.set()
        assert stale_finished.wait(10)
        assert len(stale_errors) == 1, 'Stale handler could publish a late mutation'
        assert workflow.storage.list_job_ids('B') == []
        assert workflow.storage.get_job_status('A', 1) == 'done'
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]['outcome'] == 'done'
        assert len(owners) == 2
        assert owners[0]['session_id'] == owners[1]['session_id'] == sessions[0]['session_id']
        assert owners[0]['execution_id'] != owners[1]['execution_id']
        assert workflow.storage.get_component_reservation(('A',)) is None
        assert workflow.execution_session_context is None
    finally:
        release_stale.set()
        pool.shutdown(wait=True)
        assert stale_finished.wait(10)
        _close(workflow.storage)


@pytest.mark.parametrize('runner', ['direct', 'threaded', 'api'])
def test_known_component_task_autostart_keeps_parent_and_child_on_one_main(tmp_path, runner):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'B')])
    workflow.register_autostart_edge('A', 'B')
    parent = NodeRouter('A', runner=runner, max_threads=1)
    child = NodeRouter('B', runner=runner, max_threads=1)
    observed = []

    @parent.task
    def create(ctx):
        observed.append(workflow.storage.get_job_execution_owner(ctx.execution_id))
        job = ctx.node('B').add(autostart=True, value=7)
        return job.job_id

    @child.task
    def consume(ctx, value):
        observed.append(workflow.storage.get_job_execution_owner(ctx.execution_id))
        return value

    workflow.include_routers(parent, child)
    workflow.add_job(None, 'A')
    try:
        workflow.run_node('A')
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]['outcome'] == 'done'
        assert len(observed) == 2
        assert all(owner['session_id'] == sessions[0]['session_id'] for owner in observed)
        assert all(owner['component'] == ('A', 'B') for owner in observed)
        job = workflow.storage.load_job('B', 1)
        assert job.parent == {'from_node': 'A', 'from_job_id': 1}
        assert job.producer_component == ('A', 'B')
        assert job.job_kind == 'component'
        assert workflow.storage.get_job_status('B', 1) == 'done'
        created = [event for event in workflow.storage.read_job_events('A', 1) if event['event'] == 'jobs_created']
        assert len(created) == 1
        assert created[0]['jobs'] == [{'node': 'B', 'job_id': 1, 'params': {'value': 7}}]
        assert workflow.storage.get_component_reservation(('A', 'B')) is None
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('entry', ['run', 'run_concurrently'])
def test_disjoint_components_have_distinct_membership_under_one_main(tmp_path, entry):
    workflow = MicroWorkflow(tmp_path, runner='threaded', persist_graph=False)
    workflow.graph([('Z', 'A'), ('X', 'Y')])
    observed = []

    def work(ctx):
        owner = workflow.storage.get_job_execution_owner(ctx.execution_id)
        assert owner['component'] == (ctx.current_node,)
        observed.append(owner)
        return ctx.current_node

    for name in ('Z', 'A', 'X', 'Y'):
        router = NodeRouter(name, runner='threaded', max_threads=1)
        router.task(work)
        workflow.include_routers(router)
        workflow.add_job(None, name)
    expected_components = workflow.execution_components()
    try:
        getattr(workflow, entry)()
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]['outcome'] == 'done'
        assert sessions[0]['selected_components'] == expected_components
        assert len(observed) == 4
        assert {owner['session_id'] for owner in observed} == {sessions[0]['session_id']}
        assert all(workflow.storage.get_component_reservation(component) is None for component in expected_components)
    finally:
        _close(workflow.storage)


def test_programmatic_process_workers_keep_native_owner_without_cli_run_controls(tmp_path):
    source = tmp_path / 'src'
    behaviors = source / 'node_behavior'
    behaviors.mkdir(parents=True)
    graph = source / 'graph.py'
    graph.write_text("EDGES = [('A', 'B'), ('B', 'A')]\n", encoding='utf-8')
    for name in ('A', 'B'):
        (behaviors / f'{name}.py').write_text(
            'from micro_workflow_manager import NodeRouter\n'
            f'router = NodeRouter({name!r}, runner="process", max_threads=1)\n'
            '@router.task\n'
            'def work(ctx):\n'
            '    assert ctx.system.allowed_run_nodes is None\n'
            '    owner = ctx.system.storage.get_job_execution_owner(ctx.execution_id)\n'
            '    assert owner["component"] == ("A", "B")\n'
            '    return owner["session_id"]\n', encoding='utf-8',
        )
    workflow = MicroWorkflow(tmp_path, runner='process', process_graph_path=graph, persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    workflow.include_node_dir(behaviors)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'B')
    try:
        workflow.run()
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        main = sessions[0]
        assert main['outcome'] == 'done'
        for name in ('A', 'B'):
            assert workflow.storage.get_job_status(name, 1) == 'done'
            events = [event for event in workflow.storage.read_job_events(name, 1) if event['event'] == 'started']
            assert len(events) == 1
            assert workflow.storage.get_job_execution_owner(events[0]['execution_id'])['session_id'] == main['session_id']
        assert workflow.storage.get_component_reservation(('A', 'B')) is None
        assert workflow.execution_session_context is None
    finally:
        _close(workflow.storage)


def test_process_worker_without_native_token_refuses_before_graph_import(tmp_path):
    from micro_workflow_manager.runners.process import _init_process_worker

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    source = tmp_path / 'src'
    (source / 'node_behavior').mkdir(parents=True)
    graph = source / 'graph.py'
    marker = tmp_path / 'graph-imported.txt'
    graph.write_text(
        'from pathlib import Path\n'
        'Path(__file__).parent.parent.joinpath("graph-imported.txt").write_text("imported")\n'
        "EDGES = [('A', 'B')]\n", encoding='utf-8',
    )
    try:
        with pytest.raises(RuntimeError, match='native session'):
            _init_process_worker(str(tmp_path), str(graph), None, 'immediate', None)
        assert not marker.exists()
        assert workflow.storage.list_execution_sessions() == []
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('entry', ['nodes', 'selected'])
def test_refused_cli_call_preserves_live_programmatic_controls(tmp_path, monkeypatch, entry):
    from micro_workflow_manager.cli import run_orchestration, run_selected

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    router = NodeRouter('A', runner='direct')
    entered = Event()
    release = Event()
    prechecked = Event()
    continue_second = Event()
    observations = []

    @router.task
    def work(ctx):
        entered.set()
        assert release.wait(20)
        return 'done'

    child = NodeRouter('B', runner='direct')
    child.task(lambda ctx: 'unexpected')
    workflow.include_routers(router, child)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'B')
    module = run_orchestration if entry == 'nodes' else run_selected
    original_session = module.active_workflow_run

    @contextmanager
    def observe_session_entry(*args, **kwargs):
        observations.append((workflow.allowed_run_nodes, workflow.autostart_mode))
        with original_session(*args, **kwargs) as finish:
            yield finish

    monkeypatch.setattr(module, 'active_workflow_run', observe_session_entry)
    if entry == 'selected':
        original_precheck = run_selected.refuse_competing_run

        def hold_after_precheck(current_workflow):
            original_precheck(current_workflow)
            prechecked.set()
            assert continue_second.wait(20)

        monkeypatch.setattr(run_selected, 'refuse_competing_run', hold_after_precheck)
    pool = ThreadPoolExecutor(max_workers=2)
    try:
        if entry == 'selected':
            second = pool.submit(
                run_selected.run_selected_jobs, tmp_path, workflow, 'B', [1],
            )
            assert prechecked.wait(10)
        first = pool.submit(workflow.run_job, 'A', 1)
        assert entered.wait(10)
        context = workflow.execution_session_context
        if entry == 'nodes':
            second = pool.submit(run_orchestration.run_nodes, workflow, ['B'], 'B')
        else:
            continue_second.set()
        with pytest.raises(RuntimeError, match='already active'):
            second.result(timeout=10)
        assert observations == [(None, 'immediate')], 'Refused CLI changed the running call controls before admission'
        assert workflow.allowed_run_nodes is None
        assert workflow.autostart_mode == 'immediate'
        assert workflow.execution_session_context is context
        assert workflow.storage.get_job_status('B', 1) == 'queued'
        release.set()
        assert first.result(timeout=10) == 'done'
    finally:
        release.set()
        continue_second.set()
        pool.shutdown(wait=True)
        _close(workflow.storage)


def test_task_nested_selection_rejects_duplicates_before_execution(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    parent = NodeRouter('A', runner='direct')
    child = NodeRouter('B', runner='direct')
    ran = []
    observations = []

    @child.task
    def consume(ctx):
        ran.append(ctx.job_id)
        return 1

    @parent.task
    def work(ctx):
        before = workflow.storage.get_node_status('B')
        try:
            workflow.run_jobs('B', [1, 1], ignore_readiness=True)
        except ValueError:
            observations.append(workflow.storage.get_node_status('B') == before)
        return 'done'

    workflow.include_routers(parent, child)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'B')
    try:
        workflow.run_job('A', 1)
        assert observations == [True]
        assert ran == []
        assert workflow.storage.get_job_status('B', 1) == 'queued'
        assert len(workflow.storage.list_execution_sessions()) == 1
    finally:
        _close(workflow.storage)


def test_task_nested_admission_checks_target_reservation_before_node_status(tmp_path):
    from micro_workflow_manager.workflow.execution_session import execution_session

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    parent = NodeRouter('A', runner='direct')
    child = NodeRouter('B', runner='direct')
    observations = []
    child.task(lambda ctx: 'unexpected')

    @parent.task
    def work(ctx):
        before = workflow.storage.get_node_status('B')
        session_id = workflow.execution_session_context[0]
        with workflow.storage.db_transaction() as connection:
            row = tuple(connection.execute(
                'SELECT component_key, session_id FROM component_reservations WHERE component_key=?', ('["B"]',),
            ).fetchone())
            connection.execute('DELETE FROM component_reservations WHERE component_key=?', ('["B"]',))
        try:
            try:
                workflow.run_queued_node_jobs('B', ignore_readiness=True)
            except RuntimeError:
                observations.append(workflow.storage.get_node_status('B') == before)
        finally:
            with workflow.storage.db_transaction() as connection:
                connection.execute('INSERT INTO component_reservations(component_key, session_id) VALUES(?, ?)', row)
        assert workflow.execution_session_context[0] == session_id
        return 'done'

    workflow.include_routers(parent, child)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'B')
    try:
        with execution_session(workflow, command='run', start_node='A', nodes=['A', 'B']):
            workflow._run_job('A', 1, execution_context=workflow.execution_session_context)
        assert observations == [True]
        assert workflow.storage.get_job_status('B', 1) == 'queued'
        assert not any(event['event'] == 'started' for event in workflow.storage.read_job_events('B', 1))
    finally:
        _close(workflow.storage)


def test_sample_request_records_exact_jobs_and_manifest_after_reservation(tmp_path):
    from micro_workflow_manager.workflow.execution_session import execution_session
    from micro_workflow_manager.workflow.sample_admission import SampleRequest

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner='direct')
    router.task(lambda ctx: 1)
    workflow.include_routers(router)
    workflow.add_jobs(None, 'A', [{}, {}])
    instances = tuple(('A', job_id, workflow.storage.read_job_instance_id('A', job_id))
                      for job_id in (1, 2))
    try:
        with execution_session(
            workflow, command='run sample', start_node='A', nodes=['A'],
            sample_request=SampleRequest(('100%',), 'admission'),
        ) as driver:
            main = workflow.storage.get_live_main_session()
            assert main['selected_jobs'] == [('A', 1), ('A', 2)]
            assert driver.sample_admission.roots == instances
            assert workflow.storage.get_component_reservation(('A',)) == {
                'members': ('A',), 'session_id': main['session_id'],
            }
            assert main['details']['selection'] == driver.sample_admission.selection
        assert workflow.storage.list_execution_sessions()[0]['outcome'] == 'done'
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('entry', ['single', 'batch'])
@pytest.mark.parametrize('fault', ['reservation', 'limits', 'heartbeat'])
def test_autostart_creation_survives_failed_admission_and_retries_with_new_owner(tmp_path, monkeypatch, entry, fault):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner='direct')
    ran = []
    router.task(lambda ctx: ran.append(ctx.job_id))
    workflow.include_routers(router)
    if fault == 'heartbeat':
        target = workflow.scheduler_supervisor
        method = 'start_run_heartbeat'
    else:
        target = workflow.storage
        method = 'reserve_execution_components' if fault == 'reservation' else '_bind_pending_thread_overrides'
    original = getattr(target, method)

    def fail_after_change(*args, **kwargs):
        original(*args, **kwargs)
        raise OSError('injected programmatic admission failure')

    try:
        with monkeypatch.context() as patch:
            patch.setattr(target, method, fail_after_change)
            with pytest.raises(OSError, match='injected programmatic admission failure'):
                if entry == 'single':
                    workflow.run_one('A')
                else:
                    workflow.add_jobs(None, 'A', [{}, {}], autostart=True)
        job_ids = [1] if entry == 'single' else [1, 2]
        assert workflow.storage.list_job_ids('A') == job_ids
        assert all(workflow.storage.get_job_status('A', job_id) == 'queued' for job_id in job_ids)
        assert ran == []
        failed = workflow.storage.list_execution_sessions()[0]
        assert failed['outcome'] == 'failed'
        assert workflow.storage.get_component_reservation(('A',)) is None
        assert workflow.execution_session_context is None
        assert workflow.scheduler_supervisor._run_heartbeat is None
        workflow.run_jobs('A', job_ids)
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 2
        assert sorted(session['outcome'] for session in sessions) == ['done', 'failed']
        assert ran == job_ids
        for job_id in job_ids:
            started = [event for event in workflow.storage.read_job_events('A', job_id) if event['event'] == 'started']
            assert len(started) == 1
            assert workflow.storage.get_job_execution_owner(started[0]['execution_id'])['session_id'] != failed['session_id']
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('entry', ['component', 'queued'])
@pytest.mark.parametrize('invalid', ['missing', 'foreign'])
def test_private_dispatch_refuses_invalid_token_before_node_status(tmp_path, entry, invalid):
    from micro_workflow_manager.workflow.execution_session import execution_session

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    router = NodeRouter('A', runner='direct')
    ran = []
    router.task(lambda ctx: ran.append(ctx.job_id))
    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    status = workflow.storage.get_node_status('A')
    try:
        with execution_session(workflow, command='run', start_node='A', nodes=['A']):
            invalid_context = None if invalid == 'missing' else ('other', {}, '')
            with pytest.raises(RuntimeError):
                if entry == 'component':
                    workflow._run_component({'A'}, execution_context=invalid_context)
                else:
                    workflow._run_queued_node_jobs('A', execution_context=invalid_context)
            assert workflow.storage.get_node_status('A') == status
            assert workflow.storage.get_job_status('A', 1) == 'queued'
            assert ran == []
    finally:
        _close(workflow.storage)


def test_manual_restart_refuses_newly_active_native_attempt_without_mutation(tmp_path, monkeypatch):
    from micro_workflow_manager.models import Job
    from tests.test_084_native_current_job_owner import _claim, _session

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    storage = workflow.storage
    storage.register_component_topology(workflow.topology.snapshot())
    storage.create_job(Job(node_name='A', job_id=1, params={'value': 'root'}))
    _session(workflow, 'restart-dispatch-race')

    before_fence = Event()
    release_request = Event()
    real_lock = storage.filesystem_interprocess_lock
    execution_lock = storage.job_execution_lock_name('A', 1)
    delayed = False

    @contextmanager
    def delay_request_fence(namespace, name):
        nonlocal delayed
        if (
            not delayed
            and current_thread().name.startswith('manual-restart')
            and namespace == 'execution-fences'
            and name == execution_lock
        ):
            delayed = True
            before_fence.set()
            assert release_request.wait(10), 'Manual restart request was not released'
        with real_lock(namespace, name):
            yield

    monkeypatch.setattr(storage, 'filesystem_interprocess_lock', delay_request_fence)
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='manual-restart')
    future = pool.submit(
        storage.request_job_restart,
        'A',
        1,
        reason='manual restart dispatch race',
    )
    try:
        assert before_fence.wait(10), 'Manual restart did not reach its execution fence'
        generation, execution_id = _claim(storage, 'restart-dispatch-race')
        assert generation == 0
        owner = storage.get_job_execution_owner(execution_id)
        output = storage.output_file('A', 1)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b'active attempt output')
        storage.db_mutation_barrier()
        before_database = list(storage.db_connection().iterdump())
        before_control = storage.read_job_control('A', 1)
        before_events = storage.read_job_events('A', 1)

        release_request.set()
        with pytest.raises(RuntimeError, match='running|state changed'):
            future.result(timeout=10)

        storage.db_mutation_barrier()
        assert list(storage.db_connection().iterdump()) == before_database
        assert storage.read_job_control('A', 1) == before_control
        assert storage.read_job_events('A', 1) == before_events
        assert storage.read_job_current_owner('A', 1) == owner
        assert storage.get_job_status('A', 1) == 'running'
        assert output.read_bytes() == b'active attempt output'
    finally:
        release_request.set()
        pool.shutdown(wait=True)
        _close(storage)


def test_active_restart_preserves_missing_job_file_not_found_error(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'A')])
    try:
        with pytest.raises(FileNotFoundError, match='Job does not exist: A/1'):
            workflow.storage.request_active_job_restart('A', 1)
    finally:
        _close(workflow.storage)
