"""Measure repeated fresh API execution with declared workload growth."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
from concurrent.futures import Future
import hashlib
from importlib import metadata
import json
import math
import os
from pathlib import Path
import platform
import sqlite3
import statistics
import subprocess
import sys
import time
import traceback


if not __debug__:
    raise RuntimeError("Repeated API measurement requires unoptimized Python")


EVENTS = ("queued", "started", "task_started", "output_written", "done")


def evaluate_growth(rounds: list[list[float]]) -> dict:
    """Compare early and late measured rounds across three fresh projects."""
    if len(rounds) != 3 or any(len(row) != 4 for row in rounds):
        raise ValueError("Expected three projects with four measured rounds each")
    if any(isinstance(value, bool) or not math.isfinite(value) or value <= 0
           for row in rounds for value in row):
        raise ValueError("Every duration must be finite and positive")
    early = statistics.median(value for row in rounds for value in row[:2])
    late = statistics.median(value for row in rounds for value in row[2:])
    per_project = [
        dict(early=statistics.median(row[:2]), late=statistics.median(row[2:]),
             within_allowance=statistics.median(row[2:]) <= statistics.median(row[:2]) * 3 + 1)
        for row in rounds
    ]
    return dict(rounds=rounds, early_median=early, late_median=late,
                allowance=early * 3 + 1, ratio=late / early,
                passed=late <= early * 3 + 1, per_project=per_project)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_snapshot(source: Path) -> dict:
    return {path.relative_to(source).as_posix(): digest(path)
            for folder in ("micro_workflow_manager", "tests", "benchmarks")
            for path in sorted((source / folder).rglob("*.py"))}


def source_version(source: Path) -> str:
    module = ast.parse((source / "micro_workflow_manager" / "__init__.py").read_text(encoding="utf-8"))
    values = [ast.literal_eval(statement.value) for statement in module.body
              if isinstance(statement, ast.Assign)
              and any(isinstance(target, ast.Name) and target.id == "__version__"
                      for target in statement.targets)]
    if len(values) != 1 or not isinstance(values[0], str) or not values[0]:
        raise ValueError("Measured source must declare one literal __version__ string")
    return values[0]


def environment_metadata(source: Path) -> dict:
    return dict(executable=sys.executable, python=sys.version, sqlite=sqlite3.sqlite_version,
                python_optimization=sys.flags.optimize, python_optimize_env=os.environ.get("PYTHONOPTIMIZE"),
                api_environment={name: os.environ.get(name) for name in (
                    "MWF_API_STARTUP_STRATEGY", "MWF_API_PREFETCH", "MWF_API_JOBS_PER_STARTUP_LANE")},
                os=platform.platform(), machine=platform.node(), cpu_count=os.cpu_count(),
                architecture=platform.machine(),
                dependencies={name: metadata.version(name)
                              for name in ("networkx", "greenlet", "httpx", "h2", "pytest")},
                mwf_version=source_version(source))


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")


def measure_project(source: Path, directory: Path) -> int:
    import micro_workflow_manager
    from micro_workflow_manager import MicroWorkflow

    assert Path(micro_workflow_manager.__file__).resolve().parent.parent == source
    directory.mkdir()
    result = dict(source=str(source), source_sha256=source_snapshot(source),
                  worker_sha256=digest(Path(__file__)), command=sys.argv,
                  environment={name: os.environ.get(name)
                               for name in ("PYTHONPATH", "PYTHONDONTWRITEBYTECODE", "TEMP", "TMP")},
                  **environment_metadata(source), rounds=[])
    workflow = None
    status = 1
    try:
        workflow = MicroWorkflow(directory / "project", runner="api")
        workflow.graph([("merge", "sink")])
        workflow.active_job_restart_enabled = True
        runtime_futures = []
        original_write = workflow.storage.write_job_runtime

        def observe_runtime(*args, **kwargs):
            value = original_write(*args, **kwargs)
            if isinstance(value, Future):
                runtime_futures.append(value)
            return value

        workflow.storage.write_job_runtime = observe_runtime

        @workflow.task("merge", runner="api", max_threads=96, timeout=30, checkpoint_timeout=10)
        def merge(ctx, round_number):
            ctx.checkpoint("loading")
            ctx.checkpoint("preparing")
            ctx.write_output(f"jobs/{ctx.job_id}/command.txt", f"{round_number}:{ctx.job_id}")
            ctx.checkpoint("writing")
            return ctx.job_id

        @workflow.task("sink")
        def sink(ctx):
            return None

        cumulative_events = Counter()
        cumulative_checked = 0
        previous_executions = {}
        for round_number in range(5):
            first = round_number * 96 + 1
            executed_jobs = first + 95
            runtime_futures.clear()
            for job_id in range(first, first + 96):
                workflow.start("merge", job_id=job_id, autostart=False, round_number=round_number)
            started = time.perf_counter()
            workflow.run_node("merge", ignore_readiness=True)
            returned = time.perf_counter()
            workflow.storage.db_mutation_barrier()
            drained = time.perf_counter()
            writer = workflow.storage.mutation_writer_diagnostics()
            assert writer["pending_mutations"] == writer["queued"] == writer["durability_backlog"] == 0, writer
            pruned = workflow.storage.prune_dead_thread_connections()
            assert pruned == 0, pruned
            assert len(runtime_futures) == 4 * executed_jobs, len(runtime_futures)
            assert all(value.done() for value in runtime_futures)
            errors = [repr(value.exception()) for value in runtime_futures if value.exception() is not None]
            assert not errors, errors
            counts = workflow.storage.job_status_counts("merge")
            assert counts == dict(cancelled=0, done=first + 95, failed=0, queued=0, running=0, skipped=0), counts
            events = Counter()
            for job_id in range(1, executed_jobs + 1):
                created_round = (job_id - 1) // 96
                command = workflow.storage.output_path("merge", "jobs", str(job_id), "command.txt").read_text(encoding="utf-8")
                assert command == f"{created_round}:{job_id}", (job_id, command)
                output = json.loads(workflow.storage.output_file("merge", job_id).read_text(encoding="utf-8"))
                owner = workflow.storage.read_job_current_owner("merge", job_id)
                assert owner is not None and owner["generation"] == 0, owner
                assert owner["execution_id"] != previous_executions.get(job_id), owner
                previous_executions[job_id] = owner["execution_id"]
                assert output == dict(status="done", result_type="int", result_repr=str(job_id), generation=0,
                                      execution_id=owner["execution_id"]), output
                runtime = workflow.storage.read_job_runtime("merge", job_id)
                assert runtime["checkpoint_name"] == "writing", runtime
                assert runtime["state"] == "running" and runtime["generation"] == 0, runtime
                assert runtime["node"] == runtime["task"] == "merge" and runtime["job_id"] == job_id, runtime
                assert isinstance(runtime["watch_id"], str) and runtime["watch_id"], runtime
                assert runtime["execution_id"] == owner["execution_id"], runtime
                job_events = Counter(item["event"] for item in workflow.storage.read_job_events("merge", job_id))
                assert job_events == {name: 1 for name in EVENTS}, (job_id, job_events)
                events.update(job_events)
            cumulative_events.update(events)
            cumulative_checked += executed_jobs
            integrity = workflow.storage.db_connection().execute("PRAGMA quick_check").fetchone()[0]
            assert integrity == "ok", integrity
            result["rounds"].append(dict(
                round=round_number, warmup=round_number == 0, jobs=executed_jobs, new_jobs=96,
                run_seconds=returned - started, drain_seconds=drained - returned,
                counts=counts, writer=writer, pruned=pruned, asynchronous_errors=errors,
                runtime_future_observations=len(runtime_futures), outputs_checked=executed_jobs,
                runtimes_checked=executed_jobs,
                event_counts=dict(events), cumulative_outputs_checked=cumulative_checked,
                cumulative_events=dict(cumulative_events), integrity=integrity))
            print(json.dumps(dict(round=round_number, run_seconds=returned - started, correctness="passed")), flush=True)
        result["correctness"] = "passed"
        status = 0
    except BaseException:
        result["correctness"] = "failed"
        result["error"] = traceback.format_exc()
    finally:
        cleanup_started = time.perf_counter()
        if workflow is not None:
            try:
                workflow.storage.db_mutation_barrier()
                while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
                    assert time.perf_counter() - cleanup_started < 10, "Mutation writer did not retire"
                    time.sleep(0.01)
                final_writer = workflow.storage.mutation_writer_diagnostics()
                assert final_writer["queued"] == final_writer["pending_mutations"] == final_writer["durability_backlog"] == 0
                assert workflow.storage.prune_dead_thread_connections() == 0
                workflow.storage.close_thread_connection()
                result["cleanup"] = "passed"
            except BaseException:
                result["cleanup_error"] = traceback.format_exc()
                status = 1
        result["cleanup_seconds"] = time.perf_counter() - cleanup_started
        result["exit_code"] = status
        write_json(directory / "result.json", result)
    return status


def validate_sample(data: dict, plan: dict, command: list[str], overrides: dict) -> list[float]:
    assert data["exit_code"] == 0
    assert data["correctness"] == data["cleanup"] == "passed"
    assert data["source"] == plan["source"] and data["source_sha256"] == plan["source_sha256"]
    assert data["worker_sha256"] == plan["worker_sha256"]
    assert data["command"] == command[1:] and data["environment"] == overrides
    assert {name: data[name] for name in plan["environment"]} == plan["environment"]
    assert math.isfinite(data["cleanup_seconds"]) and data["cleanup_seconds"] >= 0
    assert len(data["rounds"]) == 5
    for position, row in enumerate(data["rounds"]):
        total = (position + 1) * 96
        cumulative = 96 * (position + 1) * (position + 2) // 2
        assert row["round"] == position and row["warmup"] == (position == 0)
        assert row["jobs"] == total and row["new_jobs"] == 96
        assert math.isfinite(row["run_seconds"]) and row["run_seconds"] > 0
        assert math.isfinite(row["drain_seconds"]) and row["drain_seconds"] >= 0
        assert row["counts"] == dict(cancelled=0, done=total, failed=0, queued=0, running=0, skipped=0)
        assert row["writer"]["pending_mutations"] == row["writer"]["queued"] == row["writer"]["durability_backlog"] == 0
        assert row["pruned"] == 0 and row["asynchronous_errors"] == []
        assert row["runtime_future_observations"] == 4 * total
        assert row["outputs_checked"] == row["runtimes_checked"] == total
        assert row["event_counts"] == {name: total for name in EVENTS}
        assert row["cumulative_outputs_checked"] == cumulative and row["integrity"] == "ok"
        assert row["cumulative_events"] == {name: cumulative for name in EVENTS}
    return [row["run_seconds"] for row in data["rounds"][1:]]


def run_repetitions(source: Path, directory: Path, plan: dict) -> list[list[float]]:
    rounds = []
    samples = []
    try:
        for repetition in range(1, 4):
            assert source_snapshot(source) == plan["source_sha256"]
            assert environment_metadata(source) == plan["environment"]
            child = directory / f"process-{repetition:02d}"
            temporary = directory / f"process-{repetition:02d}-temp"
            temporary.mkdir()
            overrides = dict(PYTHONPATH=str(source), PYTHONDONTWRITEBYTECODE="1", TEMP=str(temporary), TMP=str(temporary))
            command = [sys.executable, str(Path(__file__).resolve()), "--child", str(child), "--source", str(source)]
            sample = dict(repetition=repetition, command=command, result=str(child / "result.json"))
            samples.append(sample)
            with (directory / f"process-{repetition:02d}.log").open("w", encoding="utf-8") as stream:
                run = subprocess.run(command, cwd=source, env=dict(os.environ, **overrides),
                                     stdout=stream, stderr=subprocess.STDOUT, timeout=1200)
            sample["exit_code"] = run.returncode
            sample["result_sha256"] = digest(child / "result.json")
            data = json.loads((child / "result.json").read_text(encoding="utf-8"))
            assert run.returncode == 0, data.get("error", "Child process failed")
            measured = validate_sample(data, plan, command, overrides)
            assert source_snapshot(source) == plan["source_sha256"]
            assert environment_metadata(source) == plan["environment"]
            rounds.append(measured)
            sample["validation"] = "passed"
            print(json.dumps(dict(repetition=repetition, rounds=measured, correctness="passed")), flush=True)
    finally:
        write_json(directory / "samples.json", {"samples": samples})
    return rounds


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="New directory for the plan, raw results, logs, and projects")
    parser.add_argument("--source", type=Path, default=Path(__file__).resolve().parents[1], help="Isolated MWF source copy")
    parser.add_argument("--source-commit", help="Recorded base commit of the source copy")
    parser.add_argument("--source-state", help="Description of the copied uncommitted changes")
    parser.add_argument("--child", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    source = args.source.resolve()
    if args.child is not None:
        return measure_project(source, args.child.resolve())
    if args.output is None or not args.source_commit or not args.source_state or not args.source_state.strip():
        parser.error("--output, --source-commit, and --source-state are required")
    directory = args.output.resolve()
    directory.mkdir()
    plan = dict(source=str(source), source_commit=args.source_commit, source_state=args.source_state,
                source_sha256=source_snapshot(source), environment=environment_metadata(source),
                worker_sha256=digest(Path(__file__)), fresh_processes=3, warmup_rounds=1,
                measured_rounds=4, new_jobs_per_round=96,
                executed_jobs_per_round=[96, 192, 288, 384, 480],
                measured_boundary="run_node only; creation, barrier, validation and cleanup excluded",
                growth_rule="aggregate late median <= early median * 3 + 1 second",
                per_project_growth="recorded separately for review; not hidden by aggregate result")
    write_json(directory / "plan.json", plan)
    result = dict(plan_sha256=digest(directory / "plan.json"), samples_sha256=None, growth=None, exit_code=1)
    try:
        result["growth"] = evaluate_growth(run_repetitions(source, directory, plan))
        if not result["growth"]["passed"]:
            raise RuntimeError("Aggregate repeated-round growth exceeds the existing allowance")
        result["exit_code"] = 0
    except BaseException:
        result["error"] = traceback.format_exc()
    finally:
        samples_path = directory / "samples.json"
        if samples_path.exists():
            result["samples_sha256"] = digest(samples_path)
        elif result["exit_code"] == 0:
            result["exit_code"] = 1
            result["error"] = "Completed measurement has no raw sample record"
    write_json(directory / "result.json", result)
    print(json.dumps(result, indent=2), flush=True)
    return result["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
