from __future__ import annotations

import argparse
import json
import resource
import tempfile
import threading
import time
from pathlib import Path

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.files import InputFileSystem, NodeInputFileSystem
from micro_workflow_manager.cli.run_orchestration import run_nodes


def main() -> int:
    parser = argparse.ArgumentParser(description="idimage-like four-way DAG fanout benchmark")
    parser.add_argument("--total-jobs", type=int, default=1200)
    parser.add_argument("--sink-threads", type=int, default=100)
    parser.add_argument("--payload-bytes", type=int, default=1500)
    parser.add_argument("--nofile", type=int, default=65536)
    parser.add_argument("--payload-read-delay", type=float, default=0.0)
    args = parser.parse_args()
    _soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = args.nofile if hard == resource.RLIM_INFINITY else min(args.nofile, hard)
    resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))

    if args.payload_read_delay:
        from micro_workflow_manager.storage.base import FileStorageBase
        original_read_json = FileStorageBase.read_json

        def delayed_read_json(self, path, default=None):
            text = str(path).replace("\\", "/")
            if "/jobs/" in text and text.endswith("/input.json") and "/node/idimage/" not in text:
                time.sleep(args.payload_read_delay)
            return original_read_json(self, path, default)

        FileStorageBase.read_json = delayed_read_json

    project = Path(tempfile.mkdtemp(prefix="mwf-idimage-fanout-"))
    workflow = MicroWorkflow(project_dir=project, runner="threaded")
    sinks = ["merge", "organize", "findlabelregex", "postidimage"]
    workflow.graph([("idimage", sink) for sink in sinks])
    destination = {sink: NodeInputFileSystem(sink, f"{sink} input") for sink in sinks}
    root = NodeRouter("idimage", runner="threaded", max_threads=1)
    done = {"count": 0}
    lock = threading.Lock()

    @root.task
    def idimage(ctx):
        # Approximate Kaicenat's unequal fanout while keeping requested total fixed.
        weights = [0.72, 0.01, 0.08, 0.19]
        counts = [int(args.total_jobs * w) for w in weights]
        counts[-1] += args.total_jobs - sum(counts)
        payload = "x" * args.payload_bytes
        for sink, count in zip(sinks, counts):
            entries = []
            params = []
            for index in range(count):
                rel = f"book/{sink}/{index:06d}.json"
                entries.append((rel, {"index": index, "payload": payload}))
                params.append({"record_file": rel})
            destination[sink].write_jsons(ctx, entries, overwrite=True)
            destination[sink].add_jobs(ctx, params)
        return counts

    workflow.include_router(root)
    for sink in sinks:
        router = NodeRouter(sink, runner="threaded", max_threads=args.sink_threads)
        inp = InputFileSystem(f"{sink} input")

        def make_sink(_inp=inp):
            def run(ctx, record_file):
                _inp.file(ctx, record_file).read_json()
                with lock:
                    done["count"] += 1
                return record_file
            return run

        router.task(make_sink())
        workflow.include_router(router)

    workflow.start("idimage")
    started = time.perf_counter()
    error = None
    try:
        code = run_nodes(workflow, ["idimage", *sinks], "idimage", command="runfrom")
        if code != 0:
            raise RuntimeError(f"run_nodes returned {code}")
    except BaseException as exc:
        error = repr(exc)
    elapsed = time.perf_counter() - started
    result = {
        "benchmark": "idimage_dag_fanout",
        "total_jobs": args.total_jobs,
        "completed_jobs": done["count"],
        "elapsed_s": round(elapsed, 4),
        "jobs_per_s": round(done["count"] / max(elapsed, 1e-9), 2),
        "effective_nofile": target,
        "payload_read_delay": args.payload_read_delay,
        "error": error,
    }
    print(json.dumps(result, sort_keys=True))
    return 0 if error is None and done["count"] == args.total_jobs else 1


if __name__ == "__main__":
    raise SystemExit(main())
