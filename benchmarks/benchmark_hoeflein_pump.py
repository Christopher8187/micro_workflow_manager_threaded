from __future__ import annotations

import argparse
import json
import os
import resource
import tempfile
import threading
import time
from pathlib import Path

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.files import InputFileSystem, NodeInputFileSystem


def main() -> int:
    parser = argparse.ArgumentParser(description="Two-node Hoeflein A<->B pump benchmark")
    parser.add_argument("--seeds", type=int, default=64)
    parser.add_argument("--hops", type=int, default=40)
    parser.add_argument("--threads", type=int, default=64)
    parser.add_argument("--payload-bytes", type=int, default=1500)
    parser.add_argument("--nofile", type=int, default=65536)
    parser.add_argument("--fds-per-job", type=int, default=0)
    parser.add_argument("--project", default="")
    args = parser.parse_args()

    old_soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = args.nofile if hard == resource.RLIM_INFINITY else min(args.nofile, hard)
    resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))

    project = Path(args.project) if args.project else Path(tempfile.mkdtemp(prefix="mwf-ab-pump-"))
    workflow = MicroWorkflow(project_dir=project, runner="threaded")
    workflow.graph([("A", "B"), ("B", "A")])
    state = {"done": 0, "fd_peak": 0, "stop": False}
    lock = threading.Lock()

    routers = {}
    for node, other in (("A", "B"), ("B", "A")):
        router = NodeRouter(node, runner="threaded", max_threads=args.threads)
        inp = InputFileSystem(f"{node} input")
        out = NodeInputFileSystem(other, f"{other} input")

        def make_task(_inp=inp, _out=out, _other=other):
            def run(ctx, record_file):
                record = _inp.file(ctx, record_file).read_json()
                held = []
                try:
                    for _ in range(args.fds_per_job):
                        held.append(os.open("/dev/null", os.O_RDONLY))
                    hop = int(record["hop"])
                    if hop + 1 < args.hops:
                        nxt = {
                            "seed": int(record["seed"]),
                            "hop": hop + 1,
                            "payload": record["payload"],
                        }
                        rel = f"pump/{nxt['seed']:05d}/{nxt['hop']:05d}.json"
                        _out.file(ctx, rel).write_json(nxt, overwrite=True)
                        _out.add_job(ctx, record_file=rel)
                    with lock:
                        state["done"] += 1
                    return _other
                finally:
                    for fd in held:
                        os.close(fd)
            return run

        router.task(make_task())
        workflow.include_router(router)
        routers[node] = router

    payload = "x" * args.payload_bytes
    root = workflow.storage.node_input_dir("A")
    params = []
    for seed in range(args.seeds):
        rec = {"seed": seed, "hop": 0, "payload": payload}
        rel = f"pump/{seed:05d}/00000.json"
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(rec), encoding="utf-8")
        params.append({"record_file": rel})
    workflow.add_jobs(None, "A", params)

    def sample_fds():
        while not state["stop"]:
            try:
                count = len(os.listdir("/proc/self/fd"))
                with lock:
                    state["fd_peak"] = max(state["fd_peak"], count)
            except OSError:
                pass
            time.sleep(0.002)

    sampler = threading.Thread(target=sample_fds, daemon=True)
    sampler.start()
    started = time.perf_counter()
    error = None
    try:
        workflow.run_component({"A", "B"}, ignore_readiness=True)
    except BaseException as exc:
        error = repr(exc)
    elapsed = time.perf_counter() - started
    state["stop"] = True
    sampler.join(timeout=1)

    expected = args.seeds * args.hops
    counts = {node: workflow.storage.job_status_counts(node) for node in ("A", "B")}
    result = {
        "benchmark": "hoeflein_ab_pump",
        "seeds": args.seeds,
        "hops": args.hops,
        "threads": args.threads,
        "payload_bytes": args.payload_bytes,
        "fds_per_job": args.fds_per_job,
        "requested_nofile": args.nofile,
        "effective_nofile": target,
        "old_soft_nofile": old_soft,
        "expected_jobs": expected,
        "completed_jobs": state["done"],
        "elapsed_s": round(elapsed, 4),
        "jobs_per_s": round(state["done"] / max(elapsed, 1e-9), 2),
        "fd_peak": state["fd_peak"],
        "counts": counts,
        "error": error,
    }
    print(json.dumps(result, sort_keys=True))
    return 0 if error is None and state["done"] == expected else 1


if __name__ == "__main__":
    raise SystemExit(main())
