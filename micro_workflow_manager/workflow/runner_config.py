from ..node import JobNode
from ..runners.api import ApiRunner
from ..runners.direct import DirectRunner
from ..runners.process import ProcessPoolRunner
from ..runners.threaded import ThreadedRunner


def normalize_workflow_runner(runner: str) -> str:
    aliases = {
        "thread": "threaded",
        "io": "api",
        "network": "api",
        "processes": "process",
        "process_pool": "process",
        "processpool": "process",
    }
    runner = aliases.get(runner, runner)

    if runner not in {"direct", "threaded", "api", "process"}:
        raise ValueError(f"Unknown runner: {runner}")

    return runner


class RunnerFactoryMixin:
    def make_runner(self, node: JobNode):
        effective_runner = node.runner_override or self.runner

        if effective_runner == "direct":
            return DirectRunner()

        if effective_runner == "threaded":
            return ThreadedRunner(
                max_threads=node.max_threads,
                limit_provider=lambda: self.effective_max_threads(node.name),
                worker_cleanup=self.storage.close_thread_connection,
            )

        if effective_runner == "api":
            return ApiRunner(
                max_threads=node.max_threads,
                limit_provider=lambda: self.effective_max_threads(node.name),
                worker_cleanup=self.storage.close_thread_connection,
            )

        if effective_runner == "process":
            return ProcessPoolRunner(
                max_processes=self.effective_max_threads(node.name),
                project_dir=self.storage.project_dir,
                graph_path=self.process_graph_path,
                allowed_run_nodes=self.allowed_run_nodes,
                autostart_mode=self.autostart_mode,
            )

        raise ValueError(f"Unknown runner: {effective_runner}")
