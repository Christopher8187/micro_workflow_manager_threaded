from ..node import JobNode
from ..runners.direct import DirectRunner
from ..runners.process import ProcessPoolRunner
from ..runners.threaded import ThreadedRunner


def normalize_workflow_runner(runner: str) -> str:
    aliases = {
        "thread": "threaded",
        "processes": "process",
        "process_pool": "process",
        "processpool": "process",
    }
    runner = aliases.get(runner, runner)

    if runner not in {"direct", "threaded", "process"}:
        raise ValueError(f"Unknown runner: {runner}")

    return runner


class RunnerFactoryMixin:
    def make_runner(self, node: JobNode):
        effective_runner = node.runner_override or self.runner

        if effective_runner == "direct":
            return DirectRunner()

        if effective_runner == "threaded":
            return ThreadedRunner(max_threads=node.max_threads)

        if effective_runner == "process":
            return ProcessPoolRunner(
                max_processes=node.max_threads,
                project_dir=self.storage.project_dir,
                graph_path=self.process_graph_path,
                allowed_run_nodes=self.allowed_run_nodes,
                autostart_mode=self.autostart_mode,
            )

        raise ValueError(f"Unknown runner: {effective_runner}")
