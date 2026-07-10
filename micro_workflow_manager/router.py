from __future__ import annotations

import importlib
import sys
from dataclasses import dataclass
from typing import Any
from pathlib import Path
from types import ModuleType
from typing import Callable

from .node import (
    sequential_runner_value,
    validate_node_runner,
    validate_non_negative_int,
    validate_positive_int,
)


@dataclass
class RouterTask:
    handler: Callable
    retries: int = 0
    repeats: int = 1
    name: str | None = None


@dataclass
class RouterJobSpec:
    number: int
    params: dict[str, Any]


class NodeRouter:
    """Small, APIRouter-like object for one workflow node.

    Put one NodeRouter in each src/node_behavior/<node_name>.py file:

        router = NodeRouter("load_recipes", max_threads=1)

        @router.task
        def load_recipes(ctx, file_name):
            ...

        @router.fallback(name="plain")
        def plain(ctx, file_name, error=None):
            ...

    To force one node to run its own jobs sequentially even when the CLI runner
    is threaded, add one line in that node file:

        router.run_sequentially()
    """

    def __init__(
        self,
        name: str,
        max_threads: int = 5,
        *,
        runner: str | None = None,
        sequential: bool = False,
    ):
        self.name = name
        self.max_threads = validate_positive_int("max_threads", max_threads)
        self.runner_override = sequential_runner_value(
            runner=runner,
            sequential=sequential,
        )
        if self.runner_override == "direct":
            self.max_threads = 1
        self.main_task: RouterTask | None = None
        self.fallbacks: list[RouterTask] = []
        self.initial_jobs: list[RouterJobSpec] = []

    @classmethod
    def from_file(
        cls,
        file: str | Path,
        max_threads: int = 5,
        *,
        runner: str | None = None,
        sequential: bool = False,
    ) -> "NodeRouter":
        """Create a router whose node name is the Python file stem."""
        return cls(
            Path(file).stem,
            max_threads=max_threads,
            runner=runner,
            sequential=sequential,
        )

    def run_sequentially(self) -> "NodeRouter":
        """Force this node's jobs to run with the direct runner.

        This is a per-node override. It still allows other independent nodes to
        use the workflow's normal runner.
        """
        self.runner_override = "direct"
        self.max_threads = 1
        return self

    def use_runner(self, runner: str) -> "NodeRouter":
        """Force this node to use 'direct', 'threaded', or 'process'."""
        self.runner_override = validate_node_runner(runner)
        if self.runner_override == "direct":
            self.max_threads = 1
        return self


    def create_job(
        self,
        *,
        number: int = 1,
        params: dict[str, Any] | None = None,
    ) -> "NodeRouter":
        """Declare default jobs for this node.

        The jobs are created deterministically when the router is mounted.
        For example, ``router.create_job(number=2, params={"name": "demo"})``
        creates jobs 1 and 2 for this node, both with the same params.

        ``number`` is deliberately explicit: ``mwf run`` and ``mwf runfrom`` no
        longer invent a default starter job.
        """
        number = validate_positive_int("number", number)

        if params is None:
            params = {}

        if not isinstance(params, dict):
            raise ValueError("params must be a dict")

        self.initial_jobs.append(RouterJobSpec(number=number, params=dict(params)))
        return self

    def task(
        self,
        fn: Callable | None = None,
        *,
        retries: int = 0,
        repeats: int = 1,
        max_threads: int | None = None,
        runner: str | None = None,
        sequential: bool = False,
    ):
        """Register the main task for this node.

        Use either style:

            @router.task
            def run(ctx): ...

            @router.task(retries=2, repeats=3)
            def run(ctx): ...
        """
        retries = validate_non_negative_int("retries", retries)
        repeats = validate_positive_int("repeats", repeats)

        if max_threads is not None:
            self.max_threads = validate_positive_int("max_threads", max_threads)

        override = sequential_runner_value(runner=runner, sequential=sequential)
        if override is not None:
            self.runner_override = override

        if self.runner_override == "direct":
            self.max_threads = 1

        def decorator(handler: Callable):
            self.main_task = RouterTask(
                handler=handler,
                retries=retries,
                repeats=repeats,
            )
            return handler

        if fn is None:
            return decorator

        return decorator(fn)

    def fallback(
        self,
        fn: Callable | None = None,
        *,
        name: str | None = None,
        retries: int = 0,
        repeats: int = 1,
    ):
        """Register one fallback for this node."""

        retries = validate_non_negative_int("retries", retries)
        repeats = validate_positive_int("repeats", repeats)

        def decorator(handler: Callable):
            self.fallbacks.append(
                RouterTask(
                    handler=handler,
                    name=name or handler.__name__,
                    retries=retries,
                    repeats=repeats,
                )
            )
            return handler

        if fn is None:
            return decorator

        return decorator(fn)

    def mount_to(self, workflow):
        """Mount this router onto a MicroWorkflow instance."""
        if self.main_task is None:
            raise ValueError(f"NodeRouter {self.name} has no task")

        workflow.task(
            self.name,
            max_threads=self.max_threads,
            retries=self.main_task.retries,
            repeats=self.main_task.repeats,
            runner=self.runner_override,
        )(self.main_task.handler)

        for fallback in self.fallbacks:
            workflow.fallback(
                self.name,
                name=fallback.name,
                retries=fallback.retries,
                repeats=fallback.repeats,
            )(fallback.handler)

        next_job_id = 1
        for spec in self.initial_jobs:
            workflow.create_jobs(
                self.name,
                number=spec.number,
                params=spec.params,
                start_job_id=next_job_id,
            )
            next_job_id += spec.number

        return self


def routers_from_module(module: ModuleType) -> list[NodeRouter]:
    """Find NodeRouter objects exported by a module."""
    routers: list[NodeRouter] = []

    if isinstance(getattr(module, "router", None), NodeRouter):
        routers.append(module.router)

    many = getattr(module, "routers", None)
    if many is not None:
        for item in many:
            if isinstance(item, NodeRouter):
                routers.append(item)

    return routers


def import_modules_from_dir(
    directory: str | Path,
    package: str | None = None,
    recursive: bool = False,
) -> list[ModuleType]:
    """Import Python files from a node_behavior folder."""
    root = Path(directory).resolve()

    if not root.exists():
        raise FileNotFoundError(f"Router directory does not exist: {root}")

    pattern = "**/*.py" if recursive else "*.py"
    files = sorted(root.glob(pattern))

    modules: list[ModuleType] = []

    if package is None and (root / "__init__.py").exists():
        package = root.name

    if package is not None:
        parent = str(root.parent)
        if parent not in sys.path:
            sys.path.insert(0, parent)

    for file in files:
        if file.name == "__init__.py" or file.name.startswith("_"):
            continue

        if package is not None:
            relative = file.relative_to(root).with_suffix("")
            module_name = package + "." + ".".join(relative.parts)
            modules.append(importlib.import_module(module_name))
            continue

        module_name = "micro_workflow_node_" + file.stem
        # Execute current source directly. Generated node files are often
        # rewritten several times within one second and may keep the same file
        # size, which can make timestamp-based .pyc validation reuse obsolete
        # task code across graph reloads or test projects.
        module = ModuleType(module_name)
        module.__file__ = str(file)
        sys.modules[module_name] = module
        source = file.read_text(encoding="utf-8")
        exec(compile(source, str(file), "exec"), module.__dict__)
        modules.append(module)

    return modules
