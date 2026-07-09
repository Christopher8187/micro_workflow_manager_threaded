from pathlib import Path
from typing import Callable

from .models import QUEUED
from .node import (
    JobNode,
    sequential_runner_value,
    validate_non_negative_int,
    validate_positive_int,
)
from .router import NodeRouter, import_modules_from_dir, routers_from_module


class WorkflowRegistrationMixin:
    def graph(self, edges: list[tuple[str, str]]):
        with self.lock:
            for start, end in edges:
                self.ensure_node(start)
                self.ensure_node(end)
                self.graph_obj.add_edge(start, end)

            # Cycles are allowed. A strongly connected component is treated as
            # one communicating class for readiness and completion. This lets
            # autostart loops such as A -> A or A -> B -> A keep generating
            # jobs until the whole component becomes quiescent.
            self.storage.write_graph(edges)

    def include_router(self, router):
        """Mount a NodeRouter or a module that exports router/routers.

        This is similar in spirit to FastAPI's app.include_router(...).
        """
        if isinstance(router, NodeRouter):
            router_id = id(router)

            if router_id in self._included_router_ids:
                return router

            router.mount_to(self)
            self._included_router_ids.add(router_id)
            return router

        found = routers_from_module(router)

        if not found:
            raise ValueError("include_router expected a NodeRouter or a module with router/routers")

        for item in found:
            self.include_router(item)

        return router

    def include_routers(self, *routers):
        for router in routers:
            self.include_router(router)

    def include_router_dir(
        self,
        directory: str | Path,
        package: str | None = None,
        recursive: bool = False,
    ):
        """Import every node file in a folder and mount its router.

        This supports the recommended layout:

            src/node_behavior/load_recipes.py
            src/node_behavior/make_card.py
            src/node_behavior/index_cards.py
        """
        modules = import_modules_from_dir(
            directory=directory,
            package=package,
            recursive=recursive,
        )

        for module in modules:
            for router in routers_from_module(module):
                self.include_router(router)

        return modules

    def ensure_node(
        self,
        name: str,
        max_threads: int = 5,
        runner: str | None = None,
        sequential: bool = False,
    ) -> JobNode:
        name = self.storage.validate_node_name(name)
        max_threads = validate_positive_int("max_threads", max_threads)
        runner_override = sequential_runner_value(runner=runner, sequential=sequential)

        if runner_override == "direct":
            max_threads = 1

        with self.lock:
            if name not in self.nodes:
                self.nodes[name] = JobNode(
                    name,
                    max_threads=max_threads,
                    runner=runner_override,
                )
                self.graph_obj.add_node(name)
                self.storage.init_node_folders(name)

                if self.storage.get_node_status(name) is None:
                    self.storage.set_node_status(name, QUEUED)
            else:
                node = self.nodes[name]
                if runner_override is not None:
                    node.set_runner(runner=runner_override)

            return self.nodes[name]

    def task(
        self,
        node_name: str,
        max_threads: int = 5,
        retries: int = 0,
        repeats: int = 1,
        runner: str | None = None,
        sequential: bool = False,
    ):
        max_threads_checked = validate_positive_int("max_threads", max_threads)
        retries_checked = validate_non_negative_int("retries", retries)
        repeats_checked = validate_positive_int("repeats", repeats)
        runner_override = sequential_runner_value(runner=runner, sequential=sequential)

        if runner_override == "direct":
            max_threads_checked = 1

        def decorator(fn: Callable):
            node = self.ensure_node(
                node_name,
                max_threads=max_threads_checked,
                runner=runner_override,
            )
            node.max_threads = max_threads_checked
            if runner_override is not None:
                node.set_runner(runner=runner_override)
            node.mount_main(fn, retries=retries_checked, repeats=repeats_checked)

            assert node.main_task is not None

            self.storage.write_node_schema(
                node_name=node_name,
                allowed_params=node.main_task.allowed_params,
                required_params=node.main_task.required_params,
                retries=node.main_task.retries,
                repeats=node.main_task.repeats,
                fallbacks=node.fallback_order,
                runner_override=node.runner_override,
                max_threads=node.max_threads,
            )

            return fn

        return decorator

    def fallback(
        self,
        node_name: str,
        name: str | None = None,
        retries: int = 0,
        repeats: int = 1,
    ):
        retries_checked = validate_non_negative_int("retries", retries)
        repeats_checked = validate_positive_int("repeats", repeats)

        def decorator(fn: Callable):
            node = self.ensure_node(node_name)
            node.mount_fallback(
                handler=fn,
                name=name,
                retries=retries_checked,
                repeats=repeats_checked,
            )

            if node.main_task is not None:
                self.storage.write_node_schema(
                    node_name=node_name,
                    allowed_params=node.main_task.allowed_params,
                    required_params=node.main_task.required_params,
                    retries=node.main_task.retries,
                    repeats=node.main_task.repeats,
                    fallbacks=node.fallback_order,
                    runner_override=node.runner_override,
                    max_threads=node.max_threads,
                )

            return fn

        return decorator

    # Friendly aliases if you prefer thinking in nodes instead of routers.
    include_node = include_router
    include_nodes = include_routers
    include_node_dir = include_router_dir
