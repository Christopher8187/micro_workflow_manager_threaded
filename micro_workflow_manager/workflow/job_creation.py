from pathlib import Path
from typing import Any

from ..errors import InvalidGraphError
from ..models import Job, QUEUED
from ..node import validate_positive_int


class JobCreationMixin:
    def validate_edge(self, from_node: str, to_node: str):
        if not self.graph_obj.has_edge(from_node, to_node):
            raise InvalidGraphError(f"{from_node} cannot create jobs on {to_node}")

    def start(
        self,
        node_name: str,
        job_id: int | None = None,
        autostart: bool = False,
        **params,
    ):
        return self.add_job(
            from_node=None,
            to_node=node_name,
            job_id=job_id,
            autostart=autostart,
            **params,
        )

    def create_jobs(
        self,
        node_name: str,
        *,
        number: int = 1,
        params: dict[str, Any] | None = None,
        start_job_id: int = 1,
    ) -> list[Job]:
        """Create deterministic default jobs for a node.

        This is the workflow-level companion to ``NodeRouter.create_job``.
        Existing default jobs with the same ids are refreshed in-place instead
        of duplicated, so importing routers during CLI commands is idempotent.
        """
        number = validate_positive_int("number", number)
        start_job_id = self.storage.validate_job_id(start_job_id)

        if params is None:
            params = {}

        if not isinstance(params, dict):
            raise ValueError("params must be a dict")

        # Reject unserializable params before writing anything.
        self.storage.json_text(Path("create_job_params.json"), params)

        node = self.ensure_node(node_name)
        created: list[Job] = []
        changed_any_job = False

        with self.storage.interprocess_lock(f"node-{node_name}-jobs"):
            with node.lock:
                node.validate_params(params)

                if self.storage.default_job_spec_current(
                    node_name,
                    start_job_id=start_job_id,
                    number=number,
                    params=params,
                ):
                    return [
                        Job(
                            job_id=start_job_id + offset,
                            node_name=node_name,
                            params=dict(params),
                            parent=None,
                        )
                        for offset in range(number)
                    ]

                for offset in range(number):
                    job_id = start_job_id + offset
                    existed = self.storage.job_exists(node_name, job_id)
                    previous_params = None
                    previous_parent = None
                    previous_status = None

                    if existed:
                        previous_params = self.storage.read_json(
                            self.storage.input_file(node_name, job_id),
                            default={},
                        )
                        previous_job_data = self.storage.read_json(
                            self.storage.job_file(node_name, job_id),
                            default={},
                        )
                        previous_parent = previous_job_data.get("parent")
                        previous_status = self.storage.get_job_status(node_name, job_id)

                    job = Job(
                        job_id=job_id,
                        node_name=node_name,
                        params=dict(params),
                        parent=None,
                    )
                    self.storage.ensure_job(job)
                    created.append(job)

                    if (
                        not existed
                        or previous_params != job.params
                        or previous_parent is not None
                        or previous_status is None
                    ):
                        changed_any_job = True

                self.storage.write_default_job_spec(
                    node_name,
                    start_job_id=start_job_id,
                    number=number,
                    params=params,
                )

                # Router-declared jobs are mounted every time the CLI loads the
                # workflow. Re-mounting an unchanged default job must not erase a
                # previously completed node status, otherwise `mwf run A` followed by
                # `mwf run B` would make B think A is unfinished. Only mark the node
                # queued when a default job was actually created, refreshed, or fixed.
                if changed_any_job:
                    self.storage.set_node_status(node_name, QUEUED)

        return created

    def add_job(
        self,
        from_node: str | None,
        to_node: str,
        job_id: int | None = None,
        autostart: bool = False,
        _parent_job_id: int | None = None,
        **params,
    ):
        if job_id is not None:
            self.storage.validate_job_id(job_id)

        if _parent_job_id is not None:
            self.storage.validate_job_id(_parent_job_id)

        if from_node is not None:
            self.validate_edge(from_node, to_node)

        if autostart and self.allowed_run_nodes is not None and to_node not in self.allowed_run_nodes:
            parent = f"{from_node}/{_parent_job_id}" if _parent_job_id is not None else str(from_node)
            raise InvalidGraphError(
                f"Autostart from {parent} to {to_node} was blocked because "
                f"{to_node} is outside the approved run set. "
                "Use mwf run/runfrom and approve detected autostarts, or include "
                "the target node in the run set. Dynamic autostarts may not be "
                "found by the static scanner."
            )

        node = self.ensure_node(to_node)

        with self.storage.interprocess_lock(f"node-{to_node}-jobs"):
            with node.lock:
                node.validate_params(params)

                if job_id is None:
                    job_id = self.storage.next_job_id(to_node)

                parent = None
                if from_node is not None:
                    parent = {
                        "from_node": from_node,
                        "from_job_id": _parent_job_id,
                    }

                job = Job(
                    job_id=job_id,
                    node_name=to_node,
                    params=params,
                    parent=parent,
                )

                self.storage.create_job(job)
                self.storage.set_node_status(to_node, QUEUED)

        if autostart and self.autostart_mode == "immediate":
            # Outside a running task, preserve the old convenience behavior:
            # start the requested job now. Inside a running task, preserve
            # immediate DAG autostart, but never recursively execute a job in
            # the same strongly-connected component. Same-component spawns are
            # queued as game-engine entities and picked up by the component pump.
            current_node = getattr(self._job_context, "node_name", None)
            same_component_spawn = (
                current_node is not None
                and to_node in self.component_for(current_node)
            )
            if not same_component_spawn:
                return self.run_job(
                    node_name=to_node,
                    job_id=job_id,
                    ignore_readiness=True,
                )

        return job
