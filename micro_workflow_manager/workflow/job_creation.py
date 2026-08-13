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

                requested_jobs = [
                    Job(
                        job_id=start_job_id + offset,
                        node_name=node_name,
                        params=dict(params),
                        parent=None,
                    )
                    for offset in range(number)
                ]
                existing_ids = set(self.storage.list_job_ids(node_name))
                missing_jobs = [
                    job for job in requested_jobs if job.job_id not in existing_ids
                ]
                if missing_jobs:
                    self.storage.create_jobs_batch(missing_jobs)
                    changed_any_job = True

                for job in requested_jobs:
                    job_id = job.job_id
                    existed = job_id in existing_ids
                    previous_params = None
                    previous_parent = None
                    previous_status = None

                    if existed:
                        previous_params = self.storage.read_json(
                            self.storage.input_file(node_name, job_id),
                            default={},
                        )
                        previous_job_data = self.storage.read_job_metadata(
                            node_name, job_id
                        )
                        previous_parent = previous_job_data.get("parent")
                        previous_status = self.storage.get_job_status(node_name, job_id)

                    if existed:
                        if (
                            previous_params != job.params
                            or previous_parent is not None
                            or previous_status is None
                        ):
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
        _parent_event_data: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        **params,
    ):
        if job_id is not None:
            self.storage.validate_job_id(job_id)

        if _parent_job_id is not None:
            self.storage.validate_job_id(_parent_job_id)
        if _parent_event_data is not None and not isinstance(_parent_event_data, dict):
            raise TypeError("_parent_event_data must be a dict or None")

        def record_parent_event(created_job: Job) -> None:
            if (
                _parent_event_data is None
                or from_node is None
                or _parent_job_id is None
            ):
                return
            self.storage.append_job_event(
                from_node,
                _parent_job_id,
                "jobs_created",
                **_parent_event_data,
                jobs=[
                    {
                        "node": to_node,
                        "job_id": created_job.job_id,
                        "params": params,
                    }
                ],
            )

        if from_node is not None:
            self.validate_edge(from_node, to_node)
            if autostart:
                self.register_autostart_edge(from_node, to_node)

        source_component = self.component_id(from_node) if from_node is not None else None
        target_component = self.component_id(to_node)
        same_component = source_component is not None and source_component == target_component
        effective_autostart = bool(autostart or same_component)

        if effective_autostart and self.allowed_run_nodes is not None and to_node not in self.allowed_run_nodes:
            parent = f"{from_node}/{_parent_job_id}" if _parent_job_id is not None else str(from_node)
            raise InvalidGraphError(
                f"Autostart from {parent} to {to_node} was blocked because "
                f"{to_node} is outside the approved run set. "
                "Use mwf run/runfrom and approve detected autostarts, or include "
                "the target node in the run set. Dynamic autostarts may not be "
                "found by the static scanner."
            )

        node = self.ensure_node(to_node)
        node.validate_params(params)

        if idempotency_key is not None:
            existing = self.storage.lookup_idempotent_job(to_node, idempotency_key)
            if existing is not None:
                record_parent_event(existing)
                return existing

        parent = None
        if from_node is not None:
            parent = {
                "from_node": from_node,
                "from_job_id": _parent_job_id,
            }

        if job_id is not None:
            # Explicit IDs retain the cross-process compatibility lock because
            # their payload path is known before SQLite can reject a collision.
            with self.storage.interprocess_lock(f"node-{to_node}-jobs"):
                with node.lock:
                    if self.storage.job_exists(to_node, job_id):
                        raise ValueError(f"Job {to_node}/{job_id} already exists")
                    job = Job(
                        job_id=job_id,
                        node_name=to_node,
                        params=params,
                        parent=parent,
                        producer_component=source_component,
                        job_kind="component" if same_component else ("dag" if from_node is not None else None),
                    )
                    self.storage.create_job(job)
                    if idempotency_key is not None:
                        self.storage.record_idempotent_job(to_node, idempotency_key, job_id)
                    self.storage.set_node_status(to_node, QUEUED)
                    record_parent_event(job)
        else:
            # Stage the unpublished input first, then allocate its ID and
            # publish the payload/row/event in one priority queue mutation.
            job = self.storage.create_auto_id_job(
                node_name=to_node,
                params=params,
                parent=parent,
                producer_component=source_component,
                job_kind="component" if same_component else ("dag" if from_node is not None else None),
                idempotency_key=idempotency_key,
                parent_event=(
                    (from_node, _parent_job_id, _parent_event_data)
                    if (
                        from_node is not None
                        and _parent_job_id is not None
                        and _parent_event_data is not None
                    )
                    else None
                ),
            )
            job_id = job.job_id

        if effective_autostart and self.autostart_mode == "immediate":
            # Outside a running task, preserve the old convenience behavior:
            # start the requested job now. Inside a running task, preserve
            # immediate DAG autostart, but never recursively execute a job in
            # the same strongly-connected component. Same-component spawns are
            # queued as game-engine entities and picked up by the component pump.
            current_node = getattr(self._job_context, "node_name", None)
            same_component_spawn = (
                current_node is not None
                and self.component_id(current_node) == self.component_id(to_node)
            )
            if not same_component_spawn:
                return self.run_job(
                    node_name=to_node,
                    job_id=job_id,
                    ignore_readiness=True,
                )

        return job

    def add_jobs(
        self,
        from_node: str | None,
        to_node: str,
        params_list: list[dict[str, Any]],
        *,
        autostart: bool = False,
        _parent_job_id: int | None = None,
        idempotency_keys: list[str | None] | None = None,
    ):
        """Create a high-fanout batch while keeping one job per params object.

        The batch path uses SQLite transactions directly instead of one advisory
        lock per producer. IDs are reserved quickly, job payload files are written
        concurrently, and one final transaction resolves idempotency races.
        """
        if not isinstance(params_list, list):
            raise TypeError("params_list must be a list of dicts")
        if not params_list:
            return []
        if any(not isinstance(params, dict) for params in params_list):
            raise TypeError("each params_list entry must be a dict")
        if _parent_job_id is not None:
            self.storage.validate_job_id(_parent_job_id)

        if idempotency_keys is None:
            idempotency_keys = [None] * len(params_list)
        if not isinstance(idempotency_keys, list) or len(idempotency_keys) != len(params_list):
            raise ValueError("idempotency_keys must be a list matching params_list")
        non_null_keys = [key for key in idempotency_keys if key is not None]
        if len(non_null_keys) != len(set(non_null_keys)):
            raise ValueError("idempotency_keys must be unique within a batch")

        if from_node is not None:
            self.validate_edge(from_node, to_node)
            if autostart:
                self.register_autostart_edge(from_node, to_node)

        source_component = self.component_id(from_node) if from_node is not None else None
        target_component = self.component_id(to_node)
        same_component = source_component is not None and source_component == target_component
        effective_autostart = bool(autostart or same_component)

        if effective_autostart and self.allowed_run_nodes is not None and to_node not in self.allowed_run_nodes:
            parent = f"{from_node}/{_parent_job_id}" if _parent_job_id is not None else str(from_node)
            raise InvalidGraphError(
                f"Autostart from {parent} to {to_node} was blocked because "
                f"{to_node} is outside the approved run set. "
                "Use mwf run/runfrom and approve detected autostarts, or include "
                "the target node in the run set. Dynamic autostarts may not be "
                "found by the static scanner."
            )

        node = self.ensure_node(to_node)
        for params in params_list:
            node.validate_params(params)

        results: list[Job | None] = [None] * len(params_list)
        existing_by_key = self.storage.lookup_idempotent_jobs_batch(
            to_node, idempotency_keys
        )
        missing_indexes = [
            index
            for index, key in enumerate(idempotency_keys)
            if key is None or key not in existing_by_key
        ]
        reserved_ids = (
            self.storage.reserve_job_ids(to_node, len(missing_indexes))
            if missing_indexes
            else []
        )
        reserved_iter = iter(reserved_ids)
        new_jobs: list[Job] = []
        new_keys: list[str | None] = []
        new_indexes: list[int] = []

        for index, (params, key) in enumerate(zip(params_list, idempotency_keys)):
            if key is not None and key in existing_by_key:
                results[index] = existing_by_key[key]
                continue
            parent = None
            if from_node is not None:
                parent = {
                    "from_node": from_node,
                    "from_job_id": _parent_job_id,
                }
            job = Job(
                job_id=next(reserved_iter),
                node_name=to_node,
                params=dict(params),
                parent=parent,
                producer_component=source_component,
                job_kind=(
                    "component"
                    if same_component
                    else ("dag" if from_node is not None else None)
                ),
            )
            new_jobs.append(job)
            new_keys.append(key)
            new_indexes.append(index)

        self.storage.prepare_jobs_batch(new_jobs)
        try:
            committed, raced_ids = (
                self.storage.commit_prepared_jobs_batch_resolving_idempotency(
                    new_jobs,
                    idempotency_keys=new_keys,
                )
            )
        except BaseException:
            # The transaction did not commit, so every prepared directory is
            # disposable. Never remove committed payloads during result loading.
            self.storage.discard_prepared_jobs(new_jobs)
            raise

        committed_ids = {job.job_id for job in committed}
        discard = [job for job in new_jobs if job.job_id not in committed_ids]
        if discard:
            self.storage.discard_prepared_jobs(discard)

        committed_by_id = {job.job_id: job for job in committed}
        for index, job, key in zip(new_indexes, new_jobs, new_keys):
            if job.job_id in committed_by_id:
                results[index] = job
            elif key is not None and key in raced_ids:
                results[index] = self.storage.load_job(to_node, raced_ids[key])
            else:
                raise RuntimeError("batch idempotency resolution lost a job")

        created = [job for job in results if job is not None]
        if len(created) != len(params_list):
            raise RuntimeError("internal batch job creation result mismatch")

        if effective_autostart and self.autostart_mode == "immediate":
            current_node = getattr(self._job_context, "node_name", None)
            same_component_spawn = (
                current_node is not None
                and self.component_id(current_node) == self.component_id(to_node)
            )
            if not same_component_spawn:
                return [
                    self.run_job(
                        node_name=to_node,
                        job_id=job.job_id,
                        ignore_readiness=True,
                    )
                    for job in created
                ]

        return created
