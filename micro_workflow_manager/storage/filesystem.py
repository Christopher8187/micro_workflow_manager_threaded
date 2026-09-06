from __future__ import annotations

import os
import json
from pathlib import Path

from micro_workflow_manager.project_format import (
    is_link_or_reparse_point as _is_link_or_reparse_point,
    new_project_config,
    read_native_project_config,
    require_unchanged_path,
)
from .base import FileStorageBase
from .component_definitions import ComponentDefinitionStorageMixin
from .component_holds import ComponentHoldStorageMixin
from .component_misalignment import ComponentMisalignmentStorageMixin
from .component_reservations import ComponentReservationStorageMixin
from .component_states import ComponentStateStorageMixin
from .execution import JobExecutionStorageMixin
from .execution_sessions import ExecutionSessionStorageMixin
from .events import JobEventStorageMixin
from .input_publications import InputPublicationStorageMixin
from .job_index import JobIndexStorageMixin
from .jobs import JobFileStorageMixin
from .nodes import NodeFileStorageMixin
from .network_state import NetworkStateStorageMixin
from .runtime_config import RuntimeConfigStorageMixin
from .sqlite_state import SQLiteStateMixin
from .state_events import StateEventStorageMixin


class FileStorage(
    InputPublicationStorageMixin,
    ComponentMisalignmentStorageMixin,
    ComponentDefinitionStorageMixin,
    ComponentHoldStorageMixin,
    ComponentReservationStorageMixin,
    ComponentStateStorageMixin,
    ExecutionSessionStorageMixin,
    NetworkStateStorageMixin,
    StateEventStorageMixin,
    RuntimeConfigStorageMixin,
    JobEventStorageMixin,
    JobExecutionStorageMixin,
    JobFileStorageMixin,
    JobIndexStorageMixin,
    NodeFileStorageMixin,
    SQLiteStateMixin,
    FileStorageBase,
):
    """Hybrid storage: user payload files plus SQLite framework state."""

    def __init__(self, project_dir):
        root = Path(project_dir).resolve()
        if os.path.lexists(root / ".mwf"):
            self._initialize_storage(root)
        else:
            self._initialize_new_project_state(root)

    def _initialize_storage(self, project_dir, *, create=False):
        if not create:
            read_native_project_config(Path(project_dir))
            database = Path(project_dir) / '.mwf' / 'state.sqlite3'
            if _is_link_or_reparse_point(database):
                raise RuntimeError("Native MWF state database must not be a link")
        super().__init__(project_dir)
        self._init_network_state_publisher()
        self._init_state_event_broker()
        self._init_sqlite_state(create=create)
        self._init_job_execution_state()
        self._component_arrival_latches = {}

    @classmethod
    def _create_new_project_state(cls, project_dir):
        """Create native storage, refusing any preexisting runtime state."""
        storage = cls.__new__(cls)
        storage._initialize_new_project_state(project_dir)
        return storage

    def _initialize_new_project_state(self, project_dir):
        cls = type(self)
        root = Path(project_dir).resolve()
        cls._refuse_existing_runtime(root)
        # Claim the state directory before initializing the sole native format.
        metadata = root / ".mwf"
        metadata.mkdir(parents=True, exist_ok=False)
        directory_identity = metadata.stat()
        storage = self
        storage._fresh_database_owned = False
        configuration_identity = None
        database_identity = None
        storage._new_project_path_identities = [(metadata, directory_identity)]
        try:
            require_unchanged_path(metadata, directory_identity)
            with (metadata / 'state.sqlite3').open('xb') as handle:
                database_identity = os.fstat(handle.fileno())
                storage._fresh_database_owned = True
                storage._new_project_path_identities.append((metadata / 'state.sqlite3', database_identity))
            storage._initialize_storage(root, create=True)
            for path, identity in storage._new_project_path_identities:
                require_unchanged_path(path, identity)
            # Publish admission only after the native database is committed.
            # Exclusive creation preserves any unexpected configuration file.
            with (metadata / 'project.json').open('x', encoding='utf-8') as handle:
                configuration_identity = os.fstat(handle.fileno())
                storage._new_project_path_identities.append((metadata / 'project.json', configuration_identity))
                for path, identity in storage._new_project_path_identities:
                    require_unchanged_path(path, identity)
                handle.write(json.dumps(new_project_config(), indent=2) + '\n')
                handle.flush()
                os.fsync(handle.fileno())
            for path, identity in storage._new_project_path_identities:
                require_unchanged_path(path, identity)
        except BaseException as error:
            try:
                finalizer = getattr(storage, "_connection_finalizer", None)
                if finalizer is not None:
                    finalizer()
                path = getattr(storage, "_state_database_path_cached", None)
                if path is not None and storage._fresh_database_owned:
                    cls._initialized_databases.discard((path, os.getpid()))
                current_identity = metadata.lstat()
                if (_is_link_or_reparse_point(metadata)
                    or (directory_identity.st_dev, directory_identity.st_ino)
                    != (current_identity.st_dev, current_identity.st_ino)):
                    raise RuntimeError("The new state directory changed during initialization")
                if configuration_identity is not None:
                    configuration = metadata / 'project.json'
                    current_configuration = configuration.lstat()
                    if (_is_link_or_reparse_point(configuration)
                        or (configuration_identity.st_dev, configuration_identity.st_ino)
                        != (current_configuration.st_dev, current_configuration.st_ino)):
                        raise RuntimeError("The new project configuration changed during initialization")
                    configuration.unlink()
                # Owning the directory does not make a replacement file ours.
                if storage._fresh_database_owned:
                    database = metadata / "state.sqlite3"
                    current_database = database.lstat()
                    if (_is_link_or_reparse_point(database)
                        or (database_identity.st_dev, database_identity.st_ino)
                        != (current_database.st_dev, current_database.st_ino)):
                        raise RuntimeError("The new state database changed during initialization")
                    # SQLite closes its own journals with its connections. Do
                    # not open it again or remove companion files by name:
                    # another writer may have placed those files here.
                    database.unlink()
                # Preserve unexpected entries and let rmdir refuse them.
                metadata.rmdir()
            except Exception as cleanup_error:
                raise error from cleanup_error
            raise
        finally:
            storage._new_project_path_identities = ()
        return storage

    @staticmethod
    def _refuse_existing_runtime(root: Path) -> None:
        def refuse(path):
            raise RuntimeError(f"Fresh session storage requires no existing MWF runtime state: {path}")

        for name in (".mwf", ".mwf_run.json", ".mwf_threads.json", ".mwf_locks"):
            path = root / name
            if os.path.lexists(path):
                refuse(path)
        nodes = root / "node"
        if _is_link_or_reparse_point(nodes) or (os.path.lexists(nodes) and not nodes.is_dir()):
            refuse(nodes)
        if not nodes.is_dir():
            return
        for node in nodes.iterdir():
            if _is_link_or_reparse_point(node):
                refuse(node)
            if not node.is_dir():
                continue
            for name in ("schema.json", "node_state.json", "default_jobs.json",
                         "job_index.json", "job_index.dirty", "queued", "idempotency", "jobs"):
                path = node / name
                if os.path.lexists(path):
                    refuse(path)
