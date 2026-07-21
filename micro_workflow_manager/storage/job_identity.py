from __future__ import annotations

import hashlib
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from micro_workflow_manager.models import Job, QUEUED


class JobIdentityStorageMixin:
    """Idempotency, job IDs, and default-job declarations."""

    def idempotency_key_hash(self, key: str) -> str:
        if not isinstance(key, str) or not key.strip():
            raise ValueError("idempotency_key must be a non-empty string")
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    def lookup_idempotent_job(self, node_name: str, key: str) -> Job | None:
        key_hash = self.idempotency_key_hash(key)
        row = self.db_connection().execute(
            "SELECT job_id FROM idempotency WHERE node_name=? AND key_hash=?",
            (node_name, key_hash),
        ).fetchone()
        if row is None or not self.job_exists(node_name, int(row["job_id"])):
            return None
        return self.load_job(node_name, int(row["job_id"]))

    def record_idempotent_job(self, node_name: str, key: str, job_id: int):
        job_id = self.validate_job_id(job_id)
        key_hash = self.idempotency_key_hash(key)
        with self.db_transaction() as connection:
            connection.execute(
                "INSERT INTO idempotency(node_name, key_hash, key_text, job_id) "
                "VALUES(?, ?, ?, ?) "
                "ON CONFLICT(node_name, key_hash) DO UPDATE SET "
                "key_text=excluded.key_text, job_id=excluded.job_id",
                (node_name, key_hash, key, job_id),
            )

    def lookup_idempotent_jobs_batch(
        self,
        node_name: str,
        keys: list[str | None],
    ) -> dict[str, Job]:
        """Resolve existing idempotent jobs with one targeted SQLite query."""
        requested = {
            self.idempotency_key_hash(key): key
            for key in keys
            if key is not None
        }
        if not requested:
            return {}
        hashes = list(requested)
        placeholders = ",".join("?" for _ in hashes)
        rows = self.db_connection().execute(
            "SELECT i.key_hash, i.key_text, i.job_id "
            "FROM idempotency AS i "
            "JOIN jobs AS j ON j.node_name=i.node_name AND j.job_id=i.job_id "
            f"WHERE i.node_name=? AND i.key_hash IN ({placeholders})",
            [node_name, *hashes],
        ).fetchall()
        result: dict[str, Job] = {}
        for row in rows:
            key_hash = str(row["key_hash"])
            requested_key = requested[key_hash]
            if str(row["key_text"]) != requested_key:
                raise RuntimeError(
                    f"idempotency hash collision for node {node_name!r}"
                )
            result[requested_key] = self.load_job(node_name, int(row["job_id"]))
        return result

    def next_job_id(self, node_name: str) -> int:
        row = self.db_connection().execute(
            "SELECT next_job_id FROM job_sequences WHERE node_name=?",
            (node_name,),
        ).fetchone()
        if row is not None:
            return int(row["next_job_id"])
        row = self.db_connection().execute(
            "SELECT COALESCE(MAX(job_id), 0) + 1 AS next_id FROM jobs WHERE node_name=?",
            (node_name,),
        ).fetchone()
        return int(row["next_id"])

    def reserve_job_ids(self, node_name: str, count: int) -> list[int]:
        if type(count) is not int or count < 1:
            raise ValueError("count must be a positive integer")
        node_name = self.validate_node_name(node_name)

        def reserve(connection):
            row = connection.execute(
                "SELECT next_job_id FROM job_sequences WHERE node_name=?",
                (node_name,),
            ).fetchone()
            if row is None:
                start = int(
                    connection.execute(
                        "SELECT COALESCE(MAX(job_id), 0) + 1 FROM jobs WHERE node_name=?",
                        (node_name,),
                    ).fetchone()[0]
                )
                connection.execute(
                    "INSERT INTO job_sequences(node_name, next_job_id) VALUES(?, ?)",
                    (node_name, start + count),
                )
            else:
                start = int(row["next_job_id"])
                connection.execute(
                    "UPDATE job_sequences SET next_job_id=? WHERE node_name=?",
                    (start + count, node_name),
                )
            return list(range(start, start + count))

        # Queue publication outranks claims/checkpoints while a live router is
        # still handing work to its component peers. This prevents consumer
        # startup from throttling the producer that is feeding it.
        return self.submit_db_mutation(reserve, priority=0)

    def advance_job_sequence(self, node_name: str, next_job_id: int) -> None:
        node_name = self.validate_node_name(node_name)
        next_job_id = self.validate_job_id(next_job_id)
        with self.db_transaction() as connection:
            connection.execute(
                "INSERT INTO job_sequences(node_name, next_job_id) VALUES(?, ?) "
                "ON CONFLICT(node_name) DO UPDATE SET "
                "next_job_id=MAX(job_sequences.next_job_id, excluded.next_job_id)",
                (node_name, next_job_id),
            )

    def rewind_job_sequence_to_available(self, node_name: str) -> int:
        """Reset a quiescent node's allocator to its first available tail ID.

        Fresh ``run``/``runfrom`` cleanup may deliberately delete jobs that will
        be recreated immediately. The high-fanout sequence allocator introduced
        in 0.3.10 otherwise keeps advancing and changes deterministic job IDs on
        every fresh run. Call this only while the active-run slot is held and no
        worker is registering jobs for the node.
        """
        node_name = self.validate_node_name(node_name)
        with self.db_transaction() as connection:
            next_id = int(
                connection.execute(
                    "SELECT COALESCE(MAX(job_id), 0) + 1 FROM jobs WHERE node_name=?",
                    (node_name,),
                ).fetchone()[0]
            )
            connection.execute(
                "INSERT INTO job_sequences(node_name, next_job_id) VALUES(?, ?) "
                "ON CONFLICT(node_name) DO UPDATE SET next_job_id=excluded.next_job_id",
                (node_name, next_id),
            )
        return next_id

    def job_exists(self, node_name: str, job_id: int) -> bool:
        job_id = self.validate_job_id(job_id)
        row = self.db_connection().execute(
            "SELECT 1 FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, job_id),
        ).fetchone()
        return row is not None

    def default_job_spec_key(self, start_job_id: int, number: int) -> str:
        return f"{start_job_id}:{number}"

    def default_job_spec_current(
        self,
        node_name: str,
        *,
        start_job_id: int,
        number: int,
        params: dict[str, Any],
    ) -> bool:
        key = self.default_job_spec_key(start_job_id, number)
        row = self.db_connection().execute(
            "SELECT start_job_id, number, params_signature FROM default_job_specs "
            "WHERE node_name=? AND spec_key=?",
            (node_name, key),
        ).fetchone()
        if row is None:
            return False
        if (
            int(row["start_job_id"]) != start_job_id
            or int(row["number"]) != number
            or row["params_signature"] != self.json_signature(params)
        ):
            return False
        count = self.db_connection().execute(
            "SELECT COUNT(*) FROM jobs WHERE node_name=? AND job_id>=? AND job_id<?",
            (node_name, start_job_id, start_job_id + number),
        ).fetchone()[0]
        return int(count) == number

    def write_default_job_spec(
        self,
        node_name: str,
        *,
        start_job_id: int,
        number: int,
        params: dict[str, Any],
    ):
        key = self.default_job_spec_key(start_job_id, number)
        with self.db_transaction() as connection:
            connection.execute(
                "INSERT INTO default_job_specs"
                "(node_name, spec_key, start_job_id, number, params_signature) "
                "VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(node_name, spec_key) DO UPDATE SET "
                "start_job_id=excluded.start_job_id, number=excluded.number, "
                "params_signature=excluded.params_signature",
                (node_name, key, start_job_id, number, self.json_signature(params)),
            )
