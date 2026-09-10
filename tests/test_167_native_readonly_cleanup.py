"""Recover read-only native outputs, including interrupted private deletion."""

from __future__ import annotations

import os
from pathlib import Path
import stat

import pytest

from micro_workflow_manager import cli
from tests.test_064_read_only_previews import _snapshot
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_156_native_session_recovery_receipts import (
    SimulatedRecoveryProcessLoss,
    _leave_committed_native_recovery,
)
from tests.test_159_native_recovery_saved_cleanup import _receipt_manifest


@pytest.fixture(autouse=True)
def _project_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


def _leave_readonly_committed_output(tmp_path, monkeypatch):
    readonly = tmp_path / "node" / "A" / "jobs" / "1" / "output.json"
    write_bytes = Path.write_bytes
    recorded = []

    def write_readonly_output(path, data):
        result = write_bytes(path, data)
        if path == readonly:
            path.chmod(0o444)
            recorded.append(path.stat().st_mode)
        return result

    with monkeypatch.context() as patch:
        patch.setattr(Path, "write_bytes", write_readonly_output)
        committed = _leave_committed_native_recovery(tmp_path, monkeypatch)
    assert len(recorded) == 1
    assert stat.S_IMODE(recorded[0]) == 0o444
    receipt, manifest, _ = _receipt_manifest(tmp_path, committed)
    entry = next(item for item in manifest["paths"]
                 if item["original"]["path"] == "node/A/jobs/1/output.json")
    assert stat.S_IMODE(entry["original"]["marker"][2]) == 0o444
    saved = committed["staging"] / entry["saved"]
    assert stat.S_IMODE(saved.stat().st_mode) == 0o444
    return committed, receipt, manifest, entry, saved.read_bytes()


@pytest.mark.parametrize("interrupt_before_unlink", [False, True])
def test_recover_cleans_readonly_native_output_and_retries_interrupted_deletion(
    tmp_path, monkeypatch, interrupt_before_unlink,
):
    committed, receipt, manifest, entry, original_bytes = (
        _leave_readonly_committed_output(tmp_path, monkeypatch)
    )
    before_rows = _closed_database_rows(tmp_path)
    before_node = _snapshot(tmp_path / "node")
    detached = tmp_path.joinpath(*manifest["discard"]["directory"].split("/")) / entry["saved"]

    if interrupt_before_unlink:
        unlink = Path.unlink
        interrupted = []
        process_loss = SimulatedRecoveryProcessLoss("lost after readonly cleanup preparation")

        def lose_before_unlink(path, *args, **kwargs):
            if path == detached:
                assert path.read_bytes() == original_bytes
                assert stat.S_IMODE(path.stat().st_mode) == (0o666 if os.name == "nt" else 0o444)
                interrupted.append(path)
                raise process_loss
            return unlink(path, *args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(Path, "unlink", lose_before_unlink)
            with pytest.raises(SimulatedRecoveryProcessLoss) as caught:
                cli.main(["recover"])
        assert caught.value is process_loss
        assert interrupted == [detached]
        assert detached.read_bytes() == original_bytes
        assert committed["staging"].is_dir()
        assert _closed_database_rows(tmp_path) == before_rows
        assert _snapshot(tmp_path / "node") == before_node

    assert cli.main(["recover"]) == 0
    assert not committed["staging"].exists()
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path / "node") == before_node
    assert _receipt_manifest(tmp_path, committed)[0] == receipt

    assert cli.main(["recover"]) == 0
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path / "node") == before_node
