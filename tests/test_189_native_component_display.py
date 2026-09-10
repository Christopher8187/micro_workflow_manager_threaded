"""CLI component lifecycle remains distinct from raw-node job counts."""

import json

from micro_workflow_manager import cli
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_149_interrupt_declarations import _router_source


def test_monitor_and_inspect_share_component_state_for_queued_and_sampled_members(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('A', 'B'), ('B', 'A')]",
        files={"A": _router_source("A", jobs=2),
               "B": _router_source("B", jobs=1, waiting=True, wait_for=("A",))},
    )
    capsys.readouterr()
    assert cli.main(["monitor", "--once", "--json"]) == 0
    initial = json.loads(capsys.readouterr().out)
    assert [(row["node"], row["status"]) for row in initial["nodes"]] == [
        ("A", "queued"), ("B", "queued"),
    ]
    assert initial["waiting_nodes"] == []
    assert cli.main(["run", "A", "sample", "50%", "--seed", "display", "--runner", "direct"]) == 0
    capsys.readouterr()
    assert cli.main(["monitor", "--once", "--json"]) == 0
    sampled = json.loads(capsys.readouterr().out)
    for row in sampled["nodes"]:
        assert row["status"] == row["state"] == "sampled"
        assert row["component"] == ["A", "B"]
        assert row["stability"] == "stable"
        assert row["instability_origin"] is None
        assert row["misaligned"] is False
        assert row["misalignment_causes"] == []
    assert [(row["queued"], row["done"]) for row in sampled["nodes"]] == [(1, 1), (1, 0)]
    assert sampled["waiting_nodes"] == []
    assert cli.main(["monitor", "--once"]) == 0
    rendered = capsys.readouterr().out
    assert "state=sampled" in rendered and "stability=stable" in rendered
    assert cli.main(["inspect", "B"]) == 0
    inspected = capsys.readouterr().out
    assert "state: sampled" in inspected
    assert "stability: stable" in inspected
    assert "misaligned: no" in inspected


def test_monitor_and_inspect_preserve_interrupt_origin_and_late_arrival_cause(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('P', 'I')]",
        files={
            "P": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("P", runner="direct")
                router.create_job()
                @router.task
                def run(ctx):
                    ctx.node("I").write_input("late.txt", "later input", overwrite=True)
            ''',
            "I": _router_source("I", jobs=2, interrupt="True"),
        },
    )
    assert cli.main([
        "run", "I", "sample", "50%", "--seed", "display-origin",
        "--interrupt", "--runner", "direct",
    ]) == 0
    capsys.readouterr()
    assert cli.main(["monitor", "--once", "--json"]) == 0
    before = json.loads(capsys.readouterr().out)
    first, = before["sessions"]
    assert first["session_kind"] == "interrupt"
    origin = first["session_id"]
    assert cli.main(["run", "P", "job", "1", "--runner", "direct"]) == 0
    capsys.readouterr()
    assert cli.main(["monitor", "--once", "--json"]) == 0
    after = json.loads(capsys.readouterr().out)
    row, = [item for item in after["nodes"] if item["node"] == "I"]
    assert row["state"] == row["status"] == "sampled"
    assert row["stability"] == "unstable" and row["instability_origin"] == origin
    assert row["misaligned"] is True
    cause, = row["misalignment_causes"]
    assert cause["receiver_node"] == "I"
    assert cause["producer_node"] == "P" and cause["producer_job_id"] == 1
    assert cause["path"] == "P/late.txt" and cause["arrival_kind"] == "managed-input"
    assert cli.main(["monitor", "--once"]) == 0
    rendered = capsys.readouterr().out
    assert "state=sampled stability=unstable instability_origin=" + origin in rendered
    assert '"path": "P/late.txt"' in rendered and "misaligned=yes" in rendered
    assert cli.main(["inspect", "I"]) == 0
    inspected = capsys.readouterr().out
    assert "state: sampled" in inspected and "stability: unstable" in inspected
    assert "instability origin: " + origin in inspected and "misaligned: yes" in inspected
    assert '"path": "P/late.txt"' in inspected
