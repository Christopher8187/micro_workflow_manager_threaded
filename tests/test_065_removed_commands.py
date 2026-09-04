from __future__ import annotations

import json
from hashlib import sha256

import pytest

from micro_workflow_manager import cli


def _project(root):
    (root / '.mwf').write_text(json.dumps({
        'edges': [['A', 'B']], 'graph_path': 'src/graph.py',
    }), encoding='utf-8')
    (root / 'src' / 'node_behavior').mkdir(parents=True)
    (root / 'src' / 'graph.py').write_text("EDGES = [('A', 'B')]\n", encoding='utf-8')
    for node in ('A', 'B'):
        (root / 'node' / node).mkdir(parents=True)


def _snapshot(root):
    return {
        path.relative_to(root): sha256(path.read_bytes()).hexdigest() if path.is_file() else 'directory'
        for path in root.rglob('*')
    }


@pytest.mark.parametrize('command', ['clean', 'cleanfrom', 'wipe', 'wipefrom'])
def test_removed_command_is_rejected_before_project_bootstrap(tmp_path, monkeypatch, capsys, command):
    monkeypatch.chdir(tmp_path)
    _project(tmp_path)
    before = _snapshot(tmp_path)

    with pytest.raises(SystemExit) as rejected:
        cli.main([command, 'A', '--yes'])

    assert rejected.value.code == 2
    assert 'invalid choice' in capsys.readouterr().err
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize('command', ['clean', 'cleanfrom', 'wipe', 'wipefrom'])
def test_removed_command_has_no_description_or_generated_example(tmp_path, monkeypatch, capsys, command):
    monkeypatch.chdir(tmp_path)
    _project(tmp_path)
    before = _snapshot(tmp_path)

    assert cli.main(['--describe', command]) == 1

    assert 'Unknown command for --describe' in capsys.readouterr().err
    assert _snapshot(tmp_path) == before
    assert cli.main([]) == 0
    assert f'mwf {command}' not in capsys.readouterr().out
    assert _snapshot(tmp_path) == before
