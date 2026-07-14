import zipfile
from pathlib import Path
from micro_workflow_manager import cli


def test_init_extracts_deployment_and_nested_node_archives(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    node_zip = source / 'A.zip'
    with zipfile.ZipFile(node_zip, 'w') as z:
        z.writestr('input/data.txt', 'hello')
    archive = tmp_path / 'deployment.zip'
    with zipfile.ZipFile(archive, 'w') as z:
        z.writestr('README.md', 'project')
        z.write(node_zip, 'node/A.zip')
    target = tmp_path / 'target'
    target.mkdir()
    monkeypatch.chdir(target)
    assert cli.main(['init', str(archive)]) == 0
    assert (target / 'README.md').read_text() == 'project'
    assert (target / 'node' / 'A' / 'input' / 'data.txt').read_text() == 'hello'
    assert not (target / 'node' / 'A.zip').exists()
    assert (target / '.mwf' / 'project.json').exists()


def test_copy_paste_and_debug(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert cli.main(['init']) == 0
    (tmp_path / 'src' / 'node_behavior').mkdir(parents=True)
    (tmp_path / 'src' / 'graph.py').write_text("EDGES=[('A','B')]", encoding='utf-8')
    (tmp_path / 'src' / 'node_behavior' / 'A.py').write_text("from micro_workflow_manager import NodeRouter\nrouter=NodeRouter('A')\n@router.task\ndef run(ctx): return None\n", encoding='utf-8')
    (tmp_path / 'src' / 'node_behavior' / 'B.py').write_text("from micro_workflow_manager import NodeRouter\nrouter=NodeRouter('B')\n@router.task\ndef run(ctx): return None\n", encoding='utf-8')
    assert cli.main(['graph','src/graph.py']) == 0
    debug = tmp_path / 'node' / 'A' / 'output' / 'debug.txt'
    debug.write_text('first\n', encoding='utf-8')
    assert cli.main(['copy','A']) == 0
    debug.write_text('changed\n', encoding='utf-8')
    assert cli.main(['paste','A']) == 0
    assert debug.read_text() == 'first\n'
    assert cli.main(['inspect','A','debug']) == 0
    assert 'first' in capsys.readouterr().out
