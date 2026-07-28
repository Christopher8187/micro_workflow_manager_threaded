from pathlib import Path


def test_reference_layout_contains_required_resources():
    root = Path(__file__).resolve().parents[1]
    assert (root / "src" / "graph.py").exists()
    assert (root / "src" / "utils" / "http_client.py").exists()
    assert (root / "node" / "research_worker" / "input" / "system_prompt.md").exists()
