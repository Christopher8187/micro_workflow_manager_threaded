from __future__ import annotations

import json
import secrets
import threading
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import networkx as nx

from micro_workflow_manager.paths import LEGACY_CONFIG_NAME, config_file

from .autostart_scan import scan_autostarts
from .files import safe_node_name
from .project import resolve_stored_graph_path


def _read_config_without_migration(root: Path) -> dict:
    """Read synchronized project metadata without creating or migrating files."""
    current = config_file(root)
    legacy = root / LEGACY_CONFIG_NAME
    if current.is_file():
        path = current
    elif legacy.is_file():
        path = legacy
    else:
        raise RuntimeError("Not an mwf project. Run: mwf init")

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Cannot read MWF project configuration: {error}") from error
    if not isinstance(value, dict):
        raise RuntimeError("Invalid MWF project configuration: expected a JSON object")
    return value


def _stored_edges(config: dict) -> list[tuple[str, str]]:
    raw = config.get("edges")
    if not isinstance(raw, list):
        raise RuntimeError("No synchronized graph is stored. Run: mwf graph src/graph.py")

    edges: list[tuple[str, str]] = []
    for edge in raw:
        if not isinstance(edge, (list, tuple)) or len(edge) != 2:
            raise RuntimeError("Invalid synchronized graph edge in .mwf/project.json")
        start, end = edge
        if not isinstance(start, str) or not isinstance(end, str):
            raise RuntimeError("Invalid synchronized graph edge in .mwf/project.json")
        edges.append((safe_node_name(start), safe_node_name(end)))
    return edges


def _read_autostart_edges(root: Path, config: dict, edges: list[tuple[str, str]]) -> set[tuple[str, str]]:
    graph_path = config.get("graph_path")
    if not isinstance(graph_path, str) or not graph_path.strip():
        return set()
    graph_file = resolve_stored_graph_path(root, graph_path)
    behavior_dir = graph_file.parent / "node_behavior"
    if not behavior_dir.is_dir():
        return set()

    declared = set(edges)
    try:
        found = scan_autostarts(behavior_dir)
    except (OSError, SyntaxError, ValueError):
        # The synchronized graph remains useful even while a behavior file is
        # temporarily broken. Engine must never import or repair project code.
        return set()
    return {
        (start, end)
        for start, targets in found.items()
        for end in targets
        if (start, end) in declared
    }


def build_engine_snapshot(root: Path) -> dict:
    """Build the graph-only engine model from synchronized, read-only metadata."""
    root = root.resolve()
    config = _read_config_without_migration(root)
    edges = _stored_edges(config)
    graph = nx.DiGraph()
    graph.add_edges_from(edges)
    if not graph.nodes:
        raise RuntimeError("The synchronized graph has no nodes to display")

    autostart_edges = _read_autostart_edges(root, config, edges)
    augmented = graph.copy()
    for start, end in autostart_edges:
        augmented.add_edge(end, start)

    components = [tuple(sorted(component)) for component in nx.strongly_connected_components(augmented)]
    components.sort()
    component_for = {
        node: component
        for component in components
        for node in component
    }

    quotient = nx.DiGraph()
    quotient.add_nodes_from(components)
    for start, end in edges:
        source = component_for[start]
        target = component_for[end]
        if source != target:
            quotient.add_edge(source, target)

    # Longest-path generations make fan-out and fan-in visually explicit while
    # keeping each component in one stable left-to-right layer.
    generations = [list(generation) for generation in nx.topological_generations(quotient)]
    for generation in generations:
        generation.sort()

    card_width = 244
    card_height = 92
    column_gap = 148
    row_gap = 72
    layer_heights = [
        len(generation) * card_height + max(0, len(generation) - 1) * row_gap
        for generation in generations
    ]
    canvas_height = max(420, max(layer_heights, default=card_height) + 160)

    component_ids = {
        component: f"component-{index}"
        for index, component in enumerate(components, 1)
    }
    nodes = []
    for column, generation in enumerate(generations):
        layer_height = layer_heights[column]
        top = (canvas_height - layer_height) / 2
        for row, component in enumerate(generation):
            cyclic = len(component) > 1 or any(graph.has_edge(name, name) for name in component)
            nodes.append(
                {
                    "id": component_ids[component],
                    "members": list(component),
                    "cyclic": cyclic,
                    "x": 100 + column * (card_width + column_gap),
                    "y": top + row * (card_height + row_gap),
                    "width": card_width,
                    "height": card_height,
                }
            )

    rendered_edges = [
        {
            "source": component_ids[source],
            "target": component_ids[target],
        }
        for source, target in quotient.edges
    ]
    canvas_width = max(
        620,
        200 + len(generations) * card_width + max(0, len(generations) - 1) * column_gap,
    )
    return {
        "nodes": nodes,
        "edges": rendered_edges,
        "canvas": {"width": canvas_width, "height": canvas_height},
    }


def render_engine_html(snapshot: dict) -> str:
    data = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MWF engine</title>
<style>
:root {{ color-scheme: light dark; --bg:#0b0f15; --grid:#18212d; --card:#151c26; --edge:#53647a; --text:#edf4ff; --muted:#91a0b4; --accent:#7ca7ff; --cycle:#b393ff; --shadow:rgba(0,0,0,.42); }}
@media (prefers-color-scheme:light) {{ :root {{ --bg:#f3f6fa; --grid:#dce4ee; --card:#fff; --edge:#8b9bb0; --text:#152030; --muted:#647287; --accent:#235fc4; --cycle:#7047b8; --shadow:rgba(31,45,61,.16); }} }}
* {{ box-sizing:border-box; }}
html,body {{ width:100%; height:100%; margin:0; overflow:hidden; background:var(--bg); }}
body {{ font-family:ui-monospace,SFMono-Regular,Cascadia Code,Consolas,Liberation Mono,monospace; }}
svg {{ width:100%; height:100%; display:block; cursor:grab; touch-action:none; user-select:none; }}
svg.dragging {{ cursor:grabbing; }}
.grid {{ fill:url(#grid); }}
.edge {{ fill:none; stroke:var(--edge); stroke-width:2; opacity:.78; }}
.edge-glow {{ fill:none; stroke:var(--accent); stroke-width:7; opacity:.045; }}
.node {{ cursor:default; }}
.node.multi {{ cursor:pointer; }}
.card {{ fill:var(--card); stroke:color-mix(in srgb,var(--edge) 76%,transparent); stroke-width:1.4; filter:url(#shadow); }}
.node:hover .card {{ stroke:var(--accent); stroke-width:2; }}
.cycle-ring {{ fill:none; stroke:var(--cycle); stroke-width:2; stroke-dasharray:5 5; opacity:.85; }}
.name {{ fill:var(--text); font-size:15px; font-weight:650; }}
.kind {{ fill:var(--muted); font-size:11px; letter-spacing:.04em; }}
.count {{ fill:var(--cycle); font-size:12px; }}
.member-dot {{ fill:var(--cycle); opacity:.82; }}
.detail {{ display:none; pointer-events:none; }}
.node.open .detail {{ display:block; }}
.detail-bg {{ fill:var(--card); stroke:var(--cycle); stroke-width:1.5; filter:url(#shadow); }}
.detail-line {{ stroke:var(--edge); stroke-width:1.2; opacity:.65; }}
.detail-name {{ fill:var(--text); font-size:12px; }}
@media (prefers-reduced-motion:no-preference) {{ .card,.edge {{ transition:stroke .16s ease,opacity .16s ease; }} }}
</style>
</head>
<body>
<svg id="graph" role="img" aria-label="MWF workflow graph">
  <defs>
    <pattern id="grid" width="28" height="28" patternUnits="userSpaceOnUse"><path d="M 28 0 L 0 0 0 28" fill="none" stroke="var(--grid)" stroke-width="1"/></pattern>
    <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="var(--edge)"/></marker>
    <filter id="shadow" x="-30%" y="-30%" width="160%" height="170%"><feDropShadow dx="0" dy="8" stdDeviation="10" flood-color="var(--shadow)"/></filter>
  </defs>
  <rect class="grid" x="-10000" y="-10000" width="20000" height="20000"/>
  <g id="viewport"></g>
</svg>
<script>
const model={data};
const svg=document.getElementById('graph');
const viewport=document.getElementById('viewport');
const ns='http://www.w3.org/2000/svg';
const byId=new Map(model.nodes.map(n=>[n.id,n]));
const make=(name,attrs={{}})=>{{const el=document.createElementNS(ns,name);for(const [k,v] of Object.entries(attrs))el.setAttribute(k,v);return el;}};
const edgePath=(a,b)=>{{const x1=a.x+a.width,y1=a.y+a.height/2,x2=b.x,y2=b.y+b.height/2,c=Math.max(70,(x2-x1)*.48);return `M${{x1}},${{y1}} C${{x1+c}},${{y1}} ${{x2-c}},${{y2}} ${{x2}},${{y2}}`;}};
for(const edge of model.edges){{const a=byId.get(edge.source),b=byId.get(edge.target),d=edgePath(a,b);viewport.append(make('path',{{d,class:'edge-glow'}}));viewport.append(make('path',{{d,class:'edge','marker-end':'url(#arrow)'}}));}}
for(const node of model.nodes){{
  const g=make('g',{{class:`node${{node.members.length>1?' multi':''}}`,transform:`translate(${{node.x}} ${{node.y}})`,tabindex:node.members.length>1?'0':'-1','aria-label':node.members.join(', ')}});
  g.append(make('rect',{{class:'card',width:node.width,height:node.height,rx:14}}));
  if(node.cyclic)g.append(make('rect',{{class:'cycle-ring',x:7,y:7,width:node.width-14,height:node.height-14,rx:10}}));
  const primary=make('text',{{class:'name',x:20,y:34}});primary.textContent=node.members.length===1?node.members[0]:`${{node.members[0]}} + ${{node.members.length-1}}`;g.append(primary);
  const kind=make('text',{{class:'kind',x:20,y:59}});kind.textContent=node.members.length===1?'NODE':`HOEFLEIN COMPONENT · ${{node.members.length}} NODES`;g.append(kind);
  if(node.members.length>1){{for(let i=0;i<Math.min(node.members.length,11);i++)g.append(make('circle',{{class:'member-dot',cx:20+i*11,cy:75,r:2.6}}));
    const panelHeight=32+node.members.length*25;
    const detailY=node.y>panelHeight+36?-panelHeight-16:node.height+16;
    const detail=make('g',{{class:'detail',transform:`translate(${{(node.width-270)/2}} ${{detailY}})`}});
    detail.append(make('rect',{{class:'detail-bg',width:270,height:panelHeight,rx:12}}));
    node.members.forEach((member,index)=>{{const y=24+index*25;if(index)detail.append(make('line',{{class:'detail-line',x1:16,y1:y-14,x2:254,y2:y-14}}));const t=make('text',{{class:'detail-name',x:18,y}});t.textContent=member;detail.append(t);}});g.append(detail);
    const toggle=()=>{{for(const other of viewport.querySelectorAll('.node.open'))if(other!==g)other.classList.remove('open');g.classList.toggle('open');}};
    g.addEventListener('click',e=>{{e.stopPropagation();toggle();}});g.addEventListener('keydown',e=>{{if(e.key==='Enter'||e.key===' '){{e.preventDefault();toggle();}}}});
  }}
  viewport.append(g);
}}
let scale=1,tx=0,ty=0,drag=null;
const apply=()=>viewport.setAttribute('transform',`translate(${{tx}} ${{ty}}) scale(${{scale}})`);
const fit=()=>{{const box=svg.getBoundingClientRect(),focus=model.nodes.find(n=>n.cyclic)||model.nodes[Math.floor(model.nodes.length/2)];scale=Math.max(.72,Math.min(1,(box.height-96)/model.canvas.height));const graphFits=model.canvas.width*scale<=box.width-96;tx=graphFits?(box.width-model.canvas.width*scale)/2:box.width/2-(focus.x+focus.width/2)*scale;ty=(box.height-model.canvas.height*scale)/2;apply();}};
fit();addEventListener('resize',fit);
svg.addEventListener('wheel',e=>{{e.preventDefault();const rect=svg.getBoundingClientRect(),px=e.clientX-rect.left,py=e.clientY-rect.top,old=scale;scale=Math.max(.22,Math.min(3.2,scale*Math.exp(-e.deltaY*.0012)));tx=px-(px-tx)*(scale/old);ty=py-(py-ty)*(scale/old);apply();}},{{passive:false}});
svg.addEventListener('pointerdown',e=>{{if(e.target.closest('.node'))return;drag={{x:e.clientX,y:e.clientY,tx,ty}};svg.setPointerCapture(e.pointerId);svg.classList.add('dragging');for(const open of viewport.querySelectorAll('.node.open'))open.classList.remove('open');}});
svg.addEventListener('pointermove',e=>{{if(!drag)return;tx=drag.tx+e.clientX-drag.x;ty=drag.ty+e.clientY-drag.y;apply();}});
const stop=()=>{{drag=null;svg.classList.remove('dragging');}};svg.addEventListener('pointerup',stop);svg.addEventListener('pointercancel',stop);
</script>
</body>
</html>"""


class _EngineServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, handler, *, token: str, html: bytes):
        super().__init__(address, handler)
        self.token = token
        self.html = html


class _EngineHandler(BaseHTTPRequestHandler):
    server: _EngineServer

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path == f"/{self.server.token}/":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(self.server.html)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'")
            self.end_headers()
            self.wfile.write(self.server.html)
            return
        if self.path.endswith("/favicon.ico"):
            self.send_response(HTTPStatus.NO_CONTENT)
            self.end_headers()
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self.send_error(HTTPStatus.METHOD_NOT_ALLOWED)

    def log_message(self, format: str, *args) -> None:
        return


def engine_command(root: Path) -> int:
    snapshot = build_engine_snapshot(root)
    html = render_engine_html(snapshot).encode("utf-8")
    token = secrets.token_urlsafe(24)
    server = _EngineServer(("127.0.0.1", 0), _EngineHandler, token=token, html=html)
    port = int(server.server_address[1])
    url = f"http://127.0.0.1:{port}/{token}/"
    opened = webbrowser.open(url, new=2)
    if not opened:
        print(url)
    print("MWF engine is displaying the graph. Press Ctrl+C to close it.")
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
        # ThreadingHTTPServer owns only daemon request threads, but explicitly
        # yield once so a just-opened browser request can finish cleanly.
        threading.Event().wait(0)
    return 0
