HELP_EPILOG = """
Common help commands:
  mwf --help
  mwf init --help
  mwf graph --help
  mwf clean --help
  mwf reset --help
  mwf wipe --help
  mwf run --help
  mwf runfrom --help
  mwf monitor --help

Command descriptions:
  mwf --describe init
  mwf --describe graph
  mwf --describe clean
  mwf --describe reset
  mwf --describe wipe
  mwf --describe run
  mwf --describe runfrom
  mwf --describe monitor

Typical flow:
  mwf init
  mwf graph src/graph.py
  mwf run start_node
  mwf run start_node job 1 3 8-10
  mwf runfrom start_node
  mwf runfrom start_node --stats
  mwf monitor              # live workflow statistics in a second terminal
  mwf monitor --once       # one status snapshot

Cleaning:
  mwf clean node_name   # clear output/files/jobs for one node, keep input files
  mwf clean *           # clean every node in the graph
  mwf reset node_name   # clear outputs and rerun existing jobs, keep job definitions
  mwf reset *           # reset every node in the graph
  mwf wipe node_name    # clean one node and remove its input folder too
  mwf wipe *            # wipe every node in the graph
"""

COMMAND_DESCRIPTIONS = {
    "init": """
init creates the project marker file .mwf in the current directory.

Code context:
  init does not import your graph.py or node_behavior files.

File-system context:
  writes .mwf with the default runner set to threaded
  stores graph_path as null until you run mwf graph <path>
  does not create node folders until a graph is loaded

Use when:
  starting a new micro-workflow project folder
""",
    "graph": """
graph records the Python file that defines your DAG edges.

Code context:
  imports the supplied graph file
  reads EDGES or edges from that module
  imports sibling node_behavior/*.py files when loading the workflow
  mounts NodeRouter objects found in those behavior files

File-system context:
  updates .mwf with graph_path, runner, and edges
  creates node/<node-name>/ folders for graph nodes
  each node folder contains input/, output/, jobs/, node_state.json, and schema.json when applicable

Use when:
  you add or change graph edges, or you want to change the stored default runner:
    mwf graph src/graph.py --runner threaded
    mwf graph src/graph.py --runner process
    mwf graph src/graph.py --runner direct
""",
    "clean": """
clean resets runnable state for one or more nodes while preserving node input files.

Code context:
  loads .mwf, graph.py, and node_behavior/*.py so it can validate node names
  does not run your node task code

File-system context for each selected node:
  deletes node/<node>/output/
  keeps node/<node>/input/
  deletes node/<node>/jobs/ entirely
  resets node_state.json to queued
  the next CLI load will recreate router.create_job(...) defaults if declared
  use reset instead when you want to keep existing job definitions and inputs

Targets:
  mwf clean node_name   cleans one node
  mwf clean a b c       cleans several named nodes
  mwf clean *           cleans every node in the graph

Shell note:
  in shells that expand *, quote it as mwf clean "*" if needed.
""",
    "reset": """
reset reruns existing jobs for one or more nodes while preserving node input files and job definitions.

Code context:
  loads .mwf, graph.py, and node_behavior/*.py so it can validate node names
  does not run your node task code

File-system context for each selected node:
  deletes node/<node>/output/
  keeps node/<node>/input/
  keeps node/<node>/jobs/<id>/job.json and input.json
  removes per-job status/output/files so those jobs are queued again
  resets node_state.json to queued
  unlike clean, it does not delete the jobs folder or erase existing job inputs

Targets:
  mwf reset node_name   resets one node
  mwf reset a b c       resets several named nodes
  mwf reset *           resets every node in the graph

Shell note:
  in shells that expand *, quote it as mwf reset "*" if needed.
""",
    "wipe": """
wipe is clean plus input removal.

Code context:
  loads .mwf, graph.py, and node_behavior/*.py so it can validate node names
  does not run your node task code

File-system context for each selected node:
  does everything clean does
  also deletes and recreates node/<node>/input/

Targets:
  mwf wipe node_name   wipes one node
  mwf wipe a b c       wipes several named nodes
  mwf wipe *           wipes every node in the graph

Shell note:
  in shells that expand *, quote it as mwf wipe "*" if needed.
""",
    "run": """
run executes one ready node, or selected jobs inside that node.

Job-selection syntax:
  mwf run node_name job 1
  mwf run node_name job 1 3 8-10

The job form reruns only the selected job IDs. Other jobs on the same node are
left untouched. Selected jobs are reset before execution, so previously done or
failed jobs can be rerun.

Code context:
  loads .mwf, graph.py, and node_behavior/*.py
  validates the requested node exists in the graph
  may inspect node_behavior source code for simple autostart calls
  uses the stored runner from .mwf unless --runner is provided
  honors per-node runner overrides such as runner="direct" or router.run_sequentially()

File-system context:
  does not invent starter jobs; declare them with router.create_job(...) in node_behavior/<node>.py
  resets the requested node's existing job artifacts before running it
  if detected autostart nodes are included, resets those downstream nodes before running
  reads jobs from node/<node>/jobs/
  writes task artifacts under node/<node>/jobs/<job-id>/files/ and node/<node>/output/
  in job-selection mode, clears only the selected job folders' status/output/files artifacts

Use when:
  you want to rerun a single ready node, or a node plus detected autostart chain
  you want to rerun exact jobs without disturbing the rest of the node:
    mwf run tagify job 1 3 8-10
  you want compact inline status while it runs:
    mwf run tagify --stats
""",

    "monitor": """
monitor prints workflow statistics from the file-backed node/job folders.

Code context:
  loads .mwf, graph.py, and node_behavior/*.py so it knows graph nodes,
  component order, node max_threads, and runner overrides
  does not run your task code

File-system context:
  reads node/<node>/node_state.json
  reads node/<node>/jobs/<id>/job.json and status.json
  reads .mwf_run.json when a run or runfrom command is active or recently finished
  estimates ETA from duration_seconds written to completed job status files

Use when:
  you want a second terminal dashboard while another terminal runs work:
    mwf runfrom start_node
    mwf monitor

Options:
  mwf monitor --once       print one snapshot and exit
  mwf monitor A B          monitor only selected nodes
  mwf monitor --json       output machine-readable JSON
""",
    "runfrom": """
runfrom executes a node and its descendants in graph order, with threaded execution where possible.

Code context:
  loads .mwf, graph.py, and node_behavior/*.py
  computes all descendants of the requested start node
  may inspect node_behavior source code for simple autostart calls outside that descendant set
  uses the stored runner from .mwf unless --runner is provided
  honors per-node runner overrides such as runner="direct", runner="threaded", or runner="process"

File-system context:
  resets the starting node's existing job artifacts before running it
  resets descendant nodes selected for this run while preserving router.create_job(...) jobs
  removes stale parent-generated descendant jobs so upstream nodes can regenerate them
  queues autostart-created jobs instead of letting them escape the selected run set
  does not invent starter jobs; declare them with router.create_job(...) in node_behavior/<node>.py
  reads and writes each selected node's node/<node>/jobs/, node/<node>/output/, and node_state.json
  refuses to continue if direct upstream nodes outside the run set are incomplete, unless you confirm

Use when:
  you want a safe partial workflow rerun from one node onward
  you want compact inline status while it runs:
    mwf runfrom split --stats
  for a full dashboard in another terminal, use:
    mwf monitor
""",
}
