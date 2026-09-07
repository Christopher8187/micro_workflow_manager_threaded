from micro_workflow_manager.paths import MWF_DIR_NAME

MWF_FILE = f"{MWF_DIR_NAME}/project.json"

RUNNER_CHOICES = ["threaded", "api", "process", "direct", "thread", "io", "network", "processes", "process_pool", "processpool"]

COMMAND_NAMES = [
    "init", "copy", "paste", "graph", "engine", "doctor", "inspect", "trace", "filter", "recover",
    "reset", "resetfrom", "resetbetween", "run", "runbetween", "restart",
    "threads", "deploy", "resume", "runfrom", "resumefrom", "resumebetween", "monitor", "top",
]
