from micro_workflow_manager.paths import MWF_DIR_NAME

MWF_FILE = f"{MWF_DIR_NAME}/project.json"

RUNNER_CHOICES = ["threaded", "api", "process", "direct", "thread", "io", "network", "processes", "process_pool", "processpool"]

COMMAND_NAMES = [
    "init", "graph", "doctor", "migrate", "inspect", "trace", "recover", "clean",
    "reset", "wipe", "run", "restart", "threads", "deploy", "resume",
    "runfrom", "resumefrom", "monitor",
]
