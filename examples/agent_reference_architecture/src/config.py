from __future__ import annotations

import os

AGENT_URL = os.getenv("MWF_EXAMPLE_AGENT_URL", "").strip()
AGENT_TOKEN = os.getenv("MWF_EXAMPLE_AGENT_TOKEN", "").strip()
CONNECT_TIMEOUT_SECONDS = 5.0
READ_TIMEOUT_SECONDS = 45.0
MAX_REVISIONS = 2
