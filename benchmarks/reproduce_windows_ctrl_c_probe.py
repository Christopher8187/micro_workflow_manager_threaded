"""Demonstrate why os.kill(pid, 0) is unsafe as a Windows PID probe.

Run on Windows in an expendable terminal. The fixed framework does not call this
operation; this file exists solely to preserve the diagnosis.
"""

from __future__ import annotations

import os
import sys


def main() -> int:
    if os.name != "nt":
        print("Windows-only reproduction; no signal sent.")
        return 0
    if "--send-ctrl-c" not in sys.argv:
        print("Refusing to send CTRL_C_EVENT without --send-ctrl-c")
        return 2
    print("Calling the old liveness probe against this console process...")
    os.kill(os.getpid(), 0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
