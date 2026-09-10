"""Move an observed file or directory without replacing a destination."""

from __future__ import annotations

import ctypes
import os
from pathlib import Path
import sys


def move_without_replacement(source, destination):
    source, destination = Path(source), Path(destination)
    if os.name == 'nt':
        # Python documents that Windows rename refuses an existing destination.
        # https://docs.python.org/3/library/os.html#os.rename
        source.rename(destination)
        return
    library = ctypes.CDLL(None, use_errno=True)
    source_bytes, destination_bytes = os.fsencode(source), os.fsencode(destination)
    if sys.platform.startswith('linux') and hasattr(library, 'renameat2'):
        # RENAME_NOREPLACE is atomic, including when the destination is a directory.
        # https://man7.org/linux/man-pages/man2/rename.2.html
        rename = library.renameat2
        rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        result = rename(-100, source_bytes, -100, destination_bytes, 1)
    elif sys.platform == 'darwin' and hasattr(library, 'renamex_np'):
        rename = library.renamex_np
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        result = rename(source_bytes, destination_bytes, 4)
    else:
        raise RuntimeError('This platform cannot perform an exclusive recovery rename')
    if result:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(destination))
