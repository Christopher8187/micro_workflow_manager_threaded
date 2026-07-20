"""Public API for micro-workflow-manager.

The public objects are imported lazily so lightweight control commands such as
``mwf restart`` can fence a running job without paying the graph/networkx import
cost first. Existing ``from micro_workflow_manager import NodeRouter`` usage is
unchanged.
"""

from __future__ import annotations

from importlib import import_module

__version__ = "0.3.16"


__all__ = [
    "MicroWorkflow",
    "GraphJobSystem",
    "JobNode",
    "JobContext",
    "NodeRouter",
    "DirectedFan",
    "fan",
    "FileSystem",
    "FileSystemEntry",
    "InputFileSystem",
    "OutputFileSystem",
    "JobFileSystem",
    "NodeInputFileSystem",
    "SharedHTTPTransport",
    "shared_http_transport",
    "configure_shared_http_transport",
    "close_shared_http_transport",
]


_EXPORTS = {
    "MicroWorkflow": (".system", "MicroWorkflow"),
    "JobNode": (".node", "JobNode"),
    "JobContext": (".context", "JobContext"),
    "NodeRouter": (".router", "NodeRouter"),
    "DirectedFan": (".graph", "DirectedFan"),
    "fan": (".graph", "fan"),
    "FileSystem": (".files", "FileSystem"),
    "FileSystemEntry": (".files", "FileSystemEntry"),
    "InputFileSystem": (".files", "InputFileSystem"),
    "OutputFileSystem": (".files", "OutputFileSystem"),
    "JobFileSystem": (".files", "JobFileSystem"),
    "NodeInputFileSystem": (".files", "NodeInputFileSystem"),
    "SharedHTTPTransport": (".networking", "SharedHTTPTransport"),
    "shared_http_transport": (".networking", "shared_http_transport"),
    "configure_shared_http_transport": (".networking", "configure_shared_http_transport"),
    "close_shared_http_transport": (".networking", "close_shared_http_transport"),
}


def __getattr__(name: str):
    if name == "GraphJobSystem":
        value = __getattr__("MicroWorkflow")
        globals()[name] = value
        return value

    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(name)

    module_name, attribute = target
    value = getattr(import_module(module_name, __name__), attribute)
    globals()[name] = value
    return value


def __dir__():
    return sorted({*globals(), *__all__})


# Install cooperative bridges once public imports are available.
from .fibers import install_bridges as _install_fiber_bridges
_install_fiber_bridges()
del _install_fiber_bridges
