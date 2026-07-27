from __future__ import annotations

import json
from typing import Any


WIDTH = 82


def _line(char: str = "-") -> str:
    return char * WIDTH


def _block(title: str, body: str | None = None) -> None:
    print(_line("="))
    print(title)
    print(_line("-"))
    if body:
        print(body.rstrip())
    print(_line("="))


def _json(value: Any, *, limit: int = 12000) -> str:
    text = json.dumps(value, ensure_ascii=False, indent=2, default=str)
    return text if len(text) <= limit else text[:limit] + "\n... [truncated]"


def _task_label(event: dict[str, Any], *, default: str = "MAIN TASK") -> str:
    role_value = event.get("task_role")
    if role_value is None:
        return default
    role = str(role_value)
    task = str(event.get("task") or "")
    if role == "fallback":
        return task or "FALLBACK"
    return "MAIN TASK" if role == "main" else (task or default)


def _body_without(event: dict[str, Any], *keys: str) -> dict[str, Any]:
    excluded = {"time", "event", "task", "task_role", "attempt", "repeat_index", *keys}
    return {key: value for key, value in event.items() if key not in excluded and value is not None}


def _origin_label(parent: Any) -> str:
    if isinstance(parent, dict):
        from_node = parent.get("from_node")
        from_job_id = parent.get("from_job_id")
        if from_node is not None and from_job_id is not None:
            return f"{from_node} job {from_job_id}"
        return _json(parent)
    return "script (root job defined by node behavior/router)"


def _origin_change_body(event: dict[str, Any]) -> str:
    previous = event.get("previous_origin")
    current = event.get("current_origin")
    if not isinstance(previous, dict) or not isinstance(current, dict):
        return (
            "Previous: " + _origin_label(event.get("previous_parent"))
            + "\nCurrent: " + _origin_label(event.get("current_parent"))
        )

    def describe(label: str, origin: dict[str, Any]) -> list[str]:
        lines = [f"{label}: {_origin_label(origin.get('parent'))}"]
        producer = origin.get("producer_component") or []
        if producer:
            lines.append(f"{label} producer component: {{{', '.join(map(str, producer))}}}")
        if origin.get("job_kind") is not None:
            lines.append(f"{label} job kind: {origin['job_kind']}")
        return lines

    return "\n".join(describe("Previous", previous) + describe("Current", current))


def trace_command(workflow, node: str, job_id: int) -> int:
    storage = workflow.storage
    if not storage.job_exists(node, job_id):
        raise RuntimeError(f"Job does not exist: {node}/{job_id}")
    job = storage.load_job(node, job_id)
    events = storage.read_job_events(node, job_id)
    status = storage.read_job_status_data(node, job_id) or {"status": "queued"}
    control = storage.read_job_control(node, job_id)

    origin = _origin_label(job.parent)
    _block("ORIGIN", origin)

    current_task = "MAIN TASK"
    for event in events:
        kind = event.get("event")
        stamp = event.get("time", "?")
        if kind == "task_started":
            current_task = _task_label(event)
            if event.get("task_role") == "fallback":
                title = f"({stamp}) {node} FALLBACK {event.get('task')} STARTED"
            else:
                title = f"({stamp}) {node} MAIN TASK STARTED"
            details = _body_without(event, "previous_error")
            if event.get("previous_error"):
                details["previous_error"] = event["previous_error"]
            _block(title, _json(details) if details else None)
        elif kind == "trace":
            label = _task_label(event, default=current_task)
            title = f"({stamp}) TRACE {event.get('name', 'trace')} FOR {label}"
            _block(title, _json(_body_without(event, "name")))
        elif kind == "origin_changed":
            _block(
                f"({stamp}) ORIGIN CHANGED",
                _origin_change_body(event),
            )
        elif kind == "output_written":
            label = _task_label(event, default=current_task)
            _block(f"({stamp}) OUTPUT FOR {label}", _json(_body_without(event)))
        elif kind == "input_forwarded":
            label = _task_label(event, default=current_task)
            _block(f"({stamp}) INPUT FORWARD FOR {label}", _json(_body_without(event)))
        elif kind == "jobs_created":
            jobs = event.get("jobs") or []
            lines = []
            for item in jobs:
                if isinstance(item, dict):
                    lines.append(f"{item.get('node')} job {item.get('job_id')}\n{_json(item.get('params') or {})}")
                else:
                    lines.append(str(item))
            _block(f"({stamp}) JOBS CREATED", "\n\n".join(lines) or "(none)")
        elif kind in {"fallback_started", "retry_started"}:
            # task_started is the canonical 0.4.8 rendering; retain old events in
            # the journal for compatibility without printing duplicate blocks.
            continue

    final_output = storage.read_json(storage.output_file(node, job_id), default=None)
    if final_output is not None:
        _block(
            f"OUTPUT FOR {current_task}",
            _json({
                "path": f"output/jobs/{job_id}/output.json",
                "content": final_output,
            }),
        )

    terminal = {
        "state": status.get("status", "queued"),
        "started_at": status.get("started_at"),
        "finished_at": status.get("finished_at"),
        "duration_seconds": status.get("duration_seconds"),
        "generation": control.get("generation", 0),
        "parent": job.parent,
        "producer_component": list(job.producer_component or ()),
        "job_kind": job.job_kind or "root",
    }
    if isinstance(final_output, dict) and final_output.get("error"):
        terminal["error"] = final_output["error"]
    _block("JOB ENDED", _json({key: value for key, value in terminal.items() if value is not None}))
    return 0
