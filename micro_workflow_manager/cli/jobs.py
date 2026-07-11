from __future__ import annotations

def selected_job_ids_from_args(
    job_mode: str | None,
    job_specs: list[str],
    *,
    command: str = "run",
) -> list[int] | None:
    if job_mode is None:
        if job_specs:
            raise RuntimeError(
                f"Job selectors require the 'job' keyword, for example: "
                f"mwf {command} node job 1 3 8-10"
            )
        return None

    if job_mode not in {"job", "jobs"}:
        raise RuntimeError(
            f"Unexpected {command} argument: {job_mode}. "
            f"Use: mwf {command} <node> job 1 3 8-10"
        )

    if not job_specs:
        raise RuntimeError(
            f"No jobs selected. Use: mwf {command} <node> job 1 3 8-10"
        )

    return parse_job_selectors(job_specs)

def parse_job_selectors(selectors: list[str]) -> list[int]:
    ids: list[int] = []
    seen: set[int] = set()

    for selector in selectors:
        if "-" in selector:
            parts = selector.split("-")
            if len(parts) != 2 or not parts[0] or not parts[1]:
                raise RuntimeError(f"Invalid job range: {selector}")

            start = parse_positive_job_id(parts[0])
            end = parse_positive_job_id(parts[1])

            if end < start:
                raise RuntimeError(f"Invalid job range: {selector}; end must be >= start")

            values = range(start, end + 1)
        else:
            values = [parse_positive_job_id(selector)]

        for job_id in values:
            if job_id not in seen:
                seen.add(job_id)
                ids.append(job_id)

    return ids

def parse_positive_job_id(text: str) -> int:
    if not text.isdigit():
        raise RuntimeError(f"Invalid job id: {text}")

    job_id = int(text)
    if job_id < 1:
        raise RuntimeError(f"Invalid job id: {text}; job IDs start at 1")

    return job_id
