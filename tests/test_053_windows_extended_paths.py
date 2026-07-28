from pathlib import Path

import pytest

from micro_workflow_manager.context import JobContext
from micro_workflow_manager.paths import relative_path, relative_posix


def test_relative_posix_accepts_windows_extended_length_alias():
    base = r"C:\Users\Chris\Desktop\Projects\kaicenat\node\explode\output"
    path = (
        r"\\?\C:\Users\Chris\Desktop\Projects\kaicenat\node\explode\output"
        r"\ArtinAlgebra\0006_chapter_6_symmetry"
        r"\000313_6.5_discrete_groups_of_isometries\routes\000313_000008.json"
    )

    assert relative_posix(path, base) == (
        "ArtinAlgebra/0006_chapter_6_symmetry/"
        "000313_6.5_discrete_groups_of_isometries/routes/000313_000008.json"
    )


def test_relative_path_still_rejects_sibling_windows_tree():
    with pytest.raises(ValueError):
        relative_path(
            r"\\?\C:\Users\Chris\Desktop\Projects\other\file.json",
            r"C:\Users\Chris\Desktop\Projects\kaicenat",
        )


def test_job_context_records_extended_output_path_without_crashing():
    events = []

    class RecordingContext(JobContext):
        @property
        def job_id(self):
            return 208

        @property
        def output_dir(self):
            return Path(r"C:\Users\Chris\Desktop\Projects\kaicenat\node\explode\output")

        @property
        def files_dir(self):
            return Path(
                r"C:\Users\Chris\Desktop\Projects\kaicenat\node\explode\output\jobs\208\files"
            )

        def _record_event(self, event, **data):
            events.append((event, data))

    ctx = object.__new__(RecordingContext)

    output = Path(
        r"\\?\C:\Users\Chris\Desktop\Projects\kaicenat\node\explode\output"
        r"\ArtinAlgebra\0006_chapter_6_symmetry"
        r"\000313_6.5_discrete_groups_of_isometries\routes\000313_000008.json"
    )
    ctx._record_output(output, {"ok": True})

    assert events[0][0] == "output_written"
    assert events[0][1]["path"] == (
        "output/ArtinAlgebra/0006_chapter_6_symmetry/"
        "000313_6.5_discrete_groups_of_isometries/routes/000313_000008.json"
    )
