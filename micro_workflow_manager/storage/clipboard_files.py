"""Stage, publish, restore, and retire one native clipboard tree."""

from __future__ import annotations

import shutil
import stat
from pathlib import Path

from .recovery_errors import add_recovery_note
from .recovery_moves import move_without_replacement
from .recovery_output import recovery_path
from .recovery_tree import (
    capture_recovery_tree,
    directory_marker,
    known_remaining_tree,
    validate_tree_record,
)
from .clipboard_snapshot import SNAPSHOT_NAME, SNAPSHOT_SIDECARS


def _operation_relative(operation_id):
    return ".mwf/clipboard-operations/" + operation_id


def _copy_source(source, incoming, *, snapshot_source=None):
    source = Path(source)

    def ignore(directory, names):
        return (
            {SNAPSHOT_NAME}
            if Path(directory) == source and SNAPSHOT_NAME in names else set()
        )

    shutil.copytree(source, incoming, symlinks=False, ignore=ignore)
    if snapshot_source is not None:
        shutil.copy2(snapshot_source, incoming / SNAPSHOT_NAME)


def _require_no_snapshot_sidecars(tree):
    children = tree.get("children", {}) if tree is not None else {}
    unexpected = sorted(set(children).intersection(SNAPSHOT_SIDECARS))
    if unexpected:
        raise RuntimeError("Clipboard source contains native snapshot journal sidecars: "
                           + ", ".join(unexpected))


class PreparedClipboardAllocation:
    """A complete private tree that can enter the project in one rename."""

    def __init__(self, root, temporary, manifest):
        self.root = Path(root)
        self.temporary = Path(temporary)
        self.manifest = manifest

    def install(self):
        destination = recovery_path(self.root, self.manifest["staging"]["path"])
        destination.parent.mkdir(parents=True, exist_ok=True)
        move_without_replacement(self.temporary, destination)
        files = ClipboardFiles(self.root, self.manifest)
        files.require_prepared()
        return files


def prepare_clipboard_allocation(
    root, temporary_root, operation_id, *, source_relative, destination_relative,
    include_snapshot, snapshot_source=None,
):
    if include_snapshot != (snapshot_source is not None):
        raise ValueError("Clipboard snapshot capture arguments are inconsistent")
    relative = _operation_relative(operation_id)
    directory = Path(temporary_root) / operation_id
    source = recovery_path(root, source_relative)
    before_source = capture_recovery_tree(root, source_relative)
    if before_source is None or before_source["kind"] != "directory":
        raise RuntimeError("Clipboard source is missing or is not an ordinary directory")
    _require_no_snapshot_sidecars(before_source)
    before_destination = capture_recovery_tree(root, destination_relative)
    directory.mkdir(exist_ok=False)
    try:
        incoming = directory / "incoming"
        _copy_source(
            source, incoming, snapshot_source=snapshot_source,
        )
        if capture_recovery_tree(root, source_relative) != before_source:
            raise RuntimeError("Clipboard source changed during capture")
        manifest = {
            "staging": {"path": relative, "marker": directory_marker(directory.lstat())},
            "source": {"path": source_relative, "tree": before_source},
            "destination": {"path": destination_relative, "tree": before_destination},
            "incoming": capture_recovery_tree(directory, "incoming"),
        }
        return PreparedClipboardAllocation(root, directory, manifest)
    except BaseException as error:
        try:
            shutil.rmtree(directory)
        except BaseException as cleanup_error:
            add_recovery_note(error, "Incomplete temporary clipboard capture remains at " + str(directory)
                              + ": " + str(cleanup_error))
        raise


class ClipboardFiles:
    def __init__(self, root, manifest):
        self.root = root
        self.manifest = manifest
        self.relative = manifest["staging"]["path"]
        self.directory = recovery_path(root, self.relative)
        self.source = recovery_path(root, manifest["source"]["path"])
        self.destination = recovery_path(root, manifest["destination"]["path"])
        self.incoming = self.directory / "incoming"
        self.previous = self.directory / "previous"
        self._validate_manifest()

    def _validate_manifest(self):
        import re

        if (not re.fullmatch(r"\.mwf/clipboard-operations/[0-9a-f]{32}", self.relative)
                or set(self.manifest) != {"staging", "source", "destination", "incoming"}):
            raise RuntimeError("Invalid clipboard file manifest")
        marker = self.manifest["staging"].get("marker")
        if (set(self.manifest["staging"]) != {"path", "marker"}
                or not isinstance(marker, list) or len(marker) != 3
                or any(type(value) is not int for value in marker)
                or not stat.S_ISDIR(marker[2])):
            raise RuntimeError("Invalid clipboard staging identity")
        for name in ("source", "destination"):
            item = self.manifest[name]
            if set(item) != {"path", "tree"}:
                raise RuntimeError("Invalid clipboard path observation")
            recovery_path(self.root, item["path"])
            validate_tree_record(item["tree"])
        validate_tree_record(self.manifest["incoming"])
        if (self.manifest["source"]["tree"] is None
                or self.manifest["source"]["tree"]["kind"] != "directory"
                or self.manifest["incoming"] is None
                or self.manifest["incoming"]["kind"] != "directory"):
            raise RuntimeError("Clipboard operation requires ordinary source and incoming trees")

    def _staging_identity(self):
        try:
            marker = directory_marker(self.directory.lstat())
        except FileNotFoundError as error:
            raise RuntimeError("Clipboard staging directory is missing") from error
        if marker != self.manifest["staging"]["marker"]:
            raise RuntimeError("Clipboard staging directory identity changed")

    def require_prepared(self):
        self._staging_identity()
        if {item.name for item in self.directory.iterdir()} != {"incoming"}:
            raise RuntimeError("Clipboard staging has unexpected entries before publication")
        if capture_recovery_tree(self.root, self.relative + "/incoming") != self.manifest["incoming"]:
            raise RuntimeError("Clipboard incoming tree changed")
        if capture_recovery_tree(self.root, self.manifest["source"]["path"]) != self.manifest["source"]["tree"]:
            raise RuntimeError("Clipboard source changed before publication")
        if capture_recovery_tree(
            self.root, self.manifest["destination"]["path"],
        ) != self.manifest["destination"]["tree"]:
            raise RuntimeError("Clipboard destination changed before publication")

    def publish(self):
        self.require_prepared()
        original = self.manifest["destination"]["tree"]
        if original is not None:
            move_without_replacement(self.destination, self.previous)
            try:
                if capture_recovery_tree(
                    self.root, self.relative + "/previous",
                ) != original:
                    raise RuntimeError("Clipboard destination changed during detachment")
            except BaseException as error:
                try:
                    move_without_replacement(self.previous, self.destination)
                except BaseException as return_error:
                    add_recovery_note(
                        error, "Changed clipboard destination remains private: "
                        + str(return_error),
                    )
                raise
        try:
            move_without_replacement(self.incoming, self.destination)
        except BaseException as error:
            if original is not None:
                try:
                    move_without_replacement(self.previous, self.destination)
                except BaseException as restore_error:
                    add_recovery_note(error, "Clipboard original remains private: " + str(restore_error))
            raise
        self.require_published()

    def require_published(self):
        self._staging_identity()
        if capture_recovery_tree(self.root, self.manifest["destination"]["path"]) != self.manifest["incoming"]:
            raise RuntimeError("Clipboard published tree changed")
        if capture_recovery_tree(self.root, self.relative + "/incoming") is not None:
            raise RuntimeError("Clipboard incoming tree remains private after publication")
        original = self.manifest["destination"]["tree"]
        actual = capture_recovery_tree(self.root, self.relative + "/previous")
        if actual != original:
            raise RuntimeError("Clipboard previous tree changed")

    def restoration_steps(self):
        self._staging_identity()
        visible = capture_recovery_tree(self.root, self.manifest["destination"]["path"])
        incoming = capture_recovery_tree(self.root, self.relative + "/incoming")
        previous = capture_recovery_tree(self.root, self.relative + "/previous")
        original = self.manifest["destination"]["tree"]
        if visible == original and incoming == self.manifest["incoming"] and previous is None:
            return "restored"
        if visible == self.manifest["incoming"] and incoming is None and previous == original:
            return "published"
        if (original is not None and visible is None
                and incoming == self.manifest["incoming"] and previous == original):
            return "detached"
        raise RuntimeError("Clipboard prepared files cannot be restored safely")

    def restore(self):
        state = self.restoration_steps()
        if state == "restored":
            return
        moved_incoming = False
        try:
            if state == "published":
                move_without_replacement(self.destination, self.incoming)
                moved_incoming = True
                if capture_recovery_tree(
                    self.root, self.relative + "/incoming",
                ) != self.manifest["incoming"]:
                    raise RuntimeError("Clipboard replacement changed before restoration")
            if self.manifest["destination"]["tree"] is not None:
                move_without_replacement(self.previous, self.destination)
        except BaseException as error:
            if moved_incoming:
                original = self.manifest["destination"]["tree"]
                try:
                    visible = capture_recovery_tree(
                        self.root, self.manifest["destination"]["path"],
                    )
                    previous = capture_recovery_tree(self.root, self.relative + "/previous")
                    if visible == original and previous is None:
                        raise error
                    try:
                        move_without_replacement(self.incoming, self.destination)
                    except BaseException as return_error:
                        add_recovery_note(
                            error, "Clipboard replacement remains private: " + str(return_error),
                        )
                except BaseException as return_error:
                    if return_error is not error:
                        add_recovery_note(
                            error, "Clipboard restore state could not be read: " + str(return_error),
                        )
            raise
        self.require_restored()

    def require_restored(self):
        if capture_recovery_tree(
            self.root, self.manifest["destination"]["path"],
        ) != self.manifest["destination"]["tree"]:
            raise RuntimeError("Clipboard restoration changed the previous destination")
        if capture_recovery_tree(self.root, self.relative + "/incoming") != self.manifest["incoming"]:
            raise RuntimeError("Clipboard replacement is missing after restoration")
        if capture_recovery_tree(self.root, self.relative + "/previous") is not None:
            raise RuntimeError("Clipboard previous tree remains after restoration")

    def _remove_tree(self, relative, expected):
        actual = capture_recovery_tree(self.root, relative)
        if actual is None:
            return
        if not known_remaining_tree(actual, expected):
            raise RuntimeError("Clipboard private material changed: " + relative)
        path = recovery_path(self.root, relative)
        if actual["kind"] == "file":
            path.chmod(stat.S_IMODE(path.stat().st_mode) | stat.S_IWRITE)
            if not known_remaining_tree(capture_recovery_tree(self.root, relative), expected):
                raise RuntimeError("Clipboard private file changed before cleanup")
            path.unlink()
            return
        for name in sorted(actual["children"]):
            self._remove_tree(relative + "/" + name, expected["children"][name])
        remaining = capture_recovery_tree(self.root, relative)
        if (remaining is None or remaining["kind"] != "directory"
                or remaining["marker"] != expected["marker"]
                or remaining["children"]):
            raise RuntimeError("Clipboard private directory changed before cleanup")
        path.rmdir()

    def require_discardable(self, *, committed):
        self._staging_identity()
        incoming = capture_recovery_tree(self.root, self.relative + "/incoming")
        previous = capture_recovery_tree(self.root, self.relative + "/previous")
        original = self.manifest["destination"]["tree"]
        if committed:
            valid = incoming is None and known_remaining_tree(previous, original)
        else:
            valid = previous is None and known_remaining_tree(incoming, self.manifest["incoming"])
        if not valid:
            raise RuntimeError("Clipboard terminal files changed before cleanup")

    def discard(self, *, committed):
        self.require_discardable(committed=committed)
        if committed:
            original = self.manifest["destination"]["tree"]
            if original is not None:
                self._remove_tree(self.relative + "/previous", original)
        else:
            self._remove_tree(self.relative + "/incoming", self.manifest["incoming"])
        if tuple(self.directory.iterdir()):
            raise RuntimeError("Clipboard staging contains unrecognized material")
        self.directory.rmdir()
