from __future__ import annotations

import shutil
import uuid
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from micro_workflow_manager.paths import deploy_dir

from .deploy_ignore import MWFIgnore, ensure_mwfignore


@dataclass(frozen=True)
class DeploymentStats:
    copied_files: int = 0
    copied_bytes: int = 0
    node_archives: int = 0
    ignored_paths: int = 0
    skipped_links: int = 0

def _build_staging(root: Path, staging: Path, ignore: MWFIgnore) -> DeploymentStats:
    copied_files = 0
    copied_bytes = 0
    node_archives = 0
    ignored_paths = 0
    skipped_links = 0
    node_root = root / "node"

    def account_file(source: Path, destination: Path) -> None:
        nonlocal copied_files, copied_bytes
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        copied_files += 1
        copied_bytes += source.stat().st_size
        if copied_files <= 10 or copied_files % 100 == 0:
            print(f"  copied {copied_files} file(s): {source.relative_to(root).as_posix()}")

    # Copy all non-node content while respecting ignore rules. The .mwf deploy
    # directory is always skipped independently to prevent recursive copying even
    # if a user accidentally re-includes it.
    for source in root.rglob("*"):
        relative = source.relative_to(root)
        relative_text = relative.as_posix()
        if relative.parts and relative.parts[0] == ".mwf":
            ignored_paths += 1
            continue
        if relative.parts and relative.parts[0] == "node":
            continue
        if source.is_symlink():
            skipped_links += 1
            continue
        is_dir = source.is_dir()
        if ignore.is_ignored(relative_text, is_dir=is_dir):
            ignored_paths += 1
            continue
        destination = staging / relative
        if is_dir:
            destination.mkdir(parents=True, exist_ok=True)
        elif source.is_file():
            account_file(source, destination)

    # Each direct node child is independently compressed. This dramatically
    # reduces the number of small files copied to a remote filesystem.
    if node_root.is_dir() and not ignore.is_ignored("node", is_dir=True):
        target_node = staging / "node"
        target_node.mkdir(parents=True, exist_ok=True)
        for child in sorted(node_root.iterdir(), key=lambda item: item.name.lower()):
            relative_child = child.relative_to(root).as_posix()
            if child.is_symlink():
                skipped_links += 1
                continue
            if ignore.is_ignored(relative_child, is_dir=child.is_dir()):
                ignored_paths += 1
                continue
            if child.is_file():
                account_file(child, target_node / child.name)
                continue
            if not child.is_dir():
                continue

            included, ignored_count, link_count = _included_node_entries(root, child, ignore)
            ignored_paths += ignored_count
            skipped_links += link_count
            archive_path = target_node / f"{child.name}.zip"
            if ignored_count == 0 and link_count == 0:
                print(f"  zipping node/{child.name} directly (no ignored content inside)")
            else:
                print(f"  zipping filtered node/{child.name} ({ignored_count} ignored path(s))")
            _zip_selected_tree(child, archive_path, included)
            node_archives += 1
            copied_files += sum(1 for path in included if path.is_file())
            copied_bytes += sum(path.stat().st_size for path in included if path.is_file())

    return DeploymentStats(copied_files, copied_bytes, node_archives, ignored_paths, skipped_links)

def _included_node_entries(root: Path, node_dir: Path, ignore: MWFIgnore) -> tuple[list[Path], int, int]:
    included: list[Path] = []
    ignored = 0
    links = 0
    for path in node_dir.rglob("*"):
        if path.is_symlink():
            links += 1
            continue
        relative = path.relative_to(root).as_posix()
        if ignore.is_ignored(relative, is_dir=path.is_dir()):
            ignored += 1
            continue
        included.append(path)
    return included, ignored, links

def _zip_selected_tree(base: Path, archive: Path, entries: Iterable[Path]) -> None:
    archive.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as handle:
        entries = list(entries)
        if not entries:
            # Preserve an empty node folder on extraction.
            handle.writestr(".mwf-empty", "")
            return
        for path in entries:
            arcname = path.relative_to(base).as_posix()
            if path.is_dir():
                handle.writestr(arcname.rstrip("/") + "/", "")
            elif path.is_file():
                handle.write(path, arcname)

def _zip_tree(base: Path, archive: Path) -> None:
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as handle:
        for path in sorted(base.rglob("*")):
            arcname = path.relative_to(base).as_posix()
            if path.is_dir():
                handle.writestr(arcname.rstrip("/") + "/", "")
            elif path.is_file():
                handle.write(path, arcname)

def local_archive_path(root: Path) -> Path:
    return deploy_dir(root) / "local" / "deployment.zip"

def _format_bytes(value: int) -> str:
    number = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if number < 1024 or unit == "TiB":
            return f"{number:.1f} {unit}" if unit != "B" else f"{int(number)} B"
        number /= 1024
    return f"{number:.1f} TiB"

def deploy_local(root: Path) -> int:
    ignore_path, created = ensure_mwfignore(root)
    if created:
        print(f"Created {ignore_path}")
        print("IMPORTANT: review .mwfignore before sharing or deploying this project.")
    else:
        print(f"Using ignore rules from {ignore_path}")

    ignore = MWFIgnore.from_file(ignore_path)
    deploy_root = deploy_dir(root)
    final_local = deploy_root / "local"
    building = deploy_root / f".local-building-{uuid.uuid4().hex}"
    staging = building / "staging"
    archive = building / "deployment.zip"

    print("Preparing a fresh local deployment; the previous local deployment will be replaced.")
    if building.exists():
        shutil.rmtree(building)
    staging.mkdir(parents=True, exist_ok=True)

    stats = _build_staging(root, staging, ignore)
    print("Creating the final deployment archive...")
    _zip_tree(staging, archive)
    archive_size = archive.stat().st_size
    shutil.rmtree(staging)

    deploy_root.mkdir(parents=True, exist_ok=True)
    if final_local.exists():
        print(f"Removing previous local deployment: {final_local}")
        shutil.rmtree(final_local)
    building.replace(final_local)
    final_archive = final_local / "deployment.zip"

    print("Local deployment complete")
    print(f"  archive: {final_archive}")
    print(f"  copied files: {stats.copied_files}")
    print(f"  copied size before compression: {_format_bytes(stats.copied_bytes)}")
    print(f"  node subfolder archives: {stats.node_archives}")
    print(f"  ignored paths: {stats.ignored_paths}")
    if stats.skipped_links:
        print(f"  skipped symbolic links: {stats.skipped_links}")
    print(f"  final archive size: {_format_bytes(archive_size)}")
    print("The next local deployment will overwrite this archive to avoid accumulating large copies.")
    return 0
