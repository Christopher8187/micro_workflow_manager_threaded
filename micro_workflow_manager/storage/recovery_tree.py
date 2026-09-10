"""Capture ordinary file trees for exact interrupted-operation restoration."""

from __future__ import annotations

import os
import re
import stat

from .recovery_output import observe_recovery_file, recovery_path


def directory_marker(observed):
    return [observed.st_dev, observed.st_ino, observed.st_mode]


def capture_recovery_tree(root, relative):
    path = recovery_path(root, relative)
    try:
        before = path.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISREG(before.st_mode):
        observed = observe_recovery_file(root, relative)
        if observed.marker is None:
            raise RuntimeError('Recovery tree changed during observation: ' + relative)
        return dict(kind='file', marker=list(observed.marker), sha256=observed.manifest()['sha256'])
    if not stat.S_ISDIR(before.st_mode):
        raise RuntimeError('Recovery tree contains a special file: ' + relative)
    names = sorted(item.name for item in path.iterdir())
    children = {name: capture_recovery_tree(root, relative + '/' + name) for name in names}
    after = recovery_path(root, relative).lstat()
    if (any(child is None for child in children.values())
            or names != sorted(item.name for item in path.iterdir())
            or directory_marker(before) != directory_marker(after)
            or before.st_mtime_ns != after.st_mtime_ns):
        raise RuntimeError('Recovery directory changed during observation: ' + relative)
    return dict(kind='directory', marker=directory_marker(before), children=children)


def validate_tree_record(record):
    if record is None:
        return
    if not isinstance(record, dict) or record.get('kind') not in ('file', 'directory'):
        raise RuntimeError('Invalid recovery tree record')
    marker = record.get('marker')
    length = 5 if record['kind'] == 'file' else 3
    if (not isinstance(marker, list) or len(marker) != length
            or any(type(value) is not int for value in marker)):
        raise RuntimeError('Invalid recovery filesystem identity')
    if record['kind'] == 'file':
        if (set(record) != {'kind', 'marker', 'sha256'} or not stat.S_ISREG(marker[2])
                or marker[3] < 0 or not isinstance(record['sha256'], str)
                or not re.fullmatch(r'[0-9a-f]{64}', record['sha256'])):
            raise RuntimeError('Invalid recovery file record')
    else:
        if (set(record) != {'kind', 'marker', 'children'} or not stat.S_ISDIR(marker[2])
                or not isinstance(record['children'], dict)):
            raise RuntimeError('Invalid recovery directory record')
        for name, child in record['children'].items():
            if (not isinstance(name, str) or not name or name in ('.', '..')
                    or any(character in name for character in '/\\:') or child is None):
                raise RuntimeError('Invalid recovery directory member')
            validate_tree_record(child)


def same_original_content(actual, expected):
    if actual is None or expected is None:
        return actual is expected
    return (actual['kind'] == expected['kind'] == 'file'
            and actual['marker'][2:4] == expected['marker'][2:4]
            and actual['sha256'] == expected['sha256'])


def writable_file_mode(mode):
    if os.name == 'nt':
        return stat.S_IFMT(mode) | 0o666
    return mode | stat.S_IWRITE


def known_remaining_tree(actual, expected):
    if actual is None:
        return True
    if expected is None or actual['kind'] != expected['kind']:
        return False
    if actual['kind'] == 'file':
        return (actual['marker'][:2] == expected['marker'][:2]
                and actual['marker'][3:] == expected['marker'][3:]
                and actual['marker'][2] in (expected['marker'][2], writable_file_mode(expected['marker'][2]))
                and actual['sha256'] == expected['sha256'])
    return (actual['marker'] == expected['marker']
            and actual['children'].keys() <= expected['children'].keys()
            and all(known_remaining_tree(child, expected['children'][name])
                    for name, child in actual['children'].items()))
