#!/usr/bin/env python3
#
# Copyright (c) 2025 SnapFS, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from .models import Content, File, PathEntry, Snapshot, SnapshotFile


def _split_path(path: str) -> tuple[str, str]:
    """Split a full path into (dir, name)."""
    import os

    dir_, name = os.path.split(path)
    if not dir_:
        dir_ = "/"
    return dir_, name or ""


def _split_ext(name: str) -> Optional[str]:
    """Split a filename into its extension, if any."""
    if "." in name:
        return name.rsplit(".", 1)[-1]
    return None


def _get_or_create_content(
    session: Session,
    algo: Optional[str],
    hash_: Optional[str],
    size: Optional[int],
) -> Optional[Content]:
    """Get or create a Content object by (algo, hash, size)."""
    if not algo or not hash_ or size is None:
        return None

    size = int(size)

    content = (
        session.query(Content).filter_by(algo=algo, hash=hash_, size=size).one_or_none()
    )
    if content is None:
        content = Content(algo=algo, hash=hash_, size=size)
        session.add(content)
    return content


def _get_or_create_file(
    session: Session,
    dev: int,
    inode: int,
) -> File:
    """Get or create a File object by (dev, inode)."""
    file = session.query(File).filter_by(dev=dev, inode=inode).one_or_none()
    if file is None:
        file = File(dev=dev, inode=inode)
        session.add(file)
    return file


def _get_or_create_path(
    session: Session,
    full_path: str,
    file: File,
    is_deleted: bool = False,
) -> PathEntry:
    """Get or create a PathEntry by full_path."""
    dir_, name = _split_path(full_path)
    ext = _split_ext(name)

    path = session.query(PathEntry).filter_by(full_path=full_path).one_or_none()
    if path is None:
        path = PathEntry(
            full_path=full_path,
            dir=dir_,
            name=name,
            ext=ext,
            file=file,
            is_deleted=is_deleted,
        )
        session.add(path)
    else:
        path.file = file
        path.dir = dir_
        path.name = name
        path.ext = ext
        path.is_deleted = is_deleted

    return path


def ingest_file_event(
    session: Session,
    data: Dict[str, Any],
    snapshot: Optional[Snapshot] = None,
) -> File:
    """
    Normalize a file.upsert event into Content / File / PathEntry (+ SnapshotFile).

    `data` is expected to be the scanner event payload with at least:
        path, size, mtime, dev, inode, algo, hash, type, etc.
    """
    path = data.get("path") or ""
    dev = int(data.get("dev") or 0)
    inode = int(data.get("inode") or 0)
    size = int(data.get("size") or 0)

    algo = data.get("algo")
    hash_ = data.get("hash")

    # 1) Logical content
    content = _get_or_create_content(session, algo, hash_, size)

    # 2) Physical file
    file = _get_or_create_file(session, dev=dev, inode=inode)

    file.content = content
    if data.get("nlinks") is not None:
        file.nlinks = int(data["nlinks"])
    file.mtime = float(data.get("mtime") or 0.0)
    if data.get("atime") is not None:
        file.atime = float(data["atime"])
    if data.get("ctime") is not None:
        file.ctime = float(data["ctime"])

    if data.get("owner") is not None:
        file.owner = data["owner"]
    if data.get("group") is not None:
        file.group = data["group"]
    if data.get("uid") is not None:
        file.uid = int(data["uid"])
    if data.get("gid") is not None:
        file.gid = int(data["gid"])
    if data.get("mode") is not None:
        file.mode = int(data["mode"])
    file.type = data.get("type") or file.type or "file"

    # 3) Path entry
    is_deleted = bool(data.get("is_deleted") or False)
    path_entry = _get_or_create_path(
        session,
        full_path=path,
        file=file,
        is_deleted=is_deleted,
    )

    # 4) Snapshot membership (optional)
    if snapshot is not None and not is_deleted:
        sf = (
            session.query(SnapshotFile)
            .filter_by(snapshot_id=snapshot.id, path_id=path_entry.id)
            .one_or_none()
        )
        if sf is None:
            sf = SnapshotFile(
                snapshot=snapshot,
                file=file,
                path=path_entry,
                size_bytes=size,
            )
            session.add(sf)
        else:
            sf.file = file
            sf.size_bytes = size

    return file
