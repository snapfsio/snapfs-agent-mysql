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

from typing import Any, Dict, Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from .db import session_scope
from .models import File


def _upsert_file(session: Session, data: Dict[str, Any]) -> None:
    """
    Insert or update a File row based on 'path'.
    """
    path = data["path"]
    stmt = select(File).where(File.path == path)
    row = session.execute(stmt).scalars().first()

    if row is None:
        row = File(path=path)
        session.add(row)

    # Map event data to columns
    row.dir = data.get("dir") or ""
    row.name = data.get("name") or ""
    row.ext = data.get("ext")
    row.type = data.get("type") or "file"

    row.size = int(data.get("size") or 0)
    row.fsize_du = int(data.get("fsize_du") or 0)

    row.mtime = float(data.get("mtime") or 0.0)
    row.atime = float(data.get("atime") or 0.0)
    row.ctime = float(data.get("ctime") or 0.0)

    row.nlinks = int(data.get("nlinks") or 1)
    row.inode = data.get("inode")
    row.dev = data.get("dev")

    row.owner = data.get("owner")
    row.group = data.get("group")
    row.uid = data.get("uid")
    row.gid = data.get("gid")
    row.mode = data.get("mode")

    row.algo = data.get("algo")
    row.hash = data.get("hash")


def apply_events(events: Iterable[Dict[str, Any]]) -> int:
    """
    Apply a batch of events.

    Expected shape (from gateway):

        { "type": "file.upsert", "data": { ... file fields ... } }

    Returns the number of events successfully applied.
    """
    count = 0
    with session_scope() as session:
        for ev in events:
            if ev.get("type") != "file.upsert":
                # ignoring other event types for now
                continue
            data = ev.get("data") or {}
            _upsert_file(session, data)
            count += 1
    return count
