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

from .db import SessionLocal
from .models import File

# TODO: This is a naive implementation that does a SELECT
# If/when this becomes a bottleneck, the natural next step
# is a Core INSERT ... ON DUPLICATE KEY UPDATE using File.__table__,
# but we can defer that.


async def apply_events(events: Iterable[Dict[str, Any]]) -> None:
    """
    Apply a batch of SnapFS events.

    Expected shape (as produced by the gateway / scanner):
        { "type": "file.upsert", "data": { ... file metadata ... } }

    For now we only handle file.upsert; other event types are ignored.
    """
    async with SessionLocal() as session:
        async with session.begin():
            for ev in events:
                if ev.get("type") != "file.upsert":
                    continue

                data = ev.get("data") or {}
                path = data.get("path")
                if not path:
                    continue

                # Try to find existing file by path
                result = await session.execute(select(File).where(File.path == path))
                existing = result.scalars().first()

                if existing:
                    # Update fields in-place
                    updated = File.from_event(data)
                    for attr in (
                        "dir",
                        "name",
                        "ext",
                        "type",
                        "size",
                        "fsize_du",
                        "mtime",
                        "atime",
                        "ctime",
                        "nlinks",
                        "inode",
                        "dev",
                        "owner",
                        "group",
                        "uid",
                        "gid",
                        "mode",
                        "algo",
                        "hash",
                    ):
                        setattr(existing, attr, getattr(updated, attr))
                else:
                    # New row
                    session.add(File.from_event(data))
        # session.commit() is implicit with session.begin()
