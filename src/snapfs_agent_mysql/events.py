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

from .db import SessionLocal
from .ingest import ingest_file_event


async def apply_events(events: Iterable[Dict[str, Any]]) -> None:
    """
    Apply a batch of SnapFS events into the normalized schema.

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
                    # no point ingesting without a path
                    continue

                # Delegate to the synchronous ingest logic using run_sync.
                # run_sync will provide a sync Session bound to the same connection.
                def _sync_ingest(sync_session):
                    ingest_file_event(sync_session, data)

                await session.run_sync(_sync_ingest)

        # session.commit() is implicit via session.begin()
