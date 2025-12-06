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

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .config import settings


engine: AsyncEngine = create_async_engine(
    settings.mysql_url,
    pool_pre_ping=True,
    future=True,
)

SessionLocal = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,
)


async def init_db(Base) -> None:
    """
    Create tables and views if they don't exist yet.
    In production you'd move this to Alembic, but this is fine for bootstrapping.
    """
    async with engine.begin() as conn:
        # Create concrete tables
        await conn.run_sync(Base.metadata.create_all)

        # Create file_cache view for gateway lookups
        await conn.execute(
            text(
                """
                CREATE OR REPLACE VIEW file_cache AS
                SELECT
                    p.full_path AS path,
                    c.algo,
                    c.hash,
                    c.size AS size,
                    f.mtime AS mtime,
                    f.dev AS dev,
                    f.inode AS inode
                FROM paths p
                JOIN files f   ON p.file_id    = f.id
                JOIN content c ON f.content_id = c.id
                WHERE p.is_deleted = 0;
            """
            )
        )
