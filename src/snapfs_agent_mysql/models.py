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

from typing import Optional

from sqlalchemy import (
    BigInteger,
    Float,
    Integer,
    String,
    Text,
    Index,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class File(Base):
    __tablename__ = "files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Identity / path
    path: Mapped[str] = mapped_column(Text, nullable=False)
    dir: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    ext: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    type: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)

    size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    fsize_du: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    mtime: Mapped[float] = mapped_column(Float, nullable=False)
    atime: Mapped[float] = mapped_column(Float, nullable=True)
    ctime: Mapped[float] = mapped_column(Float, nullable=True)

    nlinks: Mapped[int] = mapped_column(Integer, nullable=True)
    inode: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    dev: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    owner: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    group: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    uid: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    gid: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    mode: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    algo: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    hash: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    created_at: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=lambda: float(func.now().execute().scalar() if False else 0.0),
    )
    updated_at: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=0.0,
    )

    __table_args__ = (
        # Unique index on first 512 chars of path
        Index("uq_files_path", "path", mysql_length=512, unique=True),
        # Hash index is small, no prefix needed
        Index("ix_files_hash", "hash"),
        # Index on first 255 chars of dir for directory queries
        Index("ix_files_dir", "dir", mysql_length=255),
    )

    @classmethod
    def from_event(cls, data: dict) -> "File":
        """
        Helper to construct/merge from a file.upsert event data dict.
        Missing fields are allowed and will be stored as NULL/defaults.
        """
        path = data.get("path") or ""
        # crude dir/name/ext extraction; ideally scanner sends these
        name = data.get("name")
        dir_ = data.get("dir")
        ext = data.get("ext")

        if path and (not dir_ or not name):
            # fallback: derive dir/name from path
            import os

            dir_, name = os.path.split(path)

        if ext is None and name:
            # simple extension extraction
            if "." in name:
                ext = name.rsplit(".", 1)[-1]

        return cls(
            path=path,
            dir=dir_ or "",
            name=name or "",
            ext=ext,
            type=data.get("type") or "file",
            size=int(data.get("size") or 0),
            fsize_du=int(data.get("fsize_du") or 0),
            mtime=float(data.get("mtime") or 0.0),
            atime=(float(data["atime"]) if data.get("atime") is not None else None),
            ctime=(float(data["ctime"]) if data.get("ctime") is not None else None),
            nlinks=(int(data["nlinks"]) if data.get("nlinks") is not None else None),
            inode=data.get("inode"),
            dev=data.get("dev"),
            owner=data.get("owner"),
            group=data.get("group"),
            uid=(int(data["uid"]) if data.get("uid") is not None else None),
            gid=(int(data["gid"]) if data.get("gid") is not None else None),
            mode=(int(data["mode"]) if data.get("mode") is not None else None),
            algo=data.get("algo"),
            hash=data.get("hash"),
        )
