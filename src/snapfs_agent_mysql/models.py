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

from __future__ import annotations

from typing import Optional

import time

from sqlalchemy import (
    BigInteger,
    Boolean,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Content(Base):
    """Logical content object identified by (algo, hash, size)"""

    __tablename__ = "content"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    algo: Mapped[str] = mapped_column(String(16), nullable=False)
    hash: Mapped[str] = mapped_column(String(128), nullable=False)
    size: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # Optional: logical vs disk size, compression info, etc. if you want later
    # fsize_du: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    created_at: Mapped[float] = mapped_column(
        Float, nullable=False, default=lambda: time.time()
    )
    updated_at: Mapped[float] = mapped_column(
        Float, nullable=False, default=lambda: time.time(), onupdate=time.time
    )

    files: Mapped[list["File"]] = relationship(back_populates="content")

    __table_args__ = (
        UniqueConstraint("algo", "hash", "size", name="uq_content_algo_hash_size"),
        Index("ix_content_hash", "hash"),
    )


class File(Base):
    """Physical file object identified by (dev, inode)"""

    __tablename__ = "files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Physical identity (posix-ish)
    dev: Mapped[int] = mapped_column(BigInteger, nullable=False)
    inode: Mapped[int] = mapped_column(BigInteger, nullable=False)
    nlinks: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Link to logical content
    content_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("content.id", ondelete="SET NULL"), nullable=True
    )
    content: Mapped[Optional[Content]] = relationship(back_populates="files")

    # Timestamps (epoch seconds)
    mtime: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    atime: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ctime: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Ownership / perms
    owner: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    group: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    uid: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    gid: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    mode: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Type: "file", "dir", "symlink", etc.
    type: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)

    created_at: Mapped[float] = mapped_column(
        Float, nullable=False, default=lambda: time.time()
    )
    updated_at: Mapped[float] = mapped_column(
        Float, nullable=False, default=lambda: time.time(), onupdate=time.time
    )

    paths: Mapped[list["PathEntry"]] = relationship(
        back_populates="file",
        cascade="all, delete-orphan",
    )
    snapshot_links: Mapped[list["SnapshotFile"]] = relationship(
        back_populates="file",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint("dev", "inode", name="uq_files_dev_inode"),
        Index("ix_files_content_id", "content_id"),
    )


class PathEntry(Base):
    """View of the namespace (full_path -> file)"""

    __tablename__ = "paths"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    file_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("files.id", ondelete="CASCADE"), nullable=False
    )
    file: Mapped[File] = relationship(back_populates="paths")

    # Full path and derived components
    full_path: Mapped[str] = mapped_column(Text, nullable=False)

    # TODO: add a path hash column to enforce uniqueness
    # full_path_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    dir: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    ext: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)

    # Soft delete for "current view" of the tree
    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    created_at: Mapped[float] = mapped_column(
        Float, nullable=False, default=lambda: time.time()
    )
    updated_at: Mapped[float] = mapped_column(
        Float, nullable=False, default=lambda: time.time(), onupdate=time.time
    )

    __table_args__ = (
        # Unique index on first 512 chars of full_path (MySQL quirk)
        Index(
            "uq_paths_full_path",
            "full_path",
            mysql_length=512,
            unique=True,
        ),
        # Optional: index on dir; also TEXT, so add prefix
        Index("ix_paths_dir", "dir", mysql_length=255),
    )


class Snapshot(Base):
    """A logical snapshot of a scan run"""

    __tablename__ = "snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Root path this snapshot was run against (may be a subtree)
    root_path: Mapped[str] = mapped_column(Text, nullable=False)

    # Optional label / description per run
    label: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    created_at: Mapped[float] = mapped_column(
        Float, nullable=False, default=lambda: time.time()
    )

    files: Mapped[list["SnapshotFile"]] = relationship(
        back_populates="snapshot",
        cascade="all, delete-orphan",
    )


class SnapshotFile(Base):
    """The membership of files/paths in a snapshot"""

    __tablename__ = "snapshot_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    snapshot_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=False
    )
    file_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("files.id", ondelete="CASCADE"), nullable=False
    )
    path_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("paths.id", ondelete="CASCADE"), nullable=False
    )

    # Size attributed to this snapshot membership. This can be logical
    # content size, or du-style size, depending on what you want to graph.
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)

    snapshot: Mapped[Snapshot] = relationship(back_populates="files")
    file: Mapped[File] = relationship(back_populates="snapshot_links")
    path: Mapped[PathEntry] = relationship()

    created_at: Mapped[float] = mapped_column(
        Float, nullable=False, default=lambda: time.time()
    )

    __table_args__ = (
        UniqueConstraint(
            "snapshot_id",
            "path_id",
            name="uq_snapshot_files_snapshot_path",
        ),
        Index("ix_snapshot_files_snapshot", "snapshot_id"),
        Index("ix_snapshot_files_file", "file_id"),
    )
