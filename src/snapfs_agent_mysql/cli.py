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

import sys

from .db import get_engine, Base
from .config import settings


def initdb_main():
    """
    CLI entrypoint: create tables in the configured MySQL database.

    Usage:
        SNAPFS_MYSQL_URL=... snapfs-agent-mysql-initdb
    """
    engine = get_engine()
    print(f"Using DB URL: {settings.mysql_url}", file=sys.stderr)
    Base.metadata.create_all(bind=engine)
    print("SnapFS MySQL schema initialized.", file=sys.stderr)
