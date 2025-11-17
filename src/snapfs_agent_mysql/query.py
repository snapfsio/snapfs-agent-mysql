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

from typing import Any, Dict, List, Optional

from sqlalchemy import text

from .db import get_engine


def run_sql(sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Execute a raw SQL query and return a list of dict rows.

    This is primarily intended for internal tooling and the /query/sql gateway endpoint.
    """
    engine = get_engine()
    out: List[Dict[str, Any]] = []

    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        cols = result.keys()
        for row in result:
            out.append({col: row[idx] for idx, col in enumerate(cols)})

    return out
