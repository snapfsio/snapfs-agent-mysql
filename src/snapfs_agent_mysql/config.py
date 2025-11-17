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

import os
from pydantic import BaseModel


class Settings(BaseModel):
    # Gateway WebSocket endpoint
    gateway_ws: str = os.getenv("GATEWAY_WS", "ws://localhost:8000")

    # Event stream config
    subject: str = os.getenv("SNAPFS_SUBJECT", "snapfs.files")
    durable: str = os.getenv("SNAPFS_DURABLE", "mysql")
    batch: int = int(os.getenv("SNAPFS_BATCH", "100"))

    # Async SQLAlchemy URL using aiomysql driver
    mysql_url: str = os.getenv(
        "MYSQL_URL",
        "mysql+aiomysql://snapfs:snapfs@mysql:3306/snapfs",
    )


settings = Settings()
