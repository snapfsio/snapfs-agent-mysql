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

import asyncio
import socket

import aiohttp

from .config import settings
from .events import apply_events


async def ws_loop():
    uri = (
        f"{settings.gateway_ws}/stream"
        f"?subject={settings.subject}"
        f"&durable={settings.durable}"
        f"&batch={settings.batch}"
    )

    connector = aiohttp.TCPConnector(
        limit=0,
        family=socket.AF_INET,  # force IPv4 in typical docker setups
    )

    backoff = 1

    while True:
        try:
            print(f"[agent-mysql] connecting {uri}")
            async with aiohttp.ClientSession(connector=connector) as sess:
                async with sess.ws_connect(uri) as ws:
                    print(f"[agent-mysql] connected {uri}")
                    backoff = 1

                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue

                        payload = msg.json()
                        if payload.get("type") != "events":
                            # could also handle "error" messages here
                            continue

                        batch_id = payload["batch"]
                        messages = payload["messages"]

                        all_events = []
                        for m in messages:
                            data = m.get("data") or {}
                            evs = data.get("events") or []
                            all_events.extend(evs)

                        if all_events:
                            await apply_events(all_events)

                        # ACK the batch back to gateway
                        await ws.send_json({"type": "ack", "batch": batch_id})

        except Exception as e:
            print(f"[agent-mysql] WS loop error: {e}; retrying in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
