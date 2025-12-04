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
import traceback
from typing import Any, Dict, List

import aiohttp

from .config import settings
from .events import apply_events


async def ws_loop() -> None:
    uri = (
        f"{settings.gateway_ws}/stream"
        f"?subject={settings.subject}"
        f"&durable={settings.durable}"
        f"&batch={settings.batch}"
    )

    backoff = 1

    while True:
        try:
            print(f"[agent-mysql] connecting {uri}")

            # Fresh session per connection attempt
            async with aiohttp.ClientSession() as sess:
                async with sess.ws_connect(uri) as ws:
                    print(f"[agent-mysql] connected {uri}")
                    payload: Dict[str, Any] = {}
                    msg_type: str = ""
                    backoff = 1

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            payload = msg.json()
                            msg_type = payload.get("type")
                        elif msg.type == aiohttp.WSMsgType.CLOSE:
                            print("[agent-mysql] WS CLOSE from server")
                            break
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSING,
                            aiohttp.WSMsgType.CLOSED,
                        ):
                            print(
                                f"[agent-mysql] WS is closing/closed (type={msg.type})"
                            )
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("[agent-mysql] WS ERROR:", repr(ws.exception()))
                            break
                        else:
                            print(f"[agent-mysql] unhandled WS msg type {msg.type}")
                            continue

                        if msg_type == "events":
                            batch_id = payload["batch"]
                            messages: List[Dict[str, Any]] = payload["messages"]

                            all_events: List[Dict[str, Any]] = []
                            for m in messages:
                                data = m.get("data") or {}
                                evs = data.get("events") or []
                                all_events.extend(evs)

                            if all_events:
                                try:
                                    # process in smaller DB transactions
                                    for i in range(
                                        0, len(all_events), settings.chunk_size
                                    ):
                                        chunk = all_events[i : i + settings.chunk_size]
                                        print(
                                            f"[agent-mysql] applying chunk {i//settings.chunk_size + 1} "
                                            f"of batch {batch_id}: {len(chunk)} events"
                                        )
                                        await apply_events(chunk)

                                    print(
                                        f"[agent-mysql] apply_events OK for batch {batch_id} "
                                        f"({len(all_events)} events in chunks of {settings.chunk_size})"
                                    )
                                except Exception as e:
                                    print(
                                        f"[agent-mysql] apply_events error for batch {batch_id}: {e}"
                                    )
                                    traceback.print_exc()
                                    # Do NOT ack the batch; let JetStream redeliver
                                    break

                            # Only ACK after *all* chunks in this WS batch succeeded
                            try:
                                await ws.send_json({"type": "ack", "batch": batch_id})
                                print(f"[agent-mysql] ACK sent for batch {batch_id}")
                            except aiohttp.ClientConnectionError as e:
                                print(
                                    f"[agent-mysql] failed to send ACK for batch {batch_id}: {e}"
                                )
                                raise

                        elif msg_type == "error":
                            # Error from gateway (e.g. JetStream problems)
                            print(
                                f"[agent-mysql] gateway error: {payload.get('message')}"
                            )
                            break

                        else:
                            # Unknown message type; ignore for now
                            continue

            print("[agent-mysql] websocket closed; will reconnect")

        except Exception as e:
            print(f"[agent-mysql] WS loop error: {e}; retrying in {backoff}s")
            traceback.print_exc()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
