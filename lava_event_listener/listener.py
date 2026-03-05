import asyncio
import json
import logging

import aiohttp

from .config import LavaServerConfig
from .handler import EventHandler

logger = logging.getLogger(__name__)

INITIAL_BACKOFF = 5
MAX_BACKOFF = 300
BACKOFF_FACTOR = 2


class LavaListener:
    def __init__(self, server_config: LavaServerConfig, handler: EventHandler):
        self._config = server_config
        self._handler = handler
        self._name = server_config.name
        self.connected = False

    async def run(self):
        backoff = INITIAL_BACKOFF
        while True:
            try:
                await self._connect_and_listen()
                backoff = INITIAL_BACKOFF
            except asyncio.CancelledError:
                logger.info("[%s] Listener cancelled, shutting down.", self._name)
                raise
            except Exception:
                logger.exception("[%s] Connection error.", self._name)
            finally:
                self.connected = False

            logger.info("[%s] Reconnecting in %ds...", self._name, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)

    async def _connect_and_listen(self):
        ws_url = self._config.ws_url
        logger.info("[%s] Connecting to %s", self._name, ws_url)

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url, heartbeat=30) as ws:
                logger.info("[%s] Connected.", self._name)
                self.connected = True

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._process_message(msg.data)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error("[%s] WS error: %s", self._name, ws.exception())
                        break
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                        break

                logger.warning("[%s] Connection closed.", self._name)

    async def _process_message(self, raw: str):
        try:
            message = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("[%s] Non-JSON message: %s", self._name, raw[:200])
            return

        if isinstance(message, dict) and "error" in message:
            logger.error("[%s] Server error: %s", self._name, message["error"])
            return

        if not isinstance(message, list) or len(message) < 5:
            return

        topic = message[0]
        if not topic.endswith(".device"):
            return

        timestamp = message[2]
        data_str = message[4]

        try:
            data = json.loads(data_str) if isinstance(data_str, str) else data_str
        except json.JSONDecodeError:
            logger.warning("[%s] Bad data payload: %s", self._name, str(data_str)[:200])
            return

        device = data.get("device", "unknown")
        device_type = data.get("device_type", "unknown")
        health = data.get("health", "")
        device_state = data.get("state", "")

        logger.debug(
            "[%s] Device event: %s health=%s state=%s",
            self._name, device, health, device_state,
        )

        try:
            await self._handler.handle_device_event(
                self._name, device, device_type, health, device_state, timestamp
            )
        except Exception:
            logger.exception("[%s] Error handling event for device %s", self._name, device)
