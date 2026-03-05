import asyncio
import logging

import aiohttp

from .config import BetterStackConfig
from .listener import LavaListener

logger = logging.getLogger(__name__)


async def run_heartbeat(
    config: BetterStackConfig,
    listeners: list[LavaListener],
):
    while True:
        await asyncio.sleep(config.interval_seconds)

        any_connected = any(listener.connected for listener in listeners)
        if not any_connected:
            logger.warning("Skipping heartbeat: no LAVA listeners are connected.")
            continue

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(config.heartbeat_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.ok:
                        logger.debug("Heartbeat sent successfully.")
                    else:
                        logger.warning("Heartbeat returned HTTP %d.", resp.status)
        except Exception:
            logger.exception("Failed to send heartbeat.")
