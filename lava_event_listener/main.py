import argparse
import asyncio
import logging
import signal
import sys

from .config import load_config
from .handler import EventHandler
from .heartbeat import run_heartbeat
from .jira_client import JiraClient
from .listener import LavaListener
from .state import StateManager

logger = logging.getLogger(__name__)


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


async def run_all(listeners: list[LavaListener], heartbeat_coro=None):
    tasks = [asyncio.create_task(listener.run()) for listener in listeners]

    if heartbeat_coro is not None:
        tasks.append(asyncio.create_task(heartbeat_coro))

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Received shutdown signal.")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    await stop_event.wait()

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("All tasks stopped. Exiting.")


def main():
    parser = argparse.ArgumentParser(description="LAVA Device Health Event Listener")
    parser.add_argument(
        "-c", "--config",
        default="/etc/lava-event-listener/config.yaml",
        help="Path to YAML configuration file",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    setup_logging(config.log_level)

    if config.sentry_dsn:
        try:
            import sentry_sdk
            sentry_sdk.init(dsn=config.sentry_dsn, traces_sample_rate=0)
            logger.info("Sentry initialized.")
        except ImportError:
            logger.warning("sentry-sdk not installed, error tracking disabled.")

    state = StateManager(config.state_file)
    jira = JiraClient(config.jira)
    handler = EventHandler(jira, state)

    listeners = [LavaListener(srv, handler) for srv in config.lava_servers]

    logger.info(
        "Starting LAVA Event Listener with %d server(s): %s",
        len(listeners),
        ", ".join(srv.name for srv in config.lava_servers),
    )

    heartbeat_coro = None
    if config.betterstack:
        heartbeat_coro = run_heartbeat(config.betterstack, listeners)
        logger.info("BetterStack heartbeat enabled (interval: %ds).", config.betterstack.interval_seconds)

    asyncio.run(run_all(listeners, heartbeat_coro))


if __name__ == "__main__":
    main()
