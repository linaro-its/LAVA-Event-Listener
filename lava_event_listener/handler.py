import asyncio
import logging

from .jira_client import JiraClient, JiraError
from .state import StateManager

logger = logging.getLogger(__name__)

BAD_HEALTH_STATUSES = {"Bad", "Maintenance", "Retired"}


class EventHandler:
    def __init__(self, jira: JiraClient, state: StateManager):
        self._jira = jira
        self._state = state

    async def handle_device_event(
        self,
        server_name: str,
        device: str,
        device_type: str,
        health: str,
        device_state: str,
        timestamp: str,
    ):
        device_id = f"{server_name}/{device}"
        loop = asyncio.get_running_loop()

        try:
            if health in BAD_HEALTH_STATUSES:
                await self._handle_bad_health(
                    loop, device_id, device, device_type, server_name, health, device_state, timestamp
                )
            elif health == "Good":
                await self._handle_good_health(
                    loop, device_id, device, server_name, timestamp
                )
        except JiraError:
            logger.exception("Jira error handling event for %s.", device_id)

    async def _handle_bad_health(
        self, loop, device_id, device, device_type, server_name, health, device_state, timestamp
    ):
        existing = self._state.get_device(device_id)

        if existing:
            is_open = await loop.run_in_executor(
                None, self._jira.is_issue_open, existing.ticket_key
            )

            if is_open:
                if existing.health != health:
                    comment = (
                        f"Device {device} on {server_name} health changed "
                        f"from {existing.health} to {health} at {timestamp}."
                    )
                    await loop.run_in_executor(
                        None, self._jira.add_comment, existing.ticket_key, comment
                    )
                    self._state.set_device(
                        device_id, existing.ticket_key, health, timestamp
                    )
                    logger.info("Updated %s for %s: %s -> %s", existing.ticket_key, device_id, existing.health, health)
                return

        summary = f"[LAVA] {device}: {health} on {server_name}"
        description = (
            f"Device: {device}\n"
            f"Device type: {device_type}\n"
            f"LAVA server: {server_name}\n"
            f"Health: {health}\n"
            f"Device state: {device_state}\n"
            f"Detected at: {timestamp}\n\n"
            f"This ticket was created automatically by the LAVA Event Listener."
        )
        ticket_key = await loop.run_in_executor(
            None, self._jira.create_ticket, summary, description
        )
        self._state.set_device(device_id, ticket_key, health, timestamp)
        logger.info("Created %s for %s (health: %s).", ticket_key, device_id, health)

    async def _handle_good_health(self, loop, device_id, device, server_name, timestamp):
        existing = self._state.get_device(device_id)
        if not existing:
            return

        is_open = await loop.run_in_executor(
            None, self._jira.is_issue_open, existing.ticket_key
        )
        if is_open:
            comment = (
                f"Device {device} on {server_name} has recovered. "
                f"Health is now Good as of {timestamp}."
            )
            await loop.run_in_executor(
                None, self._jira.add_comment, existing.ticket_key, comment
            )
            self._state.set_device(device_id, existing.ticket_key, "Good", timestamp)
            logger.info("Added recovery comment to %s for %s.", existing.ticket_key, device_id)
        else:
            self._state.remove_device(device_id)
            logger.info("Ticket %s is closed, removing %s from state.", existing.ticket_key, device_id)
