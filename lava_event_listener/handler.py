import asyncio
import logging

from .config import LavaServerConfig
from .jira_client import JiraClient, JiraError
from .lava_client import LavaClient, LavaError
from .state import StateManager

logger = logging.getLogger(__name__)

BAD_HEALTH_STATUSES = {"Bad", "Maintenance", "Retired"}

BAD_WORKER_HEALTH = {"Maintenance", "Retired"}
BAD_WORKER_STATE = {"Offline"}


class EventHandler:
    def __init__(self, jira: JiraClient, state: StateManager, servers: list[LavaServerConfig] | None = None):
        self._jira = jira
        self._state = state
        self._servers: dict[str, LavaServerConfig] = {s.name: s for s in (servers or [])}

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

        srv_config = self._servers.get(server_name)
        if srv_config and srv_config.healthcheck.enabled:
            if srv_config.token:
                asyncio.ensure_future(
                    self._run_healthcheck_and_report(srv_config, device, ticket_key)
                )
            else:
                logger.warning(
                    "Healthcheck enabled for %s but no token configured — skipping.", server_name
                )

    async def _run_healthcheck_and_report(
        self, srv_config: LavaServerConfig, device: str, ticket_key: str
    ):
        hc = srv_config.healthcheck
        lava = LavaClient(srv_config)
        loop = asyncio.get_running_loop()

        try:
            job_id = await loop.run_in_executor(None, lava.submit_healthcheck, device)
        except LavaError:
            logger.exception("Failed to submit healthcheck for %s on %s.", device, srv_config.name)
            await loop.run_in_executor(
                None,
                self._jira.add_comment,
                ticket_key,
                f"Healthcheck could not be submitted for {device}: see listener logs for details.",
            )
            return

        job_url = lava.job_url(job_id)
        await loop.run_in_executor(
            None,
            self._jira.add_comment,
            ticket_key,
            f"Healthcheck job submitted: {job_url}\nJob ID: {job_id} — polling for results...",
        )

        deadline = hc.timeout_minutes * 60
        elapsed = 0
        status = None

        while elapsed < deadline:
            await asyncio.sleep(hc.poll_interval_seconds)
            elapsed += hc.poll_interval_seconds
            try:
                status = await loop.run_in_executor(None, lava.get_job_status, job_id)
            except LavaError:
                logger.warning("Error polling healthcheck job %d for %s; will retry.", job_id, device)
                continue

            if status["state"] == "Finished":
                break
        else:
            await loop.run_in_executor(
                None,
                self._jira.add_comment,
                ticket_key,
                f"Healthcheck job {job_id} did not finish within {hc.timeout_minutes} minutes.\n"
                f"Check manually: {job_url}",
            )
            logger.warning("Healthcheck job %d for %s timed out after %d min.", job_id, device, hc.timeout_minutes)
            return

        health = status["health"]
        failure_tags = status["failure_tags"]
        failure_comment = status["failure_comment"]

        if health == "Complete":
            result_line = "Healthcheck result: PASS"
        else:
            result_line = f"Healthcheck result: {health.upper() if health else 'UNKNOWN'}"

        lines = [result_line, f"Job ID: {job_id} — {job_url}"]
        if failure_tags:
            lines.append(f"Failure tags: {', '.join(failure_tags)}")
        if failure_comment:
            lines.append(f"Failure comment: {failure_comment}")

        await loop.run_in_executor(
            None,
            self._jira.add_comment,
            ticket_key,
            "\n".join(lines),
        )
        logger.info("Healthcheck job %d for %s finished: %s.", job_id, device, health)

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

    async def handle_worker_event(
        self,
        server_name: str,
        worker: str,
        health: str,
        worker_state: str,
        timestamp: str,
    ):
        worker_id = f"{server_name}/{worker}"
        loop = asyncio.get_running_loop()

        try:
            if health in BAD_WORKER_HEALTH or worker_state in BAD_WORKER_STATE:
                await self._handle_bad_worker(
                    loop, worker_id, worker, server_name, health, worker_state, timestamp
                )
            elif health == "Active" and worker_state == "Online":
                await self._handle_recovered_worker(
                    loop, worker_id, worker, server_name, health, worker_state, timestamp
                )
        except JiraError:
            logger.exception("Jira error handling worker event for %s.", worker_id)

    async def _handle_bad_worker(
        self, loop, worker_id, worker, server_name, health, worker_state, timestamp
    ):
        existing = self._state.get_worker(worker_id)

        if existing:
            is_open = await loop.run_in_executor(
                None, self._jira.is_issue_open, existing.ticket_key
            )
            if is_open:
                if existing.health != health or existing.state != worker_state:
                    comment = (
                        f"Worker {worker} on {server_name} condition changed "
                        f"from health={existing.health}, state={existing.state} "
                        f"to health={health}, state={worker_state} at {timestamp}."
                    )
                    await loop.run_in_executor(
                        None, self._jira.add_comment, existing.ticket_key, comment
                    )
                    self._state.set_worker(
                        worker_id, existing.ticket_key, health, worker_state, timestamp
                    )
                    logger.info(
                        "Updated %s for %s: health=%s state=%s -> health=%s state=%s",
                        existing.ticket_key, worker_id,
                        existing.health, existing.state, health, worker_state,
                    )
                return

        summary = f"[LAVA] Worker {worker}: {health}, {worker_state} on {server_name}"
        description = (
            f"Worker: {worker}\n"
            f"LAVA server: {server_name}\n"
            f"Health: {health}\n"
            f"State: {worker_state}\n"
            f"Detected at: {timestamp}\n\n"
            f"This ticket was created automatically by the LAVA Event Listener."
        )
        ticket_key = await loop.run_in_executor(
            None, self._jira.create_ticket, summary, description
        )
        self._state.set_worker(worker_id, ticket_key, health, worker_state, timestamp)
        logger.info("Created %s for %s (health: %s, state: %s).", ticket_key, worker_id, health, worker_state)

    async def _handle_recovered_worker(
        self, loop, worker_id, worker, server_name, health, worker_state, timestamp
    ):
        existing = self._state.get_worker(worker_id)
        if not existing:
            return

        is_open = await loop.run_in_executor(
            None, self._jira.is_issue_open, existing.ticket_key
        )
        if is_open:
            comment = (
                f"Worker {worker} on {server_name} has recovered. "
                f"Health is Active and state is Online as of {timestamp}."
            )
            await loop.run_in_executor(
                None, self._jira.add_comment, existing.ticket_key, comment
            )
            self._state.set_worker(worker_id, existing.ticket_key, "Active", "Online", timestamp)
            logger.info("Added recovery comment to %s for %s.", existing.ticket_key, worker_id)
        else:
            self._state.remove_worker(worker_id)
            logger.info("Ticket %s is closed, removing %s from state.", existing.ticket_key, worker_id)
