import asyncio
import logging
import re

from .config import SpireConfig, SlackConfig
from .lms_client import LmsClient, LmsError
from .slack_client import SlackClient
from .spire_client import SpireClient, SpireError

logger = logging.getLogger(__name__)

LAA_DEVICE_PATTERN = re.compile(r"^(laa-\d+)-(.+)$")


def is_laa_device(device_name: str) -> bool:
    return LAA_DEVICE_PATTERN.match(device_name) is not None


class SpireHandler:
    def __init__(
        self,
        spire_config: SpireConfig,
        slack: SlackClient | None = None,
        slack_config: SlackConfig | None = None,
    ):
        self._config = spire_config
        self._slack = slack
        self._slack_config = slack_config

        cache_dir = spire_config.cache_dir.rstrip("/")
        self._spire_prod = SpireClient(spire_config.production, f"{cache_dir}/biscuit_prod.json")
        self._spire_staging = SpireClient(spire_config.staging, f"{cache_dir}/biscuit_staging.json")
        self._lms_prod = LmsClient(spire_config.production.lms_base, self._spire_prod.get_biscuit)
        self._lms_staging = LmsClient(spire_config.staging.lms_base, self._spire_staging.get_biscuit)

    async def handle_device_event(
        self,
        server_name: str,
        device: str,
        device_type: str,
        health: str,
        timestamp: str,
    ):
        if health == "Unknown":
            await self._handle_device_appeared(server_name, device, device_type, timestamp)
        elif health == "Retired":
            await self._handle_device_removed(server_name, device, device_type, timestamp)

    async def _handle_device_appeared(
        self, server_name: str, device: str, device_type: str, timestamp: str
    ):
        loop = asyncio.get_running_loop()
        match = LAA_DEVICE_PATTERN.match(device)
        if not match:
            return
        laa_name = match.group(1)

        try:
            env, subscription_id, appliance_uuid = await loop.run_in_executor(
                None, self._resolve_subscription, device, laa_name
            )
        except _UnresolvedSubscription:
            logger.warning("Could not resolve subscription for %s on %s.", device, server_name)
            if self._slack and self._slack_config and self._slack_config.alert_on_unresolved_subscription:
                self._slack.send_unresolved_subscription(device)
            return
        except (SpireError, LmsError) as exc:
            logger.exception("Error resolving subscription for %s.", device)
            self._alert_and_dead_letter(device, "resolve subscription", str(exc), {
                "server": server_name, "device": device, "device_type": device_type,
                "health": "Unknown", "timestamp": timestamp,
            })
            return

        spire = self._spire_prod if env == "production" else self._spire_staging
        external_id = f"fqdn:{device}:{appliance_uuid}:{device_type}"

        try:
            existing = await loop.run_in_executor(
                None, spire.get_resource_by_external_id, external_id
            )
            if existing:
                logger.info("SPIRE resource already exists for %s (id: %s), skipping.", device, existing["id"])
                return

            resource = await loop.run_in_executor(
                None,
                spire.create_resource,
                device,
                f"resource_type:lava:dut",
                subscription_id,
                external_id,
            )
            logger.info(
                "Created SPIRE resource for %s (id: %s, env: %s, subscription: %s).",
                device, resource["id"], env, subscription_id,
            )
        except SpireError as exc:
            logger.exception("Failed to create SPIRE resource for %s.", device)
            self._alert_and_dead_letter(device, "create resource", str(exc), {
                "server": server_name, "device": device, "device_type": device_type,
                "health": "Unknown", "timestamp": timestamp, "external_id": external_id,
            })

    async def _handle_device_removed(
        self, server_name: str, device: str, device_type: str, timestamp: str
    ):
        loop = asyncio.get_running_loop()
        match = LAA_DEVICE_PATTERN.match(device)
        if not match:
            return
        laa_name = match.group(1)

        try:
            env, subscription_id, appliance_uuid = await loop.run_in_executor(
                None, self._resolve_subscription, device, laa_name
            )
        except _UnresolvedSubscription:
            logger.warning("Could not resolve subscription for retired device %s on %s.", device, server_name)
            if self._slack and self._slack_config and self._slack_config.alert_on_unresolved_subscription:
                self._slack.send_unresolved_subscription(device)
            return
        except (SpireError, LmsError) as exc:
            logger.exception("Error resolving subscription for retired device %s.", device)
            self._alert_and_dead_letter(device, "resolve subscription (retired)", str(exc), {
                "server": server_name, "device": device, "device_type": device_type,
                "health": "Retired", "timestamp": timestamp,
            })
            return

        spire = self._spire_prod if env == "production" else self._spire_staging
        external_id = f"fqdn:{device}:{appliance_uuid}:{device_type}"

        try:
            existing = await loop.run_in_executor(
                None, spire.get_resource_by_external_id, external_id
            )
            if not existing:
                logger.info("No SPIRE resource found for retired device %s, nothing to delete.", device)
                return

            await loop.run_in_executor(None, spire.delete_resource, existing["id"])
            logger.info(
                "Deleted SPIRE resource for %s (id: %s, env: %s).",
                device, existing["id"], env,
            )
        except SpireError as exc:
            logger.exception("Failed to delete SPIRE resource for %s.", device)
            self._alert_and_dead_letter(device, "delete resource", str(exc), {
                "server": server_name, "device": device, "device_type": device_type,
                "health": "Retired", "timestamp": timestamp, "external_id": external_id,
            })

    def _resolve_subscription(self, device: str, laa_name: str) -> tuple[str, str, str]:
        """
        Try to find the subscription for a device by looking up the LAA in LMS.
        Returns (environment, subscription_id, appliance_uuid).
        Raises _UnresolvedSubscription if not found in either environment.
        """
        # Try production first
        appliance = self._lms_prod.get_appliance_by_name(laa_name)
        if appliance:
            subscription_id = (
                appliance.get("subscription_id")
                or appliance.get("subscription", {}).get("id")
            )
            appliance_uuid = appliance.get("id", "")
            if subscription_id:
                sub = self._spire_prod.get_subscription(subscription_id)
                if sub:
                    return ("production", subscription_id, appliance_uuid)

        # Try staging
        appliance = self._lms_staging.get_appliance_by_name(laa_name)
        if appliance:
            subscription_id = (
                appliance.get("subscription_id")
                or appliance.get("subscription", {}).get("id")
            )
            appliance_uuid = appliance.get("id", "")
            if subscription_id:
                sub = self._spire_staging.get_subscription(subscription_id)
                if sub:
                    return ("staging", subscription_id, appliance_uuid)

        raise _UnresolvedSubscription(f"Subscription not found for device {device} (LAA: {laa_name})")

    def _alert_and_dead_letter(self, device: str, operation: str, error: str, event_data: dict):
        if self._slack:
            self._slack.send_error(device, operation, error)
            self._slack.write_dead_letter(event_data, error)


class _UnresolvedSubscription(Exception):
    pass
