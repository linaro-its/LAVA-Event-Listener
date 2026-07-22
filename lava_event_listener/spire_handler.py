import asyncio
import logging
import re
from urllib.parse import urlparse

from .config import LavaServerConfig, SpireConfig, SlackConfig
from .lava_client import LavaClient, LavaError
from .lms_client import LmsClient, LmsError
from .slack_client import SlackClient
from .spire_client import SpireClient, SpireError

logger = logging.getLogger(__name__)

LAA_DEVICE_PATTERN = re.compile(r"^(laa-\d+)-(.+)$")

SUBSCRIPTION_TAG_PREFIX = "sub-"
APPLIANCE_TAG_PREFIX = "dev-"


def is_laa_device(device_name: str) -> bool:
    return LAA_DEVICE_PATTERN.match(device_name) is not None


def _bare_host(url: str) -> str:
    """Normalise a LAVA server URL to a bare hostname, matching how the sync
    tool (lava-gateway) keys SPIRE external_ids."""
    parsed = urlparse(url)
    netloc = parsed.netloc or parsed.path
    return netloc.split("@")[-1].rstrip("/")


def _tag_value(tags: list[str], prefix: str) -> str | None:
    for tag in tags:
        if tag.startswith(prefix):
            return tag[len(prefix):]
    return None


class SpireHandler:
    def __init__(
        self,
        spire_config: SpireConfig,
        servers: list[LavaServerConfig] | None = None,
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

        # Per-server LAVA clients let us read a device's tags on the instance the
        # event came from, which is the reliable way to find its subscription and
        # appliance UUID without depending on LMS naming conventions.
        self._lava_clients: dict[str, LavaClient] = {}
        self._lava_fqdns: dict[str, str] = {}
        for srv in (servers or []):
            self._lava_clients[srv.name] = LavaClient(srv)
            self._lava_fqdns[srv.name] = _bare_host(srv.url)

    async def handle_device_event(
        self,
        server_name: str,
        device: str,
        device_type: str,
        health: str,
        timestamp: str,
    ):
        logger.info(
            "[%s] SPIRE handler received event: server=%s, device_type=%s, health=%s, timestamp=%s",
            device, server_name, device_type, health, timestamp,
        )
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
                None, self._resolve_subscription, server_name, device, laa_name
            )
        except _UnresolvedSubscription:
            logger.warning("Could not resolve subscription for %s on %s.", device, server_name)
            if self._slack and self._slack_config and self._slack_config.alert_on_unresolved_subscription:
                self._slack.send_unresolved_subscription(device)
            return
        except (SpireError, LmsError, LavaError) as exc:
            logger.exception("Error resolving subscription for %s.", device)
            self._alert_and_dead_letter(device, "resolve subscription", str(exc), {
                "server": server_name, "device": device, "device_type": device_type,
                "health": "Unknown", "timestamp": timestamp,
            })
            return

        spire = self._spire_prod if env == "production" else self._spire_staging
        external_id = self._build_external_id(server_name, device, appliance_uuid, device_type)

        # Track which SPIRE call we're making so a failure is attributed to the
        # right operation. These steps hit different endpoints with different
        # permission requirements (e.g. the resource-type lookup reads the
        # service catalogue, which needs a different right than creating the
        # resource), so lumping them under "create resource" hides the real
        # cause of a failure.
        operation = "look up resource"
        try:
            existing = await loop.run_in_executor(
                None, spire.get_resource_by_external_id, external_id
            )
            if existing:
                logger.info("SPIRE resource already exists for %s (id: %s), skipping.", device, existing["id"])
                return

            operation = "resolve resource type"
            type_id = await loop.run_in_executor(None, spire.get_dut_type_id)

            operation = "create resource"
            resource = await loop.run_in_executor(
                None,
                spire.create_resource,
                device,
                type_id,
                subscription_id,
                external_id,
            )
            logger.info(
                "Created SPIRE resource for %s (id: %s, env: %s, subscription: %s).",
                device, resource["id"], env, subscription_id,
            )
        except SpireError as exc:
            logger.exception("Failed during '%s' for SPIRE resource %s.", operation, device)
            self._alert_and_dead_letter(device, operation, str(exc), {
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
                None, self._resolve_subscription, server_name, device, laa_name
            )
        except _UnresolvedSubscription:
            logger.warning("Could not resolve subscription for retired device %s on %s.", device, server_name)
            if self._slack and self._slack_config and self._slack_config.alert_on_unresolved_subscription:
                self._slack.send_unresolved_subscription(device)
            return
        except (SpireError, LmsError, LavaError) as exc:
            logger.exception("Error resolving subscription for retired device %s.", device)
            self._alert_and_dead_letter(device, "resolve subscription (retired)", str(exc), {
                "server": server_name, "device": device, "device_type": device_type,
                "health": "Retired", "timestamp": timestamp,
            })
            return

        spire = self._spire_prod if env == "production" else self._spire_staging
        external_id = self._build_external_id(server_name, device, appliance_uuid, device_type)

        operation = "look up resource (retired)"
        try:
            existing = await loop.run_in_executor(
                None, spire.get_resource_by_external_id, external_id
            )
            if not existing:
                logger.info("No SPIRE resource found for retired device %s, nothing to delete.", device)
                return

            operation = "delete resource"
            await loop.run_in_executor(None, spire.delete_resource, existing["id"])
            logger.info(
                "Deleted SPIRE resource for %s (id: %s, env: %s).",
                device, existing["id"], env,
            )
        except SpireError as exc:
            logger.exception("Failed during '%s' for SPIRE resource %s.", operation, device)
            self._alert_and_dead_letter(device, operation, str(exc), {
                "server": server_name, "device": device, "device_type": device_type,
                "health": "Retired", "timestamp": timestamp, "external_id": external_id,
            })

    def _build_external_id(
        self, server_name: str, device: str, appliance_uuid: str, device_type: str
    ) -> str:
        """
        Build the SPIRE resource external_id, matching the format the sync tool
        (lava-gateway) uses so create/lookup/delete all reference the same record:
          Tier 3 (appliance known): "<fqdn>:<device>:<appliance_uuid>:<device_type>"
          otherwise:                 "<fqdn>:<device>:<device_type>"
        """
        fqdn = self._lava_fqdns.get(server_name, server_name)
        if appliance_uuid:
            return f"{fqdn}:{device}:{appliance_uuid}:{device_type}"
        return f"{fqdn}:{device}:{device_type}"

    def _resolve_subscription(
        self, server_name: str, device: str, laa_name: str
    ) -> tuple[str, str, str]:
        """
        Resolve (environment, subscription_id, appliance_uuid) for a device.

        Primary path: read the device's tags on the LAVA instance the event came
        from. LMS-managed devices are tagged with their subscription UUID
        ("sub-<uuid>") and appliance UUID ("dev-<uuid>"), which sidesteps the
        naming mismatch between LAVA hostnames and LMS appliance names.

        Fallback: the legacy LMS lookup by LAA name.

        Raises _UnresolvedSubscription if neither path resolves.
        """
        tags = self._get_device_tags(server_name, device)
        subscription_id = _tag_value(tags, SUBSCRIPTION_TAG_PREFIX)
        appliance_uuid = _tag_value(tags, APPLIANCE_TAG_PREFIX)

        if subscription_id:
            logger.info(
                "[%s] Resolved subscription %s from LAVA device tags (appliance=%s).",
                device, subscription_id, appliance_uuid or "unknown",
            )
            env = self._env_for_subscription(device, subscription_id)
            if env:
                return (env, subscription_id, appliance_uuid or "")
            logger.info(
                "[%s] Subscription %s from tags not found in any SPIRE environment; "
                "falling back to LMS lookup.",
                device, subscription_id,
            )
        else:
            logger.info(
                "[%s] No subscription tag on LAVA device; falling back to LMS lookup for '%s'.",
                device, laa_name,
            )

        return self._resolve_subscription_via_lms(device, laa_name)

    def _get_device_tags(self, server_name: str, device: str) -> list[str]:
        lava = self._lava_clients.get(server_name)
        if not lava:
            logger.info(
                "[%s] No LAVA client configured for server '%s'; cannot read device tags.",
                device, server_name,
            )
            return []
        try:
            tags = lava.get_device_tags(device)
            logger.info("[%s] LAVA device tags: %s", device, tags)
            return tags
        except LavaError as exc:
            logger.info("[%s] Failed to read LAVA device tags: %s", device, exc)
            return []

    def _env_for_subscription(self, device: str, subscription_id: str) -> str | None:
        """Return the environment whose SPIRE knows this subscription, or None."""
        for env, spire in (("production", self._spire_prod), ("staging", self._spire_staging)):
            try:
                if spire.get_subscription(subscription_id):
                    logger.info("[%s] Subscription %s found in %s SPIRE.", device, subscription_id, env)
                    return env
            except SpireError as exc:
                logger.info("[%s] %s SPIRE subscription check failed: %s", device, env, exc)
        return None

    def _resolve_subscription_via_lms(self, device: str, laa_name: str) -> tuple[str, str, str]:
        """
        Legacy resolution: find the LAA appliance in LMS by name and read its
        subscription. Kept as a fallback for devices that are not tagged.
        Returns (environment, subscription_id, appliance_uuid).
        Raises _UnresolvedSubscription if not found in either environment.
        """
        # Try production first
        try:
            logger.info("[%s] Trying production LMS for appliance '%s'...", device, laa_name)
            appliance = self._lms_prod.get_appliance_by_name(laa_name)
            if appliance:
                logger.info("[%s] Production LMS found appliance: id=%s", device, appliance.get("id"))
                subscription_id = (
                    appliance.get("subscription_id")
                    or appliance.get("subscription", {}).get("id")
                )
                appliance_uuid = appliance.get("id", "")
                if subscription_id:
                    logger.info("[%s] Appliance has subscription_id=%s, verifying in production SPIRE...", device, subscription_id)
                    sub = self._spire_prod.get_subscription(subscription_id)
                    if sub:
                        logger.info("[%s] Resolved: production, subscription=%s, appliance=%s", device, subscription_id, appliance_uuid)
                        return ("production", subscription_id, appliance_uuid)
                    else:
                        logger.info("[%s] Subscription %s not found in production SPIRE.", device, subscription_id)
                else:
                    logger.info("[%s] Production appliance has no subscription_id. Raw: %s", device, appliance)
            else:
                logger.info("[%s] Appliance '%s' not found in production LMS.", device, laa_name)
        except (LmsError, SpireError) as exc:
            logger.info("[%s] Production lookup failed: %s", device, exc)

        # Try staging
        try:
            logger.info("[%s] Trying staging LMS for appliance '%s'...", device, laa_name)
            appliance = self._lms_staging.get_appliance_by_name(laa_name)
            if appliance:
                logger.info("[%s] Staging LMS found appliance: id=%s", device, appliance.get("id"))
                subscription_id = (
                    appliance.get("subscription_id")
                    or appliance.get("subscription", {}).get("id")
                )
                appliance_uuid = appliance.get("id", "")
                if subscription_id:
                    logger.info("[%s] Appliance has subscription_id=%s, verifying in staging SPIRE...", device, subscription_id)
                    sub = self._spire_staging.get_subscription(subscription_id)
                    if sub:
                        logger.info("[%s] Resolved: staging, subscription=%s, appliance=%s", device, subscription_id, appliance_uuid)
                        return ("staging", subscription_id, appliance_uuid)
                    else:
                        logger.info("[%s] Subscription %s not found in staging SPIRE.", device, subscription_id)
                else:
                    logger.info("[%s] Staging appliance has no subscription_id. Raw: %s", device, appliance)
            else:
                logger.info("[%s] Appliance '%s' not found in staging LMS.", device, laa_name)
        except (LmsError, SpireError) as exc:
            logger.info("[%s] Staging lookup failed: %s", device, exc)

        raise _UnresolvedSubscription(f"Subscription not found for device {device} (LAA: {laa_name})")

    def _alert_and_dead_letter(self, device: str, operation: str, error: str, event_data: dict):
        if self._slack:
            self._slack.send_error(device, operation, error)
            self._slack.write_dead_letter(event_data, error)


class _UnresolvedSubscription(Exception):
    pass
