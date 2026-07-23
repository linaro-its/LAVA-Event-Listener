import logging
import time
from urllib.parse import urlparse

import requests

from .config import LavaServerConfig

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
INITIAL_BACKOFF = 2  # seconds
BACKOFF_FACTOR = 2


class LavaError(Exception):
    pass


class LavaClient:
    def __init__(self, config: LavaServerConfig):
        self._base_url = config.url.rstrip("/")
        self._session = requests.Session()
        if config.token:
            self._session.headers["Authorization"] = f"Token {config.token}"
        self._session.headers["Content-Type"] = "application/json"

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self._base_url}{path}"
        backoff = INITIAL_BACKOFF

        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = self._session.request(method, url, **kwargs)
            except requests.ConnectionError as exc:
                if attempt == MAX_RETRIES:
                    raise LavaError(f"Connection failed after {MAX_RETRIES} retries: {exc}") from exc
                logger.warning("LAVA connection error (attempt %d/%d): %s", attempt + 1, MAX_RETRIES, exc)
                time.sleep(backoff)
                backoff *= BACKOFF_FACTOR
                continue

            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt == MAX_RETRIES:
                    raise LavaError(f"LAVA API error {resp.status_code} after {MAX_RETRIES} retries: {resp.text[:500]}")
                retry_after = resp.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and retry_after.isdigit() else backoff
                logger.warning("LAVA %d (attempt %d/%d), retrying in %ds.", resp.status_code, attempt + 1, MAX_RETRIES, wait)
                time.sleep(wait)
                backoff *= BACKOFF_FACTOR
                continue

            if not resp.ok:
                raise LavaError(f"LAVA API error {resp.status_code}: {resp.text[:500]}")

            return resp

        raise LavaError("Unreachable: retry loop exhausted.")

    def get_device_tags(self, hostname: str) -> list[str]:
        """
        Return the list of tag *names* assigned to a device.

        The LAVA REST API (v0.2) returns a device's tags as numeric tag IDs, so
        we resolve those IDs to names via the tags endpoint. LMS-managed (Tier 3)
        devices are tagged with their LAA name, subscription UUID and appliance
        UUID, e.g. ["laa-00049", "sub-<uuid>", "dev-<uuid>"].
        """
        # Pass all=true so retired devices are still returned. The LAVA REST API
        # (DeviceViewSet.get_queryset) excludes health=Retired devices from the
        # queryset unless this flag is set, so a plain lookup 404s for a device
        # that has just been retired — which is exactly when a Retired event
        # fires and we need to read its sub-/dev- tags to reconcile SPIRE.
        resp = self._request(
            "GET",
            f"/api/v0.2/devices/{requests.utils.quote(hostname)}/",
            params={"all": "true"},
        )
        raw_tags = resp.json().get("tags", []) or []

        names: list[str] = []
        unresolved_ids: list[int] = []
        for tag in raw_tags:
            # Some LAVA versions return tag names directly; handle both.
            if isinstance(tag, str):
                names.append(tag)
            else:
                unresolved_ids.append(tag)

        if unresolved_ids:
            id_to_name = self._tag_name_map()
            for tag_id in unresolved_ids:
                name = id_to_name.get(tag_id)
                if name:
                    names.append(name)

        return names

    def _tag_name_map(self) -> dict[int, str]:
        """Build a {tag_id: tag_name} map from the LAVA tags endpoint."""
        mapping: dict[int, str] = {}
        path: str | None = "/api/v0.2/tags/?limit=1000"

        while path:
            resp = self._request("GET", path)
            data = resp.json()
            results = data.get("results", []) if isinstance(data, dict) else data
            for tag in results or []:
                tag_id = tag.get("id")
                name = tag.get("name")
                if tag_id is not None and name:
                    mapping[tag_id] = name

            next_url = data.get("next") if isinstance(data, dict) else None
            if next_url:
                parsed = urlparse(next_url)
                path = parsed.path + (f"?{parsed.query}" if parsed.query else "")
            else:
                path = None

        return mapping

    def submit_healthcheck(self, device: str) -> int:
        """Trigger a LAVA health check job for the given device. Returns the job ID."""
        resp = self._request("POST", f"/api/v0/devices/{device}/healthcheck/")
        job_id = resp.json()["job_id"]
        logger.info("Submitted healthcheck job %d for device %s.", job_id, device)
        return job_id

    def get_job_status(self, job_id: int) -> dict:
        """Return the job status dict with keys: state, health, failure_tags, failure_comment."""
        resp = self._request("GET", f"/api/v0/jobs/{job_id}/")
        data = resp.json()
        return {
            "state": data.get("state", ""),
            "health": data.get("health", ""),
            "failure_tags": data.get("failure_tags", []),
            "failure_comment": data.get("failure_comment", ""),
        }

    def job_url(self, job_id: int) -> str:
        return f"{self._base_url}/scheduler/job/{job_id}"
