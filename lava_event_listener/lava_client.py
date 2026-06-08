import logging
import time

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
