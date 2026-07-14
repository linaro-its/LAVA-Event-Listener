import logging
import time
from typing import Callable

import requests

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
INITIAL_BACKOFF = 1
BACKOFF_FACTOR = 4


class LmsError(Exception):
    pass


class LmsClient:
    def __init__(self, base_url: str, get_biscuit: Callable[[], str]):
        self._base_url = base_url.rstrip("/")
        self._get_biscuit = get_biscuit
        self._session = requests.Session()
        self._session.headers["Content-Type"] = "application/json"

    def get_appliance_by_name(self, name: str) -> dict | None:
        try:
            resp = self._request("GET", f"/appliances?name={requests.utils.quote(name)}")
            data = resp.json()
            items = data.get("data", data) if isinstance(data, dict) else data
            if isinstance(items, list) and items:
                return items[0]
            if isinstance(items, dict) and items.get("id"):
                return items
            return None
        except LmsError as exc:
            if "404" in str(exc):
                return None
            raise

    def get_appliance_subscription(self, appliance_id: str) -> str | None:
        try:
            resp = self._request("GET", f"/appliances/{appliance_id}")
            data = resp.json()
            appliance = data.get("data", data) if isinstance(data, dict) else data
            return appliance.get("subscription_id") or appliance.get("subscription", {}).get("id")
        except LmsError:
            logger.exception("Failed to get subscription for appliance %s.", appliance_id)
            return None

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self._base_url}{path}"
        headers = {
            "Authorization": f"Bearer {self._get_biscuit()}",
            "Content-Type": "application/json",
        }
        backoff = INITIAL_BACKOFF

        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = self._session.request(method, url, headers=headers, timeout=30, **kwargs)
            except requests.ConnectionError as exc:
                if attempt == MAX_RETRIES:
                    raise LmsError(f"LMS connection failed after {MAX_RETRIES} retries: {exc}") from exc
                logger.warning("LMS connection error (attempt %d/%d): %s", attempt + 1, MAX_RETRIES, exc)
                time.sleep(backoff)
                backoff *= BACKOFF_FACTOR
                continue

            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt == MAX_RETRIES:
                    raise LmsError(f"LMS API error {resp.status_code} after {MAX_RETRIES} retries: {resp.text[:300]}")
                time.sleep(backoff)
                backoff *= BACKOFF_FACTOR
                continue

            if not resp.ok:
                raise LmsError(f"LMS API error {resp.status_code}: {resp.text[:300]}")

            return resp

        raise LmsError("Unreachable: retry loop exhausted.")
