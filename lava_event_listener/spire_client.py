import json
import logging
import time
from pathlib import Path

import requests

from .config import SpireEnvironmentConfig

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
INITIAL_BACKOFF = 1
BACKOFF_FACTOR = 4
TOKEN_BUFFER_SECONDS = 300


class SpireError(Exception):
    pass


def _prefixed_subscription_id(subscription_id: str) -> str:
    """SPIRE expects subscription ids in 'sub:<uuid>' form; tags/LMS give raw UUIDs."""
    return subscription_id if subscription_id.startswith("sub:") else f"sub:{subscription_id}"


class SpireClient:
    def __init__(self, config: SpireEnvironmentConfig, cache_file: str):
        self._config = config
        self._cache_file = cache_file
        self._session = requests.Session()
        self._session.headers["Content-Type"] = "application/json"
        self._biscuit: str | None = None
        self._biscuit_expires_at: float = 0
        self._type_id_cache: dict[str, str] = {}

    def get_biscuit(self) -> str:
        self._ensure_biscuit()
        return self._biscuit

    def get_resource_by_external_id(self, external_id: str) -> dict | None:
        self._ensure_biscuit()
        try:
            resp = self._request(
                "GET",
                f"/resource?external_id={requests.utils.quote(external_id)}",
            )
            resource = resp.json()["data"]
            if resource.get("state") != "active":
                return None
            return resource
        except SpireError as exc:
            if "404" in str(exc):
                return None
            raise

    def get_subscription(self, subscription_id: str) -> dict | None:
        self._ensure_biscuit()
        try:
            prefixed = _prefixed_subscription_id(subscription_id)
            resp = self._request(
                "GET", f"/subscription/{requests.utils.quote(prefixed, safe='')}"
            )
            return resp.json().get("data")
        except SpireError as exc:
            if "404" in str(exc):
                return None
            raise

    def resolve_resource_type_id(self, service_name: str, type_name: str) -> str:
        """Resolve a SPIRE resource type to its prefixed 'type:<uuid>' id (cached)."""
        cache_key = f"{service_name}:{type_name}"
        if cache_key in self._type_id_cache:
            return self._type_id_cache[cache_key]

        self._ensure_biscuit()
        services = self._request("GET", "/services").json()["data"]
        service = next((s for s in services if s.get("id") == f"service:{service_name}"), None)
        if not service:
            raise SpireError(f"SPIRE service '{service_name}' not found")

        types = self._request(
            "GET", f"/service/{service['id']}/resource_types"
        ).json()["data"]
        resource_type = next((t for t in types if t.get("name") == type_name), None)
        if not resource_type:
            raise SpireError(
                f"Resource type '{type_name}' not found in service '{service_name}'"
            )

        self._type_id_cache[cache_key] = resource_type["id"]
        return resource_type["id"]

    def get_dut_type_id(self) -> str:
        """The SPIRE resource type id for LAVA DUTs (service 'lms', type 'dut')."""
        return self.resolve_resource_type_id("lms", "dut")

    def create_resource(self, name: str, type_id: str, subscription_id: str, external_id: str) -> dict:
        self._ensure_biscuit()
        body = {
            "name": name,
            "type_id": type_id,
            "subscription_id": _prefixed_subscription_id(subscription_id),
            "external_id": external_id,
        }
        resp = self._request("POST", "/resource", json=body)
        return resp.json()["data"]

    def delete_resource(self, resource_id: str) -> None:
        self._ensure_biscuit()
        self._request("DELETE", f"/resource/{resource_id}")

    def _ensure_biscuit(self):
        if self._biscuit and self._biscuit_expires_at > time.time() + TOKEN_BUFFER_SECONDS:
            return

        cached = self._read_cache()
        if cached and cached["expires_at"] > time.time() + TOKEN_BUFFER_SECONDS:
            self._biscuit = cached["biscuit"]
            self._biscuit_expires_at = cached["expires_at"]
            return

        self._acquire_biscuit()

    def _acquire_biscuit(self):
        cfg = self._config
        auth0_host = cfg.auth0_domain.replace("https://", "").replace("http://", "").rstrip("/")
        token_url = f"https://{auth0_host}/oauth/token"

        token_resp = requests.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": cfg.auth0_client_id,
                "client_secret": cfg.auth0_client_secret,
                "audience": cfg.auth0_audience,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        if token_resp.status_code >= 400:
            raise SpireError(f"Auth0 token request failed ({token_resp.status_code}): {token_resp.text[:300]}")

        access_token = token_resp.json()["access_token"]

        session_url = f"{cfg.spire_api_base.rstrip('/')}/user/self/session"
        session_resp = requests.get(
            session_url,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30,
        )
        if session_resp.status_code >= 400:
            raise SpireError(f"SPIRE session exchange failed ({session_resp.status_code}): {session_resp.text[:300]}")

        biscuit_data = session_resp.json()["biscuit"]
        self._biscuit = biscuit_data["token"]
        raw_expires = biscuit_data["expires_at"]
        parsed = float(raw_expires) if str(raw_expires).replace(".", "").isdigit() else 0
        self._biscuit_expires_at = parsed if parsed > 1e12 else parsed * 1000 if parsed > 1e9 else time.time() + 3600
        # Normalise to seconds
        if self._biscuit_expires_at > 1e12:
            self._biscuit_expires_at = self._biscuit_expires_at / 1000

        self._write_cache({"biscuit": self._biscuit, "expires_at": self._biscuit_expires_at})
        logger.info("Acquired new SPIRE biscuit (expires %.0f).", self._biscuit_expires_at)

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self._config.spire_api_base.rstrip('/')}{path}"
        headers = {
            "Authorization": f"Bearer {self._biscuit}",
            "Content-Type": "application/json",
        }
        backoff = INITIAL_BACKOFF

        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = self._session.request(method, url, headers=headers, timeout=30, **kwargs)
            except requests.ConnectionError as exc:
                if attempt == MAX_RETRIES:
                    raise SpireError(f"SPIRE connection failed after {MAX_RETRIES} retries: {exc}") from exc
                logger.warning("SPIRE connection error (attempt %d/%d): %s", attempt + 1, MAX_RETRIES, exc)
                time.sleep(backoff)
                backoff *= BACKOFF_FACTOR
                continue

            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt == MAX_RETRIES:
                    raise SpireError(f"SPIRE API error {resp.status_code} after {MAX_RETRIES} retries: {resp.text[:300]}")
                time.sleep(backoff)
                backoff *= BACKOFF_FACTOR
                continue

            if not resp.ok:
                raise SpireError(f"SPIRE API error {resp.status_code}: {resp.text[:300]}")

            return resp

        raise SpireError("Unreachable: retry loop exhausted.")

    def _read_cache(self) -> dict | None:
        try:
            data = Path(self._cache_file).read_text()
            return json.loads(data)
        except (OSError, json.JSONDecodeError):
            return None

    def _write_cache(self, data: dict):
        try:
            Path(self._cache_file).parent.mkdir(parents=True, exist_ok=True)
            Path(self._cache_file).write_text(json.dumps(data, indent=2))
        except OSError:
            logger.warning("Failed to write biscuit cache to %s.", self._cache_file)
