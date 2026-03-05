import logging
import time

import requests

from .config import JiraConfig

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
INITIAL_BACKOFF = 2  # seconds
BACKOFF_FACTOR = 2
TERMINAL_STATUSES = {"done", "closed", "resolved", "canceled", "cancelled"}


class JiraError(Exception):
    pass


class JiraClient:
    def __init__(self, config: JiraConfig):
        self._url = config.url.rstrip("/")
        self._project_key = config.project_key
        self._issue_type = config.issue_type
        self._session = requests.Session()
        self._session.auth = (config.email, config.api_token)
        self._session.headers["Content-Type"] = "application/json"

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self._url}{path}"
        backoff = INITIAL_BACKOFF

        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = self._session.request(method, url, **kwargs)
            except requests.ConnectionError as exc:
                if attempt == MAX_RETRIES:
                    raise JiraError(f"Connection failed after {MAX_RETRIES} retries: {exc}") from exc
                logger.warning("Jira connection error (attempt %d/%d): %s", attempt + 1, MAX_RETRIES, exc)
                time.sleep(backoff)
                backoff *= BACKOFF_FACTOR
                continue

            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt == MAX_RETRIES:
                    raise JiraError(f"Jira API error {resp.status_code} after {MAX_RETRIES} retries: {resp.text[:500]}")
                retry_after = resp.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and retry_after.isdigit() else backoff
                logger.warning("Jira %d (attempt %d/%d), retrying in %ds.", resp.status_code, attempt + 1, MAX_RETRIES, wait)
                time.sleep(wait)
                backoff *= BACKOFF_FACTOR
                continue

            if not resp.ok:
                raise JiraError(f"Jira API error {resp.status_code}: {resp.text[:500]}")

            return resp

        raise JiraError("Unreachable: retry loop exhausted.")

    def create_ticket(self, summary: str, description: str) -> str:
        payload = {
            "fields": {
                "project": {"key": self._project_key},
                "issuetype": {"name": self._issue_type},
                "summary": summary,
                "description": description,
            }
        }
        resp = self._request("POST", "/rest/api/2/issue", json=payload)
        key = resp.json()["key"]
        logger.info("Created Jira ticket %s: %s", key, summary)
        return key

    def add_comment(self, issue_key: str, comment: str):
        self._request("POST", f"/rest/api/2/issue/{issue_key}/comment", json={"body": comment})
        logger.info("Added comment to %s.", issue_key)

    def get_issue_status(self, issue_key: str) -> str | None:
        try:
            resp = self._request("GET", f"/rest/api/2/issue/{issue_key}", params={"fields": "status"})
            return resp.json()["fields"]["status"]["name"]
        except JiraError as exc:
            if "404" in str(exc):
                logger.warning("Jira ticket %s not found.", issue_key)
                return None
            raise

    def is_issue_open(self, issue_key: str) -> bool:
        status = self.get_issue_status(issue_key)
        if status is None:
            return False
        return status.lower() not in TERMINAL_STATUSES
