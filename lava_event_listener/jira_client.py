import logging
import time

import requests

from .config import JiraConfig

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
INITIAL_BACKOFF = 2  # seconds
BACKOFF_FACTOR = 2
TERMINAL_CATEGORIES = {"done"}


class JiraError(Exception):
    pass


class JiraClient:
    def __init__(self, config: JiraConfig):
        self._url = config.url.rstrip("/")
        self._project_key = config.project_key
        self._request_type_name = config.request_type
        self._session = requests.Session()
        self._session.auth = (config.email, config.api_token)
        self._session.headers["Content-Type"] = "application/json"
        self._service_desk_id: str | None = None
        self._request_type_id: str | None = None
        self._account_id_cache: dict[str, str] = {}

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

    def _ensure_jsm_ids(self):
        """Look up the service desk ID and request type ID on first use."""
        if self._service_desk_id and self._request_type_id:
            return

        # Find the service desk ID for our project key
        resp = self._request("GET", "/rest/servicedeskapi/servicedesk")
        for desk in resp.json().get("values", []):
            if desk.get("projectKey") == self._project_key:
                self._service_desk_id = str(desk["id"])
                break
        if not self._service_desk_id:
            raise JiraError(
                f"No service desk found for project key '{self._project_key}'. "
                f"Available: {[d.get('projectKey') for d in resp.json().get('values', [])]}"
            )
        logger.info("Resolved service desk ID: %s for project %s", self._service_desk_id, self._project_key)

        # Find the request type ID by name
        resp = self._request(
            "GET",
            f"/rest/servicedeskapi/servicedesk/{self._service_desk_id}/requesttype",
        )
        for rt in resp.json().get("values", []):
            if rt.get("name") == self._request_type_name:
                self._request_type_id = str(rt["id"])
                break
        if not self._request_type_id:
            available = [rt.get("name") for rt in resp.json().get("values", [])]
            raise JiraError(
                f"Request type '{self._request_type_name}' not found in service desk {self._service_desk_id}. "
                f"Available: {available}"
            )
        logger.info("Resolved request type ID: %s for '%s'", self._request_type_id, self._request_type_name)

    def create_ticket(self, summary: str, description: str) -> str:
        self._ensure_jsm_ids()
        payload = {
            "serviceDeskId": self._service_desk_id,
            "requestTypeId": self._request_type_id,
            "requestFieldValues": {
                "summary": summary,
                "description": description,
            },
        }
        resp = self._request("POST", "/rest/servicedeskapi/request", json=payload)
        key = resp.json()["issueKey"]
        logger.info("Created JSM ticket %s: %s", key, summary)
        return key

    def _resolve_email_to_account_id(self, email: str) -> str | None:
        if email in self._account_id_cache:
            return self._account_id_cache[email]
        try:
            resp = self._request("GET", "/rest/api/3/user/search", params={"query": email})
        except JiraError:
            logger.warning("Failed to search for Jira user '%s'.", email)
            return None
        for user in resp.json():
            if user.get("emailAddress", "").lower() == email.lower():
                account_id = user["accountId"]
                self._account_id_cache[email] = account_id
                return account_id
        logger.warning("No Jira account found for email '%s'.", email)
        return None

    def add_participants(self, issue_key: str, emails: list[str]) -> None:
        resolved: list[tuple[str, str]] = []
        unresolved: list[str] = []
        for email in emails:
            account_id = self._resolve_email_to_account_id(email)
            if account_id:
                resolved.append((email, account_id))
            else:
                unresolved.append(email)

        comment_lines: list[str] = []

        if resolved:
            try:
                self._request(
                    "POST",
                    f"/rest/servicedeskapi/request/{issue_key}/participant",
                    json={"accountIds": [aid for _, aid in resolved]},
                )
                added = ", ".join(email for email, _ in resolved)
                comment_lines.append(f"Request participants added: {added}")
                logger.info("Added participants to %s: %s", issue_key, added)
            except JiraError as exc:
                failed = ", ".join(email for email, _ in resolved)
                comment_lines.append(f"Failed to add request participants ({failed}): {exc}")
                logger.warning("Failed to add participants to %s: %s", issue_key, exc)
        else:
            comment_lines.append(
                "Could not add any request participants — no Jira accounts found for: "
                + ", ".join(emails)
            )

        if unresolved:
            comment_lines.append(
                "Could not resolve to a Jira account (skipped): " + ", ".join(unresolved)
            )

        self.add_comment(issue_key, "\n".join(comment_lines))

    def add_comment(self, issue_key: str, comment: str):
        payload = {"body": comment, "public": True}
        self._request(
            "POST",
            f"/rest/servicedeskapi/request/{issue_key}/comment",
            json=payload,
        )
        logger.info("Added comment to %s.", issue_key)

    @staticmethod
    def is_comment_limit_error(exc: Exception) -> bool:
        """True if a JiraError is Jira's per-issue comment-limit rejection.

        Jira returns HTTP 413 with 'COMMENTS_PER_ISSUE_LIMIT_EXCEEDED' once an
        issue reaches its 5000-comment cap. We match the specific marker rather
        than the bare 413 so an oversized single comment isn't misclassified.
        """
        return "COMMENTS_PER_ISSUE_LIMIT_EXCEEDED" in str(exc)

    def close_ticket(self, issue_key: str) -> bool:
        """Best-effort transition of an issue to a terminal ('done') status.

        Used when rolling a full ticket over to a follow-on. Returns True if a
        terminal transition was applied, False if none was available. Does not
        add a comment (the ticket is typically full at this point).
        """
        resp = self._request("GET", f"/rest/api/3/issue/{issue_key}/transitions")
        transitions = resp.json().get("transitions", [])

        target = next(
            (t for t in transitions
             if t.get("to", {}).get("statusCategory", {}).get("key") == "done"),
            None,
        )
        if not target:
            wanted = {"done", "close", "closed", "resolve", "resolved", "cancel", "cancelled", "complete", "completed"}
            target = next(
                (t for t in transitions if t.get("name", "").strip().lower() in wanted),
                None,
            )
        if not target:
            available = [t.get("name") for t in transitions]
            logger.warning(
                "No terminal transition available for %s; leaving it open. Available: %s",
                issue_key, available,
            )
            return False

        self._request(
            "POST",
            f"/rest/api/3/issue/{issue_key}/transitions",
            json={"transition": {"id": target["id"]}},
        )
        logger.info("Closed ticket %s via transition '%s'.", issue_key, target.get("name"))
        return True

    def get_issue_status(self, issue_key: str) -> str | None:
        try:
            resp = self._request("GET", f"/rest/servicedeskapi/request/{issue_key}")
            return resp.json()["currentStatus"]["statusCategory"]
        except JiraError as exc:
            if "404" in str(exc):
                logger.warning("Jira ticket %s not found.", issue_key)
                return None
            raise

    def is_issue_open(self, issue_key: str) -> bool:
        category = self.get_issue_status(issue_key)
        if category is None:
            return False
        return category.lower() not in TERMINAL_CATEGORIES
