import json
import logging
import os
import time
from pathlib import Path

import requests

from .config import SlackConfig

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
INITIAL_BACKOFF = 1
BACKOFF_FACTOR = 4


class SlackClient:
    def __init__(self, config: SlackConfig, dead_letter_path: str):
        self._webhook_url = config.webhook_url
        self._rate_limit_seconds = config.alert_rate_limit_seconds
        self._dead_letter_path = dead_letter_path
        self._last_alert_times: dict[str, float] = {}
        self._suppressed_count: int = 0
        self._last_summary_time: float = 0

    def send_startup(self):
        self._post_message({
            "text": ":white_check_mark: LAVA Event Listener started.",
        })

    def send_shutdown(self):
        self._post_message({
            "text": ":stop_sign: LAVA Event Listener shutting down.",
        })

    def send_error(self, device: str, operation: str, error: str):
        rate_key = f"error:{device}"
        if self._is_rate_limited(rate_key):
            self._suppressed_count += 1
            self._maybe_send_summary()
            return

        self._post_message({
            "attachments": [{
                "color": "#d32f2f",
                "title": f"SPIRE sync failure: {device}",
                "fields": [
                    {"title": "Operation", "value": operation, "short": True},
                    {"title": "Device", "value": device, "short": True},
                    {"title": "Error", "value": error[:500]},
                ],
            }],
        })
        self._last_alert_times[rate_key] = time.time()

    def send_unresolved_subscription(self, device: str):
        rate_key = f"unresolved:{device}"
        if self._is_rate_limited(rate_key):
            return

        self._post_message({
            "attachments": [{
                "color": "#9e9e9e",
                "title": f"Unresolved subscription: {device}",
                "text": (
                    "Device event received but subscription could not be resolved "
                    "in either production or staging SPIRE. "
                    "This is expected for lab-owned devices."
                ),
            }],
        })
        self._last_alert_times[rate_key] = time.time()

    def write_dead_letter(self, event_data: dict, error: str):
        entry = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "event": event_data,
            "error": error,
        }
        try:
            parent = Path(self._dead_letter_path).parent
            parent.mkdir(parents=True, exist_ok=True)
            with open(self._dead_letter_path, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except OSError:
            logger.exception("Failed to write dead letter entry.")

    def _is_rate_limited(self, key: str) -> bool:
        last = self._last_alert_times.get(key, 0)
        return (time.time() - last) < self._rate_limit_seconds

    def _maybe_send_summary(self):
        now = time.time()
        if (now - self._last_summary_time) < self._rate_limit_seconds:
            return
        if self._suppressed_count == 0:
            return

        self._post_message({
            "attachments": [{
                "color": "#ff9800",
                "title": "Suppressed alerts summary",
                "text": (
                    f"{self._suppressed_count} additional failure(s) suppressed "
                    f"in the last {self._rate_limit_seconds}s due to rate limiting."
                ),
            }],
        })
        self._suppressed_count = 0
        self._last_summary_time = now

    def _post_message(self, payload: dict):
        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = requests.post(
                    self._webhook_url,
                    json=payload,
                    timeout=10,
                )
                if resp.ok:
                    return
                if resp.status_code == 429 or resp.status_code >= 500:
                    if attempt == MAX_RETRIES:
                        logger.error("Slack webhook failed after %d retries: %d", MAX_RETRIES, resp.status_code)
                        return
                    time.sleep(backoff)
                    backoff *= BACKOFF_FACTOR
                    continue
                logger.error("Slack webhook error %d: %s", resp.status_code, resp.text[:200])
                return
            except requests.RequestException:
                if attempt == MAX_RETRIES:
                    logger.exception("Slack webhook connection failed after %d retries.", MAX_RETRIES)
                    return
                time.sleep(backoff)
                backoff *= BACKOFF_FACTOR
