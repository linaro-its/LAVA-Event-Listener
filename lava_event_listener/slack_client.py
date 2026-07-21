import json
import logging
import queue
import threading
import time
from pathlib import Path

import requests

from .config import SlackConfig

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
INITIAL_BACKOFF = 1
BACKOFF_FACTOR = 4
MAX_BACKOFF = 60
# Cap on how long we'll honour a Slack `Retry-After` before giving up on a
# single send attempt, so a hostile/huge value can't wedge the sender.
MAX_RETRY_AFTER = 300
# Bound the outbound queue so a sustained Slack outage can't grow memory
# without limit; anything that doesn't fit is dead-lettered instead of dropped.
QUEUE_MAXSIZE = 1000
# How many suppressed failures to name in the summary before truncating.
MAX_SUPPRESSED_DETAILS = 20


class SlackClient:
    """Posts alerts to a Slack incoming webhook.

    Delivery happens on a dedicated background thread so that Slack rate
    limiting (HTTP 429) or slowness never blocks the asyncio event loop that
    drives the websocket listeners. Any message that still can't be delivered
    after retries is written to an "undelivered" dead-letter file rather than
    being silently dropped, so nothing is lost to rate limiting.
    """

    def __init__(self, config: SlackConfig, dead_letter_path: str):
        self._webhook_url = config.webhook_url
        self._rate_limit_seconds = config.alert_rate_limit_seconds
        self._dead_letter_path = dead_letter_path
        # Undelivered Slack messages sit next to the event dead-letter file.
        self._undelivered_path = str(
            Path(dead_letter_path).with_name("slack_undelivered.jsonl")
        )

        # Suppression/rate-limit bookkeeping. Only touched from the caller
        # (event loop) thread and, during shutdown, from close(); the two never
        # run concurrently, so no lock is required.
        self._last_alert_times: dict[str, float] = {}
        self._suppressed_count: int = 0
        self._suppressed_details: list[str] = []
        self._last_summary_time: float = 0

        # Background delivery.
        self._session = requests.Session()
        self._queue: queue.Queue = queue.Queue(maxsize=QUEUE_MAXSIZE)
        self._stop = threading.Event()
        self._worker = threading.Thread(
            target=self._run_worker, name="slack-sender", daemon=True
        )
        self._worker.start()

    # ------------------------------------------------------------------ #
    # Public API (called from the event loop thread)                     #
    # ------------------------------------------------------------------ #

    def send_startup(self):
        self._enqueue({"text": ":white_check_mark: LAVA Event Listener started."})

    def send_shutdown(self):
        self._enqueue({"text": ":octagonal_sign: LAVA Event Listener shutting down."})

    def send_error(self, device: str, operation: str, error: str):
        rate_key = f"error:{device}"
        if self._is_rate_limited(rate_key):
            self._record_suppressed(device, operation)
            self._maybe_send_summary()
            return

        self._enqueue(
            {
                "attachments": [{
                    "color": "#d32f2f",
                    "title": f"SPIRE sync failure: {device}",
                    "fields": [
                        {"title": "Operation", "value": operation, "short": True},
                        {"title": "Device", "value": device, "short": True},
                        {"title": "Error", "value": error[:500]},
                    ],
                }],
            },
            context={"kind": "error", "device": device, "operation": operation},
        )
        self._last_alert_times[rate_key] = time.time()

    def send_unresolved_subscription(self, device: str):
        rate_key = f"unresolved:{device}"
        if self._is_rate_limited(rate_key):
            return

        self._enqueue(
            {
                "attachments": [{
                    "color": "#9e9e9e",
                    "title": f"Unresolved subscription: {device}",
                    "text": (
                        "Device event received but subscription could not be resolved "
                        "in either production or staging SPIRE. "
                        "This is expected for lab-owned devices."
                    ),
                }],
            },
            context={"kind": "unresolved", "device": device},
        )
        self._last_alert_times[rate_key] = time.time()

    def write_dead_letter(self, event_data: dict, error: str):
        entry = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "event": event_data,
            "error": error,
        }
        self._append_jsonl(self._dead_letter_path, entry, "dead letter entry")

    def close(self, timeout: float = 15.0):
        """Flush pending alerts and stop the background worker.

        Called during shutdown after the event loop has stopped, so touching
        suppression state here is safe.
        """
        self._flush_pending_summary()

        deadline = time.time() + timeout
        # Let the worker drain whatever is already queued.
        while not self._queue.empty() and time.time() < deadline:
            time.sleep(0.1)

        self._stop.set()
        self._worker.join(timeout=max(0.0, deadline - time.time()))

        # Anything still queued after the worker stops is persisted, not lost.
        self._drain_queue_to_disk()

    # ------------------------------------------------------------------ #
    # Suppression / summary bookkeeping                                  #
    # ------------------------------------------------------------------ #

    def _is_rate_limited(self, key: str) -> bool:
        last = self._last_alert_times.get(key, 0)
        return (time.time() - last) < self._rate_limit_seconds

    def _record_suppressed(self, device: str, operation: str):
        self._suppressed_count += 1
        detail = f"{device} ({operation})"
        if detail not in self._suppressed_details:
            self._suppressed_details.append(detail)
            if len(self._suppressed_details) > MAX_SUPPRESSED_DETAILS:
                self._suppressed_details.pop(0)

    def _maybe_send_summary(self):
        if (time.time() - self._last_summary_time) < self._rate_limit_seconds:
            return
        self._flush_pending_summary()

    def _flush_pending_summary(self):
        if self._suppressed_count == 0:
            return

        detail = ", ".join(self._suppressed_details)
        if self._suppressed_count > len(self._suppressed_details):
            detail += ", …"

        self._enqueue({
            "attachments": [{
                "color": "#ff9800",
                "title": "Suppressed alerts summary",
                "text": (
                    f"{self._suppressed_count} additional failure(s) suppressed "
                    f"in the last {self._rate_limit_seconds}s due to rate limiting."
                    + (f"\nAffected: {detail}" if detail else "")
                ),
            }],
        })
        self._suppressed_count = 0
        self._suppressed_details = []
        self._last_summary_time = time.time()

    # ------------------------------------------------------------------ #
    # Background delivery                                                #
    # ------------------------------------------------------------------ #

    def _enqueue(self, payload: dict, context: dict | None = None):
        try:
            self._queue.put_nowait({"payload": payload, "context": context})
        except queue.Full:
            logger.error(
                "Slack outbound queue full (%d items); persisting message to %s.",
                QUEUE_MAXSIZE, self._undelivered_path,
            )
            self._write_undelivered(payload, "queue full", context)

    def _run_worker(self):
        while not (self._stop.is_set() and self._queue.empty()):
            try:
                item = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                if not self._deliver(item["payload"]):
                    self._write_undelivered(
                        item["payload"],
                        "delivery failed after retries",
                        item.get("context"),
                    )
            except Exception:  # never let the worker thread die
                logger.exception("Unexpected error delivering Slack message.")
                self._write_undelivered(
                    item["payload"], "unexpected delivery error", item.get("context")
                )
            finally:
                self._queue.task_done()

    def _deliver(self, payload: dict) -> bool:
        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = self._session.post(self._webhook_url, json=payload, timeout=10)
            except requests.RequestException:
                if attempt == MAX_RETRIES:
                    logger.exception(
                        "Slack webhook connection failed after %d retries.", MAX_RETRIES
                    )
                    return False
                self._sleep(backoff)
                backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
                continue

            if resp.ok:
                return True

            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt == MAX_RETRIES:
                    logger.error(
                        "Slack webhook failed after %d retries: %d",
                        MAX_RETRIES, resp.status_code,
                    )
                    return False
                self._sleep(self._retry_delay(resp, backoff))
                backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)
                continue

            logger.error(
                "Slack webhook error %d: %s", resp.status_code, resp.text[:200]
            )
            return False
        return False

    def _retry_delay(self, resp: requests.Response, fallback: float) -> float:
        """Honour Slack's `Retry-After` header (seconds), falling back to our
        own exponential backoff when it's absent or unparseable."""
        header = resp.headers.get("Retry-After")
        if header:
            try:
                return min(max(float(header), fallback), MAX_RETRY_AFTER)
            except ValueError:
                pass
        return fallback

    def _sleep(self, seconds: float):
        # Interruptible so shutdown doesn't have to wait out a long backoff.
        self._stop.wait(timeout=seconds)

    # ------------------------------------------------------------------ #
    # Persistence helpers                                                #
    # ------------------------------------------------------------------ #

    def _drain_queue_to_disk(self):
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                return
            self._write_undelivered(
                item["payload"], "not delivered before shutdown", item.get("context")
            )

    def _write_undelivered(self, payload: dict, reason: str, context: dict | None):
        entry = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "reason": reason,
            "context": context,
            "payload": payload,
        }
        self._append_jsonl(self._undelivered_path, entry, "undelivered Slack message")

    def _append_jsonl(self, path: str, entry: dict, what: str):
        try:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except OSError:
            logger.exception("Failed to write %s.", what)
