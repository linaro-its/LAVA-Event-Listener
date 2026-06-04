import json
import logging
import os
import tempfile
import threading
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class DeviceState:
    ticket_key: str
    health: str
    since: str


@dataclass
class WorkerState:
    ticket_key: str
    health: str
    state: str
    since: str


class StateManager:
    def __init__(self, state_file: str):
        self._path = state_file
        self._lock = threading.Lock()
        self._devices: dict[str, DeviceState] = {}
        self._workers: dict[str, WorkerState] = {}
        self._load()

    def _load(self):
        if not os.path.exists(self._path):
            logger.info("No existing state file at %s, starting fresh.", self._path)
            return

        try:
            with open(self._path) as f:
                raw = json.load(f)
            for device_id, entry in raw.get("devices", {}).items():
                self._devices[device_id] = DeviceState(
                    ticket_key=entry["ticket_key"],
                    health=entry["health"],
                    since=entry["since"],
                )
            for worker_id, entry in raw.get("workers", {}).items():
                self._workers[worker_id] = WorkerState(
                    ticket_key=entry["ticket_key"],
                    health=entry["health"],
                    state=entry["state"],
                    since=entry["since"],
                )
            logger.info(
                "Loaded state for %d devices, %d workers.",
                len(self._devices),
                len(self._workers),
            )
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Failed to load state file %s: %s. Starting fresh.", self._path, exc)
            self._devices = {}
            self._workers = {}

    def _save(self):
        data = {
            "devices": {
                device_id: {
                    "ticket_key": state.ticket_key,
                    "health": state.health,
                    "since": state.since,
                }
                for device_id, state in self._devices.items()
            },
            "workers": {
                worker_id: {
                    "ticket_key": state.ticket_key,
                    "health": state.health,
                    "state": state.state,
                    "since": state.since,
                }
                for worker_id, state in self._workers.items()
            },
        }
        parent = os.path.dirname(self._path) or "."
        try:
            fd, tmp_path = tempfile.mkstemp(dir=parent, suffix=".tmp")
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, indent=2)
                f.write("\n")
            os.replace(tmp_path, self._path)
        except OSError:
            logger.exception("Failed to save state file.")

    def get_device(self, device_id: str) -> DeviceState | None:
        with self._lock:
            return self._devices.get(device_id)

    def set_device(self, device_id: str, ticket_key: str, health: str, since: str):
        with self._lock:
            self._devices[device_id] = DeviceState(
                ticket_key=ticket_key,
                health=health,
                since=since,
            )
            self._save()

    def remove_device(self, device_id: str):
        with self._lock:
            if device_id in self._devices:
                del self._devices[device_id]
                self._save()

    def get_worker(self, worker_id: str) -> WorkerState | None:
        with self._lock:
            return self._workers.get(worker_id)

    def set_worker(self, worker_id: str, ticket_key: str, health: str, state: str, since: str):
        with self._lock:
            self._workers[worker_id] = WorkerState(
                ticket_key=ticket_key,
                health=health,
                state=state,
                since=since,
            )
            self._save()

    def remove_worker(self, worker_id: str):
        with self._lock:
            if worker_id in self._workers:
                del self._workers[worker_id]
                self._save()
