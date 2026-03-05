import sys
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import yaml


@dataclass
class LavaServerConfig:
    name: str
    url: str
    token: str | None = None
    username: str | None = None

    @property
    def ws_url(self) -> str:
        parsed = urlparse(self.url)
        scheme = "wss" if parsed.scheme == "https" else "ws"
        path = parsed.path.rstrip("/") + "/ws/"
        netloc = parsed.netloc
        if self.username and self.token:
            host = netloc.split("@")[-1] if "@" in netloc else netloc
            netloc = f"{self.username}:{self.token}@{host}"
        return urlunparse((scheme, netloc, path, "", "", ""))


@dataclass
class JiraConfig:
    url: str
    email: str
    api_token: str
    project_key: str
    request_type: str = "Service Request"


@dataclass
class BetterStackConfig:
    heartbeat_url: str
    interval_seconds: int = 60


@dataclass
class AppConfig:
    lava_servers: list[LavaServerConfig]
    jira: JiraConfig
    state_file: str = "state.json"
    log_level: str = "INFO"
    sentry_dsn: str | None = None
    betterstack: BetterStackConfig | None = None


def load_config(path: str) -> AppConfig:
    config_path = Path(path)
    if not config_path.exists():
        print(f"Configuration file not found: {path}", file=sys.stderr)
        sys.exit(1)

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        print("Configuration file must be a YAML mapping.", file=sys.stderr)
        sys.exit(1)

    # Parse LAVA servers
    raw_servers = raw.get("lava_servers", [])
    if not raw_servers:
        print("At least one LAVA server must be configured.", file=sys.stderr)
        sys.exit(1)

    servers = []
    for i, srv in enumerate(raw_servers):
        name = srv.get("name")
        url = srv.get("url")
        if not name or not url:
            print(f"LAVA server #{i + 1} must have 'name' and 'url'.", file=sys.stderr)
            sys.exit(1)
        servers.append(LavaServerConfig(
            name=name,
            url=url,
            token=srv.get("token"),
            username=srv.get("username"),
        ))

    # Parse Jira config
    raw_jira = raw.get("jira", {})
    for key in ("url", "email", "api_token", "project_key"):
        if not raw_jira.get(key):
            print(f"Jira config must include '{key}'.", file=sys.stderr)
            sys.exit(1)

    jira = JiraConfig(
        url=raw_jira["url"],
        email=raw_jira["email"],
        api_token=raw_jira["api_token"],
        project_key=raw_jira["project_key"],
        request_type=raw_jira.get("request_type", "Service Request"),
    )

    # Parse BetterStack config (optional)
    betterstack = None
    raw_bs = raw.get("betterstack")
    if raw_bs and raw_bs.get("heartbeat_url"):
        betterstack = BetterStackConfig(
            heartbeat_url=raw_bs["heartbeat_url"],
            interval_seconds=raw_bs.get("interval_seconds", 60),
        )

    # Validate state file directory exists
    state_file = raw.get("state_file", "state.json")
    state_dir = Path(state_file).parent
    if str(state_dir) != "." and not state_dir.exists():
        print(f"State file directory does not exist: {state_dir}", file=sys.stderr)
        sys.exit(1)

    return AppConfig(
        lava_servers=servers,
        jira=jira,
        state_file=state_file,
        log_level=raw.get("log_level", "INFO"),
        sentry_dsn=raw.get("sentry", {}).get("dsn") if raw.get("sentry") else None,
        betterstack=betterstack,
    )
