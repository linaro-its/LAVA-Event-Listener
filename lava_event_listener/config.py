import sys
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import yaml


@dataclass
class HealthcheckConfig:
    enabled: bool = False
    poll_interval_seconds: int = 30
    timeout_minutes: int = 30


@dataclass
class LavaServerConfig:
    name: str
    url: str
    token: str | None = None
    username: str | None = None
    healthcheck: HealthcheckConfig = field(default_factory=HealthcheckConfig)
    participants: list[str] = field(default_factory=list)

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
class SpireEnvironmentConfig:
    auth0_domain: str
    auth0_client_id: str
    auth0_client_secret: str
    auth0_audience: str
    spire_api_base: str
    lms_base: str


@dataclass
class SpireConfig:
    production: SpireEnvironmentConfig
    staging: SpireEnvironmentConfig
    cache_dir: str = "/var/lib/lava-event-listener"


@dataclass
class SlackConfig:
    webhook_url: str
    alert_rate_limit_seconds: int = 600
    alert_on_unresolved_subscription: bool = True


@dataclass
class AppConfig:
    lava_servers: list[LavaServerConfig]
    jira: JiraConfig
    state_file: str = "state.json"
    log_level: str = "INFO"
    sentry_dsn: str | None = None
    betterstack: BetterStackConfig | None = None
    spire: SpireConfig | None = None
    slack: SlackConfig | None = None


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
        raw_hc = srv.get("healthcheck") or {}
        healthcheck = HealthcheckConfig(
            enabled=bool(raw_hc.get("enabled", False)),
            poll_interval_seconds=int(raw_hc.get("poll_interval_seconds", 30)),
            timeout_minutes=int(raw_hc.get("timeout_minutes", 30)),
        )
        servers.append(LavaServerConfig(
            name=name,
            url=url,
            token=srv.get("token"),
            username=srv.get("username"),
            healthcheck=healthcheck,
            participants=srv.get("participants") or [],
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

    # Parse SPIRE config (optional)
    spire = None
    raw_spire = raw.get("spire")
    if raw_spire:
        envs = {}
        for env_name in ("production", "staging"):
            raw_env = raw_spire.get(env_name)
            if not raw_env:
                print(f"SPIRE config must include '{env_name}' environment.", file=sys.stderr)
                sys.exit(1)
            for key in (
                "auth0_domain", "auth0_client_id", "auth0_client_secret", "auth0_audience",
                "spire_api_base", "lms_base",
            ):
                if not raw_env.get(key):
                    print(f"SPIRE {env_name} config must include '{key}'.", file=sys.stderr)
                    sys.exit(1)
            envs[env_name] = SpireEnvironmentConfig(
                auth0_domain=raw_env["auth0_domain"],
                auth0_client_id=raw_env["auth0_client_id"],
                auth0_client_secret=raw_env["auth0_client_secret"],
                auth0_audience=raw_env["auth0_audience"],
                spire_api_base=raw_env["spire_api_base"],
                lms_base=raw_env["lms_base"],
            )
        spire = SpireConfig(
            production=envs["production"],
            staging=envs["staging"],
            cache_dir=raw_spire.get("cache_dir", "/var/lib/lava-event-listener"),
        )

    # Parse Slack config (optional)
    slack = None
    raw_slack = raw.get("slack")
    if raw_slack:
        if not raw_slack.get("webhook_url"):
            print("Slack config must include 'webhook_url'.", file=sys.stderr)
            sys.exit(1)
        slack = SlackConfig(
            webhook_url=raw_slack["webhook_url"],
            alert_rate_limit_seconds=int(raw_slack.get("alert_rate_limit_seconds", 600)),
            alert_on_unresolved_subscription=bool(raw_slack.get("alert_on_unresolved_subscription", True)),
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
        spire=spire,
        slack=slack,
    )
