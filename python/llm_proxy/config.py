"""Environment configuration, mirroring the Lua's require_var/require_number
startup validation and the compose-level TEMP_CHECK_* conditional rules."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


class ConfigError(Exception):
    pass


def _require_var(name: str, v: str | None) -> str:
    if not v:
        raise ConfigError(f"missing configuration variable {name} (set it in .env)")
    return v


def _require_number(name: str, v: str | None) -> float:
    raw = _require_var(name, v)
    try:
        return float(raw)
    except ValueError:
        raise ConfigError(f"invalid numeric value for {name}: {raw}") from None


def _enabled(v: str | None) -> bool:
    return str(v or "").lower() in ("1", "true")


@dataclass
class Config:
    port: int = 9999
    llm_backend: str = ""
    top_p: float = -1.0
    log_dir: Path = Path("/var/log/nginx/completions")
    stats_log: Path = Path("")
    toolname_log: Path = Path("")
    tool_name_guard: str = "remap"
    tool_name_retry: bool = True
    keepalive_delay: float = 0.0
    keepalive_interval: float = 0.0
    upstream_timeout: float = 0.0
    upstream_retries: int = 0
    concurrency_limit: int = 0
    temp_checks_enabled: bool = False
    temp_check_fail_open: bool = False
    temp_check_endpoint: str = ""
    temp_check_stall_above: float = 0.0
    temp_check_resume_below: float = 0.0
    temp_check_poll_interval: float = 0.0
    temp_check_cache_ttl: float = 0.0
    temp_check_http_timeout: float = 0.0
    temp_check_stall_log: Path = Path("")
    temp_host: str | None = None
    temp_port: int | None = None
    temp_path: str = "/"


def parse_temp_endpoint(url: str) -> tuple[str, int, str] | None:
    """Split a full endpoint URL into (host, port, path); None when the URL
    is missing or malformed. Mirrors the Lua parse_temp_endpoint."""
    import re

    rest = re.sub(r"^https?://", "", url or "")
    if not rest:
        return None
    m = re.match(r"^([^/]+)(/.*)$", rest)
    if m:
        hostport, path = m.group(1), m.group(2)
    else:
        hostport, path = rest, "/"
    m = re.match(r"^(.*):(\d+)$", hostport)
    if m:
        host, port = m.group(1), int(m.group(2))
    else:
        host, port = hostport, 80
    return host, port, path


def load(env: dict[str, str] | None = None) -> Config:
    e = env if env is not None else os.environ
    cfg = Config()

    cfg.port = int(e.get("PORT") or 9999)
    cfg.llm_backend = _require_var("LLM_BACKEND", e.get("LLM_BACKEND"))
    top_p = e.get("TOP_P")
    try:
        cfg.top_p = float(top_p) if top_p is not None and top_p != "" else -1.0
    except ValueError:
        cfg.top_p = -1.0

    cfg.log_dir = Path(e.get("LOG_DIR") or "/var/log/nginx/completions")
    cfg.stats_log = cfg.log_dir / "stats.jsonl"
    cfg.toolname_log = cfg.log_dir / "toolname_fixes.jsonl"

    from .toolguard import normalize_bool, normalize_guard

    cfg.tool_name_guard = normalize_guard(e.get("TOOL_NAME_GUARD"))
    cfg.tool_name_retry = normalize_bool(e.get("TOOL_NAME_RETRY"))

    cfg.keepalive_delay = _require_number("KEEPALIVE_DELAY", e.get("KEEPALIVE_DELAY"))
    cfg.keepalive_interval = _require_number("KEEPALIVE_INTERVAL", e.get("KEEPALIVE_INTERVAL"))
    cfg.upstream_timeout = _require_number("UPSTREAM_TIMEOUT", e.get("UPSTREAM_TIMEOUT"))
    if cfg.upstream_timeout <= 0:
        raise ConfigError("UPSTREAM_TIMEOUT must be greater than 0 seconds")
    retries = _require_number("UPSTREAM_RETRIES", e.get("UPSTREAM_RETRIES"))
    if retries != int(retries) or retries < 0:
        raise ConfigError("UPSTREAM_RETRIES must be a non-negative integer")
    cfg.upstream_retries = int(retries)

    # Concurrent-request cap: 0 (the default) means unlimited.
    limit = _require_number(
        "CONCURRENCY_LIMIT", e.get("CONCURRENCY_LIMIT") or "0")
    if limit != int(limit) or limit < 0:
        raise ConfigError(
            "CONCURRENCY_LIMIT must be a non-negative whole number "
            "(0 = unlimited)")
    cfg.concurrency_limit = int(limit)

    cfg.temp_checks_enabled = _enabled(e.get("TEMP_CHECK_ENABLED"))
    # When the monitor is unreachable: fail open (forward as if the
    # temperatures were OK) instead of the conservative stall. Only
    # meaningful while TEMP_CHECK_ENABLED is on; parsed unconditionally.
    cfg.temp_check_fail_open = _enabled(e.get("TEMP_CHECK_FAIL_OPEN"))
    if cfg.temp_checks_enabled:
        cfg.temp_check_endpoint = _require_var(
            "TEMP_CHECK_ENDPOINT", e.get("TEMP_CHECK_ENDPOINT"))
        cfg.temp_check_stall_above = _require_number(
            "TEMP_CHECK_STALL_ABOVE", e.get("TEMP_CHECK_STALL_ABOVE"))
        cfg.temp_check_resume_below = _require_number(
            "TEMP_CHECK_RESUME_BELOW", e.get("TEMP_CHECK_RESUME_BELOW"))
        cfg.temp_check_poll_interval = _require_number(
            "TEMP_CHECK_POLL_INTERVAL", e.get("TEMP_CHECK_POLL_INTERVAL"))
        cfg.temp_check_cache_ttl = _require_number(
            "TEMP_CHECK_CACHE_TTL", e.get("TEMP_CHECK_CACHE_TTL"))
        cfg.temp_check_http_timeout = _require_number(
            "TEMP_CHECK_HTTP_TIMEOUT", e.get("TEMP_CHECK_HTTP_TIMEOUT"))
        cfg.temp_check_stall_log = Path(_require_var(
            "TEMP_CHECK_STALL_LOG", e.get("TEMP_CHECK_STALL_LOG")))
        parsed = parse_temp_endpoint(cfg.temp_check_endpoint)
        if parsed is None:
            raise ConfigError(
                "invalid TEMP_CHECK_ENDPOINT URL, expected http://host:port/path: "
                + cfg.temp_check_endpoint)
        cfg.temp_host, cfg.temp_port, cfg.temp_path = parsed
    return cfg
