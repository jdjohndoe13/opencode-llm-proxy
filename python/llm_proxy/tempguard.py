"""GPU temperature guard: hysteresis stall/resume with a shared snapshot
cache. Mirrors the Lua fetch_temperatures / update_temp_cache /
read_temp_cache / wait_for_cool_down."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time

import aiohttp

from .config import Config

log = logging.getLogger("llm_proxy.temp")

# Sanity bounds for runtime threshold edits: the monitor reports plain
# degrees Celsius, so anything outside this range is a typo rather than a
# deliberate limit. The dashboard gets the same message back as a 400.
TEMP_MIN = -50.0
TEMP_MAX = 250.0


def validate_thresholds(stall_above: float, resume_below: float) -> str | None:
    """None when the hysteresis pair is usable, otherwise a one-line
    explanation for the dashboard. resume_below must sit strictly below
    stall_above: equal or inverted thresholds would make the guard either
    oscillate or never release a stalled request."""
    for name, v in (("stall_above", stall_above), ("resume_below", resume_below)):
        if not isinstance(v, (int, float)) or isinstance(v, bool) \
                or not math.isfinite(v):
            return f"{name} must be a finite number"
        if not TEMP_MIN <= v <= TEMP_MAX:
            return f"{name} must be between {TEMP_MIN:g} and {TEMP_MAX:g} °C"
    if resume_below >= stall_above:
        return (f"resume_below ({resume_below:g}) must be lower than "
                f"stall_above ({stall_above:g})")
    return None


class TempGuard:
    def __init__(self, cfg: Config, session=None):
        self.cfg = cfg
        self.session = session  # aiohttp.ClientSession | None
        self._lock = asyncio.Lock()
        self._max_temp: float | None = None
        # Monotonic stamp for the cache TTL; the dashboard needs a wall-clock
        # value, so keep both (monotonic epochs are meaningless to a client).
        self._checked_at: float = 0.0
        self._checked_at_wall: float | None = None
        # Runtime on/off switch (dashboard), in-memory, default on. The
        # guard is active only when both the config AND this switch are on,
        # so a restart always re-arms it (fail-closed on restart).
        self.user_enabled: bool = True
        # Runtime-adjustable hysteresis thresholds (dashboard), seeded from
        # the config; in-memory like the switch, so a restart re-reads .env.
        self.stall_above: float = cfg.temp_check_stall_above
        self.resume_below: float = cfg.temp_check_resume_below
        self.last_error: str | None = None

    @property
    def effective_enabled(self) -> bool:
        return self.cfg.temp_checks_enabled and self.user_enabled

    def set_user_enabled(self, enabled: bool) -> None:
        self.user_enabled = bool(enabled)
        log.info("temperature guard %s via dashboard",
                 "enabled" if self.user_enabled else "disabled")

    def set_thresholds(self, stall_above: float | None = None,
                       resume_below: float | None = None) -> tuple[bool, str | None]:
        """Apply a runtime threshold edit (either value may be omitted).
        The pair is validated as a whole, so a partial edit cannot leave
        the hysteresis inverted. Returns (applied, error_message)."""
        s = self.stall_above if stall_above is None else stall_above
        r = self.resume_below if resume_below is None else resume_below
        err = validate_thresholds(s, r)
        if err is not None:
            log.warning("temperature threshold rejected: %s", err)
            return False, err
        self.stall_above, self.resume_below = s, r
        log.info("temperature thresholds set to stall > %s / resume < %s "
                 "via dashboard", s, r)
        return True, None

    def reset_thresholds(self) -> None:
        """Restore the .env values (the dashboard's 'reset' button)."""
        self.stall_above = self.cfg.temp_check_stall_above
        self.resume_below = self.cfg.temp_check_resume_below
        log.info("temperature thresholds reset to .env defaults "
                 "(stall > %s / resume < %s)", self.stall_above, self.resume_below)

    def status(self) -> dict:
        """Snapshot for the dashboard state endpoint."""
        out = {
            "enabled": self.effective_enabled,
            "config_enabled": self.cfg.temp_checks_enabled,
            "user_enabled": self.user_enabled,
            "fail_open": self.cfg.temp_check_fail_open,
            "max_temp": self._max_temp,
            "checked_at": self._checked_at_wall,
            "last_error": self.last_error,
        }
        # The thresholds only exist when the guard is armed (config off
        # leaves them unset), so the UI can hide the editor entirely.
        if self.cfg.temp_checks_enabled:
            out["stall_above"] = self.stall_above
            out["resume_below"] = self.resume_below
            out["default_stall_above"] = self.cfg.temp_check_stall_above
            out["default_resume_below"] = self.cfg.temp_check_resume_below
        return out

    def _read_cache(self) -> float | None:
        if self._max_temp is None:
            return None
        if time.monotonic() - self._checked_at > self.cfg.temp_check_cache_ttl:
            return None
        return self._max_temp

    def _update_cache(self, temps: list[float]) -> float:
        self._max_temp = max(temps)
        self._checked_at = time.monotonic()
        self._checked_at_wall = time.time()
        return self._max_temp

    async def fetch_temperatures(self) -> list[float] | None:
        cfg = self.cfg
        url = f"{cfg.temp_check_endpoint}"
        try:
            assert self.session is not None
            timeout = cfg.temp_check_http_timeout / 1000.0
            async with self.session.get(
                url, timeout=aiohttp.ClientTimeout(total=timeout)
            ) as resp:
                if resp.status != 200:
                    self.last_error = f"endpoint returned status {resp.status}"
                    log.error("temperature endpoint returned status %s", resp.status)
                    return None
                body = await resp.text()
        except Exception as err:
            self.last_error = f"connect failed: {err}"
            log.error("temperature endpoint connect failed: %s", err)
            return None
        try:
            obj = json.loads(body)
        except (ValueError, TypeError):
            obj = None
        if not isinstance(obj, dict):
            self.last_error = "endpoint returned invalid JSON"
            log.error("temperature endpoint returned invalid JSON")
            return None
        temps = [float(v) for v in obj.values() if isinstance(v, (int, float))]
        if not temps:
            self.last_error = "endpoint returned no numeric values"
            log.error("temperature endpoint returned no numeric values")
            return None
        self.last_error = None
        return temps

    async def wait_for_cool_down(self, ctx: dict, on_wait=None) -> bool:
        """Stall until every GPU temperature is below RESUME_BELOW (hysteresis).
        Returns True when safe to proceed, False if the client aborted while
        waiting. ctx is the per-request context dict (keys: _stall_start,
        _stall_end, _aborted). on_wait is awaited periodically so streaming
        clients keep receiving keep-alive chunks during the stall."""
        cfg = self.cfg
        stalled = False
        while True:
            # Re-checked every tick: the dashboard toggle (or a config
            # reload) releases requests that are ALREADY stalled.
            if not self.effective_enabled:
                if stalled and ctx.get("_stall_start") and not ctx.get("_stall_end"):
                    ctx["_stall_end"] = time.time()
                return True
            if ctx.get("_aborted"):
                return False

            max_temp = self._read_cache()
            if max_temp is None:
                async with self._lock:
                    max_temp = self._read_cache()
                    if max_temp is None:
                        temps = await self.fetch_temperatures()
                        if temps is None:
                            if cfg.temp_check_fail_open:
                                log.warning("temperature check failed; "
                                            "TEMP_CHECK_FAIL_OPEN is on, "
                                            "forwarding the request anyway")
                                return True
                            log.error("temperature check failed; conservatively "
                                      "stalling request")
                            ctx.setdefault("_stall_start", time.time())
                            stalled = True
                        else:
                            max_temp = self._update_cache(temps)

            if max_temp is not None:
                if stalled:
                    if max_temp < self.resume_below:
                        ctx["_stall_end"] = time.time()
                        log.info("GPU cooled to %s (<%s), resuming request",
                                 max_temp, self.resume_below)
                        return True
                elif max_temp > self.stall_above:
                    ctx.setdefault("_stall_start", time.time())
                    log.info("GPU temperature %s > %s, stalling request",
                             max_temp, self.stall_above)
                    stalled = True
                else:
                    return True

            # Wait one poll interval, waking periodically so the caller can
            # notice client aborts and feed keep-alive chunks.
            deadline = time.monotonic() + cfg.temp_check_poll_interval
            while time.monotonic() < deadline:
                if ctx.get("_aborted"):
                    return False
                if on_wait is not None:
                    await on_wait()
                await asyncio.sleep(0.2)
