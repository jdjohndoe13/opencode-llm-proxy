"""GPU temperature guard: hysteresis stall/resume with a shared snapshot
cache. Mirrors the Lua fetch_temperatures / update_temp_cache /
read_temp_cache / wait_for_cool_down."""
from __future__ import annotations

import asyncio
import json
import logging
import time

import aiohttp

from .config import Config

log = logging.getLogger("llm_proxy.temp")


class TempGuard:
    def __init__(self, cfg: Config, session=None):
        self.cfg = cfg
        self.session = session  # aiohttp.ClientSession | None
        self._lock = asyncio.Lock()
        self._max_temp: float | None = None
        self._checked_at: float = 0.0

    def _read_cache(self) -> float | None:
        if self._max_temp is None:
            return None
        if time.monotonic() - self._checked_at > self.cfg.temp_check_cache_ttl:
            return None
        return self._max_temp

    def _update_cache(self, temps: list[float]) -> float:
        self._max_temp = max(temps)
        self._checked_at = time.monotonic()
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
                    log.error("temperature endpoint returned status %s", resp.status)
                    return None
                body = await resp.text()
        except Exception as err:
            log.error("temperature endpoint connect failed: %s", err)
            return None
        try:
            obj = json.loads(body)
        except (ValueError, TypeError):
            log.error("temperature endpoint returned invalid JSON")
            return None
        if not isinstance(obj, dict):
            log.error("temperature endpoint returned invalid JSON")
            return None
        temps = [float(v) for v in obj.values() if isinstance(v, (int, float))]
        if not temps:
            log.error("temperature endpoint returned no numeric values")
            return None
        return temps

    async def wait_for_cool_down(self, ctx: dict, on_wait=None) -> bool:
        """Stall until every GPU temperature is below RESUME_BELOW (hysteresis).
        Returns True when safe to proceed, False if the client aborted while
        waiting. ctx is the per-request context dict (keys: _stall_start,
        _stall_end, _aborted). on_wait is awaited periodically so streaming
        clients keep receiving keep-alive chunks during the stall."""
        cfg = self.cfg
        if not cfg.temp_checks_enabled:
            return True
        stalled = False
        while True:
            if ctx.get("_aborted"):
                return False

            max_temp = self._read_cache()
            if max_temp is None:
                async with self._lock:
                    max_temp = self._read_cache()
                    if max_temp is None:
                        temps = await self.fetch_temperatures()
                        if temps is None:
                            log.error("temperature check failed; conservatively "
                                      "stalling request")
                            ctx.setdefault("_stall_start", time.time())
                            stalled = True
                        else:
                            max_temp = self._update_cache(temps)

            if max_temp is not None:
                if stalled:
                    if max_temp < cfg.temp_check_resume_below:
                        ctx["_stall_end"] = time.time()
                        log.info("GPU cooled to %s (<%s), resuming request",
                                 max_temp, cfg.temp_check_resume_below)
                        return True
                elif max_temp > cfg.temp_check_stall_above:
                    ctx.setdefault("_stall_start", time.time())
                    log.info("GPU temperature %s > %s, stalling request",
                             max_temp, cfg.temp_check_stall_above)
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
