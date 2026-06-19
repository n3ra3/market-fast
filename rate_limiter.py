"""
Единый rate limiter для запросов к market.csgo.com.

Стратегия — СТРОГИЙ минимальный интервал между запросами (= 1 / MAX_RPS).
Это гарантирует, что мы НИКОГДА не превышаем MAX_RPS даже всплеском
(token-bucket со стартовым бакетом давал бёрст и мог triggernуть снятие ключа).

Приоритет покупки: пока есть ожидающий запрос покупки (priority=True), новые
служебные/сканер-запросы (priority=False) встают у «гейта» и пропускают покупку
вперёд. В этой архитектуре сканер вообще не ходит в REST (источник — WebSocket),
поэтому реальная конкуренция минимальна и покупка практически не ждёт.
"""
from __future__ import annotations

import asyncio
import time

import config


class RateLimiter:
    __slots__ = ("_interval", "_lock", "_last", "_buy_pending", "_scan_gate")

    def __init__(self, rps: float) -> None:
        self._interval = 1.0 / max(rps, 0.001)
        self._lock = asyncio.Lock()
        self._last = -1e18
        self._buy_pending = 0
        self._scan_gate = asyncio.Event()
        self._scan_gate.set()

    async def acquire(self, *, priority: bool = False) -> None:
        if priority:
            self._buy_pending += 1
            self._scan_gate.clear()
        try:
            if not priority:
                while self._buy_pending > 0:
                    await self._scan_gate.wait()
            # строгая сериализация: один запрос за интервал, без всплесков
            async with self._lock:
                now = time.monotonic()
                wait = self._last + self._interval - now
                if wait > 0:
                    await asyncio.sleep(wait)
                self._last = time.monotonic()
        finally:
            if priority:
                self._buy_pending -= 1
                if self._buy_pending == 0:
                    self._scan_gate.set()


limiter = RateLimiter(config.MARKET_MAX_RPS)
